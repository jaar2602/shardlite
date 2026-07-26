//! A read-only SQLite VFS whose "files" are S3 objects, served page-by-page through an [`S3Pager`].
//!
//! This is what makes failover reads instant: instead of downloading a shard's snapshot before
//! serving it, a survivor opens the snapshot *in place* over this VFS — SQLite's page reads become
//! range-`GET`s (cached), so the database is queryable immediately, with no restore step.
//!
//! The snapshot object is a clean, checkpointed `.db`, so the database is opened with `immutable=1`:
//! SQLite then reads the main file directly and never touches a WAL, `-shm`, or a lock. That
//! collapses the VFS to the read path — `xRead` and `xFileSize` — with everything else a no-op or a
//! read-only refusal. Writes to a failed-over shard go through a *different*, local-WAL path (the
//! next slice), not this VFS.
//!
//! # Safety
//!
//! Unsafe FFI. [`S3File`] is `#[repr(C)]` with the SQLite file header first, so a `*mut
//! sqlite3_file` is our struct. The pager is held as a raw `Arc` acquired in `xOpen` and released
//! in `xClose`, so it outlives every call.

use std::collections::HashMap;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use libsqlite3_sys as ffi;

use super::{S3Client, S3Pager};

pub const VFS_NAME: &str = "shardlite-s3-ro";

struct S3VfsState {
    base: *mut ffi::sqlite3_vfs,
    /// Token (the path SQLite is opened with) → the pager to serve it. `xOpen` takes the entry.
    registry: Mutex<HashMap<String, Arc<S3Pager>>>,
}

// `base` is owned by SQLite for the process; `registry` is behind a mutex.
unsafe impl Send for S3VfsState {}
unsafe impl Sync for S3VfsState {}

static STATE: OnceLock<&'static S3VfsState> = OnceLock::new();
static NEXT_TOKEN: AtomicU64 = AtomicU64::new(0);

#[repr(C)]
struct S3File {
    /// Must be first: SQLite casts between `sqlite3_file` and this.
    base: ffi::sqlite3_file,
    /// Raw `Arc<S3Pager>`, or null. Owned — released in `xClose`.
    pager: *const S3Pager,
}

/// Open a read-only connection to the S3 object at `key`, served page-by-page over this VFS.
pub fn open_readonly(client: Arc<S3Client>, key: &str) -> crate::Result<rusqlite::Connection> {
    register()?;
    let pager = Arc::new(S3Pager::open(client, key).map_err(to_err)?);

    // A unique token that identifies this open to `xOpen`. It is not a filesystem path.
    let token = format!(
        "shardlite-s3-{}",
        NEXT_TOKEN.fetch_add(1, Ordering::Relaxed)
    );
    state()
        .registry
        .lock()
        .unwrap()
        .insert(token.clone(), pager);

    // immutable=1 → SQLite reads the main file directly: no WAL, no -shm, no locking.
    let uri = format!("file:{token}?immutable=1&mode=ro");
    let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI;
    rusqlite::Connection::open_with_flags_and_vfs(&uri, flags, VFS_NAME)
        .map_err(|e| crate::Error::Protocol(format!("opening s3 snapshot: {e}")))
}

fn state() -> &'static S3VfsState {
    STATE.get().expect("s3 VFS registered")
}

fn to_err(e: super::S3Error) -> crate::Error {
    crate::Error::Protocol(format!("s3: {e}"))
}

/// Register the VFS. Idempotent.
pub fn register() -> crate::Result<()> {
    if STATE.get().is_some() {
        return Ok(());
    }
    unsafe {
        let base = ffi::sqlite3_vfs_find(ptr::null());
        if base.is_null() {
            return Err(crate::Error::VfsRegistration(
                "no default SQLite VFS found".into(),
            ));
        }
        let st: &'static S3VfsState = Box::leak(Box::new(S3VfsState {
            base,
            registry: Mutex::new(HashMap::new()),
        }));
        let name = CString::new(VFS_NAME).expect("static name");
        let vfs: &'static mut ffi::sqlite3_vfs = Box::leak(Box::new(ffi::sqlite3_vfs {
            iVersion: 1,
            szOsFile: size_of::<S3File>() as c_int,
            mxPathname: (*base).mxPathname,
            pNext: ptr::null_mut(),
            zName: name.into_raw(),
            pAppData: st as *const S3VfsState as *mut c_void,
            xOpen: Some(x_open),
            xDelete: Some(x_delete),
            xAccess: Some(x_access),
            xFullPathname: Some(x_full_pathname),
            xDlOpen: Some(x_dlopen),
            xDlError: Some(x_dlerror),
            xDlSym: Some(x_dlsym),
            xDlClose: Some(x_dlclose),
            xRandomness: Some(x_randomness),
            xSleep: Some(x_sleep),
            xCurrentTime: Some(x_current_time),
            xGetLastError: Some(x_get_last_error),
            xCurrentTimeInt64: None,
            xSetSystemCall: None,
            xGetSystemCall: None,
            xNextSystemCall: None,
        }));
        if ffi::sqlite3_vfs_register(vfs, 0) != ffi::SQLITE_OK {
            return Err(crate::Error::VfsRegistration(
                "sqlite3_vfs_register (s3) failed".into(),
            ));
        }
        let _ = STATE.set(st);
    }
    Ok(())
}

// ---------------------------------------------------------------- vfs entry points

unsafe extern "C" fn x_open(
    _vfs: *mut ffi::sqlite3_vfs,
    z_name: *const c_char,
    file: *mut ffi::sqlite3_file,
    _flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    unsafe {
        let sf = file as *mut S3File;
        (*sf).base.pMethods = ptr::null();
        (*sf).pager = ptr::null();
        if z_name.is_null() {
            return ffi::SQLITE_CANTOPEN;
        }
        let Ok(name) = CStr::from_ptr(z_name).to_str() else {
            return ffi::SQLITE_CANTOPEN;
        };
        // The main database is the only thing this VFS serves. Anything else (a would-be journal)
        // is not present.
        let Some(pager) = state().registry.lock().unwrap().remove(name) else {
            return ffi::SQLITE_CANTOPEN;
        };
        (*sf).pager = Arc::into_raw(pager);
        (*sf).base.pMethods = &METHODS;
        if !out_flags.is_null() {
            *out_flags = ffi::SQLITE_OPEN_READONLY;
        }
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn x_delete(_vfs: *mut ffi::sqlite3_vfs, _z: *const c_char, _s: c_int) -> c_int {
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_access(
    _vfs: *mut ffi::sqlite3_vfs,
    z_name: *const c_char,
    _flags: c_int,
    out: *mut c_int,
) -> c_int {
    unsafe {
        let name = CStr::from_ptr(z_name).to_str().unwrap_or("");
        // Only a registered token "exists"; there are no journals or WAL files.
        *out = i32::from(state().registry.lock().unwrap().contains_key(name));
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn x_full_pathname(
    _vfs: *mut ffi::sqlite3_vfs,
    z_name: *const c_char,
    n_out: c_int,
    z_out: *mut c_char,
) -> c_int {
    unsafe {
        // Tokens are already "full"; copy verbatim so the base VFS never prepends a working dir.
        let src = CStr::from_ptr(z_name).to_bytes_with_nul();
        if src.len() as c_int > n_out {
            return ffi::SQLITE_CANTOPEN;
        }
        ptr::copy_nonoverlapping(src.as_ptr() as *const c_char, z_out, src.len());
        ffi::SQLITE_OK
    }
}

// The dynamic-loader, randomness, sleep and clock methods are not read-path concerns; delegate to
// the default VFS rather than reimplement them.
unsafe extern "C" fn x_dlopen(_vfs: *mut ffi::sqlite3_vfs, z: *const c_char) -> *mut c_void {
    unsafe {
        let b = state().base;
        (*b).xDlOpen.map_or(ptr::null_mut(), |f| f(b, z))
    }
}
unsafe extern "C" fn x_dlerror(_vfs: *mut ffi::sqlite3_vfs, n: c_int, z: *mut c_char) {
    unsafe {
        let b = state().base;
        if let Some(f) = (*b).xDlError {
            f(b, n, z);
        }
    }
}
type DlSym = Option<unsafe extern "C" fn(*mut ffi::sqlite3_vfs, *mut c_void, *const c_char)>;
unsafe extern "C" fn x_dlsym(
    _vfs: *mut ffi::sqlite3_vfs,
    p: *mut c_void,
    z: *const c_char,
) -> DlSym {
    unsafe {
        let b = state().base;
        (*b).xDlSym.and_then(|f| f(b, p, z))
    }
}
unsafe extern "C" fn x_dlclose(_vfs: *mut ffi::sqlite3_vfs, p: *mut c_void) {
    unsafe {
        let b = state().base;
        if let Some(f) = (*b).xDlClose {
            f(b, p);
        }
    }
}
unsafe extern "C" fn x_randomness(_vfs: *mut ffi::sqlite3_vfs, n: c_int, z: *mut c_char) -> c_int {
    unsafe {
        let b = state().base;
        (*b).xRandomness.map_or(ffi::SQLITE_OK, |f| f(b, n, z))
    }
}
unsafe extern "C" fn x_sleep(_vfs: *mut ffi::sqlite3_vfs, micros: c_int) -> c_int {
    unsafe {
        let b = state().base;
        (*b).xSleep.map_or(ffi::SQLITE_OK, |f| f(b, micros))
    }
}
unsafe extern "C" fn x_current_time(_vfs: *mut ffi::sqlite3_vfs, out: *mut f64) -> c_int {
    unsafe {
        let b = state().base;
        (*b).xCurrentTime.map_or(ffi::SQLITE_OK, |f| f(b, out))
    }
}
unsafe extern "C" fn x_get_last_error(
    _vfs: *mut ffi::sqlite3_vfs,
    n: c_int,
    z: *mut c_char,
) -> c_int {
    unsafe {
        let b = state().base;
        (*b).xGetLastError.map_or(ffi::SQLITE_OK, |f| f(b, n, z))
    }
}

// ---------------------------------------------------------------- file entry points

static METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 1,
    xClose: Some(f_close),
    xRead: Some(f_read),
    xWrite: Some(f_write),
    xTruncate: Some(f_truncate),
    xSync: Some(f_sync),
    xFileSize: Some(f_file_size),
    xLock: Some(f_lock),
    xUnlock: Some(f_unlock),
    xCheckReservedLock: Some(f_check_reserved_lock),
    xFileControl: Some(f_file_control),
    xSectorSize: Some(f_sector_size),
    xDeviceCharacteristics: Some(f_device_characteristics),
    xShmMap: None,
    xShmLock: None,
    xShmBarrier: None,
    xShmUnmap: None,
    xFetch: None,
    xUnfetch: None,
};

unsafe fn pager<'a>(f: *mut ffi::sqlite3_file) -> &'a S3Pager {
    unsafe { &*(*(f as *mut S3File)).pager }
}

unsafe extern "C" fn f_close(f: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let sf = f as *mut S3File;
        if !(*sf).pager.is_null() {
            drop(Arc::from_raw((*sf).pager));
            (*sf).pager = ptr::null();
        }
        (*sf).base.pMethods = ptr::null();
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn f_read(
    f: *mut ffi::sqlite3_file,
    buf: *mut c_void,
    amt: c_int,
    ofst: i64,
) -> c_int {
    unsafe {
        let out = std::slice::from_raw_parts_mut(buf as *mut u8, amt as usize);
        match pager(f).read_at(ofst as u64, out) {
            Ok(n) if n == amt as usize => ffi::SQLITE_OK,
            Ok(n) => {
                // A read past end-of-file: zero-fill the tail and report a short read, which SQLite
                // treats as reading zeros (e.g. probing a freshly grown file).
                out[n..].fill(0);
                ffi::SQLITE_IOERR_SHORT_READ
            }
            Err(_) => ffi::SQLITE_IOERR_READ,
        }
    }
}

unsafe extern "C" fn f_write(
    _f: *mut ffi::sqlite3_file,
    _buf: *const c_void,
    _amt: c_int,
    _ofst: i64,
) -> c_int {
    ffi::SQLITE_READONLY
}

unsafe extern "C" fn f_truncate(_f: *mut ffi::sqlite3_file, _size: i64) -> c_int {
    ffi::SQLITE_READONLY
}

unsafe extern "C" fn f_sync(_f: *mut ffi::sqlite3_file, _flags: c_int) -> c_int {
    ffi::SQLITE_OK
}

unsafe extern "C" fn f_file_size(f: *mut ffi::sqlite3_file, size: *mut i64) -> c_int {
    unsafe {
        *size = pager(f).size() as i64;
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn f_lock(_f: *mut ffi::sqlite3_file, _level: c_int) -> c_int {
    ffi::SQLITE_OK
}
unsafe extern "C" fn f_unlock(_f: *mut ffi::sqlite3_file, _level: c_int) -> c_int {
    ffi::SQLITE_OK
}
unsafe extern "C" fn f_check_reserved_lock(_f: *mut ffi::sqlite3_file, out: *mut c_int) -> c_int {
    unsafe {
        *out = 0;
        ffi::SQLITE_OK
    }
}
unsafe extern "C" fn f_file_control(
    _f: *mut ffi::sqlite3_file,
    _op: c_int,
    _arg: *mut c_void,
) -> c_int {
    ffi::SQLITE_NOTFOUND
}
unsafe extern "C" fn f_sector_size(_f: *mut ffi::sqlite3_file) -> c_int {
    4096
}
unsafe extern "C" fn f_device_characteristics(_f: *mut ffi::sqlite3_file) -> c_int {
    ffi::SQLITE_IOCAP_IMMUTABLE
}
