//! A **read-write overlay** VFS for instant failover writes.
//!
//! The main database is read from S3 (read-only), but the `-wal` and every other auxiliary file
//! live on **local disk**. New transactions commit to the local WAL; a read merges the local WAL
//! over the S3 base, which SQLite does natively. So a failed-over shard takes reads *and* writes
//! immediately, with no download — only the new writes are local, the base stays in S3.
//!
//! Two settings make it work (applied by [`open_readwrite`]):
//! * `locking_mode=EXCLUSIVE` — the WAL index lives in heap memory, so there is no `-shm` file to
//!   create alongside a database whose main file is not really local. (Failover is single-writer,
//!   so exclusive locking is correct anyway.)
//! * `wal_autocheckpoint=0` — a checkpoint folds the WAL into the main db, which is read-only on
//!   S3. It is never run; the WAL simply grows until the shard is settled locally later.
//!
//! # Safety
//!
//! Unsafe FFI, structured exactly like the capture VFS: [`RwFile`] is `#[repr(C)]` with the SQLite
//! file header first. The main database file serves from an `Arc<S3Pager>` (released in `xClose`);
//! every other file delegates to the default VFS's file, allocated immediately after our struct.

use std::collections::HashMap;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::path::Path;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use libsqlite3_sys as ffi;

use super::{S3Client, S3Pager};

pub const VFS_NAME: &str = "meshdb-s3-rw";

struct RwVfsState {
    base: *mut ffi::sqlite3_vfs,
    /// Main-db path → pager. `xAccess`/`xOpen` consult it; the entry is cloned, not removed, so a
    /// re-check still sees the database as present.
    registry: Mutex<HashMap<String, Arc<S3Pager>>>,
}
unsafe impl Send for RwVfsState {}
unsafe impl Sync for RwVfsState {}

static STATE: OnceLock<&'static RwVfsState> = OnceLock::new();
static NEXT: AtomicU64 = AtomicU64::new(0);

#[repr(C)]
struct RwFile {
    /// Must be first.
    base: ffi::sqlite3_file,
    /// The default VFS's file for a delegated (local) file, or null for the S3 main db.
    real: *mut ffi::sqlite3_file,
    /// Raw `Arc<S3Pager>` for the S3 main db, or null. Released in `xClose`.
    pager: *const S3Pager,
}

/// Open the shard at S3 `key` for read **and** write: reads served from S3, writes to a local WAL
/// under `scratch_dir` (which must exist). The returned connection is a single-writer, no-checkpoint
/// overlay — settle the shard to local disk later to drain the WAL.
pub fn open_readwrite(
    client: Arc<S3Client>,
    key: &str,
    scratch_dir: &Path,
) -> crate::Result<rusqlite::Connection> {
    register()?;
    let pager = Arc::new(S3Pager::open(client, key).map_err(to_err)?);

    // A real, absolute local path so the base VFS can put the `-wal` next to it. Unique per open.
    let db_path = scratch_dir.join(format!(
        "failover-{}.db",
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let token = db_path
        .to_str()
        .ok_or_else(|| crate::Error::Protocol("scratch path is not UTF-8".into()))?
        .to_string();
    // The main database is served from S3, but SQLite's unix VFS stats the main-db path when it
    // creates the local -wal (to copy the file mode). So leave an empty placeholder there — the VFS
    // reads never touch it (they go to S3), it exists only for that stat.
    std::fs::File::create(&db_path)
        .map_err(|e| crate::Error::Protocol(format!("creating overlay placeholder: {e}")))?;
    state()
        .registry
        .lock()
        .unwrap()
        .insert(token.clone(), pager);

    let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE;
    let conn = rusqlite::Connection::open_with_flags_and_vfs(&db_path, flags, VFS_NAME)
        .map_err(|e| crate::Error::Protocol(format!("opening s3 overlay: {e}")))?;
    // Heap WAL index (no -shm), and never checkpoint (the base is read-only on S3).
    conn.execute_batch("PRAGMA locking_mode=EXCLUSIVE; PRAGMA wal_autocheckpoint=0;")
        .map_err(|e| crate::Error::Protocol(format!("configuring s3 overlay: {e}")))?;
    Ok(conn)
}

fn state() -> &'static RwVfsState {
    STATE.get().expect("s3 rw VFS registered")
}

fn to_err(e: super::S3Error) -> crate::Error {
    crate::Error::Protocol(format!("s3: {e}"))
}

pub fn register() -> crate::Result<()> {
    if STATE.get().is_some() {
        return Ok(());
    }
    unsafe {
        let base = ffi::sqlite3_vfs_find(ptr::null());
        if base.is_null() {
            return Err(crate::Error::VfsRegistration("no default VFS".into()));
        }
        let st: &'static RwVfsState = Box::leak(Box::new(RwVfsState {
            base,
            registry: Mutex::new(HashMap::new()),
        }));
        let name = CString::new(VFS_NAME).expect("static name");
        let vfs: &'static mut ffi::sqlite3_vfs = Box::leak(Box::new(ffi::sqlite3_vfs {
            iVersion: 2.min((*base).iVersion),
            szOsFile: size_of::<RwFile>() as c_int + (*base).szOsFile,
            mxPathname: (*base).mxPathname,
            pNext: ptr::null_mut(),
            zName: name.into_raw(),
            pAppData: st as *const RwVfsState as *mut c_void,
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
            xCurrentTimeInt64: Some(x_current_time_int64),
            xSetSystemCall: None,
            xGetSystemCall: None,
            xNextSystemCall: None,
        }));
        if ffi::sqlite3_vfs_register(vfs, 0) != ffi::SQLITE_OK {
            return Err(crate::Error::VfsRegistration(
                "register (s3 rw) failed".into(),
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
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    unsafe {
        let st = state();
        let rf = file as *mut RwFile;
        (*rf).base.pMethods = ptr::null();
        (*rf).real = (file as *mut u8).add(size_of::<RwFile>()) as *mut ffi::sqlite3_file;
        (*rf).pager = ptr::null();

        // The registered main database is served from S3; everything else is a local file.
        if flags & ffi::SQLITE_OPEN_MAIN_DB != 0
            && !z_name.is_null()
            && let Ok(name) = CStr::from_ptr(z_name).to_str()
            && let Some(pager) = st.registry.lock().unwrap().get(name).cloned()
        {
            (*rf).real = ptr::null_mut();
            (*rf).pager = Arc::into_raw(pager);
            (*rf).base.pMethods = &METHODS;
            // Report read-write so SQLite drives the WAL; the base itself is never written (no
            // checkpoint), and any attempt returns SQLITE_READONLY from f_write.
            if !out_flags.is_null() {
                *out_flags = flags & !ffi::SQLITE_OPEN_CREATE;
            }
            return ffi::SQLITE_OK;
        }

        let Some(open) = (*st.base).xOpen else {
            return ffi::SQLITE_CANTOPEN;
        };
        let rc = open(st.base, z_name, (*rf).real, flags, out_flags);
        if rc != ffi::SQLITE_OK {
            return rc;
        }
        (*rf).base.pMethods = &METHODS;
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn x_delete(_vfs: *mut ffi::sqlite3_vfs, z: *const c_char, s: c_int) -> c_int {
    unsafe {
        let b = state().base;
        (*b).xDelete.map_or(ffi::SQLITE_OK, |f| f(b, z, s))
    }
}

unsafe extern "C" fn x_access(
    _vfs: *mut ffi::sqlite3_vfs,
    z_name: *const c_char,
    flags: c_int,
    out: *mut c_int,
) -> c_int {
    unsafe {
        let st = state();
        if let Ok(name) = CStr::from_ptr(z_name).to_str()
            && st.registry.lock().unwrap().contains_key(name)
        {
            *out = 1;
            return ffi::SQLITE_OK;
        }
        (*st.base)
            .xAccess
            .map_or(ffi::SQLITE_OK, |f| f(st.base, z_name, flags, out))
    }
}

unsafe extern "C" fn x_full_pathname(
    _vfs: *mut ffi::sqlite3_vfs,
    z_name: *const c_char,
    n_out: c_int,
    z_out: *mut c_char,
) -> c_int {
    unsafe {
        // Verbatim: the overlay paths are already absolute, and the base must not rewrite them
        // (the -wal derives from the main db path, which is the registry token).
        let src = CStr::from_ptr(z_name).to_bytes_with_nul();
        if src.len() as c_int > n_out {
            return ffi::SQLITE_CANTOPEN;
        }
        ptr::copy_nonoverlapping(src.as_ptr() as *const c_char, z_out, src.len());
        ffi::SQLITE_OK
    }
}

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
unsafe extern "C" fn x_sleep(_vfs: *mut ffi::sqlite3_vfs, m: c_int) -> c_int {
    unsafe {
        let b = state().base;
        (*b).xSleep.map_or(ffi::SQLITE_OK, |f| f(b, m))
    }
}
unsafe extern "C" fn x_current_time(_vfs: *mut ffi::sqlite3_vfs, out: *mut f64) -> c_int {
    unsafe {
        let b = state().base;
        (*b).xCurrentTime.map_or(ffi::SQLITE_OK, |f| f(b, out))
    }
}
unsafe extern "C" fn x_current_time_int64(_vfs: *mut ffi::sqlite3_vfs, out: *mut i64) -> c_int {
    unsafe {
        let b = state().base;
        (*b).xCurrentTimeInt64.map_or(ffi::SQLITE_OK, |f| f(b, out))
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
    iVersion: 2,
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
    xShmMap: Some(f_shm_map),
    xShmLock: Some(f_shm_lock),
    xShmBarrier: Some(f_shm_barrier),
    xShmUnmap: Some(f_shm_unmap),
    xFetch: None,
    xUnfetch: None,
};

/// Is this the S3-backed main database (vs a delegated local file)?
unsafe fn is_s3(f: *mut ffi::sqlite3_file) -> bool {
    unsafe { !(*(f as *mut RwFile)).pager.is_null() }
}
unsafe fn pager<'a>(f: *mut ffi::sqlite3_file) -> &'a S3Pager {
    unsafe { &*(*(f as *mut RwFile)).pager }
}
unsafe fn real(
    f: *mut ffi::sqlite3_file,
) -> (*mut ffi::sqlite3_file, *const ffi::sqlite3_io_methods) {
    unsafe {
        let r = (*(f as *mut RwFile)).real;
        (r, (*r).pMethods)
    }
}

unsafe extern "C" fn f_close(f: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let rf = f as *mut RwFile;
        let rc = if (*rf).pager.is_null() {
            let (r, m) = real(f);
            (*m).xClose.map_or(ffi::SQLITE_OK, |g| g(r))
        } else {
            drop(Arc::from_raw((*rf).pager));
            (*rf).pager = ptr::null();
            ffi::SQLITE_OK
        };
        (*rf).base.pMethods = ptr::null();
        rc
    }
}

unsafe extern "C" fn f_read(
    f: *mut ffi::sqlite3_file,
    buf: *mut c_void,
    amt: c_int,
    ofst: i64,
) -> c_int {
    unsafe {
        if is_s3(f) {
            let out = std::slice::from_raw_parts_mut(buf as *mut u8, amt as usize);
            match pager(f).read_at(ofst as u64, out) {
                Ok(n) if n == amt as usize => ffi::SQLITE_OK,
                Ok(n) => {
                    out[n..].fill(0);
                    ffi::SQLITE_IOERR_SHORT_READ
                }
                Err(_) => ffi::SQLITE_IOERR_READ,
            }
        } else {
            let (r, m) = real(f);
            (*m).xRead
                .map_or(ffi::SQLITE_IOERR_READ, |g| g(r, buf, amt, ofst))
        }
    }
}

unsafe extern "C" fn f_write(
    f: *mut ffi::sqlite3_file,
    buf: *const c_void,
    amt: c_int,
    ofst: i64,
) -> c_int {
    unsafe {
        if is_s3(f) {
            ffi::SQLITE_READONLY // the base is on S3; only the -wal (a local file) is written
        } else {
            let (r, m) = real(f);
            (*m).xWrite
                .map_or(ffi::SQLITE_IOERR_WRITE, |g| g(r, buf, amt, ofst))
        }
    }
}

unsafe extern "C" fn f_truncate(f: *mut ffi::sqlite3_file, size: i64) -> c_int {
    unsafe {
        if is_s3(f) {
            ffi::SQLITE_READONLY
        } else {
            let (r, m) = real(f);
            (*m).xTruncate
                .map_or(ffi::SQLITE_IOERR_TRUNCATE, |g| g(r, size))
        }
    }
}

unsafe extern "C" fn f_sync(f: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    unsafe {
        if is_s3(f) {
            ffi::SQLITE_OK
        } else {
            let (r, m) = real(f);
            (*m).xSync.map_or(ffi::SQLITE_OK, |g| g(r, flags))
        }
    }
}

unsafe extern "C" fn f_file_size(f: *mut ffi::sqlite3_file, size: *mut i64) -> c_int {
    unsafe {
        if is_s3(f) {
            *size = pager(f).size() as i64;
            ffi::SQLITE_OK
        } else {
            let (r, m) = real(f);
            (*m).xFileSize
                .map_or(ffi::SQLITE_IOERR_FSTAT, |g| g(r, size))
        }
    }
}

unsafe extern "C" fn f_lock(f: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    unsafe {
        if is_s3(f) {
            ffi::SQLITE_OK
        } else {
            let (r, m) = real(f);
            (*m).xLock.map_or(ffi::SQLITE_OK, |g| g(r, level))
        }
    }
}

unsafe extern "C" fn f_unlock(f: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    unsafe {
        if is_s3(f) {
            ffi::SQLITE_OK
        } else {
            let (r, m) = real(f);
            (*m).xUnlock.map_or(ffi::SQLITE_OK, |g| g(r, level))
        }
    }
}

unsafe extern "C" fn f_check_reserved_lock(f: *mut ffi::sqlite3_file, out: *mut c_int) -> c_int {
    unsafe {
        if is_s3(f) {
            *out = 0;
            ffi::SQLITE_OK
        } else {
            let (r, m) = real(f);
            (*m).xCheckReservedLock
                .map_or(ffi::SQLITE_OK, |g| g(r, out))
        }
    }
}

unsafe extern "C" fn f_file_control(
    f: *mut ffi::sqlite3_file,
    op: c_int,
    arg: *mut c_void,
) -> c_int {
    unsafe {
        if is_s3(f) {
            ffi::SQLITE_NOTFOUND
        } else {
            let (r, m) = real(f);
            (*m).xFileControl
                .map_or(ffi::SQLITE_NOTFOUND, |g| g(r, op, arg))
        }
    }
}

unsafe extern "C" fn f_sector_size(f: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        if is_s3(f) {
            4096
        } else {
            let (r, m) = real(f);
            (*m).xSectorSize.map_or(4096, |g| g(r))
        }
    }
}

unsafe extern "C" fn f_device_characteristics(f: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        if is_s3(f) {
            0 // NOT immutable — WAL mode must be active for the overlay
        } else {
            let (r, m) = real(f);
            (*m).xDeviceCharacteristics.map_or(0, |g| g(r))
        }
    }
}

// Shared-memory methods: with locking_mode=EXCLUSIVE the main db never asks for shm, so those calls
// return an error (they should not happen); a delegated file forwards, for completeness.
unsafe extern "C" fn f_shm_map(
    f: *mut ffi::sqlite3_file,
    pg: c_int,
    sz: c_int,
    ext: c_int,
    pp: *mut *mut c_void,
) -> c_int {
    unsafe {
        if is_s3(f) {
            ffi::SQLITE_IOERR
        } else {
            let (r, m) = real(f);
            (*m).xShmMap
                .map_or(ffi::SQLITE_IOERR, |g| g(r, pg, sz, ext, pp))
        }
    }
}
unsafe extern "C" fn f_shm_lock(
    f: *mut ffi::sqlite3_file,
    ofst: c_int,
    n: c_int,
    flags: c_int,
) -> c_int {
    unsafe {
        if is_s3(f) {
            ffi::SQLITE_IOERR
        } else {
            let (r, m) = real(f);
            (*m).xShmLock
                .map_or(ffi::SQLITE_IOERR, |g| g(r, ofst, n, flags))
        }
    }
}
unsafe extern "C" fn f_shm_barrier(f: *mut ffi::sqlite3_file) {
    unsafe {
        if !is_s3(f) {
            let (r, m) = real(f);
            if let Some(g) = (*m).xShmBarrier {
                g(r);
            }
        }
    }
}
unsafe extern "C" fn f_shm_unmap(f: *mut ffi::sqlite3_file, delete: c_int) -> c_int {
    unsafe {
        if is_s3(f) {
            ffi::SQLITE_OK
        } else {
            let (r, m) = real(f);
            (*m).xShmUnmap.map_or(ffi::SQLITE_OK, |g| g(r, delete))
        }
    }
}
