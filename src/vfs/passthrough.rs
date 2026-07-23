//! A pass-through SQLite VFS that tees writes to the `-wal` file.
//!
//! Every call is delegated to the default VFS, so the database is an ordinary on-disk
//! SQLite file with ordinary durability. The only addition is that successful writes to
//! the `-wal` file are also handed to a [`WalCapture`], which reconstructs committed
//! transactions from them.
//!
//! This is the mechanism the whole replication design rests on. dqlite proves frames can
//! be captured through a VFS with stock SQLite, but dqlite's VFS keeps the entire database
//! in process memory and never writes to disk. Backing a real file *and* capturing is the
//! part that was unproven — see `tests/vfs_capture.rs`.
//!
//! # Safety
//!
//! This module is unsafe FFI. Two invariants hold it together:
//!
//! 1. [`CaptureFile`] is `#[repr(C)]` with `base` first, so a `*mut sqlite3_file` handed to
//!    us can be cast to `*mut CaptureFile`. We advertise `szOsFile` large enough for our
//!    struct *plus* the underlying VFS's file, which lives immediately after ours.
//! 2. The capture is held as a raw `Arc` pointer, acquired in `xOpen` and released in
//!    `xClose`, so it outlives every call that touches it.

use std::collections::HashMap;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::{Arc, Mutex, OnceLock};

use libsqlite3_sys as ffi;

use super::wal::WalCapture;

pub const VFS_NAME: &str = "shardlite-capture";

const KIND_OTHER: u32 = 0;
const KIND_MAIN: u32 = 1;
const KIND_WAL: u32 = 2;

struct VfsState {
    base: *mut ffi::sqlite3_vfs,
    registry: Mutex<HashMap<PathBuf, Arc<WalCapture>>>,
}

// The base pointer is owned by SQLite and lives for the process; the registry is behind a
// mutex. Both are safe to share.
unsafe impl Send for VfsState {}
unsafe impl Sync for VfsState {}

static STATE: OnceLock<&'static VfsState> = OnceLock::new();

#[repr(C)]
struct CaptureFile {
    /// Must be first: SQLite casts between `sqlite3_file` and this.
    base: ffi::sqlite3_file,
    /// The underlying VFS's file, allocated immediately after this struct.
    real: *mut ffi::sqlite3_file,
    /// Raw `Arc<WalCapture>`, or null. Owned — released in `xClose`.
    capture: *const WalCapture,
    kind: u32,
}

/// Register the VFS. Idempotent; safe to call from anywhere.
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

        let state: &'static VfsState = Box::leak(Box::new(VfsState {
            base,
            registry: Mutex::new(HashMap::new()),
        }));

        // Both leak deliberately: SQLite keeps the pointer for the life of the process.
        let name = CString::new(VFS_NAME).expect("static name");
        let vfs: &'static mut ffi::sqlite3_vfs = Box::leak(Box::new(ffi::sqlite3_vfs {
            // 2 gives us xCurrentTimeInt64; the system-call hooks of version 3 are not
            // needed and are left null.
            iVersion: 2.min((*base).iVersion),
            szOsFile: size_of::<CaptureFile>() as c_int + (*base).szOsFile,
            mxPathname: (*base).mxPathname,
            pNext: ptr::null_mut(),
            zName: name.into_raw(),
            pAppData: state as *const VfsState as *mut c_void,
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

        let rc = ffi::sqlite3_vfs_register(vfs, 0);
        if rc != ffi::SQLITE_OK {
            return Err(crate::Error::VfsRegistration(format!(
                "sqlite3_vfs_register returned {rc}"
            )));
        }
        let _ = STATE.set(state);
    }
    Ok(())
}

/// Begin capturing WAL frames for `db_path`.
///
/// Must be called **before** the database is opened — `xOpen` consults the registry when
/// the `-wal` file is first opened, and a capture registered later would miss frames.
pub fn capture_for(db_path: &Path) -> crate::Result<Arc<WalCapture>> {
    capture_for_with_limit(db_path, super::wal::DEFAULT_MAX_RETAINED_BYTES)
}

/// As [`capture_for`], with an explicit retention cap.
pub fn capture_for_with_limit(
    db_path: &Path,
    max_retained: usize,
) -> crate::Result<Arc<WalCapture>> {
    register()?;
    let state = STATE.get().expect("registered above");
    // SQLite hands the VFS a full pathname, so the registry must key on one too.
    let key = std::path::absolute(db_path).map_err(|e| {
        crate::Error::VfsRegistration(format!("resolving {}: {e}", db_path.display()))
    })?;

    let mut reg = state.registry.lock().expect("registry mutex");
    Ok(Arc::clone(reg.entry(key).or_insert_with(|| {
        Arc::new(WalCapture::with_limit(max_retained))
    })))
}

fn state() -> &'static VfsState {
    STATE.get().expect("VFS registered")
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
        let cf = file as *mut CaptureFile;

        (*cf).base.pMethods = ptr::null();
        (*cf).real = (file as *mut u8).add(size_of::<CaptureFile>()) as *mut ffi::sqlite3_file;
        (*cf).capture = ptr::null();
        (*cf).kind = if flags & ffi::SQLITE_OPEN_WAL != 0 {
            KIND_WAL
        } else if flags & ffi::SQLITE_OPEN_MAIN_DB != 0 {
            KIND_MAIN
        } else {
            KIND_OTHER
        };

        let Some(open) = (*st.base).xOpen else {
            return ffi::SQLITE_CANTOPEN;
        };
        let rc = open(st.base, z_name, (*cf).real, flags, out_flags);
        if rc != ffi::SQLITE_OK {
            return rc;
        }

        // Attach a capture if this is a WAL whose database was registered. Classified by
        // the open flag rather than the filename; the name is only used to find the
        // database it belongs to.
        if (*cf).kind == KIND_WAL
            && !z_name.is_null()
            && let Ok(name) = CStr::from_ptr(z_name).to_str()
            && let Some(db) = name.strip_suffix("-wal")
            && let Ok(reg) = st.registry.lock()
            && let Some(cap) = reg.get(Path::new(db))
        {
            (*cf).capture = Arc::into_raw(Arc::clone(cap));
        }

        (*cf).base.pMethods = &METHODS;
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn x_delete(
    _vfs: *mut ffi::sqlite3_vfs,
    z_name: *const c_char,
    sync_dir: c_int,
) -> c_int {
    unsafe {
        let st = state();
        match (*st.base).xDelete {
            Some(f) => f(st.base, z_name, sync_dir),
            None => ffi::SQLITE_OK,
        }
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
        match (*st.base).xAccess {
            Some(f) => f(st.base, z_name, flags, out),
            None => ffi::SQLITE_OK,
        }
    }
}

unsafe extern "C" fn x_full_pathname(
    _vfs: *mut ffi::sqlite3_vfs,
    z_name: *const c_char,
    n_out: c_int,
    z_out: *mut c_char,
) -> c_int {
    unsafe {
        let st = state();
        match (*st.base).xFullPathname {
            Some(f) => f(st.base, z_name, n_out, z_out),
            None => ffi::SQLITE_ERROR,
        }
    }
}

unsafe extern "C" fn x_dlopen(_vfs: *mut ffi::sqlite3_vfs, z_path: *const c_char) -> *mut c_void {
    unsafe {
        let st = state();
        match (*st.base).xDlOpen {
            Some(f) => f(st.base, z_path),
            None => ptr::null_mut(),
        }
    }
}

unsafe extern "C" fn x_dlerror(_vfs: *mut ffi::sqlite3_vfs, n: c_int, z: *mut c_char) {
    unsafe {
        let st = state();
        if let Some(f) = (*st.base).xDlError {
            f(st.base, n, z);
        }
    }
}

/// `xDlSym` in C is a function returning a function pointer, which bindgen renders as this
/// slightly odd type. It must be matched exactly rather than simplified.
type DlSym = Option<unsafe extern "C" fn(*mut ffi::sqlite3_vfs, *mut c_void, *const c_char)>;

unsafe extern "C" fn x_dlsym(
    _vfs: *mut ffi::sqlite3_vfs,
    p: *mut c_void,
    z: *const c_char,
) -> DlSym {
    unsafe {
        let st = state();
        match (*st.base).xDlSym {
            Some(f) => f(st.base, p, z),
            None => None,
        }
    }
}

unsafe extern "C" fn x_dlclose(_vfs: *mut ffi::sqlite3_vfs, p: *mut c_void) {
    unsafe {
        let st = state();
        if let Some(f) = (*st.base).xDlClose {
            f(st.base, p);
        }
    }
}

unsafe extern "C" fn x_randomness(_vfs: *mut ffi::sqlite3_vfs, n: c_int, z: *mut c_char) -> c_int {
    unsafe {
        let st = state();
        match (*st.base).xRandomness {
            Some(f) => f(st.base, n, z),
            None => ffi::SQLITE_OK,
        }
    }
}

unsafe extern "C" fn x_sleep(_vfs: *mut ffi::sqlite3_vfs, micros: c_int) -> c_int {
    unsafe {
        let st = state();
        match (*st.base).xSleep {
            Some(f) => f(st.base, micros),
            None => ffi::SQLITE_OK,
        }
    }
}

unsafe extern "C" fn x_current_time(_vfs: *mut ffi::sqlite3_vfs, p: *mut f64) -> c_int {
    unsafe {
        let st = state();
        match (*st.base).xCurrentTime {
            Some(f) => f(st.base, p),
            None => ffi::SQLITE_ERROR,
        }
    }
}

unsafe extern "C" fn x_current_time_int64(_vfs: *mut ffi::sqlite3_vfs, p: *mut i64) -> c_int {
    unsafe {
        let st = state();
        match (*st.base).xCurrentTimeInt64 {
            Some(f) => f(st.base, p),
            None => ffi::SQLITE_ERROR,
        }
    }
}

unsafe extern "C" fn x_get_last_error(
    _vfs: *mut ffi::sqlite3_vfs,
    n: c_int,
    z: *mut c_char,
) -> c_int {
    unsafe {
        let st = state();
        match (*st.base).xGetLastError {
            Some(f) => f(st.base, n, z),
            None => ffi::SQLITE_OK,
        }
    }
}

// ---------------------------------------------------------------- file entry points

static METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 2, // 2 covers the shared-memory methods WAL requires
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

/// The underlying file's method table.
unsafe fn real(
    f: *mut ffi::sqlite3_file,
) -> (*mut ffi::sqlite3_file, *const ffi::sqlite3_io_methods) {
    unsafe {
        let cf = f as *mut CaptureFile;
        ((*cf).real, (*(*cf).real).pMethods)
    }
}

unsafe extern "C" fn f_close(f: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let cf = f as *mut CaptureFile;
        let (r, m) = real(f);
        let rc = match (*m).xClose {
            Some(g) => g(r),
            None => ffi::SQLITE_OK,
        };
        if !(*cf).capture.is_null() {
            drop(Arc::from_raw((*cf).capture));
            (*cf).capture = ptr::null();
        }
        (*cf).base.pMethods = ptr::null();
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
        let (r, m) = real(f);
        match (*m).xRead {
            Some(g) => g(r, buf, amt, ofst),
            None => ffi::SQLITE_IOERR_READ,
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
        let cf = f as *mut CaptureFile;
        let (r, m) = real(f);
        let rc = match (*m).xWrite {
            Some(g) => g(r, buf, amt, ofst),
            None => return ffi::SQLITE_IOERR_WRITE,
        };

        // Capture only after the real write succeeded: a frame that failed to reach disk
        // must not be replicated as though it had.
        if rc == ffi::SQLITE_OK && (*cf).kind == KIND_WAL && !(*cf).capture.is_null() && amt > 0 {
            let bytes = std::slice::from_raw_parts(buf as *const u8, amt as usize);
            (*(*cf).capture).on_write(ofst as u64, bytes);
        }
        rc
    }
}

unsafe extern "C" fn f_truncate(f: *mut ffi::sqlite3_file, size: i64) -> c_int {
    unsafe {
        let cf = f as *mut CaptureFile;
        let (r, m) = real(f);
        let rc = match (*m).xTruncate {
            Some(g) => g(r, size),
            None => return ffi::SQLITE_IOERR_TRUNCATE,
        };
        if rc == ffi::SQLITE_OK && (*cf).kind == KIND_WAL && !(*cf).capture.is_null() {
            (*(*cf).capture).on_truncate(size as u64);
        }
        rc
    }
}

unsafe extern "C" fn f_sync(f: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    unsafe {
        let (r, m) = real(f);
        match (*m).xSync {
            Some(g) => g(r, flags),
            None => ffi::SQLITE_OK,
        }
    }
}

unsafe extern "C" fn f_file_size(f: *mut ffi::sqlite3_file, size: *mut i64) -> c_int {
    unsafe {
        let (r, m) = real(f);
        match (*m).xFileSize {
            Some(g) => g(r, size),
            None => ffi::SQLITE_IOERR_FSTAT,
        }
    }
}

unsafe extern "C" fn f_lock(f: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    unsafe {
        let (r, m) = real(f);
        match (*m).xLock {
            Some(g) => g(r, level),
            None => ffi::SQLITE_OK,
        }
    }
}

unsafe extern "C" fn f_unlock(f: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    unsafe {
        let (r, m) = real(f);
        match (*m).xUnlock {
            Some(g) => g(r, level),
            None => ffi::SQLITE_OK,
        }
    }
}

unsafe extern "C" fn f_check_reserved_lock(f: *mut ffi::sqlite3_file, out: *mut c_int) -> c_int {
    unsafe {
        let (r, m) = real(f);
        match (*m).xCheckReservedLock {
            Some(g) => g(r, out),
            None => ffi::SQLITE_OK,
        }
    }
}

unsafe extern "C" fn f_file_control(
    f: *mut ffi::sqlite3_file,
    op: c_int,
    arg: *mut c_void,
) -> c_int {
    unsafe {
        let (r, m) = real(f);
        match (*m).xFileControl {
            Some(g) => g(r, op, arg),
            None => ffi::SQLITE_NOTFOUND,
        }
    }
}

unsafe extern "C" fn f_sector_size(f: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let (r, m) = real(f);
        match (*m).xSectorSize {
            Some(g) => g(r),
            None => 512,
        }
    }
}

unsafe extern "C" fn f_device_characteristics(f: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let (r, m) = real(f);
        match (*m).xDeviceCharacteristics {
            Some(g) => g(r),
            None => 0,
        }
    }
}

unsafe extern "C" fn f_shm_map(
    f: *mut ffi::sqlite3_file,
    pg: c_int,
    pgsz: c_int,
    extend: c_int,
    pp: *mut *mut c_void,
) -> c_int {
    unsafe {
        let (r, m) = real(f);
        match (*m).xShmMap {
            Some(g) => g(r, pg, pgsz, extend, pp),
            None => ffi::SQLITE_IOERR_SHMMAP,
        }
    }
}

unsafe extern "C" fn f_shm_lock(
    f: *mut ffi::sqlite3_file,
    offset: c_int,
    n: c_int,
    flags: c_int,
) -> c_int {
    unsafe {
        let (r, m) = real(f);
        match (*m).xShmLock {
            Some(g) => g(r, offset, n, flags),
            None => ffi::SQLITE_IOERR_SHMLOCK,
        }
    }
}

unsafe extern "C" fn f_shm_barrier(f: *mut ffi::sqlite3_file) {
    unsafe {
        let (r, m) = real(f);
        if let Some(g) = (*m).xShmBarrier {
            g(r);
        }
    }
}

unsafe extern "C" fn f_shm_unmap(f: *mut ffi::sqlite3_file, delete: c_int) -> c_int {
    unsafe {
        let (r, m) = real(f);
        match (*m).xShmUnmap {
            Some(g) => g(r, delete),
            None => ffi::SQLITE_OK,
        }
    }
}
