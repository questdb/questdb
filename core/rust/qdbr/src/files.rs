/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

#[cfg(target_os = "windows")]
#[no_mangle]
pub extern "system" fn Java_io_questdb_std_Files_isDir(
    _env: jni::JNIEnv,
    _class: jni::objects::JClass,
    path: *const std::ffi::c_char,
) -> bool {
    // This is a Windows specific implementation
    // The other implementations are done in C.
    // This implementation avoids the complexity of dealing with reparse points in the Windows API.
    // See https://stackoverflow.com/questions/46383428/get-the-immediate-target-path-from-symlink-reparse-point
    // for more context.
    let path = unsafe { std::ffi::CStr::from_ptr(path) };
    let Ok(path) = path.to_str() else {
        return false;
    };
    let path = std::path::Path::new(path);
    if path.is_dir() {
        return true;
    }
    let Ok(mut resolved) = std::path::Path::new(path).read_link() else {
        return false;
    };
    if resolved.is_dir() {
        return true;
    }
    loop {
        let Ok(newly_resolved) = resolved.read_link() else {
            return false;
        };
        if newly_resolved.is_dir() {
            return true;
        }
        resolved = newly_resolved;
    }
}

// Linux names the file of a descriptor or a mapping from /proc in Java (Files.findPathOfFd, findPathOfMapping);
// macOS and Windows ask the OS here. The lookups run on a failed flush only, so they allocate.

/// The path of the file `fd` is open on, or null when the OS cannot tell. `fd` is the OS descriptor, a `HANDLE`
/// on Windows.
#[cfg(any(target_os = "macos", target_os = "windows"))]
#[no_mangle]
pub extern "system" fn Java_io_questdb_std_Files_findPathOfFd0(
    env: jni::JNIEnv,
    _class: jni::objects::JClass,
    fd: jni::sys::jint,
) -> jni::sys::jstring {
    // A panic must not cross into the JVM: it aborts the process
    let path = std::panic::catch_unwind(move || path_of::fd(fd)).unwrap_or(None);
    new_java_string(&env, path)
}

/// The path of the file mapped at `address`, anywhere in the mapping, or null when no file is mapped there.
#[cfg(any(target_os = "macos", target_os = "windows"))]
#[no_mangle]
pub extern "system" fn Java_io_questdb_std_Files_findPathOfMapping0(
    env: jni::JNIEnv,
    _class: jni::objects::JClass,
    address: jni::sys::jlong,
) -> jni::sys::jstring {
    let path = std::panic::catch_unwind(move || path_of::mapping(address as usize)).unwrap_or(None);
    new_java_string(&env, path)
}

#[cfg(any(target_os = "macos", target_os = "windows"))]
fn new_java_string(env: &jni::JNIEnv, path: Option<String>) -> jni::sys::jstring {
    // A string the JVM cannot allocate leaves its exception pending, and the Java caller takes that as no path
    match path.map(|path| env.new_string(path)) {
        Some(Ok(path)) => path.into_raw(),
        _ => std::ptr::null_mut(),
    }
}

// macOS names a file by its inode, through the file system (fsgetpath). The path proc_pidinfo gives a region comes
// from the name cache, which can lose a file's parent: the region then names the file by its last component alone,
// /v.d. A deleted file has no path: it is in no directory.
#[cfg(target_os = "macos")]
mod path_of {
    use std::ffi::{c_char, c_int, c_void, CStr};
    use std::mem::{offset_of, size_of};

    /// `PROC_PIDFDVNODEPATHINFO` of `<sys/proc_info.h>`.
    const PROC_PIDFDVNODEPATHINFO: c_int = 2;
    /// `PROC_PIDREGIONPATHINFO` of `<sys/proc_info.h>`.
    const PROC_PIDREGIONPATHINFO: c_int = 8;

    extern "C" {
        /// `fsgetpath(2)` of `<sys/fsgetpath.h>`, macOS 10.13 and later, which the libc crate does not declare.
        fn fsgetpath(
            buf: *mut c_char,
            buflen: usize,
            fsid: *mut libc::fsid_t,
            obj_id: u64,
        ) -> isize;
    }

    /// `struct proc_fileinfo` of `<sys/proc_info.h>`, which the libc crate does not declare.
    #[repr(C)]
    struct ProcFileInfo {
        fi_openflags: u32,
        fi_status: u32,
        fi_offset: i64,
        fi_type: i32,
        fi_guardflags: u32,
    }

    /// `struct vnode_fdinfowithpath` of `<sys/proc_info.h>`.
    #[repr(C)]
    struct VnodeFdInfoWithPath {
        pfi: ProcFileInfo,
        pvip: libc::vnode_info_path,
    }

    /// `struct proc_regioninfo` of `<sys/proc_info.h>`, which the libc crate does not declare.
    #[repr(C)]
    struct ProcRegionInfo {
        pri_protection: u32,
        pri_max_protection: u32,
        pri_inheritance: u32,
        pri_flags: u32,
        pri_offset: u64,
        pri_behavior: u32,
        pri_user_wired_count: u32,
        pri_user_tag: u32,
        pri_pages_resident: u32,
        pri_pages_shared_now_private: u32,
        pri_pages_swapped_out: u32,
        pri_pages_dirtied: u32,
        pri_ref_count: u32,
        pri_shadow_depth: u32,
        pri_share_mode: u32,
        pri_private_pages_resident: u32,
        pri_shared_pages_resident: u32,
        pri_obj_id: u32,
        pri_depth: u32,
        pri_address: u64,
        pri_size: u64,
    }

    /// `struct proc_regionwithpathinfo` of `<sys/proc_info.h>`.
    #[repr(C)]
    struct ProcRegionWithPathInfo {
        prp_prinfo: ProcRegionInfo,
        prp_vip: libc::vnode_info_path,
    }

    // The layout of the macOS SDK: a mismatch fails the build, not the lookup
    const _: () = assert!(offset_of!(VnodeFdInfoWithPath, pvip) == 24);
    const _: () = assert!(size_of::<VnodeFdInfoWithPath>() == 1200);
    const _: () = assert!(offset_of!(ProcRegionInfo, pri_address) == 80);
    const _: () = assert!(offset_of!(ProcRegionInfo, pri_size) == 88);
    const _: () = assert!(size_of::<ProcRegionInfo>() == 96);
    const _: () = assert!(offset_of!(ProcRegionWithPathInfo, prp_vip) == 96);
    const _: () = assert!(size_of::<ProcRegionWithPathInfo>() == 1272);

    pub(super) fn fd(fd: c_int) -> Option<String> {
        // SAFETY: the struct holds integers and arrays only, so all zeros is a value of it
        let mut info: VnodeFdInfoWithPath = unsafe { std::mem::zeroed() };
        let size = size_of::<VnodeFdInfoWithPath>() as c_int;
        // SAFETY: proc_pidfdinfo writes at most `size` bytes into info
        let written = unsafe {
            libc::proc_pidfdinfo(
                libc::getpid(),
                fd,
                PROC_PIDFDVNODEPATHINFO,
                (&mut info as *mut VnodeFdInfoWithPath).cast::<c_void>(),
                size,
            )
        };
        // No descriptor, or one of no vnode, such as a socket
        if written != size {
            return None;
        }
        path_of_vnode(&mut info.pvip.vip_vi)
    }

    pub(super) fn mapping(address: usize) -> Option<String> {
        // SAFETY: the struct holds integers and arrays only, so all zeros is a value of it
        let mut info: ProcRegionWithPathInfo = unsafe { std::mem::zeroed() };
        let size = size_of::<ProcRegionWithPathInfo>() as c_int;
        // SAFETY: proc_pidinfo writes at most `size` bytes into info
        let written = unsafe {
            libc::proc_pidinfo(
                libc::getpid(),
                PROC_PIDREGIONPATHINFO,
                address as u64,
                (&mut info as *mut ProcRegionWithPathInfo).cast::<c_void>(),
                size,
            )
        };
        // No region at or above the address
        if written != size {
            return None;
        }
        // Asked for an address no region holds, proc_pidinfo describes the next region up: the address is below it
        let region = &info.prp_prinfo;
        if (address as u64).wrapping_sub(region.pri_address) >= region.pri_size {
            return None;
        }
        path_of_vnode(&mut info.prp_vip.vip_vi)
    }

    /// The path the file system gives the inode of `vnode`: none for a deleted file, which is in no directory, nor
    /// for a region of no file, whose vnode is all zeros.
    fn path_of_vnode(vnode: &mut libc::vnode_info) -> Option<String> {
        let mut path = [0u8; libc::PATH_MAX as usize];
        // SAFETY: fsgetpath writes a NUL-terminated path of at most path.len() bytes
        let len = unsafe {
            fsgetpath(
                path.as_mut_ptr().cast::<c_char>(),
                path.len(),
                &mut vnode.vi_fsid,
                vnode.vi_stat.vst_ino,
            )
        };
        if len <= 0 {
            return None;
        }
        let path = CStr::from_bytes_until_nul(&path).ok()?;
        Some(path.to_string_lossy().into_owned())
    }

    #[cfg(test)]
    mod tests {
        use std::fs::{self, File, OpenOptions};
        use std::os::fd::AsRawFd;
        use std::path::Path;

        fn page_size() -> usize {
            // SAFETY: sysconf has no preconditions
            unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
        }

        /// A file of `pages` pages, and the path the OS names it by: macOS reaches the temp dir through /var -> /private/var.
        fn new_file(dir: &Path, name: &str, pages: usize) -> (File, String) {
            let path = dir.join(name);
            let file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            file.set_len((pages * page_size()) as u64).unwrap();
            let real = fs::canonicalize(&path)
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();
            (file, real)
        }

        fn map(file: &File, pages: usize) -> *mut libc::c_void {
            // SAFETY: maps a file of at least `pages` pages
            let address = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    pages * page_size(),
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_SHARED,
                    file.as_raw_fd(),
                    0,
                )
            };
            assert_ne!(libc::MAP_FAILED, address);
            address
        }

        #[test]
        fn fd_names_the_file() {
            let dir = tempfile::tempdir().unwrap();
            let (file, real) = new_file(dir.path(), "a.d", 1);
            assert_eq!(Some(real), super::fd(file.as_raw_fd()));
        }

        #[test]
        fn fd_names_the_file_after_a_rename() {
            let dir = tempfile::tempdir().unwrap();
            let (file, _) = new_file(dir.path(), "a.d", 1);
            fs::rename(dir.path().join("a.d"), dir.path().join("b.d")).unwrap();
            let renamed = fs::canonicalize(dir.path().join("b.d")).unwrap();
            assert_eq!(
                Some(renamed.to_str().unwrap().to_string()),
                super::fd(file.as_raw_fd())
            );
        }

        #[test]
        fn fd_names_a_directory() {
            let dir = tempfile::tempdir().unwrap();
            let file = File::open(dir.path()).unwrap();
            let real = fs::canonicalize(dir.path()).unwrap();
            assert_eq!(
                Some(real.to_str().unwrap().to_string()),
                super::fd(file.as_raw_fd())
            );
        }

        #[test]
        fn fd_of_a_deleted_file_is_none() {
            let dir = tempfile::tempdir().unwrap();
            let (file, _) = new_file(dir.path(), "a.d", 1);
            fs::remove_file(dir.path().join("a.d")).unwrap();
            assert_eq!(None, super::fd(file.as_raw_fd()));
        }

        #[test]
        fn fd_of_no_file_is_none() {
            assert_eq!(None, super::fd(-1));
        }

        #[test]
        fn mapping_names_the_file_anywhere_in_the_mapping() {
            let dir = tempfile::tempdir().unwrap();
            let (file, real) = new_file(dir.path(), "a.d", 2);
            let address = map(&file, 2) as usize;
            let page = page_size();
            assert_eq!(Some(real.clone()), super::mapping(address));
            assert_eq!(Some(real.clone()), super::mapping(address + page + 7));
            assert_eq!(Some(real), super::mapping(address + 2 * page - 1));
            // SAFETY: unmaps the mapping above
            assert_eq!(0, unsafe {
                libc::munmap(address as *mut libc::c_void, 2 * page)
            });
        }

        #[test]
        fn mapping_names_the_file_after_a_rename() {
            let dir = tempfile::tempdir().unwrap();
            let (file, _) = new_file(dir.path(), "a.d", 1);
            let address = map(&file, 1) as usize;
            fs::rename(dir.path().join("a.d"), dir.path().join("b.d")).unwrap();
            let renamed = fs::canonicalize(dir.path().join("b.d")).unwrap();
            assert_eq!(
                Some(renamed.to_str().unwrap().to_string()),
                super::mapping(address)
            );
            // SAFETY: unmaps the mapping above
            assert_eq!(0, unsafe {
                libc::munmap(address as *mut libc::c_void, page_size())
            });
        }

        #[test]
        fn mapping_above_every_region_is_none() {
            assert_eq!(None, super::mapping(usize::MAX));
        }

        #[test]
        fn mapping_below_a_mapped_file_is_not_that_file() {
            let dir = tempfile::tempdir().unwrap();
            let (file, real) = new_file(dir.path(), "a.d", 2);
            let address = map(&file, 2) as usize;
            let page = page_size();
            // A hole below the second page: proc_pidinfo answers for the region after it, which maps the file
            // SAFETY: unmaps the first page of the mapping above
            assert_eq!(0, unsafe {
                libc::munmap(address as *mut libc::c_void, page)
            });
            // Another thread may map something into the hole, but never this file
            assert_ne!(Some(real.clone()), super::mapping(address));
            assert_eq!(Some(real), super::mapping(address + page));
            // SAFETY: unmaps the second page of the mapping above
            assert_eq!(0, unsafe {
                libc::munmap((address + page) as *mut libc::c_void, page)
            });
        }

        #[test]
        fn mapping_of_a_deleted_file_is_none() {
            let dir = tempfile::tempdir().unwrap();
            let (file, _) = new_file(dir.path(), "a.d", 1);
            let address = map(&file, 1) as usize;
            fs::remove_file(dir.path().join("a.d")).unwrap();
            assert_eq!(None, super::mapping(address));
            // SAFETY: unmaps the mapping above
            assert_eq!(0, unsafe {
                libc::munmap(address as *mut libc::c_void, page_size())
            });
        }

        #[test]
        fn mapping_of_no_file_is_none() {
            let page = page_size();
            // SAFETY: maps anonymous memory
            let address = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    page,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_ANON | libc::MAP_PRIVATE,
                    -1,
                    0,
                )
            };
            assert_ne!(libc::MAP_FAILED, address);
            assert_eq!(None, super::mapping(address as usize));
            // SAFETY: unmaps the mapping above
            assert_eq!(0, unsafe { libc::munmap(address, page) });
        }
    }
}

#[cfg(target_os = "windows")]
mod path_of {
    use std::ffi::c_void;
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, GetFinalPathNameByHandleW, FILE_FLAG_BACKUP_SEMANTICS, FILE_NAME_NORMALIZED,
        FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
        VOLUME_NAME_DOS,
    };
    use windows_sys::Win32::System::ProcessStatus::K32GetMappedFileNameW;
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    /// The longest path the NT namespace takes, in UTF-16 units, the terminator included.
    const MAX_NT_PATH: usize = 32_768;

    pub(super) fn fd(fd: i32) -> Option<String> {
        // The int fd carries the handle, as FD_TO_HANDLE in files.h unpacks it
        path_of_handle(fd as isize as HANDLE)
    }

    pub(super) fn mapping(address: usize) -> Option<String> {
        let mut device_path = vec![0u16; MAX_NT_PATH];
        // SAFETY: writes at most device_path.len() UTF-16 units into device_path
        let len = unsafe {
            K32GetMappedFileNameW(
                GetCurrentProcess(),
                address as *const c_void,
                device_path.as_mut_ptr(),
                device_path.len() as u32,
            )
        } as usize;
        // 0: no file is mapped at the address; the whole buffer: the name may be cut short
        if len == 0 || len >= device_path.len() {
            return None;
        }
        // The name is in the NT namespace, \Device\HarddiskVolume3\..., which a Java path cannot name. Opening the
        // file through the GLOBALROOT link gives a handle to ask for the name a descriptor gets. The handle reads
        // attributes only, which no sharing mode of QuestDB's own handles refuses.
        let mut name: Vec<u16> = r"\\?\GLOBALROOT".encode_utf16().collect();
        name.extend_from_slice(device_path.get(..len)?);
        name.push(0);
        // SAFETY: name is NUL-terminated, and CreateFileW takes a null security descriptor and template
        let handle = unsafe {
            CreateFileW(
                name.as_ptr(),
                FILE_READ_ATTRIBUTES,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                std::ptr::null(),
                OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS,
                std::ptr::null_mut(),
            )
        };
        if handle == INVALID_HANDLE_VALUE {
            return None;
        }
        let path = path_of_handle(handle);
        // SAFETY: closes the handle CreateFileW opened above
        unsafe { CloseHandle(handle) };
        path
    }

    fn path_of_handle(handle: HANDLE) -> Option<String> {
        let mut path = vec![0u16; 1024];
        // The second call gets the size the first asked for; the third covers a rename that grew the name between
        for _ in 0..3 {
            // SAFETY: writes at most path.len() UTF-16 units into path
            let len = unsafe {
                GetFinalPathNameByHandleW(
                    handle,
                    path.as_mut_ptr(),
                    path.len() as u32,
                    FILE_NAME_NORMALIZED | VOLUME_NAME_DOS,
                )
            } as usize;
            if len == 0 {
                return None;
            }
            if len < path.len() {
                return Some(super::without_verbatim_prefix(String::from_utf16_lossy(
                    path.get(..len)?,
                )));
            }
            // Too short: len is the size the name needs, the terminator included
            if len > MAX_NT_PATH {
                return None;
            }
            path.resize(len, 0);
        }
        None
    }
}

/// The path Java gives to the path Windows names with the verbatim prefix: `\\?\C:\db` is `C:\db`, and
/// `\\?\UNC\host\share\db` is `\\host\share\db`.
#[cfg(any(target_os = "windows", test))]
fn without_verbatim_prefix(path: String) -> String {
    if let Some(rest) = path.strip_prefix(r"\\?\UNC\") {
        format!(r"\\{rest}")
    } else if let Some(rest) = path.strip_prefix(r"\\?\") {
        rest.to_string()
    } else {
        path
    }
}

#[cfg(test)]
mod tests {
    use super::without_verbatim_prefix;

    #[test]
    fn verbatim_prefix_is_dropped() {
        assert_eq!(
            r"C:\db\t\a.d",
            without_verbatim_prefix(r"\\?\C:\db\t\a.d".to_string())
        );
        assert_eq!(
            r"\\host\share\db",
            without_verbatim_prefix(r"\\?\UNC\host\share\db".to_string())
        );
        assert_eq!(r"C:\db", without_verbatim_prefix(r"C:\db".to_string()));
        assert_eq!(
            r"\\host\share",
            without_verbatim_prefix(r"\\host\share".to_string())
        );
        assert_eq!("", without_verbatim_prefix(r"\\?\".to_string()));
        assert_eq!("", without_verbatim_prefix(String::new()));
    }
}
