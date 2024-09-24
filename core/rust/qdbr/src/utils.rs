use jni::JNIEnv;
use std::fs::File;
use std::ptr;

pub(crate) fn from_raw_file_descriptor(raw: i32) -> File {
    unsafe {
        #[cfg(unix)]
        {
            use std::os::unix::io::{FromRawFd, RawFd};
            File::from_raw_fd(raw as RawFd)
        }

        #[cfg(windows)]
        {
            use std::os::windows::io::{FromRawHandle, RawHandle};
            File::from_raw_handle(raw as usize as RawHandle)
        }
    }
}

pub(crate) fn throw_java_ex<T>(
    env: &mut JNIEnv,
    method_name: &str,
    err: &impl std::fmt::Debug,
) -> *mut T {
    let msg = format!("error in {}: {:?}", method_name, err);
    env.throw_new("java/lang/RuntimeException", msg)
        .expect("failed to throw exception");
    ptr::null_mut()
}
