use jni::sys::JavaVM as SysJavaVM;
use jni::JavaVM;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

mod tokio;
mod wal_upload;
mod log;

static mut JAVA_VM: *mut SysJavaVM = std::ptr::null_mut();

fn get_jenv() -> Option<jni::JNIEnv<'static>> {
    let vm = unsafe {
        if JAVA_VM.is_null() {
            return None;
        }
        JavaVM::from_raw(JAVA_VM).expect("failed to get jvm")
    };
    let env = vm.attach_current_thread_permanently().expect("failed to attach jni env");

    // We transmute the lifetime of the JNIEnv to 'static because we know that
    // the JNIEnv is only used in the context of a thread-local variable.
    Some(unsafe { std::mem::transmute(env) })
}

thread_local! {
    /// The JNIEnv for the current thread.
    /// This will be None when running from rust tests which do not have a JVM.
    static JNI_ENV: Option<jni::JNIEnv<'static>> = get_jenv();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
