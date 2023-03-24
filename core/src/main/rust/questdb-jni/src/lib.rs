use jni::{JavaVM, JNIEnv, objects::JClass};

mod tokio;
mod wal_upload;
mod log;

static mut JAVA_VM: Option<JavaVM> = None;

fn get_jenv() -> Option<jni::JNIEnv<'static>> {
    if let Some(vm) = unsafe { &JAVA_VM } {
        let j_env = vm.attach_current_thread_permanently()
            .expect("failed to attach jni env");
        Some(j_env)
    }
    else {
        None
    }
}

/// A macro to be used in JNI functions to unwrap a `jni::errors::Result<T>`.
/// It checks if the result is `Ok(T)` and returns the value, otherwise it
/// will return from the calling function if there's a pending Java exception,
/// or wrap the error in the specified exception class and throw it and return
/// the calling function.
#[macro_export]
macro_rules! unwrap_or_throw {
    ($env:expr, $result:expr, $fallback_java_exception_class:literal $(, $return_sentinel:literal)?) => {
        match $result {
            Ok(result) => result,
            Err(e) => {
                let throwable = $env.exception_occurred().unwrap();
                if throwable.is_null() {
                    $env.throw_new($fallback_java_exception_class, &format!("{}", e))
                        .expect("failed to throw java exception");
                }
                return $($return_sentinel)?;
            }
        }
    };
    ($env:expr, $result:expr) => {
        unwrap_or_throw!($env, $result, "java/lang/RuntimeException")
    };
}

/// Entry point to register the JAVA_VM static.
/// See `Os.java` for the Java side of this.
#[no_mangle]
pub extern "system" fn Java_io_questdb_std_Os_initQuestdbJni(mut env: JNIEnv, _class: JClass) {
    let vm = unwrap_or_throw!(env, env.get_java_vm());
    unsafe { JAVA_VM = Some(vm); }
}
