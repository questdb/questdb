use jni::{JavaVM, JNIEnv, objects::JClass};

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

mod tokio;
mod wal_upload;
mod log;

pub static mut JAVA_VM: Option<JavaVM> = None;

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

#[no_mangle]
pub extern "system" fn Java_io_questdb_ServerMain_initQuestdbJni(env: JNIEnv,_class: JClass) {
    let vm = match env.get_java_vm() {
        Ok(vm) => vm,
        Err(e) => {
            env.find_class("io/questdb/jar/jni/LoadException")
                .and_then(|clazz| env.throw_new(clazz, e.to_string()))
                .expect("failed to throw TokioException");
            return;
        }
    };
    unsafe {
        JAVA_VM = Some(vm);
    }
    crate::log::install_jni_logger(&env);
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
