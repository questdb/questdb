use std::borrow::Cow;

use jni::objects::{JClass, JString};
use jni::sys::jboolean;
use jni::JNIEnv;

#[no_mangle]
pub extern "system" fn Java_io_questdb_RustCodeFailScenario_setup<'a>(
    _env: JNIEnv,
    _class: JClass,
) -> *mut fail::FailScenario<'a> {
    Box::into_raw(Box::new(fail::FailScenario::setup()))
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[no_mangle]
pub extern "system" fn Java_io_questdb_RustCodeFailScenario_teardown(
    _env: JNIEnv,
    _class: JClass,
    ptr: *mut fail::FailScenario,
) {
    if !ptr.is_null() {
        unsafe {
            drop(Box::from_raw(ptr));
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_RustCodeFailScenario_setFailpoint(
    mut env: JNIEnv,
    _class: JClass,
    failpoint: JString,
    action: JString,
) {
    let failpoint_binding = env.get_string(&failpoint).unwrap();
    let failpoint: Cow<str> = (&failpoint_binding).into();

    let action_binding = env.get_string(&action).unwrap();
    let action: Cow<str> = (&action_binding).into();

    if let Err(err) = fail::cfg(failpoint.as_ref(), action.as_ref()) {
        env.throw_new("java/lang/IllegalArgumentException", err)
            .expect("could not throw exception");
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_RustCodeFailScenario_removeFailpoint(
    mut env: JNIEnv,
    _class: JClass,
    failpoint: JString,
) {
    let failpoint_binding = env.get_string(&failpoint).unwrap();
    let failpoint: Cow<str> = (&failpoint_binding).into();
    fail::remove(failpoint.as_ref());
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_RustCodeFailScenario_hasFailpoints(
    _env: JNIEnv,
    _class: JClass,
) -> jboolean {
    fail::has_failpoints() as jboolean
}
