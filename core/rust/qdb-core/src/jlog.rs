use core::fmt::Write;
use std::cell::UnsafeCell;
use std::fmt;
use std::ops::DerefMut;
use std::sync::Once;

use dashmap::{DashMap, SharedValue};
use jni::objects::{GlobalRef, JClass, JStaticMethodID, JString, JValue};
use jni::signature::{Primitive, ReturnType};
use jni::sys::{jint, jlong};
use jni::JNIEnv;
use log::{Level, Log};

use crate::unwrap_or_throw;

#[derive(Clone, Debug)]
pub struct JavaLog {
    obj: GlobalRef,
}

impl JavaLog {
    fn new(call_state: &CallState, jenv: &mut JNIEnv, name: &str) -> Self {
        let obj = jenv
            .with_local_frame(
                2, // Required number of local references in the frame.
                |jenv| -> Result<GlobalRef, jni::errors::Error> {
                    let j_name = jenv
                        .new_string(name)
                        .expect("could not construct logger name string");
                    let j_name_value = JValue::Object(&j_name);
                    let obj = unsafe {
                        jenv.call_static_method_unchecked(
                            &call_state.log_factory_class,
                            call_state.log_factory_get_log_meth,
                            ReturnType::Object,
                            &[j_name_value.as_jni()],
                        )
                    }
                    .expect("io.questdb.log.LogFactory::getLog(String) call failed")
                    .l()
                    .expect("io.questdb.log.LogFactory::getLog(String) didn't return an object");
                    if obj.is_null() {
                        panic!("io.questdb.log.LogFactory::getLog(String) returned null");
                    }
                    let obj = jenv
                        .new_global_ref(obj)
                        .expect("could not create global reference to io.questdb.log.Log object");
                    Ok(obj)
                },
            )
            .expect("could not create local frame");
        JavaLog { obj }
    }

    #[inline]
    fn call_log_meth(&self, env: &mut JNIEnv, meth: JStaticMethodID, meth_name: &str, msg: &str) {
        let lo = msg.as_ptr() as jlong;
        let hi = lo + msg.len() as jlong;
        let lo = JValue::Long(lo);
        let hi = JValue::Long(hi);
        let obj = JValue::Object(self.obj.as_obj());
        let res = unsafe {
            env.call_static_method_unchecked(
                &get_call_state().rust_log_adaptor_class,
                meth,
                ReturnType::Primitive(Primitive::Void),
                &[obj.as_jni(), lo.as_jni(), hi.as_jni()],
            )
        };
        if let Err(e) = res {
            let throwable = env.exception_occurred().unwrap();
            if !throwable.is_null() {
                env.exception_describe().unwrap();
            } else {
                panic!(
                    "io.questdb.log.Log::{}(long, long) call failed: {}",
                    meth_name, e
                );
            }
        }
    }

    /// Log a message at the `Advisory` level.
    /// An info-type message that doesn't get suppressed if info is disabled.
    /// This is for messages that we pretty much always want to appear, so are higher
    /// priority than info. This might be start-up information logging paths and/or ports.
    #[allow(dead_code)]
    fn advisory(&self, env: &mut JNIEnv, msg: &str) {
        self.call_log_meth(env, get_call_state().log_advisory_meth, "advisoryUtf8", msg);
    }

    /// Log a message at the `Critical` level.
    /// A message that indicates a critical error.
    /// Critical log messages are forwarded to our alerting system and ping
    /// our phones when stuff goes awry.
    /// Details are usually in preceding info messages.
    #[allow(dead_code)]
    fn critical(&self, env: &mut JNIEnv, msg: &str) {
        self.call_log_meth(env, get_call_state().log_critical_meth, "criticalUtf8", msg);
    }

    /// Log a message at the `Debug` level.
    /// This level is usually turned off in most configurations.
    fn debug(&self, env: &mut JNIEnv, msg: &str) {
        self.call_log_meth(env, get_call_state().log_debug_meth, "debugUtf8", msg);
    }

    /// Log a message at the `Error` level.
    /// An error has occured, but it is recoverable.
    /// This is where users might find out that they have a bad query or a bad ILP message.
    /// Details are generally present in preceding info messages.
    fn error(&self, env: &mut JNIEnv, msg: &str) {
        self.call_log_meth(env, get_call_state().log_error_meth, "errorUtf8", msg);
    }

    /// Log a message at the `Info` level.
    /// This is the default level that's configured for logging.
    /// It often carries details about an error or critical error that's going to be logged later.
    fn info(&self, env: &mut JNIEnv, msg: &str) {
        self.call_log_meth(env, get_call_state().log_info_meth, "infoUtf8", msg);
    }
}

struct CallState {
    // concurrent map
    impls: DashMap<Box<str>, JavaLog>,
    log_factory_class: GlobalRef,
    rust_log_adaptor_class: GlobalRef,
    log_factory_get_log_meth: JStaticMethodID,
    log_advisory_meth: JStaticMethodID,
    log_critical_meth: JStaticMethodID,
    log_debug_meth: JStaticMethodID,
    log_error_meth: JStaticMethodID,
    log_info_meth: JStaticMethodID,
}

impl CallState {
    fn with_log_wrapper<F, R>(&self, jenv: &mut JNIEnv, name: &str, f: F) -> R
    where
        F: FnOnce(&mut JNIEnv, &JavaLog) -> R,
    {
        let shard_index = self.impls.determine_map(name);
        let mut shard_guard = unsafe { self.impls.shards().get_unchecked(shard_index) }.write();
        let shard = shard_guard.deref_mut();
        let (_, shared_log_wrapper) = shard.raw_entry_mut().from_key(name).or_insert_with(|| {
            let boxed_name = name.to_string().into_boxed_str();
            let log_wrapper = JavaLog::new(self, jenv, name);
            let shared_log_wrapper = SharedValue::new(log_wrapper);
            (boxed_name, shared_log_wrapper)
        });
        let log_wrapper = shared_log_wrapper.get();
        f(jenv, log_wrapper)
    }
}

static mut CALL_STATE: Option<CallState> = None;

fn get_call_state() -> &'static CallState {
    unsafe { CALL_STATE.as_ref().expect("J_CALL_INFO not initialized") }
}

#[allow(dead_code)]
pub fn get_java_log(env: &mut JNIEnv, name: &str) -> JavaLog {
    get_call_state().with_log_wrapper(env, name, |_jenv, log| log.clone())
}

fn lookup_and_set_java_call_info(env: &mut JNIEnv) -> jni::errors::Result<()> {
    let log_factory_class = env.find_class("io/questdb/log/LogFactory")?;
    let log_factory_class = env.new_global_ref(log_factory_class)?;

    let rust_log_adaptor_class = env.find_class("io/questdb/log/RustLogAdaptor")?;
    let rust_log_adaptor_class = env.new_global_ref(rust_log_adaptor_class)?;

    let log_factory_get_log_meth = env.get_static_method_id(
        "io/questdb/log/LogFactory",
        "getLog",
        "(Ljava/lang/String;)Lio/questdb/log/Log;",
    )?;

    let mut get_log_meth_id = |name: &str| {
        env.get_static_method_id(
            "io/questdb/log/RustLogAdaptor",
            name,
            "(Lio/questdb/log/Log;JJ)V",
        )
    };

    let log_advisory_meth = get_log_meth_id("advisoryUtf8")?;
    let log_critical_meth = get_log_meth_id("criticalUtf8")?;
    let log_debug_meth = get_log_meth_id("debugUtf8")?;
    let log_error_meth = get_log_meth_id("errorUtf8")?;
    let log_info_meth = get_log_meth_id("infoUtf8")?;

    let impls = DashMap::new();
    let call_state = CallState {
        impls,
        log_factory_class,
        rust_log_adaptor_class,
        log_factory_get_log_meth,
        log_advisory_meth,
        log_critical_meth,
        log_debug_meth,
        log_error_meth,
        log_info_meth,
    };
    unsafe {
        CALL_STATE = Some(call_state);
    }

    Ok(())
}

struct TrampolineLogger;

thread_local! {
    static LOG_BUF: UnsafeCell<String> = UnsafeCell::new(String::with_capacity(64));
}

/// Obtain a formatted buffer, possibly buffering using a thread-local String.
fn get_formatted_msg<'a>(is_warn: bool, line_num: Option<u32>, args: &fmt::Arguments) -> &'a str {
    if let Some(no_args_str) = args.as_str() {
        return no_args_str;
    }

    LOG_BUF.with(|msg: &UnsafeCell<String>| {
        let msg: &mut String = unsafe { &mut *msg.get() };
        msg.clear();

        if is_warn {
            write!(msg, "WARN: ").unwrap();
        }

        if let Some(line_num) = line_num {
            write!(msg, "L{} ", line_num).unwrap();
        }

        // Clone here is cheap: The args object just holds references.
        msg.write_fmt(*args).unwrap();
        msg.as_str()
    })
}

impl Log for TrampolineLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        // Drop JNI logs: These could explode into an infinite loop.
        !metadata.target().starts_with("jni")
    }

    fn log(&self, record: &log::Record) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let target = record.target();
        let is_warn = record.level() == Level::Warn;
        let msg = get_formatted_msg(is_warn, record.line(), record.args());
        let mut jenv = crate::get_jenv().expect("could not get JNIEnv");
        get_call_state().with_log_wrapper(&mut jenv, target, |jenv, log_wrapper| {
            match record.level() {
                Level::Error => log_wrapper.error(jenv, msg),
                Level::Warn => log_wrapper.info(jenv, msg),
                Level::Info => log_wrapper.info(jenv, msg),
                Level::Debug => log_wrapper.debug(jenv, msg),
                Level::Trace => log_wrapper.debug(jenv, msg),
            }
        });
    }

    fn flush(&self) {}
}

const TRAMPOLINE_LOGGER: TrampolineLogger = TrampolineLogger;
static INSTALL_JNI_LOGGER_ONCE: Once = Once::new();

pub fn install_jni_logger(env: &mut JNIEnv, max_level: Level) -> jni::errors::Result<()> {
    let mut result = Ok(());
    INSTALL_JNI_LOGGER_ONCE.call_once(|| {
        result = lookup_and_set_java_call_info(env)
            .and_then(|_| {
                log::set_logger(&TRAMPOLINE_LOGGER).map_err(|e| {
                    env.throw_new(
                        "java/lang/RuntimeException",
                        &format!("Could not set Rust logger: {}", e),
                    )
                    .unwrap();
                    jni::errors::Error::JavaException
                })
            })
            .map(|_| {
                log::set_max_level(max_level.to_level_filter());
            });
    });
    result
}

fn level_from_jint(num: jint) -> Level {
    match num {
        1 => Level::Error,
        2 => Level::Warn,
        3 => Level::Info,
        4 => Level::Debug,
        5 => Level::Trace,
        _ => panic!("invalid log level: {}", num),
    }
}

pub fn log_critical(target: &str, msg: &str) {
    let mut jenv = crate::get_jenv().expect("could not get JNIEnv");
    get_call_state().with_log_wrapper(&mut jenv, target, |jenv, log_wrapper| {
        log_wrapper.critical(jenv, msg);
    });
}

pub fn log_critical_ctx(mod_path: &str, line: u32, args: fmt::Arguments) {
    let msg = get_formatted_msg(false, Some(line), &args);
    log_critical(mod_path, msg);
}

/// Log a message that pings our phones.
/// `target` is the rust module name.
/// `msg` is the payload.
#[macro_export]
macro_rules! critical {
    // critical!(target: "my_target"; "a {} event", "log");
    (target: $target:expr, $($arg:tt)+) => ({
        $crate::jlog::log_critical_ctx(
            module_path!(),
            line!(),
            format_args!($($arg)+));
    });

    // critical!("a log event")
    ($($arg:tt)+) => (critical!(target: module_path!(), $($arg)+));
}

pub use critical;

#[no_mangle]
pub extern "system" fn Java_io_questdb_log_RustLogging_installRustLogger(
    mut env: JNIEnv,
    _class: JClass,
    max_level: jint,
) {
    let level = level_from_jint(max_level);
    unwrap_or_throw!(env, install_jni_logger(&mut env, level));
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_log_RustLogging_logMsg(
    mut env: JNIEnv,
    _class: JClass,
    level: jint,
    target: JString,
    msg: JString,
) {
    let level = level_from_jint(level);
    let target = env.get_string(&target).expect("could not get target");
    let target_str = target.to_str().expect("could not convert target to str");
    let msg = env.get_string(&msg).expect("could not get msg");
    let msg_str = msg.to_str().expect("could not convert msg to str");
    log::log!(target: target_str, level, "{}", msg_str);
    critical!("testing is going critical: {:?}", msg_str);
}
