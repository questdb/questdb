/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

use core::fmt::Write;
use std::cell::UnsafeCell;
use std::fmt;

use dashmap::DashMap;
use jni::JNIEnv;
use jni::objects::{GlobalRef, JMethodID, JStaticMethodID, JValue};
use jni::signature::{Primitive, ReturnType};
use jni::sys::jlong;
use log::{Log, Level, debug};

struct JavaLogWrapper { obj: GlobalRef }

struct CallState {
    impls: DashMap<Box<str>, JavaLogWrapper>,  // concurrent map
    log_factory_class: GlobalRef,
    log_factory_get_log_meth: JStaticMethodID,
    log_advisory_meth: JMethodID,
    log_critical_meth: JMethodID,
    log_debug_meth: JMethodID,
    log_error_meth: JMethodID,
    log_info_meth: JMethodID,
}

impl CallState {
    fn with_log_wrapper<F>(&self, jenv: &JNIEnv, name: &str, f: F)
            where F: FnOnce(&JNIEnv, &JavaLogWrapper) {
        let log_wrapper_ref = if let Some(pre_existing) = self.impls.get(name) {
            // Fast path: we already have a logger for this name.
            pre_existing
        }
        else {
            // Race to insert this logger.
            // Another thread might be trying to do the same.
            // That's OK. The latest one wins.
            let boxed_name = name.to_string().into_boxed_str();
            let log_wrapper = JavaLogWrapper::new(self, jenv, name);

            // There's a race condition here, but it's benign. If two threads
            // try to insert the same key at the same time, one of them will
            // fail and we'll just use the other one's value.
            self.impls.insert(boxed_name, log_wrapper);

            // Because of this we need to perform this second lookup.
            self.impls.get(name)
                .expect("just inserted key")
        };

        let log_wrapper = log_wrapper_ref.value();
        f(jenv, log_wrapper);
    }
}

static mut CALL_STATE: Option<CallState> = None;

fn get_call_state() -> &'static CallState {
    unsafe {
        CALL_STATE.as_ref().expect("J_CALL_INFO not initialized")
    }
}

fn lookup_and_set_java_call_info(env: &JNIEnv) -> jni::errors::Result<()> {
    let log_factory_class = env.find_class("io/questdb/log/LogFactory")?;
    let log_factory_class = env.new_global_ref(log_factory_class)?;
    let log_factory_get_log_meth = env.get_static_method_id(
        "io/questdb/log/LogFactory",
        "getLog",
        "(Ljava/lang/String;)Lio/questdb/log/Log;")?;

    let get_method_id = |name: &str| {
        env.get_method_id(
            "io/questdb/log/Log",
            name,
            "(JJ)V")
    };

    let log_advisory_meth = get_method_id("advisoryUtf8")?;
    let log_critical_meth = get_method_id("criticalUtf8")?;
    let log_debug_meth = get_method_id("debugUtf8")?;
    let log_error_meth = get_method_id("errorUtf8")?;
    let log_info_meth = get_method_id("infoUtf8")?;

    let impls = DashMap::new();
    let call_state = CallState {
        impls,
        log_factory_class,
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

impl JavaLogWrapper {
    fn new(call_state: &CallState, jenv: &JNIEnv, name: &str) -> Self {
        let j_name = jenv.new_string(name)
            .expect("could not construct logger name string");
        let j_name = jenv.auto_local(j_name);
        let obj = jenv.call_static_method_unchecked(
            call_state.log_factory_class.as_obj(),
            call_state.log_factory_get_log_meth,
            ReturnType::Object,
            &[JValue::Object(j_name.as_obj()).into()])
            .expect("io.questdb.log.LogFactory::getLog(String) call failed")
            .l()
            .expect("io.questdb.log.LogFactory::getLog(String) didn't return an object");
        if obj.is_null() {
            panic!("io.questdb.log.LogFactory::getLog(String) returned null");
        }
        let obj = jenv.new_global_ref(obj)
            .expect("could not create global reference to io.questdb.log.Log object");
        JavaLogWrapper { obj }
    }

    #[inline]
    fn call_log_meth(&self, env: &JNIEnv, meth: JMethodID, meth_name: &str, msg: &str) {
        let lo = msg.as_ptr() as jlong;
        let hi = lo + msg.len() as jlong;
        let lo = JValue::Long(lo);
        let hi = JValue::Long(hi);
        let res = env.call_method_unchecked(
            self.obj.as_obj(),
            meth,
            ReturnType::Primitive(Primitive::Void),
            &[lo.into(), hi.into()]);
        if let Err(e) = res {
            let throwable = env.exception_occurred().unwrap();
            if !throwable.is_null() {
                env.exception_describe().unwrap();
            }
            panic!("io.questdb.log.Log::{}(long, long) call failed: {}", meth_name, e);
        }
    }

    fn advisory(&self, env: &JNIEnv, msg: &str) {
        self.call_log_meth(
            env,
            get_call_state().log_advisory_meth,
            "advisoryUtf8",
            msg);
    }

    fn critical(&self, env: &JNIEnv, msg: &str) {
        self.call_log_meth(
            env,
            get_call_state().log_critical_meth,
            "criticalUtf8",
            msg);
    }

    fn debug(&self, env: &JNIEnv, msg: &str) {
        self.call_log_meth(
            env,
            get_call_state().log_debug_meth,
            "debugUtf8",
            msg);
    }

    fn error(&self, env: &JNIEnv, msg: &str) {
        self.call_log_meth(
            env,
            get_call_state().log_error_meth,
            "errorUtf8",
            msg);
    }

    fn info(&self, env: &JNIEnv, msg: &str) {
        self.call_log_meth(
            env,
            get_call_state().log_info_meth,
            "infoUtf8",
            msg);
    }
}

struct TrampolineLogger;

thread_local! {
    static LOG_BUF: UnsafeCell<String> = UnsafeCell::new(String::with_capacity(64));
}

fn get_formatted_msg<'a>(args: &fmt::Arguments) -> &'a str {
    if let Some(no_args_str) = args.as_str() {
        return no_args_str;
    }

    LOG_BUF.with(|msg: &UnsafeCell<String>| {
        let msg: &mut String = unsafe { &mut *msg.get() };
        msg.clear();

        // Clone here is cheap: The args object just holds references.
        msg.write_fmt(args.clone()).unwrap();
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
        eprintln!("TRAMPOLINE LOGGER :: (0) target: {} - record={:?}", record.target(), record);

        let target = record.target();
        let msg = get_formatted_msg(record.args());
        let jenv = crate::get_jenv()
            .expect("could not get JNIEnv");
        get_call_state().with_log_wrapper(
            &jenv,
            target, 
            |jenv, log_wrapper| {
                match record.level() {
                    Level::Error => log_wrapper.critical(jenv, msg),
                    Level::Warn => log_wrapper.error(jenv, msg),
                    Level::Info => log_wrapper.info(jenv, msg),
                    Level::Debug => log_wrapper.debug(jenv, msg),
                    Level::Trace => log_wrapper.advisory(jenv, msg),
                }
            });
    }

    fn flush(&self) {}
}

const TRAMPOLINE_LOGGER: TrampolineLogger = TrampolineLogger;
static mut LOGGER_INSTALLED: bool = false;

pub fn install_jni_logger(env: &JNIEnv) {
    if unsafe { LOGGER_INSTALLED } {
        return;
    }
    if let Err(e) = lookup_and_set_java_call_info(&env) {
        let throwable = env.exception_occurred().unwrap();
        if !throwable.is_null() {
            return;
        }
        panic!("could not set up Java call info: {}", e);
    }
    log::set_logger(&TRAMPOLINE_LOGGER).unwrap();
    log::set_max_level(log::LevelFilter::Debug);
    unsafe { LOGGER_INSTALLED = true; }
    debug!("installed JNI logger");
}
