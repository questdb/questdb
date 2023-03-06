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

use dashmap::DashMap;
use jni::JNIEnv;
use jni::objects::GlobalRef;
use jni::sys::{jclass, jlong, jobject};
use log::Log;
use lazy_static::lazy_static;

lazy_static! {
    static ref IMPLS: DashMap<&'static str, JavaLogWrapper> = DashMap::new();
}

struct JavaLogWrapper {
    logger: GlobalRef
}

impl JavaLogWrapper {
    fn new(env: &JNIEnv, name: &str) -> Self {
        let log_factory_class = env.find_class("io/questdb/log/LogFactory")
            .expect("io.questdb.log.LogFactory class not found");
        let name = env.new_string(name)
            .expect("could not construct logger name string");
        let logger = env.call_static_method(
            log_factory_class,
            "getLog",
            "(Ljava/lang/String;)Lio/questdb/log/Log;",
            &[name.into()])
            .expect("io.questdb.log.LogFactory::getLog(String) call failed")
            .l()
            .expect("io.questdb.log.LogFactory::getLog(String) returned null");
        // We need to obtain a global reference for the logger
        let logger = env.new_global_ref(logger)
            .expect("could not obtain global reference to logger");
        JavaLogWrapper { logger }
    }

    fn advisory(&self, env: JNIEnv, msg: &str) {
        let logger_class = env.find_class("io/questdb/log/Log")
            .expect("io.questdb.log.Log class not found");
        let lo = msg.as_ptr() as jlong;
        let hi = lo + msg.len() as jlong;
        env.call_method(
            self.logger.as_obj(),
            "advisoryUtf8",
            "(JJ)V",
            &[lo.into(), hi.into()])
            .expect("io.questdb.log.Log::advisoryUtf8(long, long) call failed");
    }

    // default void criticalUtf8(long lo, long hi) {
    // critical().$utf8(lo, hi);
    // }
    fn critical(&self, env: JNIEnv, msg: &str) {
        let logger_class = env.find_class("io/questdb/log/Log")
            .expect("io.questdb.log.Log class not found");
        let lo = msg.as_ptr() as jlong;
        let hi = lo + msg.len() as jlong;
        env.call_method(
            self.logger.as_obj(),
            "criticalUtf8",
            "(JJ)V",
            &[lo.into(), hi.into()])
            .expect("io.questdb.log.Log::criticalUtf8(long, long) call failed");
    }

    // default void debugUtf8(long lo, long hi) {
    // debug().$utf8(lo, hi);
    // }
    fn debug(&self, env: JNIEnv, msg: &str) {
        let logger_class = env.find_class("io/questdb/log/Log")
            .expect("io.questdb.log.Log class not found");
        let lo = msg.as_ptr() as jlong;
        let hi = lo + msg.len() as jlong;
        env.call_method(
            self.logger.as_obj(),
            "debugUtf8",
            "(JJ)V",
            &[lo.into(), hi.into()])
            .expect("io.questdb.log.Log::debugUtf8(long, long) call failed");
    }

    // default void errorUtf8(long lo, long hi) {
    // error().$utf8(lo, hi);
    // }
    fn error(&self, env: JNIEnv, msg: &str) {
        let logger_class = env.find_class("io/questdb/log/Log")
            .expect("io.questdb.log.Log class not found");
        let lo = msg.as_ptr() as jlong;
        let hi = lo + msg.len() as jlong;
        env.call_method(
            self.logger.as_obj(),
            "errorUtf8",
            "(JJ)V",
            &[lo.into(), hi.into()])
            .expect("io.questdb.log.Log::errorUtf8(long, long) call failed");
    }

    // default void infoUtf8(long lo, long hi) {
    // info().$utf8(lo, hi);
    // }
    fn info(&self, env: JNIEnv, msg: &str) {
        let logger_class = env.find_class("io/questdb/log/Log")
            .expect("io.questdb.log.Log class not found");
        let lo = msg.as_ptr() as jlong;
        let hi = lo + msg.len() as jlong;
        env.call_method(
            self.logger.as_obj(),
            "infoUtf8",
            "(JJ)V",
            &[lo.into(), hi.into()])
            .expect("io.questdb.log.Log::infoUtf8(long, long) call failed");
    }
}

pub struct ThreadLocalLogger {
    impls: DashMap<&'static str, JavaLogWrapper>,
}

impl Log for ThreadLocalLogger {
    fn enabled(&self, _metadata: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            let level = match record.level() {
                log::Level::Error => "ERROR",
                log::Level::Warn => "WARN",
                log::Level::Info => "INFO",
                log::Level::Debug => "DEBUG",
                log::Level::Trace => "TRACE",
            };
            println!("{}: {}", level, record.args());
        }
    }

    fn flush(&self) {}
}

struct TrampolineLogger;

impl Log for TrampolineLogger {
    fn enabled(&self, _metadata: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            let level = match record.level() {
                log::Level::Error => "ERROR",
                log::Level::Warn => "WARN",
                log::Level::Info => "INFO",
                log::Level::Debug => "DEBUG",
                log::Level::Trace => "TRACE",
            };
            println!("{}: {}", level, record.args());
        }
    }

    fn flush(&self) {}
}

const TRAMPOLINE_LOGGER: TrampolineLogger = TrampolineLogger;

pub fn install_jni_logger() {
    log::set_logger(&TRAMPOLINE_LOGGER).unwrap();
    log::set_max_level(log::LevelFilter::Trace);
}


// use std::cell::RefCell;
//
// pub trait Logger : Send + Sync {
//     fn debug(&self, msg: &str);
//     fn info(&self, msg: &str);
//     fn error(&self, msg: &str);
//     fn critical(&self, msg: &str);
//     fn advisory(&self, msg: &str);
// }
//
// struct StderrLogger;
//
// impl Logger for StderrLogger {
//     fn debug(&self, msg: &str) {
//         eprintln!("DEBUG: {}", msg);
//     }
//
//     fn info(&self, msg: &str) {
//         eprintln!("INFO: {}", msg);
//     }
//
//     fn error(&self, msg: &str) {
//         eprintln!("ERROR: {}", msg);
//     }
//
//     fn critical(&self, msg: &str) {
//         eprintln!("CRITICAL: {}", msg);
//     }
//
//     fn advisory(&self, msg: &str) {
//         eprintln!("ADVISORY: {}", msg);
//     }
// }
//
// static STDERR_LOGGER: StderrLogger = StderrLogger;
//
// // // Set/cleared by `tokio::jni` whenever a tokio thread is spawned/stopped.
// // thread_local! {
// //     static LOGGER: RefCell<Option<Box<dyn Logger>>> = RefCell::new(None);
// // }
// //
// // pub fn set_logger(logger: Box<dyn Logger>) {
// //     LOGGER.with(|l| {
// //         *l.borrow_mut() = Some(logger);
// //     });
// // }
// //
// // pub fn get_logger() -> &'static dyn Logger {
// //     LOGGER.with(|l| {
// //         match &*l.borrow() {
// //             Some(logger) => &**logger,
// //             None => &STDERR_LOGGER,
// //         }
// //     })
// // }
// //
// // macro_rules! log_debug {
// //     ($($arg:tt)*) => (get_logger().debug(&format!($($arg)*)));
// // }
// //
// // macro_rules! log_info {
// //     ($($arg:tt)*) => (get_logger().info(&format!($($arg)*)));
// // }
// //
// // macro_rules! log_error {
// //     ($($arg:tt)*) => (get_logger().error(&format!($($arg)*)));
// // }
// //
// // macro_rules! log_critical {
// //     ($($arg:tt)*) => (get_logger().critical(&format!($($arg)*)));
// // }
// //
// // macro_rules! log_advisory {
// //     ($($arg:tt)*) => (get_logger().advisory(&format!($($arg)*)));
// // }
