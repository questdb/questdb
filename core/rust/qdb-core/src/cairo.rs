/*******************************************************************************
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
use jni::JNIEnv;
use jni::objects::{JObject, JThrowable, JValue};
use std::backtrace::{Backtrace, BacktraceStatus};
use std::error::Error;
use std::sync::Arc;

pub struct CairoException {
    out_of_memory: bool,
    message: String,
    backtrace: Arc<Backtrace>,
}

impl CairoException {
    pub fn new(msg: impl Into<String>) -> Self {
        Self {
            out_of_memory: false,
            message: msg.into(),
            backtrace: Backtrace::disabled().into(),
        }
    }

    pub fn out_of_memory(mut self, value: bool) -> Self {
        self.out_of_memory = value;
        self
    }

    pub fn backtrace(self, backtrace: impl Into<Arc<Backtrace>>) -> Self {
        let backtrace = backtrace.into();
        Self { backtrace, ..self }
    }

    pub fn throw<T: Default>(self, env: &mut JNIEnv) -> T {
        let errno = -1; // NON_CRITICAL
        let message: JObject = env
            .new_string(&self.message)
            .expect("failed to create Java string")
            .into();
        let message: JValue = (&message).into();
        let backtrace: JObject = match self.backtrace.status() {
            BacktraceStatus::Captured => env
                .new_string(self.backtrace.to_string())
                .expect("failed to create Java string")
                .into(),
            _ => JObject::null(),
        };
        let backtrace: JValue = (&backtrace).into();
        let cairo_exc: JThrowable = env
            .call_static_method(
                "io/questdb/cairo/CairoException",
                "paramInstance",
                concat!(
                    "(",
                    "I",                        // int errno
                    "Z",                        // boolean outOfMemory
                    "Ljava/lang/CharSequence;", // CharSequence message
                    "Ljava/lang/CharSequence;", // CharSequence backtrace
                    ")Lio/questdb/cairo/CairoException;"
                ),
                &[errno.into(), self.out_of_memory.into(), message, backtrace],
            )
            .expect("failed to call CairoException::paramInstance()")
            .l()
            .expect("failed to get CairoException object")
            .into();
        env.throw(cairo_exc)
            .expect("failed to throw CairoException");
        T::default()
    }
}

/// Converts the error into a CairoException and throws it to Java.
pub trait ToCairoException<T: Default>: Error + Sized {
    fn throw_to_java(self, env: &mut JNIEnv) -> T {
        let cairo_exc = CairoException::new(self.to_string());
        cairo_exc.throw(env)
    }
}

impl<T: Error, U: Default> ToCairoException<U> for T {}

pub trait IntoResult<T, E: Error + Sized> {
    fn into_result(self) -> Result<T, E>;
}

/// Converts the error into a CairoException and throws it to Java or returns the result.
pub trait ResultToCairoException<T: Default, E: Error + Sized>: IntoResult<T, E> + Sized {
    fn or_throw_to_java(self, env: &mut JNIEnv) -> T {
        match self.into_result() {
            Ok(value) => value,
            Err(err) => err.throw_to_java(env),
        }
    }
}

impl<T, E: Error + Sized> IntoResult<T, E> for Result<T, E> {
    fn into_result(self) -> Result<T, E> {
        self
    }
}

impl<T: Default, E: Error + Sized, R: IntoResult<T, E>> ResultToCairoException<T, E> for R {}
