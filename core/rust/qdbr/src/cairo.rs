/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
use jni::objects::{JObject, JThrowable, JValue};
use jni::JNIEnv;
use std::backtrace::{Backtrace, BacktraceStatus};
use std::sync::Arc;

pub trait DefaultReturn {
    fn default_return() -> Self;
}

impl DefaultReturn for () {
    fn default_return() -> Self {}
}

impl DefaultReturn for usize {
    fn default_return() -> Self {
        0
    }
}

impl DefaultReturn for u32 {
    fn default_return() -> Self {
        0
    }
}

impl DefaultReturn for i64 {
    fn default_return() -> Self {
        0
    }
}

impl DefaultReturn for u64 {
    fn default_return() -> Self {
        0
    }
}

impl<T> DefaultReturn for *mut T {
    fn default_return() -> Self {
        std::ptr::null_mut()
    }
}

impl<T> DefaultReturn for *const T {
    fn default_return() -> Self {
        std::ptr::null()
    }
}

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

    pub fn throw<T: DefaultReturn>(self, env: &mut JNIEnv) -> T {
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
        T::default_return()
    }
}
