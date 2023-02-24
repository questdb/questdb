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

use tokio::runtime::Builder;

use jni::JNIEnv;
use jni::objects::JClass;
use jni::sys::{jint, jlong};

#[no_mangle]
pub extern "system" fn io_questdb_tokio_TokioRuntime_create(
        env: JNIEnv,
        _class: JClass,
        worker_threads: jint) -> jlong {
    let rt =
        if worker_threads == 0 {
            Builder::new_multi_thread()
                .enable_all()
                .build()
        }
        else {
            Builder::new_multi_thread()
                .enable_all()
                .worker_threads(worker_threads as usize)
                .build()
        };
    match rt {
        Ok(rt) => Box::into_raw(Box::new(rt)) as jlong,
        Err(e) => {
            env.find_class("io/questdb/tokio/TokioException")
                .and_then(|clazz| env.throw_new(clazz, e.to_string()))
                .expect("failed to throw TokioException");
            0
        }
    }
}