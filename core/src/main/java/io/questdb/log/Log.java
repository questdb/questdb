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

package io.questdb.log;

public interface Log {

    LogRecord advisory();

    @SuppressWarnings("unused")  // Called by `log.rs` across JNI.
    default void advisoryUtf8(long lo, long hi) {
        advisory().$utf8(lo, hi).$();
    }

    LogRecord advisoryW();

    LogRecord critical();

    @SuppressWarnings("unused")  // Called by `log.rs` across JNI.
    default void criticalUtf8(long lo, long hi) {
        critical().$utf8(lo, hi).$();
    }

    LogRecord criticalW();

    LogRecord debug();

    @SuppressWarnings("unused")  // Called by `log.rs` across JNI.
    default void debugUtf8(long lo, long hi) {
        debug().$utf8(lo, hi).$();
    }

    LogRecord debugW();

    LogRecord error();

    @SuppressWarnings("unused")  // Called by `log.rs` across JNI.
    default void errorUtf8(long lo, long hi) {
        error().$utf8(lo, hi).$();
    }

    LogRecord errorW();

    LogRecord info();

    @SuppressWarnings("unused")  // Called by `log.rs` across JNI.
    default void infoUtf8(long lo, long hi) {
        info().$utf8(lo, hi).$();
    }

    LogRecord infoW();

    LogRecord xDebugW();

    LogRecord xInfoW();

    LogRecord xadvisory();

    LogRecord xcritical();

    LogRecord xdebug();

    LogRecord xerror();

    LogRecord xinfo();
}
