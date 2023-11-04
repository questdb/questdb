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

import io.questdb.std.str.CharSinkBase;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;

public interface LogRecord extends CharSinkBase<LogRecord> {

    void $();

    LogRecord $(@Nullable CharSequence sequence);

    LogRecord $(@Nullable Utf8Sequence sequence);

    LogRecord $(@Nullable DirectUtf8Sequence sequence);

    LogRecord $(@NotNull CharSequence sequence, int lo, int hi);

    LogRecord $(int x);

    LogRecord $(double x);

    LogRecord $(long l);

    LogRecord $(boolean x);

    LogRecord $(char c);

    LogRecord $(@Nullable Throwable e);

    LogRecord $(@Nullable File x);

    LogRecord $(@Nullable Object x);

    LogRecord $(@Nullable Sinkable x);

    LogRecord $256(long a, long b, long c, long d);

    LogRecord $hex(long value);

    LogRecord $hexPadded(long value);

    LogRecord $ip(long ip);

    LogRecord $ts(long x);

    LogRecord $utf8(long lo, long hi);

    default void I$() {
        $(']').$();
    }

    boolean isEnabled();

    LogRecord microTime(long x);

    LogRecord ts();

    LogRecord utf8(@Nullable CharSequence sequence);
}
