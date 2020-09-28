/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.std.Sinkable;

import java.io.File;

public interface LogRecord {
    void $();

    LogRecord $(CharSequence sequence);

    LogRecord $(CharSequence sequence, int lo, int hi);

    LogRecord $(int x);

    LogRecord $(double x);

    LogRecord $(long x);

    LogRecord $(boolean x);

    LogRecord $(char c);

    LogRecord $(Throwable e);

    LogRecord $(File x);

    LogRecord $(Object x);

    LogRecord $(Sinkable x);

    LogRecord $ip(long ip);

    LogRecord $ts(long x);

    boolean isEnabled();

    LogRecord ts();

    LogRecord microTime(long x);

    LogRecord utf8(CharSequence sequence);
}
