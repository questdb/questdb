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

final class NullLogRecord implements LogRecord {

    public static final NullLogRecord INSTANCE = new NullLogRecord();

    private NullLogRecord() {
    }

    @Override
    public void $() {
    }

    @Override
    public LogRecord $(CharSequence sequence) {
        return this;
    }

    @Override
    public LogRecord $(CharSequence sequence, int lo, int hi) {
        return this;
    }

    @Override
    public LogRecord $(int x) {
        return this;
    }

    @Override
    public LogRecord $(double x) {
        return this;
    }

    @Override
    public LogRecord $(long x) {
        return this;
    }

    @Override
    public LogRecord $(boolean x) {
        return this;
    }

    @Override
    public LogRecord $(char c) {
        return this;
    }

    @Override
    public LogRecord $(Throwable e) {
        return this;
    }

    @Override
    public LogRecord $(File x) {
        return this;
    }

    @Override
    public LogRecord $(Object x) {
        return this;
    }

    @Override
    public LogRecord $(Sinkable x) {
        return this;
    }

    @Override
    public LogRecord $ip(long ip) {
        return this;
    }

    @Override
    public LogRecord $ts(long x) {
        return this;
    }

    @Override
    public LogRecord $256(long a, long b, long c, long d) {
        return this;
    }

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public LogRecord ts() {
        return this;
    }

    @Override
    public LogRecord microTime(long x) {
        return this;
    }

    @Override
    public LogRecord utf8(CharSequence sequence) {
        return this;
    }
}
