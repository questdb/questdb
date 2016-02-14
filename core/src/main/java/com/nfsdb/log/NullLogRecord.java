/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.log;

import java.io.File;

public final class NullLogRecord implements LogRecord {

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
    public LogRecord $(Enum e) {
        return this;
    }

    @Override
    public LogRecord $(Object x) {
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
    public boolean isEnabled() {
        return false;
    }

    @Override
    public LogRecord ts() {
        return this;
    }
}
