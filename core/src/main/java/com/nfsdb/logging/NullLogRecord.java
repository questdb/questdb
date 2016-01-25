/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb.logging;

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
    public LogRecord $(char c) {
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
