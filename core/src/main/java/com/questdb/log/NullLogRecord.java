/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.log;

import com.questdb.std.Sinkable;

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
    public LogRecord $(Enum e) {
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
    public boolean isEnabled() {
        return false;
    }

    @Override
    public LogRecord ts() {
        return this;
    }

    @Override
    public LogRecord utf8(CharSequence sequence) {
        return this;
    }
}
