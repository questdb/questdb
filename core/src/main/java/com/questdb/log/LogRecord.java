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

public interface LogRecord {
    void $();

    LogRecord $(CharSequence sequence);

    LogRecord $(int x);

    LogRecord $(double x);

    LogRecord $(long x);

    LogRecord $(boolean x);

    LogRecord $(char c);

    LogRecord $(Throwable e);

    LogRecord $(File x);

    LogRecord $(Enum e);

    LogRecord $(Object x);

    LogRecord $(Sinkable x);

    LogRecord $ip(long ip);

    LogRecord $ts(long x);

    boolean isEnabled();

    LogRecord ts();

    LogRecord utf8(CharSequence sequence);
}
