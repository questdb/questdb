/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.std;

import java.io.IOException;

public interface CharSink {
    void flush() throws IOException;

    CharSink put(CharSequence cs);

    CharSink put(char c);

    CharSink put(int value);

    CharSink put(long value);

    CharSink put(float value, int scale);

    CharSink put(double value, int scale);

    CharSink put(boolean value);

    CharSink put(Throwable e);

    CharSink put(Sinkable sinkable);

    CharSink putISODate(long value);

    CharSink putJson(float value, int scale);

    CharSink putJson(double value, int scale);

    CharSink putQuoted(CharSequence cs);

    CharSink putTrim(double value, int scale);

    CharSink putUtf8(CharSequence cs);

    CharSink putUtf8Escaped(CharSequence cs);

    CharSink putUtf8EscapedAndQuoted(CharSequence cs);
}
