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

package com.questdb.std.str;

import com.questdb.std.Sinkable;

import java.io.IOException;

public interface CharSink {

    CharSink encodeUtf8(CharSequence cs);

    CharSink encodeUtf8(CharSequence cs, int from, int len);

    CharSink encodeUtf8AndQuote(CharSequence cs);

    default void flush() throws IOException {
    }

    default CharSink put(CharSequence cs) {
        throw new UnsupportedOperationException();
    }

    default CharSink put(CharSequence cs, int start, int end) {
        throw new UnsupportedOperationException();
    }

    CharSink put(char c);

    CharSink putUtf8(char c);

    CharSink put(int value);

    CharSink put(long value);

    CharSink put(float value, int scale);

    CharSink put(double value, int scale);

    CharSink put(boolean value);

    CharSink put(Throwable e);

    CharSink put(Sinkable sinkable);

    CharSink putISODate(long value);

    CharSink putISODateMillis(long value);

    CharSink putQuoted(CharSequence cs);
}
