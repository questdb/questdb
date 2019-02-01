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

import com.questdb.std.Misc;

import java.io.Closeable;
import java.io.IOException;

public class DelimitedCharSink extends AbstractCharSink implements CharSink, Closeable {
    private static final String NULL = "";
    private final CharSink delegate;
    private final char delimiter;
    private final String eol;
    private boolean del = false;

    public DelimitedCharSink(CharSink delegate, char delimiter, String eol) {
        this.delegate = delegate;
        this.delimiter = delimiter;
        this.eol = eol;
    }

    @Override
    public void close() {
        Misc.free(delegate);
    }

    public void eol() {
        delegate.put(eol);
        del = false;
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    @Override
    public CharSink put(CharSequence cs) {
        delimiter();
        delegate.put(cs == null ? NULL : cs);
        return this;
    }

    @Override
    public CharSink put(char c) {
        delegate.put(c);
        return this;
    }

    public CharSink put(int value) {
        delimiter();
        return super.put(value);
    }

    public CharSink put(long value) {
        delimiter();
        return super.put(value);
    }

    public CharSink putISODate(long value) {
        delimiter();
        return super.put(value);
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    private void delimiter() {
        if (del) {
            delegate.put(delimiter);
        } else {
            del = true;
        }
    }
}
