/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 ******************************************************************************/

package com.questdb.io.sink;

import com.questdb.misc.Misc;

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
    public void close() throws IOException {
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
