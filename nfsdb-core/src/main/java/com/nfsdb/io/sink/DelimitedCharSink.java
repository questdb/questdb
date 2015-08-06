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

package com.nfsdb.io.sink;

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
        if (delegate instanceof Closeable) {
            ((Closeable) delegate).close();
        }
    }

    public DelimitedCharSink eol() {
        delegate.put(eol);
        del = false;
        return this;
    }

    @Override
    public void flush() {
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
