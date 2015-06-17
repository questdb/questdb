/*******************************************************************************
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
 ******************************************************************************/
package com.nfsdb.collections;

import com.nfsdb.utils.Chars;

public class FlyweightCharSequence implements CharSequence {
    private CharSequence delegate;
    private int lo;
    private int len;

    @Override
    public int hashCode() {
        if (len == 0) {
            return 0;
        }

        int h = 0;
        int hi = lo + len;
        for (int p = lo; p < hi; p++) {
            h = 31 * h + delegate.charAt(p);
        }
        return h;
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || obj instanceof CharSequence && Chars.equals(this, (CharSequence) obj);
    }

    @Override
    public int length() {
        return len;
    }

    @Override
    public char charAt(int index) {
        return delegate.charAt(index + lo);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return null;
    }

    public FlyweightCharSequence of(CharSequence delegate) {
        return of(delegate, 1, delegate.length() - 2);
    }

    public FlyweightCharSequence of(CharSequence delegate, int lo, int len) {
        this.delegate = delegate;
        this.lo = lo;
        this.len = len;
        return this;
    }
}
