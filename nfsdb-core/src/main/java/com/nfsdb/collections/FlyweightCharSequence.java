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

package com.nfsdb.collections;

public class FlyweightCharSequence extends AbstractCharSequence implements Mutable {
    public static final ObjectPoolFactory<FlyweightCharSequence> FACTORY = new ObjectPoolFactory<FlyweightCharSequence>() {
        @Override
        public FlyweightCharSequence newInstance() {
            return new FlyweightCharSequence();
        }
    };

    private CharSequence delegate;
    private int lo;
    private int len;

    @Override
    public void clear() {

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

    public FlyweightCharSequence of(CharSequence delegate, int lo, int len) {
        this.delegate = delegate;
        this.lo = lo;
        this.len = len;
        return this;
    }

    public FlyweightCharSequence of(CharSequence delegate) {
        return of(delegate, 1, delegate.length() - 2);
    }
}
