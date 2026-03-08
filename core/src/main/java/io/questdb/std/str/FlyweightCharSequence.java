/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.std.str;

import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;

public class FlyweightCharSequence extends AbstractCharSequence implements Mutable {
    public static final ObjectFactory<FlyweightCharSequence> FACTORY = FlyweightCharSequence::new;

    private CharSequence delegate;
    private int len = -1;
    private int lo;

    @Override
    public char charAt(int index) {
        return delegate.charAt(index + lo);
    }

    @Override
    public void clear() {
    }

    @Override
    public int length() {
        return len;
    }

    public FlyweightCharSequence of(CharSequence delegate) {
        return of(delegate, 0, delegate.length());
    }

    public FlyweightCharSequence of(CharSequence delegate, int lo, int len) {
        this.delegate = delegate;
        this.lo = lo;
        this.len = len;
        return this;
    }
}
