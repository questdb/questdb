/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.collections;

import com.nfsdb.utils.Unsafe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jetbrains.annotations.NotNull;

public class DirectCharSequence implements CharSequence {
    private long lo;
    private long hi;
    private int len;
    private StringBuilder builder;

    @Override
    public char charAt(int index) {
        return Unsafe.getUnsafe().getChar(lo + (index << 1));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof CharSequence) {
            CharSequence cs = (CharSequence) obj;
            int l;
            if ((l = this.length()) != cs.length()) {
                return false;
            }

            for (int i = 0; i < l; i++) {
                if (charAt(i) != cs.charAt(i)) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        if (lo == hi) {
            return 0;
        }

        int h = 0;
        for (long p = lo; p < hi; p += 2) {
            h = 31 * h + Unsafe.getUnsafe().getChar(p);
        }
        return h;
    }

    public DirectCharSequence init(long lo, long hi) {
        this.lo = lo;
        this.hi = hi;
        this.len = (int) ((hi - lo) / 2);
        return this;
    }

    @Override
    public int length() {
        return len;
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        DirectCharSequence seq = new DirectCharSequence();
        seq.lo = this.lo + start;
        seq.hi = this.lo + end;
        return seq;
    }

    @SuppressFBWarnings({"RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"})
    @NotNull
    @Override
    public String toString() {
        if (builder == null) {
            builder = new StringBuilder();
        } else {
            builder.setLength(0);
        }
        return builder.append(this).toString();
    }

}
