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

package com.nfsdb.ql.impl.join.asof;

import com.nfsdb.collections.DirectCharSequence;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.misc.Unsafe;

public abstract class AbstractVarMemRecord extends AbstractMemRecord {

    private final DirectCharSequence cs = new DirectCharSequence();
    private char[] strBuf;

    public AbstractVarMemRecord(RecordMetadata metadata) {
        super(metadata);
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        long address = address() + getInt(col);
        return cs.of(address + 4, address + 4 + Unsafe.getUnsafe().getInt(address) * 2);
    }

    @Override
    public CharSequence getStr(int col) {
        long address = address() + getInt(col);
        int len = Unsafe.getUnsafe().getInt(address);

        if (strBuf == null || strBuf.length < len) {
            strBuf = new char[len];
        }

        long lim = address + 4 + len * 2;
        int i = 0;
        for (long p = address + 4; p < lim; p += 2) {
            strBuf[i++] = Unsafe.getUnsafe().getChar(p);
        }

        return new String(strBuf, 0, len);
    }

    @Override
    public void getStr(int col, CharSink sink) {
        long address = address() + getInt(col);
        for (long p = address + 4, n = address + 4 + Unsafe.getUnsafe().getInt(address) * 2; p < n; p += 2) {
            sink.put(Unsafe.getUnsafe().getChar(p));
        }
    }

    @Override
    public int getStrLen(int col) {
        return Unsafe.getUnsafe().getInt(address() + getInt(col));
    }

    protected abstract long address();
}
