/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.ql.join.asof;

import com.questdb.std.Unsafe;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.DirectCharSequence;
import com.questdb.store.ColumnType;
import com.questdb.store.RecordMetadata;

abstract class AbstractVarMemRecord extends AbstractMemRecord {

    private final DirectCharSequence csA[];
    private final DirectCharSequence csB[];

    public AbstractVarMemRecord(RecordMetadata metadata) {
        int n = metadata.getColumnCount();
        DirectCharSequence csA[] = null;
        DirectCharSequence csB[] = null;


        for (int i = 0; i < n; i++) {
            if (metadata.getColumnQuick(i).getType() == ColumnType.STRING) {
                if (csA == null) {
                    csA = new DirectCharSequence[n];
                    csB = new DirectCharSequence[n];
                }

                csA[i] = new DirectCharSequence();
                csB[i] = new DirectCharSequence();
            }
        }

        this.csA = csA;
        this.csB = csB;
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        long address = address() + getInt(col);
        int len = Unsafe.getUnsafe().getInt(address);
        if (len == -1) {
            return null;
        }
        return Unsafe.arrayGet(csA, col).of(address + 4, address + 4 + len * 2);
    }

    @Override
    public CharSequence getFlyweightStrB(int col) {
        long address = address() + getInt(col);
        int len = Unsafe.getUnsafe().getInt(address);
        if (len == -1) {
            return null;
        }
        return Unsafe.arrayGet(csB, col).of(address + 4, address + 4 + len * 2);
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
