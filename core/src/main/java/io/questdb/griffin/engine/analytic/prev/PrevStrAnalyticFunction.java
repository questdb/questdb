/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package io.questdb.griffin.engine.analytic.prev;

import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.misc.Chars;
import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.impl.analytic.AnalyticFunction;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.DirectCharSequence;
import com.questdb.store.MMappedSymbolTable;
import com.questdb.store.VariableColumn;

import java.io.Closeable;
import java.io.IOException;

public class PrevStrAnalyticFunction implements AnalyticFunction, Closeable {
    private final DirectCharSequence cs = new DirectCharSequence();
    private final DirectCharSequence csB = new DirectCharSequence();
    private final VirtualColumn valueColumn;
    private boolean closed = false;
    private long bufA = 0;
    private int bufALen = -1;
    private int bufASz = 0;
    private long bufB = 0;
    private int bufBLen = -1;
    private int bufBSz = 0;
    private long buf;
    private int bufLen;

    public PrevStrAnalyticFunction(VirtualColumn valueColumn) {
        this.valueColumn = valueColumn;
        this.bufASz = 32;
        this.bufA = Unsafe.malloc(this.bufASz * 2);
        this.bufBSz = 32;
        this.bufB = Unsafe.malloc(this.bufBSz * 2);
        this.buf = bufA;
        this.bufLen = bufALen;
    }

    @Override
    public void add(Record record) {
    }

    @Override
    public byte get() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBool() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getDate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble() {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat() {
        throw new UnsupportedOperationException();
    }

    public CharSequence getStr() {
        return bufLen == -1 ? null : cs;
    }

    public CharSequence getStrB() {
        return bufLen == -1 ? null : csB.of(cs.getLo(), cs.getHi());
    }

    @Override
    public int getInt() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordColumnMetadata getMetadata() {
        return valueColumn;
    }

    @Override
    public short getShort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getStr(CharSink sink) {
        if (bufLen > -1) {
            sink.put(cs);
        }
    }

    @Override
    public int getStrLen() {
        return bufLen == -1 ? VariableColumn.NULL_LEN : bufLen;
    }

    @Override
    public String getSym() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MMappedSymbolTable getSymbolTable() {
        return null;
    }

    @Override
    public int getType() {
        return AnalyticFunction.STREAM;
    }

    @Override
    public void prepare(RecordCursor cursor) {
    }

    @Override
    public void prepareFor(Record record) {
        CharSequence cs = valueColumn.getFlyweightStr(record);

        int sz = buf == bufA ? bufASz : bufBSz;

        if (cs == null) {
            bufLen = -1;
        } else {
            int l = cs.length();
            if (l > sz) {
                long b = Unsafe.malloc(l * 2);
                Chars.putCharsOnly(b, cs);
                Unsafe.free(buf, sz * 2);

                if (buf == bufA) {
                    bufASz = l;
                    bufA = b;
                    bufALen = l;

                    buf = bufB;
                    bufLen = bufBLen;
                } else {
                    bufBSz = l;
                    bufB = b;
                    bufBLen = l;

                    buf = bufA;
                    bufLen = bufALen;
                }
            } else {
                Chars.putCharsOnly(buf, cs);
                if (buf == bufA) {
                    bufALen = l;

                    buf = bufB;
                    bufLen = bufBLen;
                } else {
                    bufBLen = l;

                    buf = bufA;
                    bufLen = bufALen;
                }
            }
        }
        this.cs.of(buf, buf + bufLen * 2);
    }

    @Override
    public void reset() {
        bufALen = -1;
        bufBLen = -1;
        buf = bufA;
    }

    @Override
    public void toTop() {
        reset();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        Unsafe.free(bufA, bufASz * 2);
        Unsafe.free(bufB, bufBSz * 2);
        closed = true;
    }
}
