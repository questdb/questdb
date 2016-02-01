/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

package com.nfsdb;

import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.misc.Hash;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.store.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

public class JournalEntryWriterImpl implements JournalEntryWriter {
    private final JournalWriter journal;
    private final ColumnMetadata meta[];
    private final int timestampIndex;
    private final boolean[] skipped;
    private final long[] koTuple;
    private AbstractColumn columns[];
    private SymbolIndexProxy indexProxies[];
    private Partition partition;
    private long timestamp;

    @SuppressFBWarnings({"CD_CIRCULAR_DEPENDENCY"})
    public JournalEntryWriterImpl(JournalWriter journal) {
        this.journal = journal;
        this.meta = new ColumnMetadata[journal.getMetadata().getColumnCount()];
        journal.getMetadata().copyColumnMetadata(meta);
        this.timestampIndex = journal.getMetadata().getTimestampIndex();
        koTuple = new long[meta.length * 2];
        skipped = new boolean[meta.length];
    }

    @SuppressFBWarnings({"PL_PARALLEL_LISTS"})
    @Override
    public void append() throws JournalException {
        for (int i = 0, l = meta.length; i < l; i++) {
            if (Unsafe.arrayGet(skipped, i)) {
                putNull(i);
            }
            Unsafe.arrayGet(columns, i).commit();

            if (meta(i).indexed) {
                Unsafe.arrayGet(indexProxies, i).getIndex().add((int) Unsafe.arrayGet(koTuple, i * 2), Unsafe.arrayGet(koTuple, i * 2 + 1));
            }
        }
        partition.applyTx(Journal.TX_LIMIT_EVAL, null);
        journal.updateTsLo(timestamp);
    }

    @Override
    public void put(int index, byte value) {
        assertType(index, ColumnType.BYTE);
        fixCol(index).putByte(value);
        skip(index);
    }

    @Override
    public void putBin(int index, InputStream value) {
        putBin0(index, value);
        skip(index);
    }

    public OutputStream putBin(int index) {
        skip(index);
        return varCol(index).putBin();
    }

    @Override
    public void putBool(int index, boolean value) {
        assertType(index, ColumnType.BOOLEAN);
        fixCol(index).putBool(value);
        skip(index);
    }

    @Override
    public void putDate(int index, long value) {
        assertType(index, ColumnType.DATE);
        fixCol(index).putLong(value);
        skip(index);
    }

    @Override
    public void putDouble(int index, double value) {
        assertType(index, ColumnType.DOUBLE);
        fixCol(index).putDouble(value);
        skip(index);
    }

    @Override
    public void putFloat(int index, float value) {
        assertType(index, ColumnType.FLOAT);
        fixCol(index).putFloat(value);
        skip(index);
    }

    @Override
    public void putInt(int index, int value) {
        assertType(index, ColumnType.INT);
        putInt0(index, value);
        skip(index);
    }

    @Override
    public void putLong(int index, long value) {
        assertType(index, ColumnType.LONG);
        fixCol(index).putLong(value);
        skip(index);
    }

    @Override
    public void putNull(int index) {
        switch (meta[index].type) {
            case STRING:
                putNullStr(index);
                break;
            case SYMBOL:
                putSymbol0(index, null);
                break;
            case INT:
                putInt0(index, Integer.MIN_VALUE);
                break;
            case FLOAT:
                putFloat(index, Float.NaN);
                break;
            case DOUBLE:
                putDouble(index, Double.NaN);
                break;
            case LONG:
                putLong(index, Long.MIN_VALUE);
                break;
            case BINARY:
                putBin0(index, null);
                break;
            default:
                fixCol(index).putNull();
        }
    }

    @Override
    public void putShort(int index, short value) {
        assertType(index, ColumnType.SHORT);
        fixCol(index).putShort(value);
        skip(index);
    }

    @Override
    public void putStr(int index, CharSequence value) {
        assertType(index, ColumnType.STRING);
        putString0(index, value);
        skip(index);
    }

    @Override
    public void putSym(int index, CharSequence value) {
        assertType(index, ColumnType.SYMBOL);
        putSymbol0(index, value);
        skip(index);
    }

    private void assertType(int index, ColumnType t) {
        if (meta[index].type != t) {
            throw new JournalRuntimeException("Expected type: " + meta[index].type);
        }
    }

    private FixedColumn fixCol(int index) {
        return (FixedColumn) Unsafe.arrayGet(columns, index);
    }

    private ColumnMetadata meta(int index) {
        return Unsafe.arrayGet(meta, index);
    }

    private void putBin0(int index, InputStream value) {
        varCol(index).putBin(value);
    }

    private void putInt0(int index, int value) {
        if (meta(index).indexed) {
            int h = value & meta(index).distinctCountHint;
            Unsafe.arrayPut(koTuple, index * 2L, h < 0 ? -h : h);
            Unsafe.arrayPut(koTuple, index * 2L + 1L, fixCol(index).putInt(value));
        } else {
            fixCol(index).putInt(value);
        }
    }

    private void putNullStr(int index) {
        if (meta(index).indexed) {
            Unsafe.arrayPut(koTuple, index * 2L, SymbolTable.VALUE_IS_NULL);
            Unsafe.arrayPut(koTuple, index * 2L + 1L, varCol(index).putNull());
        } else {
            varCol(index).putNull();
        }
    }

    private void putString0(int index, CharSequence value) {
        if (meta(index).indexed) {
            Unsafe.arrayPut(koTuple, index * 2L, value == null ? SymbolTable.VALUE_IS_NULL : Hash.boundedHash(value, Unsafe.arrayGet(meta, index).distinctCountHint));
            Unsafe.arrayPut(koTuple, index * 2L + 1L, varCol(index).putStr(value));
        } else {
            varCol(index).putStr(value);
        }
    }

    private void putSymbol0(int index, CharSequence value) {
        int key;
        if (value == null) {
            key = SymbolTable.VALUE_IS_NULL;
        } else {
            key = meta(index).symbolTable.put(value);
        }
        if (meta(index).indexed) {
            koTuple[index * 2] = key;
            koTuple[index * 2 + 1] = fixCol(index).putInt(key);
        } else {
            fixCol(index).putInt(key);
        }
    }

    void setPartition(Partition partition, long timestamp) {
        if (this.partition != partition) {
            this.columns = partition.columns;
            this.partition = partition;
            this.indexProxies = partition.sparseIndexProxies;
        }
        this.timestamp = timestamp;

        Arrays.fill(skipped, true);

        if (timestampIndex != -1) {
            putDate(timestampIndex, timestamp);
        }
    }

    private void skip(int index) {
        Unsafe.arrayPut(skipped, index, false);
    }

    private VariableColumn varCol(int index) {
        return (VariableColumn) Unsafe.arrayGet(columns, index);
    }
}
