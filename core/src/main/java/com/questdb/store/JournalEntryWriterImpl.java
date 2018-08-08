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

package com.questdb.store;

import com.questdb.std.Hash;
import com.questdb.std.Numbers;
import com.questdb.std.Unsafe;
import com.questdb.std.ex.JournalException;
import com.questdb.std.str.DirectBytes;
import com.questdb.store.factory.configuration.ColumnMetadata;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
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

    public JournalEntryWriterImpl(JournalWriter journal) {
        this.journal = journal;
        this.meta = new ColumnMetadata[journal.getMetadata().getColumnCount()];
        journal.getMetadata().copyColumnMetadata(meta);
        this.timestampIndex = journal.getMetadata().getTimestampIndex();
        koTuple = new long[meta.length * 2];
        skipped = new boolean[meta.length];
    }

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

    public void putBin(int index, ByteBuffer buf) {
        varCol(index).putBin(buf);
        skip(index);
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
        putLong0(index, value);
        skip(index);
    }

    @Override
    public void putNull(int index) {
        switch (meta[index].type) {
            case ColumnType.STRING:
                putNullStr(index);
                break;
            case ColumnType.SYMBOL:
                putSymbol0(index, null);
                break;
            case ColumnType.INT:
                putInt0(index, Integer.MIN_VALUE);
                break;
            case ColumnType.FLOAT:
                putFloat(index, Float.NaN);
                break;
            case ColumnType.DOUBLE:
                putDouble(index, Double.NaN);
                break;
            case ColumnType.LONG:
                putLong(index, Numbers.LONG_NaN);
                break;
            case ColumnType.BINARY:
                putBin0(index, null);
                break;
            case ColumnType.DATE:
                putDate(index, Numbers.LONG_NaN);
                break;
            default:
                fixCol(index).putNull();
                break;
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
        if (meta(index).indexed) {
            Unsafe.arrayPut(koTuple, index * 2, value == null ? SymbolTable.VALUE_IS_NULL : Hash.boundedHash(value, Unsafe.arrayGet(meta, index).distinctCountHint));
            Unsafe.arrayPut(koTuple, index * 2 + 1, varCol(index).putStr(value));
        } else {
            varCol(index).putStr(value);
        }
        skip(index);
    }

    @Override
    public void putStr(int index, DirectBytes value) {
        assertType(index, ColumnType.STRING);
        if (meta(index).indexed) {
            Unsafe.arrayPut(koTuple, index * 2, value == null ? SymbolTable.VALUE_IS_NULL : Hash.boundedHash(value, Unsafe.arrayGet(meta, index).distinctCountHint));
            Unsafe.arrayPut(koTuple, index * 2 + 1, varCol(index).putStr(value));
        } else {
            varCol(index).putStr(value);
        }
        skip(index);
    }

    @Override
    public void putSym(int index, CharSequence value) {
        assertType(index, ColumnType.SYMBOL);
        putSymbol0(index, value);
        skip(index);
    }

    private void assertType(int index, int columnType) {
        if (meta[index].type != columnType) {
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
            Unsafe.arrayPut(koTuple, index * 2, h < 0 ? -h : h);
            Unsafe.arrayPut(koTuple, index * 2 + 1, fixCol(index).putInt(value));
        } else {
            fixCol(index).putInt(value);
        }
    }

    private void putLong0(int index, long value) {
        if (meta(index).indexed) {
            int h = (int) (value & meta(index).distinctCountHint);
            Unsafe.arrayPut(koTuple, index * 2, h < 0 ? -h : h);
            Unsafe.arrayPut(koTuple, index * 2 + 1, fixCol(index).putLong(value));
        } else {
            fixCol(index).putLong(value);
        }
    }

    private void putNullStr(int index) {
        if (meta(index).indexed) {
            Unsafe.arrayPut(koTuple, index * 2, SymbolTable.VALUE_IS_NULL);
            Unsafe.arrayPut(koTuple, index * 2 + 1, varCol(index).putNull());
        } else {
            varCol(index).putNull();
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
