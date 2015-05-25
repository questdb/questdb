/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb;

import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.storage.*;
import com.nfsdb.utils.Hash;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.InputStream;
import java.io.OutputStream;

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
            if (skipped[i]) {
                putNull(i);
            }
            columns[i].commit();

            if (meta[i].indexed) {
                indexProxies[i].getIndex().add((int) koTuple[i * 2], koTuple[i * 2 + 1]);
            }
        }
        partition.applyTx(Journal.TX_LIMIT_EVAL, null);
        journal.updateTsLo(timestamp);
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public void put(int index, byte value) {
        assertType(index, ColumnType.BYTE);
        ((FixedColumn) columns[index]).putByte(value);
        skipped[index] = false;
    }

    @Override
    public void putBin(int index, InputStream value) {
        putBin0(index, value);
        skipped[index] = false;
    }

    public OutputStream putBin(int index) {
        skipped[index] = false;
        return ((VariableColumn) columns[index]).putBin();
    }

    @Override
    public void putBool(int index, boolean value) {
        assertType(index, ColumnType.BOOLEAN);
        ((FixedColumn) columns[index]).putBool(value);
        skipped[index] = false;
    }

    @Override
    public void putDate(int index, long value) {
        assertType(index, ColumnType.DATE);
        ((FixedColumn) columns[index]).putLong(value);
        skipped[index] = false;
    }

    @Override
    public void putDouble(int index, double value) {
        assertType(index, ColumnType.DOUBLE);
        ((FixedColumn) columns[index]).putDouble(value);
        skipped[index] = false;
    }

    @Override
    public void putFloat(int index, float value) {
        assertType(index, ColumnType.FLOAT);
        ((FixedColumn) columns[index]).putFloat(value);
        skipped[index] = false;
    }

    @Override
    public void putInt(int index, int value) {
        assertType(index, ColumnType.INT);
        putInt0(index, value);
        skipped[index] = false;
    }

    @Override
    public void putLong(int index, long value) {
        assertType(index, ColumnType.LONG);
        ((FixedColumn) columns[index]).putLong(value);
        skipped[index] = false;
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
                ((FixedColumn) columns[index]).putNull();
        }
    }

    @Override
    public void putShort(int index, short value) {
        assertType(index, ColumnType.SHORT);
        ((FixedColumn) columns[index]).putShort(value);
        skipped[index] = false;
    }

    @Override
    public void putStr(int index, CharSequence value) {
        assertType(index, ColumnType.STRING);
        putString0(index, value);
        skipped[index] = false;
    }

    @Override
    public void putSym(int index, CharSequence value) {
        assertType(index, ColumnType.SYMBOL);
        putSymbol0(index, value);
        skipped[index] = false;
    }

    public void putBin0(int index, InputStream value) {
        ((VariableColumn) columns[index]).putBin(value);
    }

    private void assertType(int index, ColumnType t) {
        if (meta[index].type != t) {
            throw new JournalRuntimeException("Expected type: " + meta[index].type);
        }
    }

    private void putInt0(int index, int value) {
        if (meta[index].indexed) {
            int h = value % meta[index].distinctCountHint;
            koTuple[index * 2] = h < 0 ? -h : h;
            koTuple[index * 2 + 1] = ((FixedColumn) columns[index]).putInt(value);
        } else {
            ((FixedColumn) columns[index]).putInt(value);
        }
    }

    private void putNullStr(int index) {
        if (meta[index].indexed) {
            koTuple[index * 2] = SymbolTable.VALUE_IS_NULL;
            koTuple[index * 2 + 1] = ((VariableColumn) columns[index]).putNull();
        } else {
            ((VariableColumn) columns[index]).putNull();
        }
    }

    private void putString0(int index, CharSequence value) {
        if (meta[index].indexed) {
            koTuple[index * 2] = value == null ? SymbolTable.VALUE_IS_NULL : Hash.boundedHash(value, meta[index].distinctCountHint);
            koTuple[index * 2 + 1] = ((VariableColumn) columns[index]).putStr(value);
        } else {
            ((VariableColumn) columns[index]).putStr(value);
        }
    }

    private void putSymbol0(int index, CharSequence value) {
        int key;
        if (value == null) {
            key = SymbolTable.VALUE_IS_NULL;
        } else {
            key = meta[index].symbolTable.put(value);
        }
        if (meta[index].indexed) {
            koTuple[index * 2] = key;
            koTuple[index * 2 + 1] = ((FixedColumn) columns[index]).putInt(key);
        } else {
            ((FixedColumn) columns[index]).putInt(key);
        }
    }

    void setPartition(Partition partition, long timestamp) {
        if (this.partition != partition) {
            this.columns = partition.columns;
            this.partition = partition;
            this.indexProxies = partition.sparseIndexProxies;
        }
        this.timestamp = timestamp;

        for (int i = 0, l = skipped.length; i < l; i++) {
            skipped[i] = true;
        }

        if (timestampIndex != -1) {
            putDate(timestampIndex, timestamp);
        }
    }
}
