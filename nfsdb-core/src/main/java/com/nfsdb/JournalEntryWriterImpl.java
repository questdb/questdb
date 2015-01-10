/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

import com.nfsdb.column.*;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.configuration.ColumnMetaWithSymTab;
import com.nfsdb.utils.Checksum;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.BitSet;

public class JournalEntryWriterImpl implements JournalEntryWriter {
    private final JournalWriter journal;
    private final ColumnMetaWithSymTab meta[];
    private final int timestampIndex;
    private final BitSet updated = new BitSet();
    private final long[] koTuple;
    private AbstractColumn columns[];
    private SymbolIndexProxy indexProxies[];
    private Partition partition;
    private long timestamp;

    public JournalEntryWriterImpl(JournalWriter journal) {
        this.journal = journal;
        this.meta = journal.columnMetadata;
        this.timestampIndex = journal.getMetadata().getTimestampIndex();
        koTuple = new long[meta.length * 2];
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public void put(int index, byte value) {
        assertType(index, ColumnType.BYTE);
        ((FixedColumn) columns[index]).putByte(value);
        updated.set(index);
    }

    @Override
    public void putLong(int index, long value) {
        assertType(index, ColumnType.LONG);
        ((FixedColumn) columns[index]).putLong(value);
        updated.set(index);
    }

    @Override
    public void putDate(int index, long value) {
        assertType(index, ColumnType.DATE);
        ((FixedColumn) columns[index]).putLong(value);
        updated.set(index);
    }

    @Override
    public void putDouble(int index, double value) {
        assertType(index, ColumnType.DOUBLE);
        ((FixedColumn) columns[index]).putDouble(value);
        updated.set(index);
    }

    @Override
    public void putBool(int index, boolean value) {
        assertType(index, ColumnType.BOOLEAN);
        ((FixedColumn) columns[index]).putBool(value);
        updated.set(index);
    }

    @Override
    public void putStr(int index, CharSequence value) {
        assertType(index, ColumnType.STRING);
        putString0(index, value);
        updated.set(index);
    }

    @Override
    public void putSym(int index, String value) {
        assertType(index, ColumnType.SYMBOL);
        putSymbol0(index, value);
        updated.set(index);
    }

    public void putBin0(int index, InputStream value) {
        ((VariableColumn) columns[index]).putBin(value);
    }

    @Override
    public void putBin(int index, InputStream value) {
        putBin0(index, value);
        updated.set(index);
    }

    public OutputStream putBin(int index) {
        updated.set(index);
        return ((VariableColumn) columns[index]).putBin();
    }

    @Override
    public void putNull(int index) {
        putNull0(index);
        updated.set(index);
    }

    @Override
    public void putInt(int index, int value) {
        assertType(index, ColumnType.INT);
        putInt0(index, value);
        updated.set(index);
    }

    @Override
    public void putShort(int index, short value) {
        assertType(index, ColumnType.SHORT);
        ((FixedColumn) columns[index]).putShort(value);
        updated.set(index);
    }

    @Override
    public void append() throws JournalException {
        for (int i = 0; i < meta.length; i++) {
            if (!updated.get(i)) {
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

    void setPartition(Partition partition, long timestamp) {
        if (this.partition != partition) {
            this.columns = partition.columns;
            this.partition = partition;
            this.indexProxies = partition.sparseIndexProxies;
        }
        this.timestamp = timestamp;
        updated.clear();
        if (timestampIndex != -1) {
            putDate(timestampIndex, timestamp);
        }
    }

    private void assertType(int index, ColumnType t) {
        if (meta[index].type != t) {
            throw new JournalRuntimeException("Expected type: " + meta[index].type);
        }
    }

    private void putString0(int index, CharSequence value) {
        if (meta[index].indexed) {
            koTuple[index * 2] = value == null ? SymbolTable.VALUE_IS_NULL : Checksum.hash(value, meta[index].distinctCountHint);
            koTuple[index * 2 + 1] = ((VariableColumn) columns[index]).putStr(value);
        } else {
            ((VariableColumn) columns[index]).putStr(value);
        }
    }

    private void putSymbol0(int index, String value) {
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

    private void putNull0(int index) {
        switch (meta[index].type) {
            case STRING:
                putString0(index, null);
                break;
            case SYMBOL:
                putSymbol0(index, null);
                break;
            case INT:
                putInt0(index, 0);
                break;
            case BINARY:
                putBin0(index, null);
                break;
            default:
                ((FixedColumn) columns[index]).putNull();
        }
    }

    private void putInt0(int index, int value) {
        if (meta[index].indexed) {
            koTuple[index * 2] = value % meta[index].distinctCountHint;
            koTuple[index * 2 + 1] = ((FixedColumn) columns[index]).putInt(value);
        } else {
            ((FixedColumn) columns[index]).putInt(value);
        }
    }
}
