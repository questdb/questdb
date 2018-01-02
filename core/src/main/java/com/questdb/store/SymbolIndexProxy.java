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

import com.questdb.std.Misc;
import com.questdb.std.ex.JournalException;
import com.questdb.store.factory.configuration.ColumnMetadata;
import com.questdb.store.factory.configuration.JournalMetadata;

import java.io.Closeable;
import java.io.File;

class SymbolIndexProxy<T> implements Closeable {

    private final Partition<T> partition;
    private final int columnIndex;
    private KVIndex index;
    private long txAddress;
    private boolean sequentialAccess = false;

    SymbolIndexProxy(Partition<T> partition, int columnIndex, long txAddress) {
        this.partition = partition;
        this.columnIndex = columnIndex;
        this.txAddress = txAddress;
    }

    public void close() {
        index = Misc.free(index);
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public void setSequentialAccess(boolean sequentialAccess) {
        this.sequentialAccess = sequentialAccess;
        if (index != null) {
            index.setSequentialAccess(sequentialAccess);
        }
    }

    public void setTxAddress(long txAddress) {
        this.txAddress = txAddress;
        if (index != null) {
            index.setTxAddress(txAddress);
        }
    }

    @Override
    public String toString() {
        return "SymbolIndexProxy{" +
                "index=" + index +
                '}';
    }

    KVIndex getIndex() throws JournalException {
        if (index == null) {
            index = openIndex();
            index.setSequentialAccess(sequentialAccess);
        }
        return index;
    }

    private KVIndex openIndex() throws JournalException {
        JournalMetadata<T> meta = partition.getJournal().getMetadata();
        ColumnMetadata columnMetadata = meta.getColumnQuick(columnIndex);

        if (!columnMetadata.indexed) {
            throw new JournalException("There is no index for column: %s", columnMetadata.name);
        }

        return new KVIndex(
                new File(partition.getPartitionDir(), columnMetadata.name),
                columnMetadata.distinctCountHint,
                meta.getRecordHint(),
                meta.getTxCountHint(),
                partition.getJournal().getMode(),
                txAddress,
                sequentialAccess
        );
    }
}
