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

package com.nfsdb;

import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.misc.Misc;
import com.nfsdb.storage.KVIndex;

import java.io.Closeable;
import java.io.File;

class SymbolIndexProxy<T> implements Closeable {

    private final Partition<T> partition;
    private final int columnIndex;
    private KVIndex index;
    private long txAddress;

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
            openIndex();
        }
        return index;
    }

    private void openIndex() throws JournalException {
        JournalMetadata<T> meta = partition.getJournal().getMetadata();
        ColumnMetadata columnMetadata = meta.getColumnQuick(columnIndex);

        if (!columnMetadata.indexed) {
            throw new JournalException("There is no index for column: %s", columnMetadata.name);
        }

        index = new KVIndex(
                new File(partition.getPartitionDir(), columnMetadata.name),
                columnMetadata.distinctCountHint,
                meta.getRecordHint(),
                meta.getTxCountHint(),
                partition.getJournal().getMode(),
                txAddress
        );
    }
}
