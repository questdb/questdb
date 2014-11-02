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

package com.nfsdb.journal;

import com.nfsdb.journal.concurrent.TimerCache;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.factory.configuration.JournalMetadata;
import com.nfsdb.journal.index.KVIndex;
import com.nfsdb.journal.logging.Logger;
import com.nfsdb.journal.utils.Dates;

import java.io.Closeable;

class SymbolIndexProxy<T> implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(SymbolIndexProxy.class);
    private final Partition<T> partition;
    private final int columnIndex;
    private final TimerCache timerCache;
    private KVIndex index;
    private long lastAccessed;
    private long txAddress;

    SymbolIndexProxy(Partition<T> partition, int columnIndex, long txAddress) {
        this.partition = partition;
        this.columnIndex = columnIndex;
        this.txAddress = txAddress;
        this.timerCache = partition.getJournal().getTimerCache();
    }

    public void close() {
        if (index != null) {
            LOGGER.trace("Closing " + this);
            index.close();
            index = null;
        }
        lastAccessed = 0L;
    }

    public long getLastAccessed() {
        return lastAccessed;
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
                ", lastAccessed=" + Dates.toString(lastAccessed) +
                '}';
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    KVIndex getIndex() throws JournalException {
        lastAccessed = timerCache.getCachedMillis();
        if (index == null) {
            JournalMetadata<T> meta = partition.getJournal().getMetadata();
            index = new KVIndex(
                    meta.getColumnIndexBase(partition.getPartitionDir(), columnIndex),
                    meta.getColumnMetadata(columnIndex).distinctCountHint,
                    meta.getRecordHint(),
                    meta.getTxCountHint(),
                    partition.getJournal().getMode(),
                    txAddress
            );
        }
        return index;
    }
}
