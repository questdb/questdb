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

package com.nfsdb.journal.lang.cst.impl.join;

import com.nfsdb.journal.Partition;
import com.nfsdb.journal.collections.AbstractImmutableIterator;
import com.nfsdb.journal.collections.RingQueue;
import com.nfsdb.journal.column.FixedColumn;
import com.nfsdb.journal.lang.cst.EntrySource;
import com.nfsdb.journal.lang.cst.JournalEntry;

import java.util.NoSuchElementException;

public class TimeSeriesJoin extends AbstractImmutableIterator<JournalEntry> implements EntrySource {
    private final EntrySource masterSource;
    private final EntrySource slaveSource;
    private final long depth;
    private final RingQueue<CachedJournalEntry> ringQueue;
    private final int masterTimestampIndex;
    private final int slaveTimestampIndex;
    private JournalEntry joinedData;
    private Partition lastMasterPartition;
    private Partition lastSlavePartition;
    private boolean nextSlave = false;
    private FixedColumn masterColumn;
    private FixedColumn slaveColumn;
    private long masterTimestamp;
    private JournalEntry nextData;
    private boolean useQueue;
    private boolean queueMarked = false;

    public TimeSeriesJoin(EntrySource masterSource, int masterTsIndex, EntrySource slaveSource, int slaveTsIndex, long depth, int cacheSize) {
        this.masterSource = masterSource;
        this.slaveSource = slaveSource;
        this.depth = depth;
        this.masterTimestampIndex = masterTsIndex;
        this.slaveTimestampIndex = slaveTsIndex;
        this.ringQueue = new RingQueue<>(CachedJournalEntry.class, cacheSize);
    }

    @Override
    public void reset() {

    }

    @Override
    public boolean hasNext() {

        if (nextData != null) {
            return true;
        }

        while (nextSlave || masterSource.hasNext()) {

            boolean sl = nextSlave;

            if (!nextSlave) {
                nextMaster();
                useQueue = true;
                queueMarked = false;
                ringQueue.toMark();
            }

            if (useQueue) {

                while ((useQueue = ringQueue.hasNext())) {
                    CachedJournalEntry data = ringQueue.next();

                    if (data.timestamp < masterTimestamp) {
                        continue;
                    }

                    if (!queueMarked) {
                        ringQueue.mark();
                        queueMarked = true;
                    }

                    if (data.timestamp > masterTimestamp + depth) {
                        nextSlave = false;
                        break;
                    }

                    nextSlave = true;
                    joinedData.slave = data;
                    nextData = joinedData;
                    return true;
                }
            }

            if (!useQueue) {

                while ((nextSlave = slaveSource.hasNext())) {
                    JournalEntry s = slaveSource.next();

                    if (lastSlavePartition != s.partition) {
                        lastSlavePartition = s.partition;
                        slaveColumn = (FixedColumn) s.partition.getAbstractColumn(slaveTimestampIndex);
                    }

                    long slaveTimestamp = slaveColumn.getLong(s.rowid);

                    if (slaveTimestamp < masterTimestamp) {
                        continue;
                    } else {
                        long pos = ringQueue.nextWritePos();
                        CachedJournalEntry data = ringQueue.get(pos);
                        if (data == null) {
                            data = new CachedJournalEntry();
                            ringQueue.put(pos, data);
                        }
                        data.timestamp = slaveTimestamp;
                        data.partition = s.partition;
                        data.rowid = s.rowid;
                    }

                    if (slaveTimestamp > masterTimestamp + depth) {
                        nextSlave = false;
                        break;
                    }

                    joinedData.slave = s;
                    nextData = joinedData;
                    return true;
                }
            }

            if (!sl) {
                joinedData.slave = null;
                nextData = joinedData;
                return true;
            }
        }

        nextData = null;
        return false;
    }

    @Override
    public JournalEntry next() {
        if (nextData == null) {
            throw new NoSuchElementException();
        }
        JournalEntry d = nextData;
        nextData = null;
        return d;
    }

    private void nextMaster() {
        JournalEntry m = masterSource.next();
        if (lastMasterPartition != m.partition) {
            lastMasterPartition = m.partition;
            masterColumn = (FixedColumn) m.partition.getAbstractColumn(masterTimestampIndex);
        }
        joinedData = m;
        masterTimestamp = masterColumn.getLong(m.rowid);
    }

    public static class CachedJournalEntry extends JournalEntry {
        private long timestamp;

        @Override
        public String toString() {
            return "CachedDataItem{" +
                    "timestamp=" + timestamp +
                    '}';
        }
    }
}
