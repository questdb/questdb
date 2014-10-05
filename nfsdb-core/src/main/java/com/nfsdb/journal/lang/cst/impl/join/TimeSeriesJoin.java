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

package com.nfsdb.journal.lang.cst.impl.join;

import com.nfsdb.journal.Partition;
import com.nfsdb.journal.collections.AbstractImmutableIterator;
import com.nfsdb.journal.collections.RingQueue;
import com.nfsdb.journal.column.FixedColumn;
import com.nfsdb.journal.lang.cst.DataItem;
import com.nfsdb.journal.lang.cst.JoinedSource;
import com.nfsdb.journal.lang.cst.JournalSource;

import java.util.NoSuchElementException;

public class TimeSeriesJoin extends AbstractImmutableIterator<DataItem> implements JoinedSource {
    private final JournalSource masterSource;
    private final JournalSource slaveSource;
    private final long depth;
    private final RingQueue<CachedDataItem> ringQueue;
    private final int masterTimestampIndex;
    private final int slaveTimestampIndex;
    private DataItem joinedData;
    private Partition lastMasterPartition;
    private Partition lastSlavePartition;
    private boolean nextSlave = false;
    private FixedColumn masterColumn;
    private FixedColumn slaveColumn;
    private long masterTimestamp;
    private DataItem nextData;
    private boolean useQueue;
    private boolean queueMarked = false;

    public TimeSeriesJoin(JournalSource masterSource, JournalSource slaveSource, long depth, int cachSize) {
        this.masterSource = masterSource;
        this.slaveSource = slaveSource;
        this.depth = depth;
        this.masterTimestampIndex = masterSource.getJournal().getMetadata().getTimestampColumnIndex();
        this.slaveTimestampIndex = slaveSource.getJournal().getMetadata().getTimestampColumnIndex();
        this.ringQueue = new RingQueue<>(CachedDataItem.class, cachSize);
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
                    CachedDataItem data = ringQueue.next();

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
                    DataItem s = slaveSource.next();

                    if (lastSlavePartition != s.partition) {
                        lastSlavePartition = s.partition;
                        slaveColumn = (FixedColumn) s.partition.getAbstractColumn(slaveTimestampIndex);
                    }

                    long slaveTimestamp = slaveColumn.getLong(s.rowid);

                    if (slaveTimestamp < masterTimestamp) {
                        continue;
                    } else {
                        long pos = ringQueue.nextWritePos();
                        CachedDataItem data = ringQueue.get(pos);
                        if (data == null) {
                            data = new CachedDataItem();
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
    public DataItem next() {
        if (nextData == null) {
            throw new NoSuchElementException();
        }
        DataItem d = nextData;
        nextData = null;
        return d;
    }

    private void nextMaster() {
        DataItem m = masterSource.next();
        if (lastMasterPartition != m.partition) {
            lastMasterPartition = m.partition;
            masterColumn = (FixedColumn) m.partition.getAbstractColumn(masterTimestampIndex);
        }
        joinedData = m;
        masterTimestamp = masterColumn.getLong(m.rowid);
    }

    public static class CachedDataItem extends DataItem {
        private long timestamp;

        @Override
        public String toString() {
            return "CachedDataItem{" +
                    "timestamp=" + timestamp +
                    '}';
        }
    }
}
