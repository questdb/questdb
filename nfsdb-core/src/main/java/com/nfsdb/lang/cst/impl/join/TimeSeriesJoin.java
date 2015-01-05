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

package com.nfsdb.lang.cst.impl.join;

import com.nfsdb.Partition;
import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.collections.RingQueue;
import com.nfsdb.column.FixedColumn;
import com.nfsdb.lang.cst.impl.qry.JoinedRecordMetadata;
import com.nfsdb.lang.cst.impl.qry.JournalRecord;
import com.nfsdb.lang.cst.impl.qry.RecordMetadata;
import com.nfsdb.lang.cst.impl.qry.RecordSource;

import java.util.NoSuchElementException;

public class TimeSeriesJoin extends AbstractImmutableIterator<JournalRecord> implements RecordSource<JournalRecord> {
    private final RecordSource<JournalRecord> masterSource;
    private final RecordSource<JournalRecord> slaveSource;
    private final long depth;
    private final RingQueue<CachedJournalRecord> ringQueue;
    private final int masterTimestampIndex;
    private final int slaveTimestampIndex;
    private final RecordMetadata metadata;
    private JournalRecord joinedData;
    private Partition lastMasterPartition;
    private Partition lastSlavePartition;
    private boolean nextSlave = false;
    private FixedColumn masterColumn;
    private FixedColumn slaveColumn;
    private long masterTimestamp;
    private JournalRecord nextData;
    private boolean useQueue;
    private boolean queueMarked = false;

    public TimeSeriesJoin(RecordSource<JournalRecord> masterSource, int masterTsIndex, RecordSource<JournalRecord> slaveSource, int slaveTsIndex, long depth, int cacheSize) {
        this.masterSource = masterSource;
        this.slaveSource = slaveSource;
        this.depth = depth;
        this.masterTimestampIndex = masterTsIndex;
        this.slaveTimestampIndex = slaveTsIndex;
        this.ringQueue = new RingQueue<>(CachedJournalRecord.class, cacheSize);
        this.metadata = new JoinedRecordMetadata(masterSource, slaveSource);
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
                    CachedJournalRecord data = ringQueue.next();

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
                    joinedData.setSlave(data);
                    nextData = joinedData;
                    return true;
                }
            }

            if (!useQueue) {

                while ((nextSlave = slaveSource.hasNext())) {
                    JournalRecord s = slaveSource.next();

                    if (lastSlavePartition != s.partition) {
                        lastSlavePartition = s.partition;
                        slaveColumn = (FixedColumn) s.partition.getAbstractColumn(slaveTimestampIndex);
                    }

                    long slaveTimestamp = slaveColumn.getLong(s.rowid);

                    if (slaveTimestamp < masterTimestamp) {
                        continue;
                    } else {
                        long pos = ringQueue.nextWritePos();
                        CachedJournalRecord data = ringQueue.get(pos);
                        if (data == null) {
                            data = new CachedJournalRecord(metadata);
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

                    joinedData.setSlave(s);
                    nextData = joinedData;
                    return true;
                }
            }

            if (!sl) {
                joinedData.setSlave(null);
                nextData = joinedData;
                return true;
            }
        }

        nextData = null;
        return false;
    }

    @Override
    public JournalRecord next() {
        if (nextData == null) {
            throw new NoSuchElementException();
        }
        JournalRecord d = nextData;
        nextData = null;
        return d;
    }

    private void nextMaster() {
        JournalRecord m = masterSource.next();
        if (lastMasterPartition != m.partition) {
            lastMasterPartition = m.partition;
            masterColumn = (FixedColumn) m.partition.getAbstractColumn(masterTimestampIndex);
        }
        joinedData = m;
        masterTimestamp = masterColumn.getLong(m.rowid);
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    public static class CachedJournalRecord extends JournalRecord {
        private long timestamp;

        public CachedJournalRecord(RecordMetadata metadata) {
            super(metadata);
        }

        @Override
        public String toString() {
            return "CachedDataItem{" +
                    "timestamp=" + timestamp +
                    '}';
        }
    }
}
