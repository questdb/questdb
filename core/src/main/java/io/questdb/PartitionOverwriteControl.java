/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb;

import io.questdb.cairo.BinarySearch;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;

import static io.questdb.cairo.TableUtils.LONGS_PER_TX_ATTACHED_PARTITION_MSB;

public class PartitionOverwriteControl {
    private static final Log LOG = LogFactory.getLog(PartitionOverwriteControl.class);
    boolean enabled;
    ConcurrentHashMap<ObjList<ReaderPartitionUsage>> readerPartitionUsageMap = new ConcurrentHashMap<>();

    public void acquirePartitions(TableReader reader) {
        if (enabled) {
            LOG.info().$("acquiring partitions [table=").$(reader.getTableToken().getTableName())
                    .$(", readerTxn=").$(reader.getTxn())
                    .I$();
            assert reader.isActive();

            LongList partitions = new LongList();
            reader.dumpPartitionInfo(partitions);

            ReaderPartitionUsage readerPartitionUsage = new ReaderPartitionUsage();
            readerPartitionUsage.owner = reader;
            readerPartitionUsage.partitionsList = partitions;
            readerPartitionUsage.ownerTxn = reader.getTxn();
            ObjList<ReaderPartitionUsage> usages = readerPartitionUsageMap.computeIfAbsent(reader.getTableToken().getDirName(), k -> new ObjList<>());
            synchronized (usages) {
                for (int i = 0, n = usages.size(); i < n; i++) {
                    ReaderPartitionUsage existing = usages.get(i);
                    if (existing.owner == reader) {
                        return;
                    }
                }
                usages.add(readerPartitionUsage);
            }
        }
    }

    public void clear() {
        if (enabled) {
            readerPartitionUsageMap.clear();
        }
    }

    public void enable() {
        this.enabled = true;
    }

    public void notifyPartitionMutates(TableToken tableToken, long partitionTimestamp, long partitionNameTxn, long mutateFromRow) {
        if (enabled) {
            ObjList<ReaderPartitionUsage> usages = readerPartitionUsageMap.get(tableToken.getDirName());
            if (usages != null) {
                synchronized (usages) {
                    for (int i = 0, n = usages.size(); i < n; i++) {
                        ReaderPartitionUsage readerPartitionUsage = usages.get(i);
                        int partitionBlockIndex = readerPartitionUsage.partitionsList.binarySearchBlock(LONGS_PER_TX_ATTACHED_PARTITION_MSB, partitionTimestamp, BinarySearch.SCAN_UP);
                        if (partitionBlockIndex >= 0) {
                            long usedPartitionNameTxn = readerPartitionUsage.partitionsList.getQuick(partitionBlockIndex + 2);
                            long visibleRows = readerPartitionUsage.partitionsList.getQuick(partitionBlockIndex + 1);

                            if (usedPartitionNameTxn == partitionNameTxn && visibleRows >= mutateFromRow) {
                                throw CairoException.critical(0).put("partition is overwritten while being in use by a reader [table=").put(tableToken.getTableName())
                                        .put(", partition=").ts(partitionTimestamp)
                                        .put(", partitionNameTxn=").put(partitionNameTxn)
                                        .put(", readerTxn=").put(readerPartitionUsage.ownerTxn)
                                        .put(", mutateFromRow=").put(mutateFromRow)
                                        .put(", visibleRows=").put(visibleRows)
                                        .put(']');
                            }
                        }
                    }
                }
            }
        }
    }

    public void releasePartitions(TableReader reader) {
        if (enabled) {
            LOG.info().$("releasing partitions [table=").$(reader.getTableToken().getTableName())
                    .$(", readerTxn=").$(reader.getTxn())
                    .I$();

            ObjList<ReaderPartitionUsage> usages = readerPartitionUsageMap.get(reader.getTableToken().getDirName());
            if (usages != null) {
                synchronized (usages) {
                    for (int i = 0, n = usages.size(); i < n; i++) {
                        ReaderPartitionUsage readerPartitionUsage = usages.get(i);
                        if (readerPartitionUsage.owner == reader) {
                            usages.remove(i);
                            return;
                        }
                    }

                }
            }

            LOG.error().$("reader not found in partition usage map [table=").$(reader.getTableToken().getTableName())
                    .$(", readerTxn=").$(reader.getTxn())
                    .I$();
        }
    }

    static class ReaderPartitionUsage {
        TableReader owner;
        long ownerTxn;
        LongList partitionsList;
    }
}