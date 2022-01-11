/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cairo;

import io.questdb.MessageBus;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.MutableCharSink;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.tasks.O3PurgeDiscoveryTask;

import java.io.Closeable;
import java.io.IOException;

public class O3PurgeDiscoveryJob extends AbstractQueueConsumerJob<O3PurgeDiscoveryTask> implements Closeable {

    private final static Log LOG = LogFactory.getLog(O3PurgeDiscoveryJob.class);
    private final CairoConfiguration configuration;
    private final MutableCharSink[] sink;
    private final StringSink[] fileNameSinks;
    private final ObjList<DirectLongList> partitionList;
    private final ObjList<TxnScoreboard> txnScoreboards;
    private final ObjList<TxReader> txnReaders;

    public O3PurgeDiscoveryJob(MessageBus messageBus, int workerCount) {
        super(messageBus.getO3PurgeDiscoveryQueue(), messageBus.getO3PurgeDiscoverySubSeq());
        this.configuration = messageBus.getConfiguration();
        this.sink = new MutableCharSink[workerCount];
        this.fileNameSinks = new StringSink[workerCount];
        this.partitionList = new ObjList<>(workerCount);
        this.txnScoreboards = new ObjList<>(workerCount);
        this.txnReaders = new ObjList<>(workerCount);

        for (int i = 0; i < workerCount; i++) {
            sink[i] = new StringSink();
            fileNameSinks[i] = new StringSink();
            partitionList.add(new DirectLongList(configuration.getPartitionPurgeListCapacity() * 2, MemoryTag.NATIVE_LONG_LIST));
            txnScoreboards.add(new TxnScoreboard(configuration.getFilesFacade(), configuration.getTxnScoreboardEntryCount()));
            txnReaders.add(new TxReader(configuration.getFilesFacade()));
        }
    }

    @Override
    public void close() throws IOException {
        Misc.freeObjList(partitionList);
        Misc.freeObjList(txnReaders);
        Misc.freeObjList(txnScoreboards);
    }

    private static void processPartition(
            FilesFacade ff,
            Path path,
            int tableRootLen,
            TxReader txReader,
            TxnScoreboard txnScoreboard,
            long partitionTimestamp,
            int partitionBy,
            DirectLongList partitionList,
            int lo,
            int hi
    ) {
        long lastCommittedPartitionName = txReader.getPartitionNameTxnByPartitionTimestamp(partitionTimestamp);
        if (lastCommittedPartitionName > -1) {
            assert hi <= partitionList.size();
            for (int i = lo + 2, n = hi - 1; i < n; i += 2) {
                long nextNameVersion = Math.min(lastCommittedPartitionName + 1, partitionList.get(i));
                long previousNameVersion = partitionList.get(i - 2);

                boolean rangeUnlocked = previousNameVersion < nextNameVersion;
                for (long txn = previousNameVersion; txn < nextNameVersion; txn++) {
                    if (txnScoreboard.getActiveReaderCount(txn) > 0) {
                        rangeUnlocked = false;
                        break;
                    }
                }

                if (rangeUnlocked) {
                    path.trimTo(tableRootLen);
                    TableUtils.setPathForPartition(path, partitionBy, partitionTimestamp, false);
                    // -1 here is to compensate +1 added when partition version parsed from folder name
                    // See comments of why +1 added there in parsePartitionDateVersion()
                    TableUtils.txnPartitionConditionally(path, previousNameVersion - 1);
                    path.slash$();

                    // previousNameVersion can be deleted
                    LOG.info().
                            $("purging [path=").$(path)
                            .$(", nameTxnToRemove=").$(previousNameVersion - 1)
                            .$(", nameTxnNext=").$(nextNameVersion)
                            .$(", lastCommittedPartitionName=").$(lastCommittedPartitionName)
                            .I$();
                    long errno;
                    if ((errno = ff.rmdir(path)) == 0) {
                        LOG.info().
                                $("purged [path=").$(path)
                                .I$();
                    } else {
                        LOG.info().
                                $("partition purge failed [path=").$(path)
                                .$(", errno=").$(errno)
                                .I$();
                    }
                }
            }
        }
    }

    private void discoverPartitions(
            FilesFacade ff,
            MutableCharSink sink,
            StringSink fileNameSink,
            DirectLongList partitionList,
            CharSequence root,
            CharSequence tableName,
            TxnScoreboard txnScoreboard,
            TxReader txReader,
            int partitionBy) {

        LOG.info().$("processing [table=").$(tableName).I$();
        Path path = Path.getThreadLocal(root);
        path.concat(tableName).slash$();
        sink.clear();
        path.slash$();

        partitionList.clear();
        DateFormat partitionByFormat = PartitionBy.getPartitionDirFormatMethod(partitionBy);

        long p = ff.findFirst(path);
        if (p > 0) {
            try {
                do {
                    long fileName = ff.findName(p);
                    if (Files.isDir(fileName, ff.findType(p), fileNameSink)) {
                        // extract txn, partition ts from name
                        parsePartitionDateVersion(fileNameSink, partitionList, tableName, partitionByFormat);
                    }
                } while (ff.findNext(p) > 0);
            } finally {
                ff.findClose(p);
            }
        }

        // find duplicate partitions
        assert partitionList.size() % 2 == 0;
        Vect.sort128BitAscInPlace(partitionList.getAddress(), partitionList.size() / 2);

        long partitionTs = Numbers.LONG_NaN;
        int lo = 0;
        int n = (int) partitionList.size();

        path.of(root).concat(tableName);
        int tableRootLen = path.length();
        try {
            txnScoreboard.ofRO(path);
            path.trimTo(tableRootLen);
            txReader.ofRO(path, partitionBy);
            loadLastTx(txReader);

            for (int i = 0; i < n; i += 2) {
                if (partitionList.get(i + 1) != partitionTs) {
                    if (i > lo + 2) {
                        processPartition(
                                ff,
                                path,
                                tableRootLen,
                                txReader,
                                txnScoreboard,
                                partitionTs,
                                partitionBy,
                                partitionList,
                                lo,
                                i
                        );
                    }
                    lo = i;
                    partitionTs = partitionList.get(i + 1);
                }
            }
            // Tail
            if (n > lo + 2) {
                processPartition(
                        ff,
                        path,
                        tableRootLen,
                        txReader,
                        txnScoreboard,
                        partitionTs,
                        partitionBy,
                        partitionList,
                        lo,
                        n
                );
            }
        } catch (CairoException ex) {
            // It is possible that table is dropped while this async job was in the queue.
            // so it can be not too bad. Log error and continue work on the queue
            LOG.error().$(ex.getFlyweightMessage()).$();
        } finally {
            txReader.clear();
            txnScoreboard.clear();
        }
    }

    private void parsePartitionDateVersion(StringSink fileNameSink, DirectLongList partitionList, CharSequence tableName, DateFormat partitionByFormat) {
        int index = Chars.lastIndexOf(fileNameSink, '.');

        if (index < 0) {
            index = fileNameSink.length();
        }
        try {
            if (index < fileNameSink.length()) {
                long partitionVersion = Numbers.parseLong(fileNameSink, index + 1, fileNameSink.length());
                // When reader locks transaction 100 it opens partition version .99 or lower.
                // Also when there is no transaction version in the name, it is counted as -1.
                // By adding +1 here we kill 2 birds in with one stone, partition versions are aligned with
                // txn scoreboard reader locks and no need to add -1 which allows us to use 128bit
                // sort to sort 2 x 64bit unsigned integers
                partitionList.add(partitionVersion + 1);
            } else {
                // This should be -1 but it is only possible to correctly sort 2 unsigned longs
                // as 128bit integer sort
                // Set 0 instead of -1 and revert it later on. There should be not possible to have .0 in the partition name
                partitionList.add(0);
            }

            try {
                long partitionTs = partitionByFormat.parse(fileNameSink, 0, index, null);
                partitionList.add(partitionTs);
            } catch (NumericException e) {
                LOG.error().$("unknown directory [table=").utf8(tableName).$(", dir=").utf8(fileNameSink).$(']').$();
                partitionList.setPos(partitionList.size() - 1); // remove partition version record
            }
        } catch (NumericException e) {
            LOG.error().$("unknown directory [table=").utf8(tableName).$(", dir=").utf8(fileNameSink).$(']').$();
        }
    }

    @Override
    protected boolean doRun(int workerId, long cursor) {
        final O3PurgeDiscoveryTask task = queue.get(cursor);
        discoverPartitions(
                configuration.getFilesFacade(),
                sink[workerId],
                fileNameSinks[workerId],
                partitionList.get(workerId),
                configuration.getRoot(),
                task.getTableName(),
                txnScoreboards.get(workerId),
                txnReaders.get(workerId),
                task.getPartitionBy()
        );
        subSeq.done(cursor);
        return true;
    }

    private void loadLastTx(TxReader txReader) {
        long txn;
        do {
            txn = txReader.unsafeReadTxnCheck();

            Unsafe.getUnsafe().loadFence();
            txReader.unsafeLoadAll();

            Unsafe.getUnsafe().loadFence();
        } while (txn != txReader.unsafeReadTxn());
    }
}
