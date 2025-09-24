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

package io.questdb.cairo;

import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.std.DirectLongList;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Vect;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.O3PartitionPurgeTask;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;
import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;

public class O3PartitionPurgeJob extends AbstractQueueConsumerJob<O3PartitionPurgeTask> implements Closeable {

    private final static Log LOG = LogFactory.getLog(O3PartitionPurgeJob.class);
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final Utf8StringSink[] fileNameSinks;
    private final AtomicBoolean halted = new AtomicBoolean(false);
    private final ObjList<DirectLongList> partitionList;
    private final ObjList<TxReader> txnReaders;

    public O3PartitionPurgeJob(CairoEngine engine, int workerCount) {
        super(engine.getMessageBus().getO3PurgeDiscoveryQueue(), engine.getMessageBus().getO3PurgeDiscoverySubSeq());
        try {
            this.engine = engine;
            this.configuration = engine.getMessageBus().getConfiguration();
            this.fileNameSinks = new Utf8StringSink[workerCount];
            this.partitionList = new ObjList<>(workerCount);
            this.txnReaders = new ObjList<>(workerCount);

            for (int i = 0; i < workerCount; i++) {
                fileNameSinks[i] = new Utf8StringSink();
                partitionList.add(new DirectLongList(configuration.getPartitionPurgeListCapacity() * 2L, MemoryTag.NATIVE_O3));
                txnReaders.add(new TxReader(configuration.getFilesFacade()));
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void close() {
        if (halted.compareAndSet(false, true)) {
            Misc.freeObjList(partitionList);
            Misc.freeObjList(txnReaders);
        }
    }

    private static void parsePartitionDateVersion(
            Utf8StringSink fileNameSink,
            DirectLongList partitionList,
            TableToken tableToken,
            DateFormat partitionByFormat
    ) {
        int index = Utf8s.lastIndexOfAscii(fileNameSink, '.');

        int len = fileNameSink.size();
        if (index < 0) {
            index = len;
        }
        try {
            if (index < len) {
                long partitionVersion = Numbers.parseLong(fileNameSink, index + 1, len);
                // When reader locks transaction 100 it opens a partition version .99 or lower.
                // Also, when there is no transaction version in the name, it is counted as -1.
                // By adding +1 here we kill 2 birds in with one stone, partition versions are aligned with
                // txn scoreboard reader locks and no need to add -1 that allows us to use 128bit
                // sort to sort 2 x 64bit unsigned integers
                partitionList.add(partitionVersion + 1);
            } else {
                // This should be -1, but it is only possible to correctly sort 2 unsigned longs
                // as 128bit integer sort
                // Set 0 instead of -1 and revert it later on. There should be not possible to have .0 in the partition name
                partitionList.add(0);
            }

            try {
                long partitionTs = partitionByFormat.parse(fileNameSink.asAsciiCharSequence(), 0, index, EN_LOCALE);
                partitionList.add(partitionTs);
            } catch (NumericException e) {
                if (!Utf8s.startsWithAscii(fileNameSink, WalUtils.WAL_NAME_BASE) && !Utf8s.equalsAscii(WalUtils.SEQ_DIR, fileNameSink)
                        && !Utf8s.equalsAscii("seq", fileNameSink)) {
                    LOG.info().$("unknown directory [table=").$(tableToken).$(", dir=").$(fileNameSink).I$();
                }
                partitionList.setPos(partitionList.size() - 1); // remove partition version record
            }
        } catch (NumericException e) {
            LOG.error().$("unknown directory [table=").$(tableToken).$(", dir=").$(fileNameSink).I$();
        }
    }

    private void discoverPartitions(
            FilesFacade ff,
            Utf8StringSink fileNameSink,
            DirectLongList partitionList,
            CharSequence root,
            TableToken tableToken,
            TxReader txReader,
            int timestampType,
            int partitionBy
    ) {
        LOG.info().$("processing [table=").$(tableToken).I$();
        Path path = Path.getThreadLocal(root).concat(tableToken);
        int plimit = path.size();
        partitionList.clear();
        DateFormat partitionByFormat = PartitionBy.getPartitionDirFormatMethod(timestampType, partitionBy);
        long p = ff.findFirst(path.$());
        if (p > 0) {
            try {
                do {
                    if (ff.isDirOrSoftLinkDirNoDots(path, plimit, ff.findName(p), ff.findType(p), fileNameSink)) {
                        parsePartitionDateVersion(fileNameSink, partitionList, tableToken, partitionByFormat);
                        path.trimTo(plimit).$();
                    }
                } while (ff.findNext(p) > 0);
            } finally {
                ff.findClose(p);
            }
        }

        // find duplicate partitions
        assert partitionList.size() % 2 == 0;
        Vect.sort128BitAscInPlace(partitionList.getAddress(), partitionList.size() / 2);

        long partitionTimestamp = Numbers.LONG_NULL;
        int lo = 0;
        int n = (int) partitionList.size();

        path.of(root).concat(tableToken);

        int tableRootLen = path.size();
        TxnScoreboard txnScoreboard = null;
        try {
            txnScoreboard = engine.getTxnScoreboard(tableToken);
            txReader.ofRO(path.trimTo(tableRootLen).concat(TXN_FILE_NAME).$(), timestampType, partitionBy);
            TableUtils.safeReadTxn(txReader, configuration.getMillisecondClock(), configuration.getSpinLockTimeout());

            for (int i = 0; i < n; i += 2) {
                long currentPartitionTs = partitionList.get(i + 1);
                if (currentPartitionTs != partitionTimestamp) {
                    if (i > lo + 2 ||
                            (i > 0 && txReader.findAttachedPartitionRawIndexByLoTimestamp(partitionTimestamp) < 0)) {
                        processPartition(
                                tableToken,
                                ff,
                                path,
                                tableRootLen,
                                txReader,
                                txnScoreboard,
                                partitionTimestamp,
                                timestampType,
                                partitionBy,
                                partitionList,
                                lo,
                                i
                        );
                    }
                    lo = i;
                    partitionTimestamp = currentPartitionTs;
                }
            }
            // Tail
            if (n > lo + 2 || txReader.getPartitionRowCountByTimestamp(partitionTimestamp) < 0) {
                processPartition(
                        tableToken,
                        ff,
                        path,
                        tableRootLen,
                        txReader,
                        txnScoreboard,
                        partitionTimestamp,
                        timestampType,
                        partitionBy,
                        partitionList,
                        lo,
                        n
                );
            }
        } catch (TableReferenceOutOfDateException e) {
            // the table is dropped and recreated since we started processing it.
            // abort the table processing
            LOG.info().$("table reference out of date, aborting [table=").$(tableToken).I$();
        } catch (CairoException ex) {
            // It is possible that the table is dropped while this async job was in the queue.
            // so it can be not too bad. Log error and continue work on the queue
            LOG.error()
                    .$("could not purge partition open [table=").$(tableToken)
                    .$(", msg=").$safe(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno())
                    .I$();
            LOG.error().$safe(ex.getFlyweightMessage()).$();
        } finally {
            txReader.clear();
            Misc.free(txnScoreboard);
        }
        LOG.info().$("processed [table=").$(tableToken).I$();
    }

    private void processDetachedPartition(
            TableToken tableToken,
            FilesFacade ff,
            Path path,
            int tableRootLen,
            TxReader txReader,
            TxnScoreboard txnScoreboard,
            long partitionTimestamp,
            int timestampType,
            int partitionBy,
            DirectLongList partitionList,
            int lo,
            int hi
    ) {
        // Partition is dropped or not fully committed.
        // It is only possible to delete when there are no readers
        long lastTxn = txReader.getTxn();
        for (int i = hi - 2, n = lo - 1; i > n; i -= 2) {
            long nameTxn = partitionList.get(i);

            // If the last committed transaction number is 4, TableWriter can write partition with ending .4 and .3
            // If the version on disk is .2 (nameTxn == 3) can remove it if the lastTxn > 3, e.g., when nameTxn < lastTxn
            boolean rangeUnlocked = nameTxn < lastTxn && txnScoreboard.isRangeAvailable(nameTxn, lastTxn);

            path.trimTo(tableRootLen);
            TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, nameTxn - 1);
            path.$();

            if (rangeUnlocked) {
                // nameTxn can be deleted
                // -1 here being to compensate +1 added when a partition version parsed from folder name
                // See comments of why +1 added there in parsePartitionDateVersion()
                purgePartition(tableToken, ff, path, tableRootLen - tableToken.getDirNameUtf8().size() - 1, "purging dropped partition directory [path=");
                lastTxn = nameTxn;
            } else {
                LOG.debug().$("cannot purge partition directory, locked for reading [path=")
                        .$substr(tableRootLen - tableToken.getDirNameUtf8().size() - 1, path)
                        .I$();
                break;
            }
        }
    }

    private void processPartition(
            TableToken tableToken,
            FilesFacade ff,
            Path path,
            int tableRootLen,
            TxReader txReader,
            TxnScoreboard txnScoreboard,
            long partitionTimestamp,
            int timestampType,
            int partitionBy,
            DirectLongList partitionList,
            int lo,
            int hi
    ) {
        boolean partitionInTxnFile = txReader.findAttachedPartitionRawIndexByLoTimestamp(partitionTimestamp) >= 0;
        if (partitionInTxnFile) {
            processPartition0(
                    tableToken,
                    ff,
                    path,
                    tableRootLen,
                    txReader,
                    txnScoreboard,
                    partitionTimestamp,
                    timestampType,
                    partitionBy,
                    partitionList,
                    lo,
                    hi
            );
        } else {
            processDetachedPartition(
                    tableToken,
                    ff,
                    path,
                    tableRootLen,
                    txReader,
                    txnScoreboard,
                    partitionTimestamp,
                    timestampType,
                    partitionBy,
                    partitionList,
                    lo,
                    hi
            );
        }
    }

    private void processPartition0(
            TableToken tableToken,
            FilesFacade ff,
            Path path,
            int tableRootLen,
            TxReader txReader,
            TxnScoreboard txnScoreboard,
            long partitionTimestamp,
            int timestampType,
            int partitionBy,
            DirectLongList partitionList,
            int lo,
            int hi
    ) {
        long lastCommittedPartitionName = txReader.getPartitionNameTxnByPartitionTimestamp(partitionTimestamp);
        if (lastCommittedPartitionName > -1) {
            assert hi <= partitionList.size();
            // lo points to the beginning element in partitionList, hi next after last
            // each partition folder represented by a pair in the partitionList (partition version, partition timestamp)
            // Skip first pair, start from second and check if it can be deleted.
            for (int i = lo + 2; i < hi; i += 2) {
                long nextNameVersion = Math.min(lastCommittedPartitionName + 1, partitionList.get(i));
                long previousNameVersion = partitionList.get(i - 2);

                boolean rangeUnlocked = previousNameVersion < nextNameVersion
                        && txnScoreboard.isRangeAvailable(previousNameVersion, nextNameVersion);

                path.trimTo(tableRootLen);
                TableUtils.setPathForNativePartition(
                        path,
                        timestampType,
                        partitionBy,
                        partitionTimestamp,
                        previousNameVersion - 1
                );
                path.$();

                if (rangeUnlocked) {
                    // previousNameVersion can be deleted
                    // -1 here is to compensate +1 added when a partition version parsed from folder name
                    // See comments of why +1 added there in parsePartitionDateVersion()
                    engine.getPartitionOverwriteControl().notifyPartitionMutates(
                            tableToken,
                            timestampType,
                            partitionTimestamp,
                            previousNameVersion - 1,
                            0
                    );
                    purgePartition(
                            tableToken,
                            ff,
                            path,
                            tableRootLen - tableToken.getDirNameUtf8().size() - 1,
                            "purging overwritten partition directory [path="
                    );
                } else {
                    LOG.info().$("cannot purge overwritten partition directory, locked for reading path=")
                            .$substr(tableRootLen - tableToken.getDirNameUtf8().size() - 1, path).I$();
                }
            }
        }
    }

    private void purgePartition(TableToken tableToken, FilesFacade ff, Path path, int pathFrom, String message) {
        if (engine.lockTableCreate(tableToken)) {
            try {
                TableToken lastToken = engine.getUpdatedTableToken(tableToken);
                if (lastToken == tableToken) {
                    LOG.info().$(message).$substr(pathFrom, path).I$();
                    ff.unlinkOrRemove(path, LOG);
                } else {
                    // the table is dropped and recreated since we started processing it.
                    // abort the table processing
                    throw new TableReferenceOutOfDateException();
                }
            } finally {
                engine.unlockTableCreate(tableToken);
            }
        } else {
            // the table is dropped and recreated since we started processing it.
            // abort the table processing
            throw new TableReferenceOutOfDateException();
        }
    }

    @Override
    protected boolean canRun() {
        // disable the purge job while database checkpoint is in progress
        return !engine.getCheckpointStatus().partitionsLocked();
    }

    @Override
    protected boolean doRun(int workerId, long cursor, RunStatus runStatus) {
        final O3PartitionPurgeTask task = queue.get(cursor);
        discoverPartitions(
                configuration.getFilesFacade(),
                fileNameSinks[workerId],
                partitionList.get(workerId),
                configuration.getDbRoot(),
                task.getTableToken(),
                txnReaders.get(workerId),
                task.getTimestampType(),
                task.getPartitionBy()
        );
        subSeq.done(cursor);
        return true;
    }
}
