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

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.CairoColumn;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.tasks.HydrateMetadataTask;

import java.io.Closeable;
import java.io.IOException;

import static io.questdb.cairo.TableUtils.validationException;


public class HydrateMetadataJob extends SynchronizedJob implements Closeable {
    public static final Log LOG = LogFactory.getLog(HydrateMetadataJob.class);
    public static boolean completed = false;
    CairoConfiguration configuration;
    FilesFacade ff;
    MemoryCMR metaMem;
    Path path = new Path();
    RingQueue<HydrateMetadataTask> tasks;

    public HydrateMetadataJob(CairoEngine engine) {
        this.tasks = messageBus.getHydrateMetadataTaskQueue();
        this.ff = ff;
        this.configuration = configuration;
    }

    @Override
    public void close() throws IOException {
        if (metaMem != null) {
            metaMem.close();
        }
        path.close();
    }

    @Override
    protected boolean runSerially() {

        if (completed) {
            // error, we should not have any tasks
            throw CairoException.nonCritical().put("Could not execute HydrateMetadataJob... already completed!");
        }

        final HydrateMetadataTask queueItem = tasks.get();
        final TableToken token = queueItem.tableTokens.get(queueItem.position);

        LOG.debugW().$("Hydrating metadata for [table=").$(token.getTableName()).I$();

        // if at end
        if (queueItem.position >= queueItem.tableTokens.size()) {
            queueItem.clear();
            completed = true;
            metaMem.close();
            path.close();
            LOG.infoW().$("Metadata hydration completed.").I$();
            return true;
        }

        if (!token.isSystem()) {

            // set up table path
            path.of(configuration.getRoot());
            path.concat(token.getDirName());
            path.concat("_meta");
            path.trimTo(path.size());

            // open metadata
            metaMem = Vm.getCMRInstance();
            metaMem.smallFile(ff, path.$(), MemoryTag.NATIVE_METADATA_READER);
            TableUtils.validateMeta(metaMem, null, ColumnType.VERSION);

            // create table to work with
            CairoTable table = new CairoTable(token);
            table.lock.writeLock().lock();

            table.setLastMetadataVersionUnsafe(Long.MIN_VALUE);

            int metadataVersion = metaMem.getInt(TableUtils.META_OFFSET_METADATA_VERSION);

            // make sure we aren't duplicating work
            try {
                CairoMetadata.INSTANCE.addTable(table);
                LOG.debugW().$("Added stub table [table=").$(token.getTableName()).I$();
            } catch (CairoException e) {
                final CairoTable alreadyHydrated = CairoMetadata.INSTANCE.getTableQuick(token.getTableName());
                LOG.debugW().$("Table already present [table=").$(token.getTableName()).I$();
                if (alreadyHydrated != null) {
                    alreadyHydrated.lock.writeLock().lock();
                    long version = alreadyHydrated.getLastMetadataVersionUnsafe();
                    alreadyHydrated.lock.writeLock().unlock();

                    if (version == metadataVersion) {
                        return true;
                    }

                    LOG.debugW().$("Updating metadata [table=").$(token.getTableName()).I$();

                    table = alreadyHydrated;

                } else {
                    throw e;
                }
            }

            // [NW] - continue hydrating table and columns etc.
            int columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
            int timestampIndex = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
            int partitionBy = metaMem.getInt(TableUtils.META_OFFSET_PARTITION_BY);
//        int tableId = metaMem.getInt(TableUtils.META_OFFSET_TABLE_ID);
            int maxUncommittedRows = metaMem.getInt(TableUtils.META_OFFSET_MAX_UNCOMMITTED_ROWS);
            long o3MaxLag = metaMem.getLong(TableUtils.META_OFFSET_O3_MAX_LAG);

//        boolean walEnabled = metaMem.getBool(TableUtils.META_OFFSET_WAL_ENABLED);

            table.setPartitionByUnsafe(PartitionBy.toString(partitionBy));
            table.setMaxUncommittedRowsUnsafe(maxUncommittedRows);
            table.setO3MaxLagUnsafe(o3MaxLag);
            table.setLastMetadataVersionUnsafe(metadataVersion);

            TableUtils.buildWriterOrderMap(metaMem, table.columnOrderMap, metaMem, columnCount);

            for (int i = 0, n = table.columnOrderMap.size(); i < n; i += 3) {
                int writerIndex = table.columnOrderMap.get(i);
                if (writerIndex < 0) {
                    continue;
                }
                int stableIndex = i / 3;
                CharSequence name = metaMem.getStrA(table.columnOrderMap.get(i + 1));
                int denseSymbolIndex = table.columnOrderMap.get(i + 2);

                assert name != null;
                int columnType = TableUtils.getColumnType(metaMem, writerIndex);

                if (columnType > -1) {
                    String colName = Chars.toString(name);
                    CairoColumn column = new CairoColumn();

                    column.setNameUnsafe(colName);
                    column.setTypeUnsafe(columnType);
                    column.setIsIndexedUnsafe(TableUtils.isColumnIndexed(metaMem, writerIndex));
                    column.setIndexBlockCapacityUnsafe(TableUtils.getIndexBlockCapacity(metaMem, writerIndex));
                    column.setIsSymbolTableStaticUnsafe(true);
                    column.setIsDedupKeyUnsafe(TableUtils.isColumnDedupKey(metaMem, writerIndex));
                    column.setWriterIndexUnsafe(writerIndex);
                    column.setDenseSymbolIndexUnsafe(denseSymbolIndex);
                    column.setStableIndex(stableIndex);

                    table.addColumnUnsafe(column);

                    int denseIndex = table.columns2.size() - 1;
                    if (!table.columnNameIndexMap.put(colName, denseIndex)) {
                        throw validationException(metaMem).put("Duplicate column [name=").put(name).put("] at ").put(i);
                    }
                    if (writerIndex == timestampIndex) {
                        table.setDesignatedTimestampIndexUnsafe(denseIndex);
                    }
                }
            }
            metaMem.close();
        }

        queueItem.position++;

        return false;
    }
}
//
//public void load() {
//    final long timeout = configuration.getSpinLockTimeout();
//    final MillisecondClock millisecondClock = configuration.getMillisecondClock();
//    long deadline = configuration.getMillisecondClock().getTicks() + timeout;
//    this.path.trimTo(plen).concat(TableUtils.META_FILE_NAME);
//    boolean existenceChecked = false;
//    while (true) {
//        try {
//            load(path.$());
//            return;
//        } catch (CairoException ex) {
//            if (!existenceChecked) {
//                path.trimTo(plen).slash();
//                if (!ff.exists(path.$())) {
//                    throw CairoException.tableDoesNotExist(tableToken.getTableName());
//                }
//                path.trimTo(plen).concat(TableUtils.META_FILE_NAME).$();
//            }
//            existenceChecked = true;
//            TableUtils.handleMetadataLoadException(tableToken.getTableName(), deadline, ex, millisecondClock, timeout);
//        }
//    }
//}


//
//public class ColumnIndexerJob extends AbstractQueueConsumerJob<ColumnIndexerTask> {
//
//    public ColumnIndexerJob(MessageBus messageBus) {
//        super(messageBus.getIndexerQueue(), messageBus.getIndexerSubSequence());
//    }
//
//    protected boolean doRun(int workerId, long cursor, RunStatus runStatus) {
//        final ColumnIndexerTask queueItem = queue.get(cursor);
//        // copy values and release queue item
//        final ColumnIndexer indexer = queueItem.indexer;
//        final long lo = queueItem.lo;
//        final long hi = queueItem.hi;
//        final long indexSequence = queueItem.sequence;
//        final SOCountDownLatch latch = queueItem.countDownLatch;
//        subSeq.done(cursor);
//
//        // On the face of it main thread could have consumed same sequence as
//        // child workers. The reason it is undesirable is that all writers
//        // share the same queue and main thread end up indexing content for other writers.
//        // Using CAS allows main thread to steal only parts of its own job.
//        if (indexer.tryLock(indexSequence)) {
//            TableWriter.indexAndCountDown(indexer, lo, hi, latch);
//            return true;
//        }
//        // This is hard to test. Condition occurs when main thread successfully steals
//        // work from under nose of this worker.
//        return false;
//    }
//}
