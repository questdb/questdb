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

package io.questdb.cutlass.text;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.Sequence;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.Path;

import java.io.Closeable;
import java.io.IOException;

/**
 * Class is responsible for pre-processing of large unordered import files meant to go into partitioned tables.
 * It :
 * - scans whole file sequentially and extract timestamps and line offsets to per-partition index files
 * Index files are stored as $inputWorkDir/$inputFileName/$partitionName.idx .
 * - starts W workers and using them
 * - sorts chunks by timestamp
 * - loads partitions in parallel into separate tables using index files
 * - deattaches partitions from temp tables and attaches them to final table
 * <p>
 */
public class FileIndexer implements Closeable, Mutable {
    private static final Log LOG = LogFactory.getLog(FileIndexer.class);

    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    private final LongList stats = new LongList();
    private final ObjList<FileSplitter> splitters = new ObjList<>();
    private final FilesFacade ff;
    //work dir path
    private final Path path = new Path();
    private final int dirMode;
    private RingQueue<TextImportTask> queue;
    private Sequence pubSeq;
    private Sequence subSeq;
    private int workerCount;
    private CharSequence inputRoot;
    private CharSequence inputWorkRoot;

    public FileIndexer(SqlExecutionContext sqlExecutionContext) {
        this.workerCount = sqlExecutionContext.getWorkerCount();

        MessageBus bus = sqlExecutionContext.getMessageBus();
        this.queue = bus.getTextImportQueue();
        this.pubSeq = bus.getTextImportPubSeq();
        this.subSeq = bus.getTextImportSubSeq();

        CairoConfiguration cfg = sqlExecutionContext.getCairoEngine().getConfiguration();
        this.ff = cfg.getFilesFacade();

        this.inputRoot = cfg.getInputRoot();
        this.inputWorkRoot = cfg.getInputWorkRoot();
        this.dirMode = cfg.getMkDirMode();
    }

    @Override
    public void clear() {
        doneLatch.reset();
        stats.clear();
        splitters.clear(); //todo: fix it
    }

    @Override
    public void close() throws IOException {
        Misc.freeObjList(splitters);
        path.close();
        clear();
    }

    public void of(int partitionBy, byte columnDelimiter, int timestampIndex, DateFormat format, boolean ignoreHeader) {
        //todo: pass to all splitters
    }

    public void process(final CharSequence inputFileName) {
        clear();

        long fileSize = ff.length(path.of(inputRoot).slash().concat(inputFileName).slash$().$());
        assert fileSize > 0;

        final long chunkSize = (fileSize + workerCount - 1) / workerCount;
        final int taskCount = (int) ((fileSize + chunkSize - 1) / chunkSize);

        //todo: splitting part
        int queuedCount = 0;
        doneLatch.reset();

        createWorkDir(path.of(inputWorkRoot).slash().concat(inputFileName).slash$().$());

        stats.setPos(taskCount * 4); // quotesEven, quotesOdd, newlineOffsetEven, newlineOffsetOdd
        stats.zero(0);

        for (int i = 0; i < taskCount; ++i) {
            final long chunkLo = i * chunkSize;
            final long chunkHi = Long.min(chunkLo + chunkSize, fileSize);

            final long seq = pubSeq.next();
            if (seq < 0) {
                // process locally
            } else {
                queue.get(seq).of(4 * i, chunkLo, chunkHi, stats, doneLatch);
                pubSeq.done(seq);
                queuedCount++;
            }
        }

        // process our own queue
        // this should fix deadlock with 1 worker configuration
        while (doneLatch.getCount() > -queuedCount) {
            long seq = subSeq.next();
            if (seq > -1) {
                queue.get(seq).run();
                subSeq.done(seq);
            }
        }

        doneLatch.await(queuedCount);

        queuedCount = 0;
        doneLatch.reset();
        //todo: parsing part
    }

    //TODO: we'll' need to lock dir or acquire table lock to make sure there are no two parallel user-issued imports of the same file
    private void createWorkDir(final Path path) {

        if (ff.exists(path)) {
            int errno = ff.rmdir(path);
            if (errno != 0) {
                throw CairoException.instance(errno).put("Can't remove import work dir ").put(path).put(" errno=").put(errno);
            }
        }

        int errno = ff.mkdir(path, dirMode);
        if (errno != 0) {
            throw CairoException.instance(errno).put("Can't create import work dir ").put(path).put(" errno=").put(errno);
        }

        LOG.info().$("created import dir ").$(path).$();
    }
}
