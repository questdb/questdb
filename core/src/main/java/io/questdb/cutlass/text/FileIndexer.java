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
import io.questdb.griffin.SqlException;
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

    //TODO: maybe fetch it from global config ?
    private static final int DEFAULT_MIN_CHUNK_SIZE = 300 * 1024 * 1024;
    private int minChunkSize = DEFAULT_MIN_CHUNK_SIZE;

    private final LongList chunkStats = new LongList();
    private final ObjList<FileSplitter> splitters = new ObjList<>();
    private final FilesFacade ff;

    //work dir path
    private final Path path = new Path();
    private final int dirMode;

    private final RingQueue<TextImportTask> queue;
    private final Sequence pubSeq;
    private final Sequence subSeq;
    private final int workerCount;
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();

    private final CharSequence inputRoot;
    private final CharSequence inputWorkRoot;

    private final int bufferLength;

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

        this.bufferLength = sqlExecutionContext.getCairoEngine().getConfiguration().getSqlCopyBufferSize();
    }

    @Override
    public void clear() {
        doneLatch.reset();
        chunkStats.clear();
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

        chunkStats.setPos(taskCount * 4); // quotesEven, quotesOdd, newlineOffsetEven, newlineOffsetOdd
        chunkStats.zero(0);

        for (int i = 0; i < taskCount; ++i) {
            final long chunkLo = i * chunkSize;
            final long chunkHi = Long.min(chunkLo + chunkSize, fileSize);

            final long seq = pubSeq.next();
            if (seq < 0) {
                // process locally
            } else {
                //queue.get(seq).of(4 * i, chunkLo, chunkHi, chunkStats, doneLatch);//TODO: fix 
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

    //returns list with N chunk boundaries
    public LongList findChunkBoundaries(long fd, Path inputFilePath) throws SqlException {
        final long fileLength = ff.length(fd);

        if (fileLength < 1) {
            return null;
        }

        path.of(inputFilePath).$();//of() doesn't copy null terminator!

        assert (workerCount > 0 && minChunkSize > 0);

        long chunkSize = fileLength / workerCount;
        chunkSize = Math.max(minChunkSize, chunkSize);
        final int chunks = (int) (fileLength / chunkSize);

        int queuedCount = 0;
        doneLatch.reset();

        //count of quotes, even new lines, odd new lines, offset to first even newline, offset to first odd newline
        chunkStats.setPos(chunks * 5);
        chunkStats.zero(0);

        for (int i = 0; i < chunks; i++) {
            final long chunkLo = i * chunkSize;
            final long chunkHi = Long.min(chunkLo + chunkSize, fileLength);

            final long seq = pubSeq.next();
            if (seq < 0) {
                countQuotes(fd, chunkLo, chunkHi, chunkStats, 5 * i, bufferLength, ff);
            } else {
                queue.get(seq).of(5 * i, this, chunkLo, chunkHi, chunkStats, doneLatch);
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

        processChunkStats(fileLength, chunks);

        LOG.info().$("Finished finding boundaries in file=").$(path).$();

        return chunkStats;
    }

    private void processChunkStats(long fileLength, int chunks) {
        long quotes = chunkStats.get(0);
        chunkStats.set(0, 0);
        int actualChunks = 1;

        for (int i = 1; i < chunks; i++) {
            long startPos;
            if ((quotes & 1) == 1) { // if number of quotes is odd then use odd starter 
                startPos = chunkStats.get(5 * i + 4);
            } else {
                startPos = chunkStats.get(5 * i + 3);
            }

            //whole chunk might belong to huge quoted string or contain one very long line
            //so it should be merged with previous chunk
            if (startPos > -1) {
                chunkStats.set(actualChunks++, startPos);
            }

            quotes += chunkStats.get(5 * i);
        }
        chunkStats.setPos(actualChunks);
        chunkStats.add(fileLength);
    }

    public static void countQuotes(long fd, long chunkStart, long chunkEnd, LongList chunkStats, int chunkIndex, int bufferLength, FilesFacade ff) throws SqlException {
        long offset = chunkStart;

        //output vars
        long quotes = 0;
        long[] nlCount = new long[2];
        long[] nlFirst = new long[]{-1, -1};

        long read;
        long ptr;
        long hi;

        long buffer = Unsafe.malloc(bufferLength, MemoryTag.NATIVE_DEFAULT);
        try {
            do {
                long leftToRead = Math.min(chunkEnd - offset, bufferLength);
                read = (int) ff.read(fd, buffer, leftToRead, offset);
                if (read < 1) {
                    break;
                }
                hi = buffer + read;
                ptr = buffer;

                while (ptr < hi) {
                    final byte c = Unsafe.getUnsafe().getByte(ptr++);
                    if (c == '"') {
                        quotes++;
                    } else if (c == '\n') {
                        nlCount[(int) (quotes & 1)]++;
                        if (nlFirst[(int) (quotes & 1)] == -1) {
                            nlFirst[(int) (quotes & 1)] = chunkStart + ptr - buffer;
                        }
                    }
                }

                offset += read;
            } while (offset < chunkEnd);

            if (read < 0 || offset < chunkEnd) {
                throw SqlException.$(/*model.getFileName().position*/1, "could not read file [errno=").put(ff.errno()).put(']');
            }

            chunkStats.set(chunkIndex, quotes);
            chunkStats.set(chunkIndex + 1, nlCount[0]);
            chunkStats.set(chunkIndex + 2, nlCount[1]);
            chunkStats.set(chunkIndex + 3, nlFirst[0]);
            chunkStats.set(chunkIndex + 4, nlFirst[1]);
        } finally {
            Unsafe.free(buffer, bufferLength, MemoryTag.NATIVE_DEFAULT);
        }
    }

    public void setMinChunkSize(int minChunkSize) {
        this.minChunkSize = minChunkSize;
    }

    Path getInputFilePath() {
        return path;
    }

    FilesFacade getFf() {
        return ff;
    }

    int getBufferLength() {
        return bufferLength;
    }
}
