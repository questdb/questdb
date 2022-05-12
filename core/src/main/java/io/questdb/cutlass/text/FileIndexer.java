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
import io.questdb.cairo.TableUtils;
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
import io.questdb.std.str.StringSink;

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

    private final Path inputFilePath = new Path();
    private final int dirMode;
    private final Path tmpPath = new Path();

    private final RingQueue<TextImportTask> queue;
    private final Sequence pubSeq;
    private final Sequence subSeq;
    private final int workerCount;
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();

    private final CharSequence inputRoot;
    private final CharSequence inputWorkRoot;

    //name of file to process in inputRoot dir
    private CharSequence inputFileName;

    private final int bufferLength;

    public FileIndexer(SqlExecutionContext sqlExecutionContext) {
        MessageBus bus = sqlExecutionContext.getMessageBus();
        this.queue = bus.getTextImportQueue();
        this.pubSeq = bus.getTextImportPubSeq();
        this.subSeq = bus.getTextImportSubSeq();

        CairoConfiguration cfg = sqlExecutionContext.getCairoEngine().getConfiguration();
        this.workerCount = sqlExecutionContext.getWorkerCount();
        for (int i = 0; i < workerCount; i++) {
            splitters.add(new FileSplitter(cfg));
        }

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

        for (int i = 0; i < splitters.size(); i++) {
            splitters.get(i).clear();
        }
    }

    @Override
    public void close() throws IOException {
        clear();
        Misc.freeObjList(splitters);
        inputFilePath.close();
        tmpPath.close();
    }

    public void of(CharSequence inputFileName, int partitionBy, byte columnDelimiter, int timestampIndex, DateFormat format, boolean ignoreHeader) {
        clear();

        this.inputFileName = inputFileName;
        inputFilePath.of(inputRoot).slash().concat(inputFileName).$();

        for (int i = 0; i < splitters.size(); i++) {
            FileSplitter splitter = splitters.get(i);
            splitter.of(inputFileName, i, partitionBy, columnDelimiter, timestampIndex, format, ignoreHeader);

            //TODO: what to do with ignoreHeader?
            if (ignoreHeader) {
                ignoreHeader = false;//only first splitter will process file with header
            }
        }
    }

    public void process() throws SqlException {
        long fd = ff.openRO(inputFilePath);
        if (fd < 0) {
            throw CairoException.instance(ff.errno()).put("Can't open input file").put(inputFilePath);
        }

        try {
            final long fileLength = ff.length(fd);
            if (fileLength < 1) {
                LOG.info().$("Ignoring file because it's empty. Path=").$(inputFilePath).$();
                return;
            }

            findChunkBoundaries(fd);
            indexChunks();

            //TODO:  sort merge phase
            //TODO:  import phase
        } finally {
            ff.close(fd);
        }
    }

    //TODO: we'll' need to lock dir or acquire table lock to make sure there are no two parallel user-issued imports of the same file
    private void createWorkDir() {
        //TODO: remove file separator and dots from input file name !
        Path workDirPath = tmpPath.of(inputWorkRoot).slash().concat(inputFileName).slash$();

        if (ff.exists(workDirPath)) {
            int errno = ff.rmdir(workDirPath);
            if (errno != 0) {
                throw CairoException.instance(errno).put("Can't remove import pre-existing work dir ").put(workDirPath).put(" errno=").put(errno);
            }
        }

        int errno = ff.mkdir(workDirPath, dirMode);
        if (errno != 0) {
            throw CairoException.instance(errno).put("Can't create import work dir ").put(workDirPath).put(" errno=").put(errno);
        }

        LOG.info().$("created import dir ").$(workDirPath).$();
    }

    //returns list with N chunk boundaries
    LongList findChunkBoundaries(long fd) throws SqlException {
        final long fileLength = ff.length(fd);

        if (fileLength < 1) {
            return null;
        }

        LOG.info().$("Started checking boundaries in file=").$(inputFilePath).$();

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
            FileSplitter splitter = splitters.get(i);

            final long chunkLo = i * chunkSize;
            final long chunkHi = Long.min(chunkLo + chunkSize, fileLength);

            final long seq = pubSeq.next();
            if (seq < 0) {
                splitter.countQuotes(chunkLo, chunkHi, chunkStats, 5 * i);
            } else {
                queue.get(seq).of(5 * i, splitter, chunkLo, chunkHi, chunkStats, doneLatch, TextImportTask.PHASE_BOUNDARY_CHECK);
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

        LOG.info().$("Finished checking boundaries in file=").$(inputFilePath).$();

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

            //if whole chunk  belongs to huge quoted string or contains one very long line
            //then it should be ignored here and merged with previous chunk
            if (startPos > -1) {
                chunkStats.set(actualChunks++, startPos);
            }

            quotes += chunkStats.get(5 * i);
        }
        chunkStats.setPos(actualChunks);
        chunkStats.add(fileLength);
    }

    void indexChunks() throws SqlException {
        int queuedCount = 0;
        doneLatch.reset();

        LOG.info().$("Started indexing file=").$(inputFilePath).$();

        createWorkDir();

        for (int i = 0, n = chunkStats.size() - 1; i < n; i++) {
            final long chunkLo = chunkStats.get(i);
            final long chunkHi = chunkStats.get(i + 1);

            FileSplitter splitter = splitters.get(i);

            final long seq = pubSeq.next();
            if (seq < 0) {
                splitter.index(chunkLo, chunkHi, 5 * i);
            } else {//TODO: maybe could re-use chunkStats to store number of rows found in each chunk  
                queue.get(seq).of(5 * i, splitter, chunkLo, chunkHi, chunkStats, doneLatch, TextImportTask.PHASE_INDEXING);
                pubSeq.done(seq);
                queuedCount++;
            }
        }

        // process our own queue (this should fix deadlock with 1 worker configuration)
        while (doneLatch.getCount() > -queuedCount) {
            long seq = subSeq.next();
            if (seq > -1) {
                queue.get(seq).run();
                subSeq.done(seq);
            }
        }

        doneLatch.await(queuedCount);//TODO: add timeout ?
        doneLatch.reset();

        LOG.info().$("Finished indexing file=").$(inputFilePath).$();
    }

    public void setMinChunkSize(int minChunkSize) {
        this.minChunkSize = minChunkSize;
    }

    Path getInputFilePath() {
        return inputFilePath;
    }

    FilesFacade getFf() {
        return ff;
    }

    int getBufferLength() {
        return bufferLength;
    }

    public void merge(final Path root, int chunkCount) {
        // reuse collections
        LongList descriptors = new LongList(chunkCount);
        DirectLongList indexes = new DirectLongList(chunkCount, MemoryTag.NATIVE_DEFAULT);
        StringSink sink = new StringSink();
        // close indexes

        int rootlen = root.length();
        long partition = ff.findFirst(root);
        if (partition > 0) {
            try {
                do {
                    // partition loop
                    long partitionName = ff.findName(partition);
                    long partitionType = ff.findType(partition);
                    if (Files.isDir(partitionName, partitionType, sink)) {
                        //todo: make parallel
                        descriptors.clear();
                        indexes.clear();
                        long mergedSize = 0;
                        root.trimTo(rootlen);
                        long chunk = ff.findFirst(root.concat(partitionName).slash$());
                        int partlen = root.length();
                        if (chunk > 0) {
                            try {
                                do {
                                    // chunk loop
                                    long chunkName = ff.findName(chunk);
                                    long chunkType = ff.findType(chunk);
                                    if (chunkType == Files.DT_FILE) {
                                        root.trimTo(partlen);
                                        root.concat(chunkName).$();
                                        try {
                                            final long fd = TableUtils.openRO(ff, root, LOG);
                                            final long size = ff.length(fd);
                                            final long address = TableUtils.mapRO(ff, fd, size, MemoryTag.MMAP_DEFAULT);

                                            descriptors.add(fd);
                                            indexes.add(address);
                                            indexes.add(size / 16);
                                            mergedSize += size;
                                        } catch (Exception e) {
                                            for (int i = 0, sz = descriptors.size(); i < sz; i++) {
                                                ff.close(descriptors.get(i));
                                            }
                                            throw e;
                                        }
                                    }
                                } while (ff.findNext(chunk) > 0);
                            } finally {
                                ff.findClose(chunk);
                            }
                        }

                        long fd = -1;
                        try {
                            root.trimTo(rootlen);
                            root.concat("merged").$();
                            fd = TableUtils.openFileRWOrFail(ff, root, CairoConfiguration.O_NONE);
                            final long address = TableUtils.mapRW(ff, fd, mergedSize, MemoryTag.MMAP_DEFAULT);
                            final long merged = Vect.mergeLongIndexesAscExt(indexes.getAddress(), (int) indexes.size() / 2, address);
                            assert merged == address;
                        } finally {
                            ff.fsync(fd);
                            ff.close(fd);

                            for (int i = 0, sz = descriptors.size(); i < sz; i++) {
                                ff.close(descriptors.get(i));
                            }
                        }
                    }
                } while (ff.findNext(partition) > 0);
            } finally {
                ff.findClose(partition);
            }
        }
    }

    public void sort(final Path path) {
        int plen = path.length();
        long srcFd = -1;
        long dstFd = -1;
        try {
            srcFd = TableUtils.openFileRWOrFail(ff, path.$(), CairoConfiguration.O_NONE);
            final long srcSize = ff.length(srcFd);
            final long srcAddress = TableUtils.mapRW(ff, srcFd, srcSize, MemoryTag.MMAP_DEFAULT);

            dstFd = TableUtils.openFileRWOrFail(ff, path.chop$().put(".s").$(), CairoConfiguration.O_NONE);
            final long dstAddress = TableUtils.mapRW(ff, dstFd, srcSize, MemoryTag.MMAP_DEFAULT);

            Vect.radixSortLongIndexAscInPlace(srcAddress, srcSize / (2 * Long.BYTES), dstAddress);
        } finally {
            if (srcFd != -1) {
                ff.close(srcFd);
                ff.remove(path);
                path.trimTo(plen);
            }
            if (dstFd != -1) {
                ff.fsync(dstFd);
                ff.close(dstFd);
            }
        }
    }
}
