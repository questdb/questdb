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
import io.questdb.cairo.*;
import io.questdb.cutlass.text.types.TimestampAdapter;
import io.questdb.cutlass.text.types.TypeAdapter;
import io.questdb.cutlass.text.types.TypeManager;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.Sequence;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.str.DirectCharSink;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.io.IOException;

/**
 * Class is responsible for pre-processing of large unordered import files meant to go into partitioned tables.
 * It :
 * - scans whole file sequentially and extract timestamps and line offsets to per-partition index files
 * Index files are stored as $inputWorkDir/$inputFileName/$partitionName/$workerId_$chunkNumber
 * - starts W workers and using them
 * - sorts chunks by timestamp
 * - loads partitions in parallel into separate tables using index files
 * - deattaches partitions from temp tables and attaches them to final table
 * <p>
 */
public class FileIndexer implements Closeable, Mutable {

    private static final Log LOG = LogFactory.getLog(FileIndexer.class);

    private static final int DEFAULT_MIN_CHUNK_SIZE = 300 * 1024 * 1024;
    private int minChunkSize = DEFAULT_MIN_CHUNK_SIZE;

    //holds result of first phase - boundary scanning
    //count of quotes, even new lines, odd new lines, offset to first even newline, offset to first odd newline
    private final LongList chunkStats = new LongList();

    //holds input for second phase - indexing: offset and start line number for each chunk
    private final LongList indexChunkStats = new LongList();
    //stats calculated during indexing phase
    private final LongList indexStats = new LongList();

    private final ObjList<TaskContext> contextObjList = new ObjList<>();

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

    //input params start
    private CharSequence tableName;
    //name of file to process in inputRoot dir
    private CharSequence inputFileName;
    //name of timestamp column
    private CharSequence timestampColumn;
    private int partitionBy;
    private byte columnDelimiter;
    private TimestampAdapter timestampAdapter;
    private boolean forceHeader;
    //input params end
    //index of timestamp column in input file
    private int timestampIndex;
    private int maxLineLength;

    private final StringSink partitionNameSink = new StringSink();
    private final ObjList<CharSequence> partitionNames = new ObjList<>();

    private final DateLocale defaultDateLocale;
    private final DirectCharSink utf8Sink;
    private final TypeManager typeManager;
    private final TextDelimiterScanner textDelimiterScanner;
    private final TextMetadataDetector textMetadataDetector;
    private final TextMetadataParser textMetadataParser;
    private final SqlExecutionContext sqlExecutionContext;

    public FileIndexer(SqlExecutionContext sqlExecutionContext) {
        this.sqlExecutionContext = sqlExecutionContext;
        MessageBus bus = sqlExecutionContext.getMessageBus();
        this.queue = bus.getTextImportQueue();
        this.pubSeq = bus.getTextImportPubSeq();
        this.subSeq = bus.getTextImportSubSeq();

        CairoConfiguration cfg = sqlExecutionContext.getCairoEngine().getConfiguration();
        this.workerCount = sqlExecutionContext.getWorkerCount();

        this.ff = cfg.getFilesFacade();

        this.inputRoot = cfg.getInputRoot();
        this.inputWorkRoot = cfg.getInputWorkRoot();
        this.dirMode = cfg.getMkDirMode();

        final TextConfiguration textConfiguration = cfg.getTextConfiguration();
        this.utf8Sink = new DirectCharSink(textConfiguration.getUtf8SinkSize());
        this.typeManager = new TypeManager(textConfiguration, utf8Sink);
        this.textDelimiterScanner = new TextDelimiterScanner(textConfiguration);
        this.textMetadataDetector = new TextMetadataDetector(typeManager, textConfiguration);
        this.textMetadataParser = new TextMetadataParser(textConfiguration, typeManager);
        this.defaultDateLocale = textConfiguration.getDefaultDateLocale();

        for (int i = 0; i < workerCount; i++) {
            contextObjList.add(new TaskContext(sqlExecutionContext, textConfiguration));
        }
    }

    @Override
    public void clear() {
        doneLatch.reset();
        chunkStats.clear();
        indexChunkStats.clear();
        indexStats.clear();
        partitionNames.clear();
        partitionNameSink.clear();
        utf8Sink.clear();
        typeManager.clear();
        textMetadataDetector.clear();
        textMetadataParser.clear();

        inputFileName = null;
        tableName = null;
        timestampColumn = null;
        timestampIndex = -1;
        partitionBy = -1;
        columnDelimiter = -1;
        timestampAdapter = null;
        forceHeader = false;
        maxLineLength = 0;

        for (int i = 0; i < contextObjList.size(); i++) {
            contextObjList.get(i).clear();
        }
    }

    @Override
    public void close() {
        clear();
        Misc.freeObjList(contextObjList);
        this.inputFilePath.close();
        this.tmpPath.close();
        this.utf8Sink.close();
        this.textMetadataDetector.close();
        this.textMetadataParser.close();
        this.textDelimiterScanner.close();
    }

    public void collectPartitionNames() {
        partitionNames.clear();
        tmpPath.of(inputWorkRoot).concat(inputFileName).slash$();
        ff.iterateDir(tmpPath, (partitionName, partitionType) -> {
            if (Files.isDir(partitionName, partitionType, partitionNameSink)) {
                partitionNames.add(partitionNameSink.toString());
            }
        });
    }

    public void mergePartitionIndexes() throws TextException {
        LOG.info().$("Started index merge").$();
        collectPartitionNames();

        final int partitionCount = partitionNames.size();
        final int chunkSize = (partitionCount + workerCount - 1) / workerCount;
        final int taskCount = (partitionCount + chunkSize - 1) / chunkSize;

        int queuedCount = 0;
        doneLatch.reset();
        for (int i = 0; i < taskCount; ++i) {
            final TaskContext context = contextObjList.get(i);
            final int lo = i * chunkSize;
            final int hi = Integer.min(lo + chunkSize, partitionCount);

            final long seq = pubSeq.next();
            if (seq < 0) {
                context.mergeIndexStage(i, lo, hi, partitionNames);
            } else {
                queue.get(seq).of(doneLatch, TextImportTask.PHASE_INDEX_MERGE, context, i, lo, hi, partitionNames);
                pubSeq.done(seq);
                queuedCount++;
            }
        }

        waitForWorkers(queuedCount);

        LOG.info().$("Finished index merge").$();
    }

    public void of(CharSequence tableName, CharSequence inputFileName, int partitionBy, byte columnDelimiter, CharSequence timestampColumn, CharSequence tsFormat, boolean forceHeader) {
        clear();

        this.tableName = tableName;
        this.inputFileName = inputFileName;
        this.timestampColumn = timestampColumn;
        this.partitionBy = partitionBy;
        this.columnDelimiter = columnDelimiter;
        if (tsFormat != null) {
            DateFormat dateFormat = typeManager.getInputFormatConfiguration().getTimestampFormatFactory().get(tsFormat);
            this.timestampAdapter = (TimestampAdapter) typeManager.nextTimestampAdapter(false, dateFormat, defaultDateLocale);
        }
        this.forceHeader = forceHeader;
        this.timestampIndex = -1;

        inputFilePath.of(inputRoot).slash().concat(inputFileName).$();
    }

    @TestOnly
    void setBufferLength(int bufferSize) {
        for (int i = 0; i < contextObjList.size(); i++) {
            TaskContext context = contextObjList.get(i);
            context.splitter.setBufferLength(bufferSize);
        }
    }

    void prepareContexts() {
        for (int i = 0; i < contextObjList.size(); i++) {
            TaskContext context = contextObjList.get(i);
            context.of(inputFileName, i, partitionBy, columnDelimiter, timestampIndex, timestampAdapter, forceHeader);

            if (forceHeader) {
                forceHeader = false;//Assumption: only first splitter will process file with header
            }
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

    public void parseStructure() throws TextException, SqlException {
        TaskContext ctx = contextObjList.get(0);
        TextLoaderBase loader = ctx.getLoader();

        int textAnalysisMaxLines = 10;
        final CairoConfiguration configuration = sqlExecutionContext.getCairoEngine().getConfiguration();

        int len = configuration.getSqlCopyBufferSize();
        long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);

        try {
            tmpPath.of(inputRoot).concat(inputFileName).$();
            long fd = ff.openRO(tmpPath);
            try {
                if (fd == -1) {
                    throw SqlException.$(0, "could not open file [errno=").put(Os.errno()).put(", path=").put(tmpPath).put(']');
                }
                long n = ff.read(fd, buf, len, 0);
                if (n > 0) {
                    if (columnDelimiter < 0) {
                        columnDelimiter = textDelimiterScanner.scan(buf, buf + n);
                    }

                    loader.setDelimiter(columnDelimiter);
                    loader.setSkipRowsWithExtraValues(false);

                    if (timestampColumn != null && timestampAdapter != null) {
                        textMetadataParser.getColumnNames().add(timestampColumn);
                        textMetadataParser.getColumnTypes().add(timestampAdapter);
                    }

                    textMetadataDetector.of(textMetadataParser.getColumnNames(), textMetadataParser.getColumnTypes(), forceHeader);
                    loader.parse(buf, buf + n, textAnalysisMaxLines, textMetadataDetector);
                    textMetadataDetector.evaluateResults(loader.getParsedLineCount(), loader.getErrorLineCount());
                    loader.restart(textMetadataDetector.isHeader());
                }
            } finally {
                ff.close(fd);
            }
        } finally {
            Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
        }

        //validate against target table
        if (partitionBy == PartitionBy.NONE) {
            throw CairoException.instance(-1).put("partition by unit can't be NONE for parallel import");
        }

        if (partitionBy < 0) {
            partitionBy = PartitionBy.NONE;
        }

        loader.configureDestination(tableName, false, true, Atomicity.SKIP_ALL, partitionBy, timestampColumn);
        loader.prepareTable(ctx.securityContext, textMetadataDetector.getColumnNames(), textMetadataDetector.getColumnTypes(), tmpPath, typeManager);
        try {
            if ((loader.getWarnings() & TextLoadWarning.PARTITION_TYPE_MISMATCH) != 0) {
                throw CairoException.instance(-1).put("declared partition by unit doesn't match table's");
            }

            if (!PartitionBy.isPartitioned(loader.getPartitionBy())) {
                throw CairoException.instance(-1).put("partition by not specified or target table is not partitioned");
            }

            timestampIndex = -1;
            if (timestampColumn != null) {
                for (int i = 0, n = textMetadataDetector.getColumnNames().size(); i < n; i++) {
                    if (Chars.equalsIgnoreCase(textMetadataDetector.getColumnNames().get(i), timestampColumn)) {
                        timestampIndex = i;
                        break;
                    }
                }
            }

            if (timestampIndex == -1) {
                throw CairoException.instance(-1).put("timestamp column not found");
            }

            if (timestampAdapter == null) {
                timestampAdapter = loader.getTimestampAdapter();
            }
        } finally {
            loader.clear();
        }

        prepareContexts();
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

            final CairoEngine engine = sqlExecutionContext.getCairoEngine();
            try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), tableName, "partitions merge")) {
                try {
                    findChunkBoundaries(fd);
                    indexChunks();
                    mergePartitionIndexes();
                    attachPartititons(writer);
                } catch (Exception e) {
                    LOG.error().$(e).$();
                }
            }
        } finally {
            ff.close(fd);
        }
    }

    private void attachPartititons(TableWriter writer) {
        for (int i = 0, sz = partitionNames.size(); i < sz; i++) {
            final CharSequence partitionDirName = partitionNames.get(i);
            try {
                final long timestamp = PartitionBy.parsePartitionDirName(partitionDirName, partitionBy);
                writer.attachPartition(timestamp);
            } catch (CairoException e) {
                LOG.error().$("Cannot parse partition directory name=").$(partitionDirName).$();
            }
        }
    }

    public void setMinChunkSize(int minChunkSize) {
        this.minChunkSize = minChunkSize;
    }

    //returns list with N chunk boundaries
    LongList findChunkBoundaries(long fd) throws SqlException {
        final long fileLength = ff.length(fd);

        if (fileLength < 1) {
            return null;
        }

        LOG.info().$("Started checking boundaries in file=").$(inputFilePath).$();

        assert (workerCount > 0 && minChunkSize > 0);

        if (workerCount == 1) {
            indexChunkStats.setPos(0);
            indexChunkStats.add(0);
            indexChunkStats.add(0);
            indexChunkStats.add(fileLength);
            indexChunkStats.add(0);
            return indexChunkStats;
        }

        long chunkSize = fileLength / workerCount;
        chunkSize = Math.max(minChunkSize, chunkSize);
        final int chunks = (int) (fileLength / chunkSize);

        int queuedCount = 0;
        doneLatch.reset();

        chunkStats.setPos(chunks * 5);
        chunkStats.zero(0);

        for (int i = 0; i < chunks; i++) {
            TaskContext context = contextObjList.get(i);
            final long chunkLo = i * chunkSize;
            final long chunkHi = Long.min(chunkLo + chunkSize, fileLength);

            final long seq = pubSeq.next();
            if (seq < 0) {
                context.countQuotesStage(5 * i, chunkLo, chunkHi, chunkStats);
            } else {
                queue.get(seq).of(doneLatch, TextImportTask.PHASE_BOUNDARY_CHECK, context, 5 * i, chunkLo, chunkHi, -1, chunkStats);
                pubSeq.done(seq);
                queuedCount++;
            }
        }

        waitForWorkers(queuedCount);
        processChunkStats(fileLength, chunks);

        LOG.info().$("Finished checking boundaries in file=").$(inputFilePath).$();

        return indexChunkStats;
    }

    void indexChunks() throws SqlException {
        int queuedCount = 0;
        doneLatch.reset();

        LOG.info().$("Started indexing file=").$(inputFilePath).$();

        createWorkDir();

        indexStats.setPos((indexChunkStats.size() - 2) / 2);
        indexStats.zero(0);

        for (int i = 0, n = indexChunkStats.size() - 2; i < n; i += 2) {
            TaskContext context = contextObjList.get(i / 2);
            final long chunkLo = indexChunkStats.get(i);
            final long lineNumber = indexChunkStats.get(i + 1);
            final long chunkHi = indexChunkStats.get(i + 2);

            final long seq = pubSeq.next();
            if (seq < 0) {
                context.buildIndexStage(chunkLo, chunkHi, lineNumber, indexStats, i / 2);
            } else {
                queue.get(seq).of(doneLatch, TextImportTask.PHASE_INDEXING, context, i / 2, chunkLo, chunkHi, lineNumber, indexStats);
                pubSeq.done(seq);
                queuedCount++;
            }
        }

        // process our own queue (this should fix deadlock with 1 worker configuration)
        waitForWorkers(queuedCount);
        processIndexStats();

        LOG.info().$("Finished indexing file=").$(inputFilePath).$();
    }

    private void processIndexStats() {
        maxLineLength = 0;
        for (int i = 0, n = indexStats.size(); i < n; i++) {
            maxLineLength = (int) Math.max(maxLineLength, indexStats.get(i));
        }
    }

    private void processChunkStats(long fileLength, int chunks) {
        long quotes = chunkStats.get(0);

        indexChunkStats.setPos(0);
        //set first chunk offset and line number
        indexChunkStats.add(0);
        indexChunkStats.add(0);

        long lines;
        long totalLines = chunks > 0 ? chunkStats.get(1) + 1 : 1;

        for (int i = 1; i < chunks; i++) {
            long startPos;
            if ((quotes & 1) == 1) { // if number of quotes is odd then use odd starter
                startPos = chunkStats.get(5 * i + 4);
                lines = chunkStats.get(5 * i + 2);
            } else {
                startPos = chunkStats.get(5 * i + 3);
                lines = chunkStats.get(5 * i + 1);
            }

            //if whole chunk  belongs to huge quoted string or contains one very long line
            //then it should be ignored here and merged with previous chunk
            if (startPos > -1) {
                indexChunkStats.add(startPos);
                indexChunkStats.add(totalLines);
            }

            quotes += chunkStats.get(5 * i);
            totalLines += lines;
        }

        if (indexChunkStats.get(indexChunkStats.size() - 2) < fileLength) {
            indexChunkStats.add(fileLength);
            indexChunkStats.add(totalLines);//doesn't matter
        }
    }

    // process our own queue (this should fix deadlock with 1 worker configuration)
    private void waitForWorkers(int queuedCount) {
        // process our own queue (this should fix deadlock with 1 worker configuration)
        while (doneLatch.getCount() > -queuedCount) {
            long seq = subSeq.next();
            if (seq > -1) {
                queue.get(seq).run();
                subSeq.done(seq);
            }
        }

        doneLatch.await(queuedCount);
        doneLatch.reset();
    }

    public class TaskContext implements Closeable, Mutable {
        private final DirectLongList mergeIndexes = new DirectLongList(64, MemoryTag.NATIVE_LONG_LIST);
        private final Path path = new Path();
        private final FileSplitter splitter;
        private final TextLoaderBase loader;
        private final CairoSecurityContext securityContext;
        private final TypeManager typeManager;

        public TaskContext(SqlExecutionContext sqlExecutionContext, TextConfiguration textConfiguration) {
            this.securityContext = sqlExecutionContext.getCairoSecurityContext();
            DirectCharSink utf8Sink = new DirectCharSink(textConfiguration.getUtf8SinkSize());
            this.typeManager = new TypeManager(textConfiguration, utf8Sink);
            final CairoEngine cairoEngine = sqlExecutionContext.getCairoEngine();
            this.splitter = new FileSplitter(cairoEngine.getConfiguration());
            this.loader = new TextLoaderBase(cairoEngine);
        }

        public void buildIndexStage(long lo, long hi, long lineNumber, LongList indexStats, int index) throws SqlException {
            splitter.index(lo, hi, lineNumber, indexStats, index);
        }

        @Override
        public void clear() {
            mergeIndexes.clear();
            splitter.clear();
            loader.clear();
        }

        @Override
        public void close() throws IOException {
            mergeIndexes.close();
            path.close();
            splitter.close();
            loader.close();
        }

        public void countQuotesStage(int index, long lo, long hi, final LongList chunkStats) throws SqlException {
            splitter.countQuotes(lo, hi, chunkStats, index);
        }

        public TextLoaderBase getLoader() {
            return loader;
        }

        public FileSplitter getSplitter() {
            return splitter;
        }

        public void importPartitionData(long address, long size) {
            int len = maxLineLength;
            long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                path.of(inputRoot).concat(inputFileName).$();
                long fd = ff.openRO(path);
                try {
                    final long count = size / (2 * Long.BYTES);
                    for (long i = 0; i < count; i++) {
                        final long offset = Unsafe.getUnsafe().getLong(address + i * 2L * Long.BYTES + Long.BYTES);
                        long n = ff.read(fd, buf, len, offset);
                        if (n > 0) {
                            loader.parse(buf, buf + n, 0); //todo: contiguous block import?
                        }
                    }
                } finally {
                    ff.close(fd);
                }
            } finally {
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
            }

        }

        private ObjList<TypeAdapter> adjust(ObjList<TypeAdapter> types) {
            ObjList<TypeAdapter> result = new ObjList<>();
            for (int i = 0, n = types.size(); i < n; i++) {
                int srcType = types.get(i).getType();
                if (srcType == ColumnType.STRING || srcType == ColumnType.SYMBOL) {
                    result.add(typeManager.getTypeAdapter(srcType));
                } else {
                    result.add(types.get(i));
                }
            }
            return result;
        }

        public void mergeIndexStage(int index, long lo, long hi, final ObjList<CharSequence> partitionNames) throws TextException {
            loader.closeWriter();
            loader.configureDestination(tableName + "_" + index, true, true, Atomicity.SKIP_ALL, loader.getPartitionBy(), timestampColumn);
            ObjList<TypeAdapter> adjustedAdapters = adjust(textMetadataDetector.getColumnTypes());
            loader.prepareTable(securityContext, textMetadataDetector.getColumnNames(), adjustedAdapters, path, typeManager);

            try {
                for (int i = (int) lo; i < hi; i++) {
                    loader.restart(false);

                    final CharSequence name = partitionNames.get(i);
                    path.of(inputWorkRoot).concat(inputFileName).concat(name);

                    mergePartitionIndexAndImportData(ff, path, mergeIndexes);

                    loader.wrapUp();
                }
            } finally {
                loader.closeWriter();
            }

            for (int i = (int) lo; i < hi; i++) {
                final CharSequence partitionName = partitionNames.get(i);
                final CharSequence srcTableName = loader.getTableName();
                movePartitionToDst(srcTableName, partitionName, tableName);
            }
        }

        public void mergePartitionIndexAndImportData(final FilesFacade ff,
                                                     final Path partitionPath,
                                                     final DirectLongList mergeIndexes) {
            mergeIndexes.resetCapacity();
            mergeIndexes.clear();

            partitionPath.slash$();
            int partitionLen = partitionPath.length();

            long mergedIndexSize = openIndexChunks(ff, partitionPath, mergeIndexes, partitionLen);

            long address = -1;
            try {
                final int indexesCount = (int) mergeIndexes.size() / 2;
                partitionPath.trimTo(partitionLen);
                partitionPath.concat(FileSplitter.INDEX_FILE_NAME).$();

                final long fd = TableUtils.openFileRWOrFail(ff, partitionPath, CairoConfiguration.O_NONE);
                address = TableUtils.mapRW(ff, fd, mergedIndexSize, MemoryTag.MMAP_DEFAULT);
                ff.close(fd);

                final long merged = Vect.mergeLongIndexesAscExt(mergeIndexes.getAddress(), indexesCount, address);
                importPartitionData(merged, mergedIndexSize);
            } finally {
                if (address != -1) {
                    ff.munmap(address, mergedIndexSize, MemoryTag.MMAP_DEFAULT);
                }
                for (long i = 0, sz = mergeIndexes.size() / 2; i < sz; i++) {
                    final long addr = mergeIndexes.get(2 * i);
                    final long size = mergeIndexes.get(2 * i + 1) * FileSplitter.INDEX_ENTRY_SIZE;
                    ff.munmap(addr, size, MemoryTag.MMAP_DEFAULT);
                }
                //todo: remove all index chunks
            }
        }

        private long openIndexChunks(FilesFacade ff, Path partitionPath, DirectLongList mergeIndexes, int partitionLen) {
            long mergedIndexSize = 0;
            long chunk = ff.findFirst(partitionPath);
            if (chunk > 0) {
                try {
                    do {
                        // chunk loop
                        long chunkName = ff.findName(chunk);
                        long chunkType = ff.findType(chunk);
                        if (chunkType == Files.DT_FILE) {
                            partitionPath.trimTo(partitionLen);
                            partitionPath.concat(chunkName).$();
                            final long fd = TableUtils.openRO(ff, partitionPath, LOG);
                            final long size = ff.length(fd);
                            final long address = TableUtils.mapRO(ff, fd, size, MemoryTag.MMAP_DEFAULT);
                            ff.close(fd);

                            mergeIndexes.add(address);
                            mergeIndexes.add(size / FileSplitter.INDEX_ENTRY_SIZE);
                            mergedIndexSize += size;
                        }
                    } while (ff.findNext(chunk) > 0);
                } finally {
                    ff.findClose(chunk);
                }
            }
            return mergedIndexSize;
        }

        public void of(CharSequence inputFileName, int index, int partitionBy, byte columnDelimiter, int timestampIndex, TimestampAdapter adapter, boolean ignoreHeader) {
            splitter.of(inputFileName, index, partitionBy, columnDelimiter, timestampIndex, adapter, ignoreHeader);
            loader.configureDestination(tableName, true, true, Atomicity.SKIP_ALL, partitionBy, timestampColumn);
            loader.setDelimiter(columnDelimiter);
        }

        private void movePartitionToDst(final CharSequence srcTableName, final CharSequence partitionFolder, final CharSequence dstTableName) {
            final CharSequence root = sqlExecutionContext.getCairoEngine().getConfiguration().getRoot();
            final Path srcPath = Path.getThreadLocal(root).concat(srcTableName).concat(partitionFolder).$();
            final Path dstPath = Path.getThreadLocal2(root).concat(dstTableName).concat(partitionFolder).$();
            if (!ff.rename(srcPath, dstPath)) {
                LOG.error().$("Can't move ").$(srcPath).$(" to ").$(dstPath).$(" errno=").$(ff.errno()).$();
            }
        }

    }

    @TestOnly
    int getMaxLineLength() {
        return maxLineLength;
    }
}
