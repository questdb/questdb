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
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cutlass.text.types.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.Sequence;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.DirectCharSink;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.io.IOException;

/**
 * Class is responsible for pre-processing of large unordered import files meant to go into partitioned tables.
 * It does the following (in parallel) :
 * - splits the file into N-chunks, scans in parallel and finds correct line start for each chunk
 * - scans each chunk and extract timestamps and line offsets to per-partition index files
 * (index files are stored as $inputWorkDir/$inputFileName/$partitionName/$workerId_$chunkNumber)
 * then it sorts each file by timestamp value
 * - merges all partiton index chunks into one index file per partition
 * - loads partitions into separate tables using merged indexes (one table per worker)
 * - deattaches partitions from temp tables and attaches them to final table
 * - removes temp tables and index files
 * <p>
 */
public class FileIndexer implements Closeable, Mutable {

    private static final Log LOG = LogFactory.getLog(FileIndexer.class);

    private static final String LOCK_REASON = "parallel import";
    private static final int NO_INDEX = -1;

    private static final int DEFAULT_MIN_CHUNK_SIZE = 300 * 1024 * 1024;
    private int minChunkSize = DEFAULT_MIN_CHUNK_SIZE;

    //holds result of first phase - boundary scanning
    //count of quotes, even new lines, odd new lines, offset to first even newline, offset to first odd newline
    private final LongList chunkStats = new LongList();

    //holds input for second phase - indexing: offset and start line number for each chunk
    private final LongList indexChunkStats = new LongList();
    //stats calculated during indexing phase, (maxLineLength:list of partition floors)  for each worker 
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
    private final CairoSecurityContext securityContext;

    private final ObjList<LongList> partitionKeys = new ObjList<>();
    private final StringSink partitionNameSink = new StringSink();
    private final ObjList<CharSequence> partitionNames = new ObjList<>();

    private final DateLocale defaultDateLocale;
    private final DirectCharSink utf8Sink;
    private final TypeManager typeManager;
    private final TextDelimiterScanner textDelimiterScanner;
    private final TextMetadataDetector textMetadataDetector;

    private final SqlExecutionContext sqlExecutionContext;
    private final CairoEngine cairoEngine;
    private final CairoConfiguration configuration;
    private int atomicity;

    public FileIndexer(SqlExecutionContext sqlExecutionContext) {
        this.sqlExecutionContext = sqlExecutionContext;
        this.cairoEngine = sqlExecutionContext.getCairoEngine();
        this.securityContext = sqlExecutionContext.getCairoSecurityContext();
        this.configuration = cairoEngine.getConfiguration();

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

        TextConfiguration textConfiguration = configuration.getTextConfiguration();
        this.utf8Sink = new DirectCharSink(textConfiguration.getUtf8SinkSize());
        this.typeManager = new TypeManager(textConfiguration, utf8Sink);
        this.textDelimiterScanner = new TextDelimiterScanner(textConfiguration);
        this.textMetadataDetector = new TextMetadataDetector(typeManager, textConfiguration);
        this.defaultDateLocale = textConfiguration.getDefaultDateLocale();

        for (int i = 0; i < workerCount; i++) {
            contextObjList.add(new TaskContext());
            partitionKeys.add(new LongList());
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

        inputFileName = null;
        tableName = null;
        timestampColumn = null;
        timestampIndex = -1;
        partitionBy = -1;
        columnDelimiter = -1;
        timestampAdapter = null;
        forceHeader = false;
        maxLineLength = 0;
        atomicity = Atomicity.SKIP_ALL;

        for (int i = 0; i < contextObjList.size(); i++) {
            contextObjList.get(i).clear();
        }

        for (int i = 0; i < partitionKeys.size(); i++) {
            partitionKeys.get(i).clear();
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
        this.textDelimiterScanner.close();
    }

    public void importPartitions() {
        LOG.info().$("Started index merge and partition load").$();

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
                context.importPartitionStage(i, lo, hi, partitionNames);
            } else {
                queue.get(seq).of(doneLatch, TextImportTask.PHASE_PARTITION_IMPORT, context, i, lo, hi, partitionNames);
                pubSeq.done(seq);
                queuedCount++;
            }
        }

        waitForWorkers(queuedCount);

        LOG.info().$("Finished index merge and partition load").$();
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
        this.atomicity = Atomicity.SKIP_ALL;

        inputFilePath.of(inputRoot).slash().concat(inputFileName).$();
    }

    @TestOnly
    void setBufferLength(int bufferSize) {
        for (int i = 0; i < contextObjList.size(); i++) {
            TaskContext context = contextObjList.get(i);
            context.splitter.setBufferLength(bufferSize);
        }
    }

    public void parseStructure() throws TextException, SqlException {
        int textAnalysisMaxLines = 10;
        final CairoConfiguration configuration = sqlExecutionContext.getCairoEngine().getConfiguration();

        int len = configuration.getSqlCopyBufferSize();
        long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);

        try (TextLexer lexer = new TextLexer(configuration.getTextConfiguration())) {
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

                    lexer.of(columnDelimiter);
                    lexer.setSkipLinesWithExtraValues(false);

                    ObjList<CharSequence> names = new ObjList<>();
                    ObjList<TypeAdapter> types = new ObjList<>();

                    if (timestampColumn != null && timestampAdapter != null) {
                        names.add(timestampColumn);
                        types.add(timestampAdapter);
                    }

                    textMetadataDetector.of(names, types, forceHeader);
                    lexer.parse(buf, buf + n, textAnalysisMaxLines, textMetadataDetector);
                    textMetadataDetector.evaluateResults(lexer.getLineCount(), lexer.getErrorCount());

                    final TaskContext context = contextObjList.get(0);
                    context.prepareTable(securityContext, textMetadataDetector.getColumnNames(), textMetadataDetector.getColumnTypes(), tmpPath, typeManager);
                }
            } finally {
                ff.close(fd);
            }
        } finally {
            Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
        }

        prepareContexts();
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

            try (TableWriter writer = cairoEngine.getWriter(sqlExecutionContext.getCairoSecurityContext(), tableName, LOCK_REASON)) {
                try {
                    findChunkBoundaries(fd);
                    indexChunks();
                    importPartitions();
                    attachPartititons(writer);
                } catch (Exception e) {
                    LOG.error().$(e).$();
                }
            }
        } finally {
            ff.close(fd);
        }
    }

    void prepareContexts() {
        boolean forceHeader = this.forceHeader;
        for (int i = 0; i < contextObjList.size(); i++) {
            TaskContext context = contextObjList.get(i);
            context.of(i, textMetadataDetector.getColumnNames(), textMetadataDetector.getColumnTypes(), forceHeader);
            if (forceHeader) {
                forceHeader = false;//Assumption: only first splitter will process file with header
            }
        }
    }

    private void attachPartititons(TableWriter writer) {
        LOG.info().$("Started attaching partitions").$();

        for (int i = 0, sz = partitionNames.size(); i < sz; i++) {
            final CharSequence partitionDirName = partitionNames.get(i);
            try {
                final long timestamp = PartitionBy.parsePartitionDirName(partitionDirName, partitionBy);
                writer.attachPartition(timestamp);
            } catch (CairoException e) {
                LOG.error().$("Cannot parse partition directory name=").$(partitionDirName).$();
            }
        }

        LOG.info().$("Finished attaching partitions").$();
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
                queue.get(seq).of(doneLatch, TextImportTask.PHASE_BOUNDARY_CHECK, context, 5 * i, chunkLo, chunkHi, -1, chunkStats, null);
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
        if (indexChunkStats.size() < 2) {
            LOG.info().$("No chunks found for indexing in file=").$(inputFilePath).$();
            return;
        }

        createWorkDir();

        indexStats.setPos((indexChunkStats.size() - 2) / 2);
        indexStats.zero(0);

        for (int i = 0, n = indexChunkStats.size() - 2; i < n; i += 2) {
            int colIdx = i / 2;

            TaskContext context = contextObjList.get(colIdx);
            final long chunkLo = indexChunkStats.get(i);
            final long lineNumber = indexChunkStats.get(i + 1);
            final long chunkHi = indexChunkStats.get(i + 2);

            final long seq = pubSeq.next();
            if (seq < 0) {
                context.buildIndexStage(chunkLo, chunkHi, lineNumber, indexStats, colIdx, partitionKeys.get(colIdx));
            } else {
                queue.get(seq).of(doneLatch, TextImportTask.PHASE_INDEXING, context, colIdx, chunkLo, chunkHi, lineNumber, indexStats, partitionKeys.get(colIdx));
                pubSeq.done(seq);
                queuedCount++;
            }
        }

        // process our own queue (this should fix deadlock with 1 worker configuration)
        waitForWorkers(queuedCount);
        processIndexStats(partitionKeys);

        LOG.info().$("Finished indexing file=").$(inputFilePath).$();
    }

    private void processIndexStats(ObjList<LongList> partitionKeys) {
        maxLineLength = 0;
        for (int i = 0, n = indexStats.size(); i < n; i++) {
            maxLineLength = (int) Math.max(maxLineLength, indexStats.get(i));
        }

        LongHashSet set = new LongHashSet();
        for (int i = 0, n = partitionKeys.size(); i < n; i++) {
            LongList keys = partitionKeys.get(i);
            for (int j = 0, m = keys.size(); j < m; j++) {
                set.add(keys.get(j));
            }
        }

        LongList uniquePartitionKeys = new LongList();
        for (int i = 0, n = set.size(); i < n; i++) {
            uniquePartitionKeys.add(set.get(i));
        }
        uniquePartitionKeys.sort();

        DateFormat dirFormat = PartitionBy.getPartitionDirFormatMethod(partitionBy);

        partitionNames.clear();
        tmpPath.of(inputWorkRoot).concat(inputFileName).slash$();
        for (int i = 0, n = uniquePartitionKeys.size(); i < n; i++) {
            partitionNameSink.clear();
            dirFormat.format(uniquePartitionKeys.get(i), null, null, partitionNameSink);
            partitionNames.add(partitionNameSink.toString());
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

    @TestOnly
    int getMaxLineLength() {
        return maxLineLength;
    }

    public class TaskContext implements Closeable, Mutable {
        private final DirectLongList mergeIndexes = new DirectLongList(64, MemoryTag.NATIVE_LONG_LIST);
        private final FileSplitter splitter;
        private final TextLexer lexer;

        private final TypeManager typeManager;
        private final DirectCharSink utf8Sink;

        private final Path path = new Path();
        private final MemoryMARW memory = Vm.getMARWInstance();
        private final TableStructureAdapter structureAdapter = new TableStructureAdapter();

        private CharSequence currentTableName;
        private ObjList<CharSequence> names;
        private ObjList<TypeAdapter> types;
        private TimestampAdapter timestampAdapter;
        private TableWriter tableWriterRef;

        private final IntList remapIndex = new IntList();
        private final ObjectPool<OtherToTimestampAdapter> otherToTimestampAdapterPool = new ObjectPool<>(OtherToTimestampAdapter::new, 4);

        public TaskContext() {
            final TextConfiguration textConfiguration = configuration.getTextConfiguration();
            this.utf8Sink = new DirectCharSink(textConfiguration.getUtf8SinkSize());
            this.typeManager = new TypeManager(textConfiguration, utf8Sink);
            this.splitter = new FileSplitter(configuration);
            this.lexer = new TextLexer(textConfiguration);
        }

        public void buildIndexStage(long lo, long hi, long lineNumber, LongList indexStats, int index, LongList partitionKeys) throws SqlException {
            splitter.index(lo, hi, lineNumber, indexStats, index, partitionKeys);
        }

        @Override
        public void clear() {
            mergeIndexes.clear();
            splitter.clear();
            lexer.clear();
            typeManager.clear();
            utf8Sink.clear();
            if (types != null) {
                types.clear();
            }
            remapIndex.clear();
            otherToTimestampAdapterPool.clear();
        }

        @Override
        public void close() throws IOException {
            clear();
            mergeIndexes.close();
            splitter.close();
            lexer.close();
            utf8Sink.close();
            path.close();
            memory.close();
        }

        public void countQuotesStage(int index, long lo, long hi, final LongList chunkStats) throws SqlException {
            splitter.countQuotes(lo, hi, chunkStats, index);
        }

        void prepareTable(
                CairoSecurityContext cairoSecurityContext,
                ObjList<CharSequence> names,
                ObjList<TypeAdapter> detectedTypes,
                Path path,
                TypeManager typeManager
        ) throws TextException {
            this.names = names;
            this.types = detectedTypes;

            if (detectedTypes.size() == 0) {
                throw CairoException.instance(0).put("cannot determine text structure");
            }

            if (partitionBy == PartitionBy.NONE) {
                throw CairoException.instance(-1).put("partition by unit can't be NONE for parallel import");
            }

            TableWriter writer = null;

            if (partitionBy < 0) {
                partitionBy = PartitionBy.NONE;
            }

            try {
                switch (cairoEngine.getStatus(cairoSecurityContext, path, tableName)) {
                    case TableUtils.TABLE_DOES_NOT_EXIST:
                        if (partitionBy == PartitionBy.NONE) {
                            throw CairoException.instance(-1).put("partition by unit must be set when importing to new table");
                        }
                        if (timestampColumn == null) {
                            throw CairoException.instance(-1).put("timestamp column must be set when importing to new table");
                        }

                        validate(null, NO_INDEX);
                        createTable(configuration.getRoot(), tableName);
                        writer = cairoEngine.getWriter(cairoSecurityContext, tableName, LOCK_REASON);
                        partitionBy = writer.getPartitionBy();
                        break;
                    case TableUtils.TABLE_EXISTS:
                        writer = openWriterAndOverrideImportTypes(names, detectedTypes, cairoSecurityContext, typeManager);
                        CharSequence designatedTimestampColumnName = writer.getDesignatedTimestampColumnName();
                        int designatedTimestampIndex = writer.getMetadata().getTimestampIndex();
                        if (PartitionBy.isPartitioned(partitionBy) && partitionBy != writer.getPartitionBy()) {
                            throw CairoException.instance(-1).put("declared partition by unit doesn't match table's");
                        }
                        partitionBy = writer.getPartitionBy();
                        if (!PartitionBy.isPartitioned(partitionBy)) {
                            throw CairoException.instance(-1).put("target table is not partitioned");
                        }
                        validate(designatedTimestampColumnName, designatedTimestampIndex);
                        break;
                    default:
                        throw CairoException.instance(0).put("name is reserved [table=").put(tableName).put(']');
                }
            } finally {
                if (writer != null) {
                    writer.close();
                }
            }

            if (timestampAdapter == null && timestampIndex != -1 &&
                    ColumnType.isTimestamp(types.getQuick(timestampIndex).getType())) {
                timestampAdapter = (TimestampAdapter) types.getQuick(timestampIndex);
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
                timestampAdapter = (TimestampAdapter) textMetadataDetector.getColumnTypes().getQuick(timestampIndex);
            }
        }

        void validate(CharSequence designatedTimestampColumnName, int designatedTimestampIndex) throws TextException {
            if (timestampColumn == null && designatedTimestampColumnName == null) {
                timestampIndex = NO_INDEX;
            } else if (timestampColumn != null) {
                timestampIndex = names.indexOf(timestampColumn);
                if (timestampIndex == NO_INDEX) {
                    throw TextException.$("invalid timestamp column '").put(timestampColumn).put('\'');
                }
            } else {
                timestampIndex = names.indexOf(designatedTimestampColumnName);
                if (timestampIndex == NO_INDEX) {
                    // columns in the imported file may not have headers, then use writer timestamp index
                    timestampIndex = designatedTimestampIndex;
                }
            }

            if (timestampIndex != NO_INDEX) {
                final TypeAdapter timestampAdapter = types.getQuick(timestampIndex);
                final int typeTag = ColumnType.tagOf(timestampAdapter.getType());
                if ((typeTag != ColumnType.LONG && typeTag != ColumnType.TIMESTAMP) || timestampAdapter == BadTimestampAdapter.INSTANCE) {
                    throw TextException.$("not a timestamp '").put(timestampColumn).put('\'');
                }
            }
        }

        private TableWriter openWriterAndOverrideImportTypes(
                ObjList<CharSequence> names,
                ObjList<TypeAdapter> detectedTypes,
                CairoSecurityContext cairoSecurityContext,
                TypeManager typeManager
        ) {
            TableWriter writer = cairoEngine.getWriter(cairoSecurityContext, tableName, LOCK_REASON);
            RecordMetadata metadata = writer.getMetadata();
            // now, compare column count.
            // Cannot continue if different
            if (metadata.getColumnCount() < detectedTypes.size()) {
                writer.close();
                throw CairoException.instance(0)
                        .put("column count mismatch [textColumnCount=").put(detectedTypes.size())
                        .put(", tableColumnCount=").put(metadata.getColumnCount())
                        .put(", table=").put(tableName)
                        .put(']');
            }

            this.types = detectedTypes;

            // now overwrite detected types with actual table column types
            remapIndex.ensureCapacity(this.types.size());
            for (int i = 0, n = this.types.size(); i < n; i++) {

                final int columnIndex = metadata.getColumnIndexQuiet(names.getQuick(i));
                final int idx = (columnIndex > -1 && columnIndex != i) ? columnIndex : i; // check for strict match ?
                remapIndex.set(i, idx);

                final int columnType = metadata.getColumnType(idx);
                final TypeAdapter detectedAdapter = this.types.getQuick(i);
                final int detectedType = detectedAdapter.getType();
                if (detectedType != columnType) {
                    // when DATE type is mis-detected as STRING we
                    // would not have either date format nor locale to
                    // use when populating this field
                    switch (ColumnType.tagOf(columnType)) {
                        case ColumnType.DATE:
                            logTypeError(i);
                            this.types.setQuick(i, BadDateAdapter.INSTANCE);
                            break;
                        case ColumnType.TIMESTAMP:
                            if (detectedAdapter instanceof TimestampCompatibleAdapter) {
                                this.types.setQuick(i, otherToTimestampAdapterPool.next().of((TimestampCompatibleAdapter) detectedAdapter));
                            } else {
                                logTypeError(i);
                                this.types.setQuick(i, BadTimestampAdapter.INSTANCE);
                            }
                            break;
                        case ColumnType.BINARY:
                            writer.close();
                            throw CairoException.instance(0).put("cannot import text into BINARY column [index=").put(i).put(']');
                        default:
                            this.types.setQuick(i, typeManager.getTypeAdapter(columnType));
                            break;
                    }
                }
            }
            return writer;
        }

        private void logTypeError(int i) {
            LOG.info()
                    .$("mis-detected [table=").$(tableName)
                    .$(", column=").$(i)
                    .$(", type=").$(ColumnType.nameOf(this.types.getQuick(i).getType()))
                    .$(']').$();
        }

        public void createTable(final CharSequence root, final CharSequence tableName) {
            setCurrentTableName(tableName);
            securityContext.checkWritePermission(); //todo: do this once
            switch (cairoEngine.getStatus(securityContext, path, currentTableName)) {
                case TableUtils.TABLE_EXISTS:
                    cairoEngine.remove(securityContext, path, currentTableName);
                case TableUtils.TABLE_DOES_NOT_EXIST:
                    TableUtils.createTable(
                            configuration.getFilesFacade(),
                            root,
                            configuration.getMkDirMode(),
                            memory,
                            path,
                            structureAdapter,
                            ColumnType.VERSION,
                            (int) cairoEngine.getNextTableId()
                    );
                    break;
                default:
                    throw CairoException.instance(0).put("name is reserved [table=").put(currentTableName).put(']');
            }
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
                            lexer.parse(buf, buf + n, 0, this::onFieldsPartitioned);
                        }
                    }
                } finally {
                    ff.close(fd);
                }
            } finally {
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
            }
        }

        public void importPartitionStage(int index, long lo, long hi, final ObjList<CharSequence> partitionNames) {
            createTable(inputWorkRoot, tableName + "_" + index);
            try (TableWriter writer = new TableWriter(configuration,
                    currentTableName,
                    cairoEngine.getMessageBus(),
                    null,
                    true,
                    DefaultLifecycleManager.INSTANCE,
                    inputWorkRoot,
                    cairoEngine.getMetrics())) {

                tableWriterRef = writer;
                try {
                    lexer.restart(false);
                    for (int i = (int) lo; i < hi; i++) {

                        final CharSequence name = partitionNames.get(i);
                        path.of(inputWorkRoot).concat(inputFileName).concat(name);

                        mergePartitionIndexAndImportData(ff, path, mergeIndexes);
                    }
                } finally {
                    lexer.parseLast();
                    writer.commit(CommitMode.SYNC);
                }

            }
            for (int i = (int) lo; i < hi; i++) {
                final CharSequence partitionName = partitionNames.get(i);
                movePartitionToDst(inputWorkRoot,
                        currentTableName,
                        partitionName,
                        cairoEngine.getConfiguration().getRoot(),
                        tableName);
            }
        }

        public void of(int index, ObjList<CharSequence> names, ObjList<TypeAdapter> types, boolean forceHeader) {
            this.names = names;
            this.types = typeManager.adjust(types);
            this.timestampAdapter = (timestampIndex > -1 && timestampIndex < types.size()) ? (TimestampAdapter) types.getQuick(timestampIndex) : null;
            this.lexer.of(columnDelimiter);
            this.lexer.setSkipLinesWithExtraValues(false);
            this.splitter.of(inputFileName, index, partitionBy, columnDelimiter, timestampIndex, timestampAdapter, forceHeader);
        }

        public void setCurrentTableName(final CharSequence tableName) {
            this.currentTableName = tableName;
            this.lexer.setTableName(tableName);
        }

        private void logError(long line, int i, final DirectByteCharSequence dbcs) {
            LogRecord logRecord = LOG.error().$("type syntax [type=").$(ColumnType.nameOf(types.getQuick(i).getType())).$("]\n\t");
            logRecord.$('[').$(line).$(':').$(i).$("] -> ").$(dbcs).$();
        }

        private void mergePartitionIndexAndImportData(final FilesFacade ff,
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

        private void movePartitionToDst(final CharSequence srcRoot,
                                        final CharSequence srcTableName,
                                        final CharSequence partitionFolder,
                                        final CharSequence dstRoot,
                                        final CharSequence dstTableName) {
            final CharSequence root = sqlExecutionContext.getCairoEngine().getConfiguration().getRoot();
            final Path srcPath = Path.getThreadLocal(srcRoot).concat(srcTableName).concat(partitionFolder).$();
            final Path dstPath = Path.getThreadLocal2(dstRoot).concat(dstTableName).concat(partitionFolder).$();
            if (!ff.rename(srcPath, dstPath)) {
                LOG.error().$("Can't move ").$(srcPath).$(" to ").$(dstPath).$(" errno=").$(ff.errno()).$();
            }
        }

        private boolean onField(long line, final DirectByteCharSequence dbcs, TableWriter.Row w, int i) {
            try {
                final int tableIndex = remapIndex.size() > 0 ? remapIndex.get(i) : i;
                types.getQuick(i).write(w, tableIndex, dbcs);
            } catch (Exception ignore) {
                logError(line, i, dbcs);
                switch (atomicity) {
                    case Atomicity.SKIP_ALL:
                        tableWriterRef.rollback();
                        throw CairoException.instance(0).put("bad syntax [line=").put(line).put(", col=").put(i).put(']');
                    case Atomicity.SKIP_ROW:
                        w.cancel();
                        return true;
                    default:
                        // SKIP column
                        break;
                }
            }
            return false;
        }

        private void onFieldsPartitioned(long line, final ObjList<DirectByteCharSequence> values, int valuesLength) {
            assert tableWriterRef != null;
            DirectByteCharSequence dbcs = values.getQuick(timestampIndex);
            try {
                final TableWriter.Row w = tableWriterRef.newRow(timestampAdapter.getTimestamp(dbcs));
                for (int i = 0; i < valuesLength; i++) {
                    dbcs = values.getQuick(i);
                    if (i == timestampIndex || dbcs.length() == 0) {
                        continue;
                    }
                    if (onField(line, dbcs, w, i)) return;
                }
                w.append();
            } catch (Exception e) {
                logError(line, timestampIndex, dbcs);
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

        private class TableStructureAdapter implements TableStructure {

            @Override
            public int getColumnCount() {
                return types.size();
            }

            @Override
            public CharSequence getColumnName(int columnIndex) {
                return names.getQuick(columnIndex);
            }

            @Override
            public int getColumnType(int columnIndex) {
                return types.getQuick(columnIndex).getType();
            }

            @Override
            public long getColumnHash(int columnIndex) {
                return configuration.getRandom().nextLong();
            }

            @Override
            public int getIndexBlockCapacity(int columnIndex) {
                return configuration.getIndexValueBlockSize();
            }

            @Override
            public boolean isIndexed(int columnIndex) {
                return types.getQuick(columnIndex).isIndexed();
            }

            @Override
            public boolean isSequential(int columnIndex) {
                return false;
            }

            @Override
            public int getPartitionBy() {
                return partitionBy;
            }

            @Override
            public boolean getSymbolCacheFlag(int columnIndex) {
                return configuration.getDefaultSymbolCacheFlag();
            }

            @Override
            public int getSymbolCapacity(int columnIndex) {
                return configuration.getDefaultSymbolCapacity();
            }

            @Override
            public CharSequence getTableName() {
                return currentTableName;
            }

            @Override
            public int getTimestampIndex() {
                return timestampIndex;
            }

            @Override
            public int getMaxUncommittedRows() {
                return configuration.getMaxUncommittedRows();
            }

            @Override
            public long getCommitLag() {
                return configuration.getCommitLag();
            }

        }
    }
}
