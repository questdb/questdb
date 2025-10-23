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

package io.questdb.cutlass.text;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.MapWriter;
import io.questdb.cairo.MetadataCacheWriter;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cutlass.text.types.BadDateAdapter;
import io.questdb.cutlass.text.types.BadTimestampAdapter;
import io.questdb.cutlass.text.types.OtherToTimestampAdapter;
import io.questdb.cutlass.text.types.TimestampAdapter;
import io.questdb.cutlass.text.types.TimestampCompatibleAdapter;
import io.questdb.cutlass.text.types.TypeAdapter;
import io.questdb.cutlass.text.types.TypeManager;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.std.Chars;
import io.questdb.std.Decimal256;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongHashSet;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;
import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;


/**
 * Class is responsible for importing of large unordered import files into partitioned tables.
 * It does the following (in parallel) :
 * - splits the file into N-chunks, scans in parallel and finds correct line start for each chunk
 * - scans each chunk and extracts timestamps and line offsets to per-partition index files
 * (index files are stored as $inputWorkDir/$inputFileName/$partitionName/$workerId_$chunkNumber)
 * then it sorts each file by timestamp value
 * - merges all partition index chunks into one index file per partition (index.m)
 * - loads partitions into separate tables using merged indexes (one table per worker)
 * - scans all symbol columns to build per-column global symbol table
 * - remaps all symbol values
 * - moves and attaches partitions from temp tables to target table
 * - removes temp tables and index files
 */
public class ParallelCsvFileImporter implements Closeable, Mutable {
    private static final int DEFAULT_MIN_CHUNK_SIZE = 300 * 1024 * 1024;
    private static final String LOCK_REASON = "parallel import";
    private static final Log LOG = LogFactory.getLog(ParallelCsvFileImporter.class);
    private static final int NO_INDEX = -1;
    private final CairoEngine cairoEngine;
    // holds result of first phase - boundary scanning
    // count of quotes, even new lines, odd new lines, offset to first even newline, offset to first odd newline
    private final LongList chunkStats;
    private final Sequence collectSeq;
    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    // holds input for second phase - indexing: offset and start line number for each chunk
    private final LongList indexChunkStats;
    private final Path inputFilePath;
    private final CharSequence inputRoot;
    private final CharSequence inputWorkRoot;
    private final CopyImportJob localImportJob;
    private final ObjectPool<OtherToTimestampAdapter> otherToTimestampAdapterPool;
    private final LongList partitionKeysAndSizes;
    private final StringSink partitionNameSink;
    private final ObjList<PartitionInfo> partitions;
    private final Sequence pubSeq;
    private final RingQueue<CopyImportTask> queue;
    private final IntList symbolCapacities;
    private final TableStructureAdapter targetTableStructure;
    // stores 3 values per task : index, lo, hi (lo, hi are indexes in partitionNames)
    private final IntList taskDistribution;
    private final TextDelimiterScanner textDelimiterScanner;
    private final TextMetadataDetector textMetadataDetector;
    private final Path tmpPath;
    private final TypeManager typeManager;
    private final DirectUtf16Sink utf16Sink;
    private final DirectUtf8Sink utf8Sink;
    private final int workerCount;
    private int atomicity;
    private ExecutionCircuitBreaker circuitBreaker;
    private byte columnDelimiter;
    private boolean createdWorkDir;
    private CharSequence errorMessage;
    private long errors;
    private boolean forceHeader;
    private long importId;
    // path to import directory under, usually $inputWorkRoot/$tableName
    private CharSequence importRoot;
    // name of file to process in inputRoot dir
    private CharSequence inputFileName;
    // incremented in phase 2
    private long linesIndexed;
    private RecordMetadata metadata;
    private int minChunkSize = DEFAULT_MIN_CHUNK_SIZE;
    private int partitionBy;
    private byte phase = CopyImportTask.PHASE_SETUP;
    private long phaseErrors;
    // row stats are incremented in phase 3
    private long rowsHandled;
    private long rowsImported;
    private long startMs; // start time of current phase (in millis)
    // import status variables
    private byte status = CopyImportTask.STATUS_STARTED;
    private final Consumer<CopyImportTask> checkStatusRef = this::updateStatus;
    private final Consumer<CopyImportTask> collectChunkStatsRef = this::collectChunkStats;
    private final Consumer<CopyImportTask> collectStubRef = this::collectStub;
    private final Consumer<CopyImportTask> collectDataImportStatsRef = this::collectDataImportStats;
    private final Consumer<CopyImportTask> collectIndexStatsRef = this::collectIndexStats;
    private PhaseStatusReporter statusReporter;
    // input params start
    private CharSequence tableName;
    private TableToken tableToken;
    private boolean targetTableCreated;
    private int targetTableStatus;
    private int taskCount;
    private TimestampAdapter timestampAdapter;
    // name of timestamp column
    private CharSequence timestampColumn;
    // input params end
    // index of timestamp column in input file
    private int timestampIndex;
    private TableWriter writer;

    public ParallelCsvFileImporter(CairoEngine cairoEngine, int workerCount) {
        if (workerCount < 1) {
            throw TextImportException.instance(CopyImportTask.PHASE_SETUP, "Invalid worker count set [value=").put(workerCount).put(']');
        }

        MessageBus bus = cairoEngine.getMessageBus();
        RingQueue<CopyImportTask> queue = bus.getCopyImportQueue();
        if (queue.getCycle() < 1) {
            throw TextImportException.instance(CopyImportTask.PHASE_SETUP, "Parallel import queue size cannot be zero!");
        }

        this.cairoEngine = cairoEngine;
        this.workerCount = workerCount;
        this.queue = queue;
        this.pubSeq = bus.getCopyImportPubSeq();
        this.collectSeq = bus.getCopyImportColSeq();

        try {
            this.localImportJob = new CopyImportJob(bus);
            this.configuration = cairoEngine.getConfiguration();

            this.ff = configuration.getFilesFacade();
            this.inputRoot = configuration.getSqlCopyInputRoot();
            this.inputWorkRoot = configuration.getSqlCopyInputWorkRoot();

            TextConfiguration textConfiguration = configuration.getTextConfiguration();
            int utf8SinkSize = textConfiguration.getUtf8SinkSize();
            this.utf16Sink = new DirectUtf16Sink(utf8SinkSize);
            this.utf8Sink = new DirectUtf8Sink(utf8SinkSize);
            this.typeManager = new TypeManager(textConfiguration, utf16Sink, utf8Sink, new Decimal256());
            this.textDelimiterScanner = new TextDelimiterScanner(textConfiguration);
            this.textMetadataDetector = new TextMetadataDetector(typeManager, textConfiguration);

            this.targetTableStructure = new TableStructureAdapter(configuration);
            this.targetTableStatus = -1;
            this.targetTableCreated = false;

            this.atomicity = Atomicity.SKIP_COL;
            this.createdWorkDir = false;
            this.otherToTimestampAdapterPool = new ObjectPool<>(OtherToTimestampAdapter::new, 4);
            this.inputFilePath = new Path();
            this.tmpPath = new Path();

            this.chunkStats = new LongList();
            this.indexChunkStats = new LongList();
            this.partitionKeysAndSizes = new LongList();
            this.partitionNameSink = new StringSink();
            this.partitions = new ObjList<>();
            this.taskDistribution = new IntList();
            this.symbolCapacities = new IntList();
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    // Load balances existing partitions between given number of workers using partition sizes.
    // Returns number of tasks.
    public static int assignPartitions(ObjList<PartitionInfo> partitions, int workerCount) {
        partitions.sort((p1, p2) -> Long.compare(p2.bytes, p1.bytes));
        long[] workerSums = new long[workerCount];

        for (int i = 0, n = partitions.size(); i < n; i++) {
            int minIdx = -1;
            long minSum = Long.MAX_VALUE;

            for (int j = 0; j < workerCount; j++) {
                if (workerSums[j] == 0) {
                    minIdx = j;
                    break;
                } else if (workerSums[j] < minSum) {
                    minSum = workerSums[j];
                    minIdx = j;
                }
            }

            workerSums[minIdx] += partitions.getQuick(i).bytes;
            partitions.getQuick(i).taskId = minIdx;
        }

        partitions.sort((p1, p2) -> {
            long workerIdDiff = p1.taskId - p2.taskId;
            if (workerIdDiff != 0) {
                return (int) workerIdDiff;
            }

            return Long.compare(p1.key, p2.key);
        });

        int taskIds = 0;
        for (int i = 0, n = workerSums.length; i < n; i++) {
            if (workerSums[i] != 0) {
                taskIds++;
            }
        }

        return taskIds;
    }

    public static void createTable(
            final FilesFacade ff,
            int mkDirMode,
            final CharSequence root,
            final CharSequence tableDir,
            final CharSequence tableName,
            TableStructure structure,
            int tableId,
            SecurityContext securityContext
    ) {
        try (Path path = new Path()) {
            switch (TableUtils.exists(ff, path, root, tableDir)) {
                case TableUtils.TABLE_EXISTS:
                    if (!ff.rmdir(path)) {
                        LOG.error()
                                .$("could not overwrite table [tableName='").$safe(tableName)
                                .$("',path='").$(path)
                                .$(", errno=").$(ff.errno())
                                .I$();
                        throw CairoException.critical(ff.errno()).put("could not overwrite [tableName=").put(tableName).put("]");
                    }
                case TableUtils.TABLE_DOES_NOT_EXIST:
                    securityContext.authorizeTableCreate();
                    try (MemoryMARW memory = Vm.getCMARWInstance()) {
                        TableUtils.createTable(
                                ff,
                                root,
                                mkDirMode,
                                memory,
                                path,
                                tableDir,
                                structure,
                                ColumnType.VERSION,
                                tableId
                        );
                    }
                    break;
                default:
                    throw TextException.$("name is reserved [tableName=").put(tableName).put(']');
            }
        }
    }

    @Override
    public void clear() {
        writer = Misc.free(writer);
        metadata = null;
        importId = -1;
        Misc.clear(chunkStats);
        Misc.clear(indexChunkStats);
        Misc.clear(partitionKeysAndSizes);
        Misc.clear(partitionNameSink);
        Misc.clear(taskDistribution);
        Misc.clear(utf16Sink);
        Misc.clear(utf8Sink);
        Misc.clear(typeManager);
        Misc.clear(symbolCapacities);
        Misc.clear(textMetadataDetector);
        Misc.clear(otherToTimestampAdapterPool);
        Misc.clear(partitions);
        linesIndexed = 0;
        rowsHandled = 0;
        rowsImported = 0;
        errors = 0;
        phaseErrors = 0;
        inputFileName = null;
        tableName = null;
        tableToken = null;
        timestampColumn = null;
        timestampIndex = -1;
        partitionBy = -1;
        columnDelimiter = -1;
        timestampAdapter = null;
        forceHeader = false;
        status = CopyImportTask.STATUS_STARTED;
        phase = CopyImportTask.PHASE_SETUP;
        errorMessage = null;
        targetTableStatus = -1;
        targetTableCreated = false;
        atomicity = Atomicity.SKIP_COL;
        taskCount = -1;
        createdWorkDir = false;
    }

    @Override
    public void close() {
        clear();
        Misc.free(this.inputFilePath);
        Misc.free(this.tmpPath);
        Misc.free(utf16Sink);
        Misc.free(this.utf8Sink);
        Misc.free(this.textMetadataDetector);
        Misc.free(this.textDelimiterScanner);
        Misc.free(this.localImportJob);
    }

    public void of(
            String tableName,
            String inputFileName,
            long importId,
            int partitionBy,
            byte columnDelimiter,
            String timestampColumn,
            String timestampFormat,
            boolean forceHeader,
            ExecutionCircuitBreaker circuitBreaker,
            int atomicity
    ) {
        clear();
        this.circuitBreaker = circuitBreaker;
        this.tableName = tableName;
        this.tableToken = cairoEngine.lockTableName(tableName);
        if (tableToken == null) {
            tableToken = cairoEngine.verifyTableName(tableName);
        }
        this.importRoot = tmpPath.of(inputWorkRoot).concat(tableToken).toString();
        this.inputFileName = inputFileName;
        this.timestampColumn = timestampColumn;
        this.partitionBy = partitionBy;
        this.columnDelimiter = columnDelimiter;
        if (timestampFormat != null) {
            DateFormat dateFormat = TypeManager.adaptiveGetTimestampFormat(timestampFormat);
            this.timestampAdapter = (TimestampAdapter) typeManager.nextTimestampAdapter(
                    false,
                    dateFormat,
                    configuration.getTextConfiguration().getDefaultDateLocale(),
                    timestampFormat
            );
        }
        this.forceHeader = forceHeader;
        this.timestampIndex = -1;
        this.status = CopyImportTask.STATUS_STARTED;
        this.phase = CopyImportTask.PHASE_SETUP;
        this.targetTableStatus = -1;
        this.targetTableCreated = false;
        this.atomicity = Atomicity.isValid(atomicity) ? atomicity : Atomicity.SKIP_ROW;
        this.importId = importId;
        inputFilePath.of(inputRoot).concat(inputFileName).$();
    }

    @TestOnly
    public void of(
            String tableName,
            String inputFileName,
            long importId,
            int partitionBy,
            byte columnDelimiter,
            String timestampColumn,
            String tsFormat,
            boolean forceHeader,
            ExecutionCircuitBreaker circuitBreaker
    ) {
        of(
                tableName,
                inputFileName,
                importId,
                partitionBy,
                columnDelimiter,
                timestampColumn,
                tsFormat,
                forceHeader,
                circuitBreaker,
                Atomicity.SKIP_COL
        );
    }

    @TestOnly
    public void of(
            String tableName,
            String inputFileName,
            long importId,
            int partitionBy,
            byte columnDelimiter,
            String timestampColumn,
            String timestampFormat,
            boolean forceHeader
    ) {
        of(
                tableName,
                inputFileName,
                importId,
                partitionBy,
                columnDelimiter,
                timestampColumn,
                timestampFormat,
                forceHeader,
                null,
                Atomicity.SKIP_COL
        );
    }

    public void parseStructure(long fd, SecurityContext securityContext) throws TextImportException {
        phasePrologue(CopyImportTask.PHASE_ANALYZE_FILE_STRUCTURE);
        final CairoConfiguration configuration = cairoEngine.getConfiguration();

        final int textAnalysisMaxLines = configuration.getTextConfiguration().getTextAnalysisMaxLines();
        int len = configuration.getSqlCopyBufferSize();
        long buf = Unsafe.malloc(len, MemoryTag.NATIVE_IMPORT);

        try (TextLexerWrapper tlw = new TextLexerWrapper(configuration.getTextConfiguration())) {
            long n = ff.read(fd, buf, len, 0);
            if (n > 0) {
                if (columnDelimiter < 0) {
                    columnDelimiter = textDelimiterScanner.scan(buf, buf + n);
                }

                AbstractTextLexer lexer = tlw.getLexer(columnDelimiter);
                lexer.setSkipLinesWithExtraValues(false);

                final ObjList<CharSequence> names = new ObjList<>();
                final ObjList<TypeAdapter> types = new ObjList<>();
                if (timestampColumn != null && timestampAdapter != null) {
                    names.add(timestampColumn);
                    types.add(timestampAdapter);
                }

                textMetadataDetector.of(tableName, names, types, forceHeader);
                lexer.parse(buf, buf + n, textAnalysisMaxLines, textMetadataDetector);
                textMetadataDetector.evaluateResults(lexer.getLineCount(), lexer.getErrorCount());
                forceHeader = textMetadataDetector.isHeader();

                prepareTable(
                        textMetadataDetector.getColumnNames(),
                        textMetadataDetector.getColumnTypes(),
                        inputFilePath,
                        typeManager,
                        securityContext
                );
                phaseEpilogue(CopyImportTask.PHASE_ANALYZE_FILE_STRUCTURE);
            } else {
                throw TextException.$("could not read from file '").put(inputFilePath).put("' to analyze structure");
            }
        } catch (CairoException e) {
            throw TextImportException.instance(CopyImportTask.PHASE_ANALYZE_FILE_STRUCTURE, e.getFlyweightMessage(), e.getErrno());
        } catch (TextException e) {
            throw TextImportException.instance(CopyImportTask.PHASE_ANALYZE_FILE_STRUCTURE, e.getFlyweightMessage());
        } finally {
            Unsafe.free(buf, len, MemoryTag.NATIVE_IMPORT);
        }
    }

    // returns list with N chunk boundaries
    public LongList phaseBoundaryCheck(long fileLength) throws TextImportException {
        phasePrologue(CopyImportTask.PHASE_BOUNDARY_CHECK);
        assert (workerCount > 0 && minChunkSize > 0);

        if (workerCount == 1) {
            indexChunkStats.setPos(0);
            indexChunkStats.add(0);
            indexChunkStats.add(0);
            indexChunkStats.add(fileLength);
            indexChunkStats.add(0);
            phaseEpilogue(CopyImportTask.PHASE_BOUNDARY_CHECK);
            return indexChunkStats;
        }

        long chunkSize = Math.max(minChunkSize, (fileLength + workerCount - 1) / workerCount);
        final int chunks = (int) Math.max((fileLength + chunkSize - 1) / chunkSize, 1);

        int queuedCount = 0;
        int collectedCount = 0;

        chunkStats.setPos(chunks * 5);
        chunkStats.zero(0);

        for (int i = 0; i < chunks; i++) {
            final long chunkLo = i * chunkSize;
            final long chunkHi = Long.min(chunkLo + chunkSize, fileLength);
            while (true) {
                final long seq = pubSeq.next();
                if (seq > -1) {
                    final CopyImportTask task = queue.get(seq);
                    task.setChunkIndex(i);
                    task.setCircuitBreaker(circuitBreaker);
                    task.ofPhaseBoundaryCheck(ff, inputFilePath, chunkLo, chunkHi);
                    pubSeq.done(seq);
                    queuedCount++;
                    break;
                } else {
                    collectedCount += collect(queuedCount - collectedCount, collectChunkStatsRef);
                }
            }
        }

        collectedCount += collect(queuedCount - collectedCount, collectChunkStatsRef);
        assert collectedCount == queuedCount;

        processChunkStats(fileLength, chunks);
        phaseEpilogue(CopyImportTask.PHASE_BOUNDARY_CHECK);
        return indexChunkStats;
    }

    public void phaseIndexing() throws TextException {
        phasePrologue(CopyImportTask.PHASE_INDEXING);

        int queuedCount = 0;
        int collectedCount = 0;

        createWorkDir();

        boolean forceHeader = this.forceHeader;
        for (int i = 0, n = indexChunkStats.size() - 2; i < n; i += 2) {
            int colIdx = i / 2;

            final long chunkLo = indexChunkStats.get(i);
            final long lineNumber = indexChunkStats.get(i + 1);
            final long chunkHi = indexChunkStats.get(i + 2);

            while (true) {
                final long seq = pubSeq.next();
                if (seq > -1) {
                    final CopyImportTask task = queue.get(seq);
                    task.setChunkIndex(colIdx);
                    task.setCircuitBreaker(circuitBreaker);
                    task.ofPhaseIndexing(
                            chunkLo,
                            chunkHi,
                            lineNumber,
                            colIdx,
                            inputFileName,
                            importRoot,
                            metadata.getTimestampType(),
                            partitionBy,
                            columnDelimiter,
                            timestampIndex,
                            timestampAdapter,
                            forceHeader,
                            atomicity
                    );
                    if (forceHeader) {
                        forceHeader = false;
                    }
                    pubSeq.done(seq);
                    queuedCount++;
                    break;
                } else {
                    collectedCount += collect(queuedCount - collectedCount, collectIndexStatsRef);
                }
            }
        }

        collectedCount += collect(queuedCount - collectedCount, collectIndexStatsRef);
        assert collectedCount == queuedCount;
        processIndexStats();

        phaseEpilogue(CopyImportTask.PHASE_INDEXING);
    }

    public void process(SecurityContext securityContext) throws TextImportException {
        final long startMs = getCurrentTimeMs();

        long fd = -1;
        try {
            try {
                updateImportStatus(CopyImportTask.STATUS_STARTED, Numbers.LONG_NULL, Numbers.LONG_NULL, 0);

                try {
                    fd = TableUtils.openRO(ff, inputFilePath.$(), LOG);
                } catch (CairoException e) {
                    throw TextImportException.instance(CopyImportTask.PHASE_SETUP, e.getFlyweightMessage(), e.getErrno());
                }

                long length = ff.length(fd);
                if (length < 1) {
                    throw TextImportException.instance(CopyImportTask.PHASE_SETUP, "ignored empty input file [file='").put(inputFilePath).put(']');
                }

                try {
                    parseStructure(fd, securityContext);
                    phaseBoundaryCheck(length);
                    phaseIndexing();
                    phasePartitionImport();
                    phaseSymbolTableMerge();
                    phaseUpdateSymbolKeys();
                    phaseBuildSymbolIndex();
                    movePartitions();
                    attachPartitions();
                    updateImportStatus(CopyImportTask.STATUS_FINISHED, rowsHandled, rowsImported, errors);
                } catch (Throwable t) {
                    cleanUp();
                    throw t;
                } finally {
                    closeWriter();
                    if (createdWorkDir) {
                        removeWorkDir();
                    }
                }
                // these are the leftovers that also need to be converted
            } catch (CairoException e) {
                throw TextImportException.instance(CopyImportTask.PHASE_CLEANUP, e.getFlyweightMessage(), e.getErrno());
            } catch (TextException e) {
                throw TextImportException.instance(CopyImportTask.PHASE_CLEANUP, e.getFlyweightMessage());
            } finally {
                ff.close(fd);
            }
        } catch (TextImportException e) {
            LOG.error()
                    .$("could not import [phase=").$(CopyImportTask.getPhaseName(e.getPhase()))
                    .$(", ex=").$safe(e.getFlyweightMessage())
                    .I$();
            throw e;
        }

        long endMs = getCurrentTimeMs();
        LOG.info()
                .$("import complete [importId=").$hexPadded(importId)
                .$(", file=`").$(inputFilePath).$('`')
                .$("', time=").$((endMs - startMs) / 1000).$("s").I$();
    }

    public void setMinChunkSize(int minChunkSize) {
        this.minChunkSize = minChunkSize;
    }

    public void setStatusReporter(final PhaseStatusReporter reporter) {
        this.statusReporter = reporter;
    }

    public void updateImportStatus(byte status, long rowsHandled, long rowsImported, long errors) {
        if (this.statusReporter != null) {
            this.statusReporter.report(CopyImportTask.NO_PHASE, status, null, rowsHandled, rowsImported, errors);
        }
    }

    public void updatePhaseStatus(byte phase, byte status, @Nullable final CharSequence msg) {
        if (this.statusReporter != null) {
            this.statusReporter.report(phase, status, msg, Numbers.LONG_NULL, Numbers.LONG_NULL, phaseErrors);
        }
    }

    private void attachPartitions() throws TextImportException {
        phasePrologue(CopyImportTask.PHASE_ATTACH_PARTITIONS);

        // Go descending, attaching last partition is more expensive than others
        for (int i = partitions.size() - 1; i > -1; i--) {
            PartitionInfo partition = partitions.getQuick(i);
            if (partition.importedRows == 0) {
                continue;
            }

            final CharSequence partitionDirName = partition.name;
            try {
                final long timestamp = PartitionBy.parsePartitionDirName(partitionDirName, metadata.getTimestampType(), partitionBy);
                writer.attachPartition(timestamp, partition.importedRows);
            } catch (CairoException e) {
                throw TextImportException.instance(CopyImportTask.PHASE_ATTACH_PARTITIONS, "could not attach [partition='")
                        .put(partitionDirName).put("', msg=")
                        .put('[').put(e.getErrno()).put("] ").put(e.getFlyweightMessage()).put(']');
            }
        }

        phaseEpilogue(CopyImportTask.PHASE_ATTACH_PARTITIONS);
    }

    private void cleanUp() {
        if (targetTableStatus == TableUtils.TABLE_EXISTS && writer != null) {
            writer.truncate();
        }
        closeWriter();
        if (targetTableStatus == TableUtils.TABLE_DOES_NOT_EXIST && targetTableCreated) {
            cairoEngine.dropTableOrMatView(tmpPath, tableToken);
        }
        if (tableToken != null) {
            cairoEngine.unlockTableName(tableToken);
        }
    }

    private void closeWriter() {
        writer = Misc.free(writer);
        metadata = null;
    }

    private int collect(int queuedCount, Consumer<CopyImportTask> consumer) {
        int collectedCount = 0;
        while (collectedCount < queuedCount) {
            final long seq = collectSeq.next();
            if (seq > -1) {
                CopyImportTask task = queue.get(seq);
                consumer.accept(task);
                task.clear();
                collectSeq.done(seq);
                collectedCount += 1;
            } else {
                stealWork();
            }
        }
        return collectedCount;
    }

    private void collectChunkStats(final CopyImportTask task) {
        updateStatus(task);
        final CopyImportTask.PhaseBoundaryCheck phaseBoundaryCheck = task.getCountQuotesPhase();
        final int chunkOffset = 5 * task.getChunkIndex();
        chunkStats.set(chunkOffset, phaseBoundaryCheck.getQuoteCount());
        chunkStats.set(chunkOffset + 1, phaseBoundaryCheck.getNewLineCountEven());
        chunkStats.set(chunkOffset + 2, phaseBoundaryCheck.getNewLineCountOdd());
        chunkStats.set(chunkOffset + 3, phaseBoundaryCheck.getNewLineOffsetEven());
        chunkStats.set(chunkOffset + 4, phaseBoundaryCheck.getNewLineOffsetOdd());
    }

    private void collectDataImportStats(final CopyImportTask task) {
        updateStatus(task);

        final CopyImportTask.PhasePartitionImport phase = task.getImportPartitionDataPhase();
        LongList rows = phase.getImportedRows();

        for (int i = 0, n = rows.size(); i < n; i += 2) {
            partitions.get((int) rows.get(i)).importedRows = rows.get(i + 1);
        }
        rowsHandled += phase.getRowsHandled();
        rowsImported += phase.getRowsImported();
        phaseErrors += phase.getErrors();
        errors += phase.getErrors();
    }

    private void collectIndexStats(final CopyImportTask task) {
        updateStatus(task);
        final CopyImportTask.PhaseIndexing phaseIndexing = task.getBuildPartitionIndexPhase();
        final LongList keys = phaseIndexing.getPartitionKeysAndSizes();
        this.partitionKeysAndSizes.add(keys);
        this.linesIndexed += phaseIndexing.getLineCount();
        this.phaseErrors += phaseIndexing.getErrorCount();
        this.errors += phaseIndexing.getErrorCount();
    }

    private void collectStub(final CopyImportTask task) {
        updateStatus(task);
    }

    private void createWorkDir() {
        // First, create the work root dir, if it doesn't exist.
        Path workDirPath = tmpPath.of(inputWorkRoot).slash();
        if (!ff.exists(workDirPath.$())) {
            int result = ff.mkdir(workDirPath.$(), configuration.getMkDirMode());
            if (result != 0) {
                throw CairoException.critical(ff.errno()).put("could not create import work root directory [path='").put(workDirPath).put("']");
            }
        }

        // Next, remove and recreate the per-table sub-dir.
        removeWorkDir();
        workDirPath = tmpPath.of(importRoot).slash();
        int result = ff.mkdir(workDirPath.$(), configuration.getMkDirMode());
        if (result != 0) {
            throw CairoException.critical(ff.errno()).put("could not create temporary import work directory [path='").put(workDirPath).put("']");
        }

        createdWorkDir = true;
        LOG.info().$("temporary import directory [path='").$(workDirPath).I$();
    }

    private long getCurrentTimeMs() {
        return configuration.getMillisecondClock().getTicks();
    }

    private int getTaskCount() {
        return taskDistribution.size() / 3;
    }

    private void initWriterAndOverrideImportMetadata(
            ObjList<CharSequence> names,
            ObjList<TypeAdapter> types,
            TypeManager typeManager,
            SecurityContext securityContext
    ) throws TextException {
        final TableWriter writer = cairoEngine.getWriter(tableToken, LOCK_REASON);
        final RecordMetadata metadata = GenericRecordMetadata.copyDense(writer.getMetadata());

        if (metadata.getColumnCount() < types.size()) {
            writer.close();
            throw TextException.$("column count mismatch [textColumnCount=").put(types.size())
                    .put(", tableColumnCount=").put(metadata.getColumnCount())
                    .put(", table=").put(tableName)
                    .put(']');
        }

        // remap index is only needed to adjust names and types
        // workers will import data into temp tables without remapping
        final IntList remapIndex = new IntList();
        remapIndex.setPos(types.size());
        for (int i = 0, n = types.size(); i < n; i++) {
            final int columnIndex = metadata.getColumnIndexQuiet(names.getQuick(i));
            final int idx = columnIndex > -1 ? columnIndex : i; // check for strict match ?
            remapIndex.set(i, idx);

            final int columnType = metadata.getColumnType(idx);
            final TypeAdapter detectedAdapter = types.getQuick(i);
            final int detectedType = detectedAdapter.getType();
            if (detectedType != columnType) {
                // when DATE type is mis-detected as STRING we
                // would not have either date format nor locale to
                // use when populating this field
                switch (ColumnType.tagOf(columnType)) {
                    case ColumnType.DATE:
                        logTypeError(i, detectedType);
                        types.setQuick(i, BadDateAdapter.INSTANCE);
                        break;
                    case ColumnType.TIMESTAMP:
                        // different timestamp type
                        if (detectedAdapter instanceof TimestampAdapter) {
                            ((TimestampAdapter) detectedAdapter).reCompileDateFormat(ColumnType.getTimestampDriver(columnType).getTimestampDateFormatFactory());
                        } else if (detectedAdapter instanceof TimestampCompatibleAdapter) {
                            types.setQuick(i, otherToTimestampAdapterPool.next().of((TimestampCompatibleAdapter) detectedAdapter, columnType));
                        } else {
                            logTypeError(i, detectedType);
                            types.setQuick(i, BadTimestampAdapter.INSTANCE);
                        }
                        break;
                    case ColumnType.BINARY:
                        writer.close();
                        throw TextException.$("cannot import text into BINARY column [index=").put(i).put(']');
                    default:
                        types.setQuick(i, typeManager.getTypeAdapter(columnType));
                        break;
                }
            }
        }

        // at this point we've to use target table columns names otherwise
        // partition attach could fail on metadata differences
        // (if header names or synthetic names are different from table's)
        for (int i = 0, n = remapIndex.size(); i < n; i++) {
            names.set(i, metadata.getColumnName(remapIndex.get(i)));
        }
        this.metadata = metadata;
        this.writer = writer;//next call can throw exception

        // authorize only columns present in the file
        securityContext.authorizeInsert(tableToken);

        // add table columns missing in input file
        if (names.size() < metadata.getColumnCount()) {
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                boolean unused = true;
                for (int r = 0, rn = remapIndex.size(); r < rn; r++) {
                    if (remapIndex.get(r) == i) {
                        unused = false;
                        break;
                    }
                }

                if (unused) {
                    names.add(metadata.getColumnName(i));
                    types.add(typeManager.getTypeAdapter(metadata.getColumnType(i)));
                    remapIndex.add(i);
                }
            }
        }

        // copy symbol capacities from the destination table to avoid
        // having default, undersized capacities in temporary tables
        symbolCapacities.setAll(remapIndex.size(), -1);
        for (int i = 0, n = remapIndex.size(); i < n; i++) {
            final int columnIndex = remapIndex.getQuick(i);
            if (ColumnType.isSymbol(metadata.getColumnType(columnIndex))) {
                final int columnWriterIndex = metadata.getWriterIndex(columnIndex);
                final MapWriter symbolWriter = writer.getSymbolMapWriter(columnWriterIndex);
                symbolCapacities.set(i, symbolWriter.getSymbolCapacity());
            }
        }
    }

    private boolean isOneOfMainDirectories(CharSequence p) {
        String path = normalize(p);
        if (path == null) {
            return false;
        }

        return path.equals(normalize(configuration.getConfRoot())) ||
                path.equals(normalize(configuration.getDbRoot())) ||
                path.equals(normalize(configuration.getDbDirectory())) ||
                path.equals(normalize(configuration.getCheckpointRoot())) ||
                path.equals(normalize(configuration.getBackupRoot()));
    }

    private void logTypeError(int i, int type) {
        LOG.info()
                .$("mis-detected [table=").$safe(tableName)
                .$(", column=").$(i)
                .$(", type=").$(ColumnType.nameOf(type))
                .$(", workerCount=").$(workerCount)
                .I$();
    }

    private void movePartitions() {
        phasePrologue(CopyImportTask.PHASE_MOVE_PARTITIONS);
        final int taskCount = getTaskCount();

        try {
            for (int i = 0; i < taskCount; i++) {
                int index = taskDistribution.getQuick(i * 3);
                int lo = taskDistribution.getQuick(i * 3 + 1);
                int hi = taskDistribution.getQuick(i * 3 + 2);

                final Path srcPath = localImportJob.getTmpPath1().of(importRoot).concat(tableName).put('_').put(index);
                final Path dstPath = localImportJob.getTmpPath2().of(configuration.getDbRoot()).concat(tableToken);

                final int srcPlen = srcPath.size();
                final int dstPlen = dstPath.size();

                if (!ff.exists(dstPath.slash$())) {
                    if (ff.mkdirs(dstPath, configuration.getMkDirMode()) != 0) {
                        throw TextException.$("could not create partition directory [path='").put(dstPath).put("', errno=").put(ff.errno()).put(']');
                    }
                }

                for (int j = lo; j < hi; j++) {
                    PartitionInfo partition = partitions.get(j);
                    if (partition.importedRows == 0) {
                        continue;
                    }
                    final CharSequence partitionName = partition.name;

                    srcPath.trimTo(srcPlen).concat(partitionName);
                    dstPath.trimTo(dstPlen).concat(partitionName).put(configuration.getAttachPartitionSuffix());

                    int res = ff.rename(srcPath.slash$(), dstPath.slash$());

                    if (res == Files.FILES_RENAME_ERR_EXDEV) {
                        LOG.info().$(srcPath).$(" and ").$(dstPath).$(" are not on the same mounted filesystem. Partitions will be copied.").$();

                        if (ff.mkdirs(dstPath, configuration.getMkDirMode()) != 0) {
                            throw TextException.$("could not create partition directory [path='").put(dstPath).put("', errno=").put(ff.errno()).put(']');
                        }

                        ff.iterateDir(
                                srcPath.$(), (long name, int type) -> {
                                    if (type == Files.DT_FILE) {
                                        srcPath.trimTo(srcPlen).concat(partitionName).concat(name);
                                        dstPath.trimTo(dstPlen).concat(partitionName).put(configuration.getAttachPartitionSuffix()).concat(name);
                                        if (ff.copy(srcPath.$(), dstPath.$()) < 0) {
                                            throw TextException.$("could not copy partition file [to='").put(dstPath).put("', errno=").put(ff.errno()).put(']');
                                        }
                                    }
                                }
                        );
                        srcPath.parent();
                    } else if (res != Files.FILES_RENAME_OK) {
                        throw CairoException.critical(ff.errno()).put("could not copy partition file [to=").put(dstPath).put(']');
                    }
                }
            }
        } catch (CairoException e) {
            throw TextImportException.instance(CopyImportTask.PHASE_MOVE_PARTITIONS, e.getFlyweightMessage(), e.getErrno());
        } catch (TextException e) {
            throw TextImportException.instance(CopyImportTask.PHASE_MOVE_PARTITIONS, e.getFlyweightMessage());
        }
        phaseEpilogue(CopyImportTask.PHASE_MOVE_PARTITIONS);
    }

    private String normalize(CharSequence c) {
        try {
            if (c == null) {
                return null;
            }
            return new File(c.toString()).getCanonicalPath().replace(File.separatorChar, '/');
        } catch (IOException e) {
            LOG.error().$("could not normalize [path='").$(c).$("', message=").$(e.getMessage()).I$();
            return null;
        }
    }

    private void phaseBuildSymbolIndex() throws TextImportException {
        phasePrologue(CopyImportTask.PHASE_BUILD_SYMBOL_INDEX);

        final int columnCount = metadata.getColumnCount();
        final int tmpTableCount = getTaskCount();

        boolean isAnyIndexed = false;
        for (int i = 0; i < columnCount; i++) {
            isAnyIndexed |= metadata.isColumnIndexed(i);
        }

        if (isAnyIndexed) {
            int queuedCount = 0;
            int collectedCount = 0;
            for (int t = 0; t < tmpTableCount; ++t) {
                while (true) {
                    final long seq = pubSeq.next();
                    if (seq > -1) {
                        final CopyImportTask task = queue.get(seq);
                        task.setChunkIndex(t);
                        task.setCircuitBreaker(circuitBreaker);
                        // this task will create its own copy of TableWriter to build indexes concurrently?
                        task.ofPhaseBuildSymbolIndex(cairoEngine, targetTableStructure, importRoot, t, metadata);
                        pubSeq.done(seq);
                        queuedCount++;
                        break;
                    } else {
                        collectedCount += collect(queuedCount - collectedCount, checkStatusRef);
                    }
                }
            }

            collectedCount += collect(queuedCount - collectedCount, checkStatusRef);
            assert collectedCount == queuedCount;
        }

        phaseEpilogue(CopyImportTask.PHASE_BUILD_SYMBOL_INDEX);
    }

    private void phaseEpilogue(byte phase) {
        throwErrorIfNotOk();
        long endMs = getCurrentTimeMs();
        LOG.info()
                .$("finished [importId=").$hexPadded(importId)
                .$(", phase=").$(CopyImportTask.getPhaseName(phase))
                .$(", file=`").$(inputFilePath)
                .$("`, duration=").$((endMs - startMs) / 1000).$('s')
                .$(", errors=").$(phaseErrors)
                .I$();
        updatePhaseStatus(phase, CopyImportTask.STATUS_FINISHED, null);
    }

    private void phasePartitionImport() throws TextImportException {
        if (partitions.size() == 0) {
            if (linesIndexed > 0) {
                throw TextImportException.instance(
                        CopyImportTask.PHASE_PARTITION_IMPORT,
                        "All rows were skipped. Possible reasons: timestamp format mismatch or rows exceed maximum line length (65k)."
                );
            } else {
                throw TextImportException.instance(
                        CopyImportTask.PHASE_PARTITION_IMPORT,
                        "No rows in input file to import."
                );
            }
        }

        phasePrologue(CopyImportTask.PHASE_PARTITION_IMPORT);
        this.taskCount = assignPartitions(partitions, workerCount);

        int queuedCount = 0;
        int collectedCount = 0;
        taskDistribution.clear();

        for (int i = 0; i < taskCount; ++i) {
            int lo = 0;
            while (lo < partitions.size() && partitions.getQuick(lo).taskId != i) {
                lo++;
            }
            int hi = lo + 1;
            while (hi < partitions.size() && partitions.getQuick(hi).taskId == i) {
                hi++;
            }

            while (true) {
                final long seq = pubSeq.next();
                if (seq > -1) {
                    final CopyImportTask task = queue.get(seq);
                    task.setChunkIndex(i);
                    task.setCircuitBreaker(circuitBreaker);
                    task.ofPhasePartitionImport(
                            cairoEngine,
                            targetTableStructure,
                            textMetadataDetector.getColumnTypes(),
                            atomicity,
                            columnDelimiter,
                            importRoot,
                            inputFileName,
                            i,
                            lo,
                            hi,
                            partitions
                    );
                    pubSeq.done(seq);
                    queuedCount++;
                    break;
                } else {
                    collectedCount += collect(queuedCount - collectedCount, collectDataImportStatsRef);
                }
            }

            taskDistribution.add(i);
            taskDistribution.add(lo);
            taskDistribution.add(hi);
        }

        collectedCount += collect(queuedCount - collectedCount, collectDataImportStatsRef);
        assert collectedCount == queuedCount;

        phaseEpilogue(CopyImportTask.PHASE_PARTITION_IMPORT);
    }

    private void phasePrologue(byte phase) {
        phaseErrors = 0;
        LOG.info()
                .$("started [importId=").$hexPadded(importId)
                .$(", phase=").$(CopyImportTask.getPhaseName(phase))
                .$(", file=`").$(inputFilePath)
                .$("`, workerCount=").$(workerCount).I$();
        updatePhaseStatus(phase, CopyImportTask.STATUS_STARTED, null);
        startMs = getCurrentTimeMs();
    }

    private void phaseSymbolTableMerge() throws TextImportException {
        phasePrologue(CopyImportTask.PHASE_SYMBOL_TABLE_MERGE);
        final int tmpTableCount = getTaskCount();

        int queuedCount = 0;
        int collectedCount = 0;

        for (int columnIndex = 0, size = metadata.getColumnCount(); columnIndex < size; columnIndex++) {
            if (ColumnType.isSymbol(metadata.getColumnType(columnIndex))) {
                final CharSequence symbolColumnName = metadata.getColumnName(columnIndex);
                int tempTableDenseSymbolIndex = targetTableStructure.getDenseSymbolIndex(symbolColumnName);
                int tempTableColumnIndex = targetTableStructure.getColumnIndex(symbolColumnName);

                while (true) {
                    final long seq = pubSeq.next();
                    if (seq > -1) {
                        final CopyImportTask task = queue.get(seq);
                        task.setChunkIndex(columnIndex);
                        task.ofPhaseSymbolTableMerge(
                                configuration,
                                importRoot,
                                writer,
                                tableToken,
                                symbolColumnName,
                                metadata.getWriterIndex(columnIndex),
                                tempTableDenseSymbolIndex,
                                tempTableColumnIndex,
                                tmpTableCount,
                                partitionBy,
                                metadata.getTimestampType()
                        );
                        pubSeq.done(seq);
                        queuedCount++;
                        break;
                    } else {
                        collectedCount += collect(queuedCount - collectedCount, collectStubRef);
                    }
                }
            }
        }

        collectedCount += collect(queuedCount - collectedCount, collectStubRef);
        assert collectedCount == queuedCount;

        phaseEpilogue(CopyImportTask.PHASE_SYMBOL_TABLE_MERGE);
    }

    private void phaseUpdateSymbolKeys() throws TextImportException {
        phasePrologue(CopyImportTask.PHASE_UPDATE_SYMBOL_KEYS);

        final int tmpTableCount = getTaskCount();
        int queuedCount = 0;
        int collectedCount = 0;
        for (int tempTableIndex = 0; tempTableIndex < tmpTableCount; ++tempTableIndex) {

            tmpPath.of(importRoot).concat(tableToken.getTableName()).put('_').put(tempTableIndex);

            try (TxReader txFile = new TxReader(ff).ofRO(tmpPath.concat(TXN_FILE_NAME).$(), metadata.getTimestampType(), partitionBy)) {
                txFile.unsafeLoadAll();
                final int partitionCount = txFile.getPartitionCount();

                for (int p = 0; p < partitionCount; p++) {
                    final long partitionSize = txFile.getPartitionSize(p);
                    final long partitionTimestamp = txFile.getPartitionTimestampByIndex(p);
                    int symbolColumnIndex = 0;

                    if (partitionSize == 0) {
                        continue;
                    }

                    for (int c = 0, size = metadata.getColumnCount(); c < size; c++) {
                        if (ColumnType.isSymbol(metadata.getColumnType(c))) {
                            final CharSequence symbolColumnName = metadata.getColumnName(c);
                            final int symbolCount = txFile.getSymbolValueCount(symbolColumnIndex++);

                            while (true) {
                                final long seq = pubSeq.next();
                                if (seq > -1) {
                                    final CopyImportTask task = queue.get(seq);
                                    task.setChunkIndex(tempTableIndex);
                                    task.setCircuitBreaker(circuitBreaker);
                                    task.ofPhaseUpdateSymbolKeys(
                                            cairoEngine,
                                            targetTableStructure,
                                            tempTableIndex,
                                            partitionSize,
                                            partitionTimestamp,
                                            importRoot,
                                            symbolColumnName,
                                            symbolCount
                                    );
                                    pubSeq.done(seq);
                                    queuedCount++;
                                    break;
                                } else {
                                    collectedCount += collect(queuedCount - collectedCount, collectStubRef);
                                }
                            }
                        }
                    }
                }
            }
        }

        collectedCount += collect(queuedCount - collectedCount, collectStubRef);
        assert collectedCount == queuedCount;

        phaseEpilogue(CopyImportTask.PHASE_UPDATE_SYMBOL_KEYS);
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

    private void processIndexStats() {
        LongHashSet set = new LongHashSet();
        for (int i = 0, n = partitionKeysAndSizes.size(); i < n; i += 2) {
            set.add(partitionKeysAndSizes.get(i));
        }

        LongList distinctKeys = new LongList();
        for (int i = 0, n = set.size(); i < n; i++) {
            distinctKeys.add(set.get(i));
        }
        distinctKeys.sort();

        LongList totalSizes = new LongList();
        for (int i = 0, n = distinctKeys.size(); i < n; i++) {
            long key = distinctKeys.getQuick(i);
            long size = 0;

            for (int j = 0, m = partitionKeysAndSizes.size(); j < m; j += 2) {
                if (partitionKeysAndSizes.getQuick(j) == key) {
                    size += partitionKeysAndSizes.get(j + 1);
                }
            }

            totalSizes.add(size);
        }

        // for now CSV importer supports
        DateFormat dirFormat = PartitionBy.getPartitionDirFormatMethod(metadata.getTimestampType(), partitionBy);

        for (int i = 0, n = distinctKeys.size(); i < n; i++) {
            long key = distinctKeys.getQuick(i);
            long size = totalSizes.getQuick(i);

            partitionNameSink.clear();
            dirFormat.format(distinctKeys.get(i), EN_LOCALE, null, partitionNameSink);
            String dirName = partitionNameSink.toString();

            partitions.add(new PartitionInfo(key, dirName, size));
        }
    }

    private void removeWorkDir() {
        Path workDirPath = tmpPath.of(importRoot);
        if (ff.exists(workDirPath.$())) {
            if (isOneOfMainDirectories(importRoot)) {
                throw TextException.$("could not remove import work directory because it points to one of main directories [path='").put(workDirPath).put("'] .");
            }

            LOG.info().$("removing import work directory [path='").$(workDirPath).$("']").$();

            if (!ff.rmdir(workDirPath)) {
                throw TextException.$("could not remove import work directory [path='").put(workDirPath).put("', errno=").put(ff.errno()).put(']');
            }
        }
    }

    private void stealWork() {
        if (localImportJob.run(0, Job.RUNNING_STATUS)) {
            return;
        }
        Os.pause();
    }

    private void throwErrorIfNotOk() {
        if (status == CopyImportTask.STATUS_FAILED) {
            throw TextImportException.instance(phase, "import failed [phase=")
                    .put(CopyImportTask.getPhaseName(phase))
                    .put(", msg=`").put(errorMessage).put("`]");
        } else if (status == CopyImportTask.STATUS_CANCELLED) {
            TextImportException ex = TextImportException.instance(phase, "import cancelled [phase=")
                    .put(CopyImportTask.getPhaseName(phase))
                    .put(", msg=`").put(errorMessage).put("`]");
            ex.setCancelled(true);
            throw ex;
        }
    }

    private void updateStatus(final CopyImportTask task) {
        boolean cancelledOrFailed = status == CopyImportTask.STATUS_FAILED || status == CopyImportTask.STATUS_CANCELLED;
        if (!cancelledOrFailed && (task.isFailed() || task.isCancelled())) {
            status = task.getStatus();
            phase = task.getPhase();
            errorMessage = task.getErrorMessage();
        }
    }

    void prepareTable(
            ObjList<CharSequence> names,
            ObjList<TypeAdapter> types,
            Path path,
            TypeManager typeManager,
            SecurityContext securityContext
    )
            throws TextException {
        if (types.size() == 0) {
            throw CairoException.nonCritical().put("cannot determine text structure");
        }
        if (partitionBy == PartitionBy.NONE) {
            throw CairoException.nonCritical().put("partition strategy for parallel import cannot be NONE");
        }

        if (partitionBy < 0) {
            partitionBy = PartitionBy.NONE;
        }

        if (timestampIndex == -1 && timestampColumn != null) {
            for (int i = 0, n = names.size(); i < n; i++) {
                if (Chars.equalsIgnoreCase(names.get(i), timestampColumn)) {
                    timestampIndex = i;
                    break;
                }
            }
        }

        try {
            targetTableStatus = cairoEngine.getTableStatus(path, tableToken);
            switch (targetTableStatus) {
                case TableUtils.TABLE_DOES_NOT_EXIST:
                    if (partitionBy == PartitionBy.NONE) {
                        throw TextException.$("partition by unit must be set when importing to new table");
                    }
                    if (timestampColumn == null) {
                        throw TextException.$("timestamp column must be set when importing to new table");
                    }
                    if (timestampIndex == -1) {
                        throw TextException.$("timestamp column '").put(timestampColumn).put("' not found in file header");
                    }

                    validate(names, types, null, NO_INDEX);
                    symbolCapacities.setAll(types.size(), -1);
                    targetTableStructure.of(tableName, names, types, symbolCapacities, timestampIndex, partitionBy);

                    createTable(
                            ff,
                            configuration.getMkDirMode(),
                            configuration.getDbRoot(),
                            tableToken.getDirName(),
                            targetTableStructure.getTableName(),
                            targetTableStructure,
                            tableToken.getTableId(),
                            securityContext
                    );
                    cairoEngine.registerTableToken(tableToken);
                    targetTableCreated = true;
                    writer = cairoEngine.getWriter(tableToken, LOCK_REASON);

                    try (MetadataCacheWriter metadataRW = cairoEngine.getMetadataCache().writeLock()) {
                        metadataRW.hydrateTable(tableToken);
                    }

                    metadata = GenericRecordMetadata.copyDense(writer.getMetadata());
                    partitionBy = writer.getPartitionBy();
                    break;
                case TableUtils.TABLE_EXISTS:
                    initWriterAndOverrideImportMetadata(names, types, typeManager, securityContext);

                    if (writer.getRowCount() > 0) {
                        throw TextException.$("target table must be empty [table=").put(tableName).put(']');
                    }

                    CharSequence designatedTimestampColumnName = writer.getDesignatedTimestampColumnName();
                    int designatedTimestampIndex = metadata.getTimestampIndex();
                    if (PartitionBy.isPartitioned(partitionBy) && partitionBy != writer.getPartitionBy()) {
                        throw TextException.$("declared partition by unit doesn't match table's");
                    }
                    partitionBy = writer.getPartitionBy();
                    if (!PartitionBy.isPartitioned(partitionBy)) {
                        throw TextException.$("target table is not partitioned");
                    }
                    validate(names, types, designatedTimestampColumnName, designatedTimestampIndex);
                    targetTableStructure.of(tableName, names, types, symbolCapacities, timestampIndex, partitionBy);
                    break;
                default:
                    throw TextException.$("name is reserved [table=").put(tableName).put(']');
            }

            inputFilePath.of(inputRoot).concat(inputFileName).$(); // getStatus might override it
            targetTableStructure.setIgnoreColumnIndexedFlag(true);

            if (timestampAdapter == null && ColumnType.isTimestamp(types.getQuick(timestampIndex).getType())) {
                timestampAdapter = (TimestampAdapter) types.getQuick(timestampIndex);
            }
        } catch (Throwable t) {
            closeWriter();
            throw t;
        }
    }

    void validate(
            ObjList<CharSequence> names,
            ObjList<TypeAdapter> types,
            CharSequence designatedTimestampColumnName,
            int designatedTimestampIndex
    ) throws TextException {
        if (timestampColumn == null && designatedTimestampColumnName == null) {
            timestampIndex = NO_INDEX;
        } else if (timestampColumn != null) {
            timestampIndex = names.indexOf(timestampColumn);
            if (timestampIndex == NO_INDEX) {
                throw TextException.$("invalid timestamp column [name='").put(timestampColumn).put("']");
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
                throw TextException.$("column is not a timestamp [no=").put(timestampIndex)
                        .put(", name='").put(timestampColumn).put("']");
            }
        }
    }

    @FunctionalInterface
    public interface PhaseStatusReporter {
        void report(byte phase, byte status, @Nullable final CharSequence msg, long rowsHandled, long rowsImported, long errors);
    }

    public static class PartitionInfo {
        final long bytes;
        final long key;
        final CharSequence name;
        long importedRows; // used to detect partitions that need skipping (because e.g. no data was imported for them)
        int taskId; // assigned worker/task id

        public PartitionInfo(long key, CharSequence name, long bytes) {
            this.key = key;
            this.name = name;
            this.bytes = bytes;
        }

        public PartitionInfo(long key, CharSequence name, long bytes, int taskId) {
            this.key = key;
            this.name = name;
            this.bytes = bytes;
            this.taskId = taskId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PartitionInfo that = (PartitionInfo) o;
            return key == that.key && bytes == that.bytes && taskId == that.taskId && importedRows == that.importedRows && name.equals(that.name);
        }

        @Override
        public String toString() {
            return "PartitionInfo{" +
                    "key=" + key +
                    ", name=" + name +
                    ", bytes=" + bytes +
                    ", taskId=" + taskId +
                    ", importedRows=" + importedRows +
                    '}';
        }
    }

    public static class TableStructureAdapter implements TableStructure {
        private final LongList columnBits = new LongList();
        private final CairoConfiguration configuration;
        private ObjList<CharSequence> columnNames;
        private boolean ignoreColumnIndexedFlag;
        private int partitionBy;
        private IntList symbolCapacities;
        private CharSequence tableName;
        private int timestampColumnIndex;

        public TableStructureAdapter(CairoConfiguration configuration) {
            this.configuration = configuration;
        }

        @Override
        public int getColumnCount() {
            return columnNames.size();
        }

        public int getColumnIndex(CharSequence symbolColumnName) {
            int index = -1;
            for (int i = 0, n = columnNames.size(); i < n; i++) {
                index++;
                if (Chars.equalsIgnoreCase(symbolColumnName, columnNames.get(i))) {
                    return index;
                }
            }
            return -1;
        }

        @Override
        public CharSequence getColumnName(int columnIndex) {
            return columnNames.getQuick(columnIndex);
        }

        @Override
        public int getColumnType(int columnIndex) {
            return Numbers.decodeLowInt(columnBits.getQuick(columnIndex));
        }

        public int getDenseSymbolIndex(CharSequence symbolColumnName) {
            int index = -1;
            for (int i = 0, n = columnNames.size(); i < n; i++) {
                if (getColumnType(i) == ColumnType.SYMBOL) {
                    index++;
                    if (Chars.equalsIgnoreCase(symbolColumnName, columnNames.get(i))) {
                        return index;
                    }
                }
            }
            return -1;
        }

        @Override
        public int getIndexBlockCapacity(int columnIndex) {
            return configuration.getIndexValueBlockSize();
        }

        @Override
        public int getMaxUncommittedRows() {
            return configuration.getMaxUncommittedRows();
        }

        @Override
        public long getO3MaxLag() {
            return configuration.getO3MaxLag();
        }

        @Override
        public int getPartitionBy() {
            return partitionBy;
        }

        @Override
        public boolean getSymbolCacheFlag(int columnIndex) {
            return false;
        }

        @Override
        public int getSymbolCapacity(int columnIndex) {
            final int capacity = symbolCapacities.getQuick(columnIndex);
            return capacity != -1 ? capacity : configuration.getDefaultSymbolCapacity();
        }

        @Override
        public CharSequence getTableName() {
            return tableName;
        }

        @Override
        public int getTimestampIndex() {
            return timestampColumnIndex;
        }

        @Override
        public boolean isDedupKey(int columnIndex) {
            return false;
        }

        @Override
        public boolean isIndexed(int columnIndex) {
            return !ignoreColumnIndexedFlag && Numbers.decodeHighInt(columnBits.getQuick(columnIndex)) != 0;
        }

        @Override
        public boolean isWalEnabled() {
            return configuration.getWalEnabledDefault() && PartitionBy.isPartitioned(partitionBy);
        }

        public void of(
                final CharSequence tableName,
                final ObjList<CharSequence> names,
                final ObjList<TypeAdapter> types,
                final IntList symbolCapacities,
                final int timestampColumnIndex,
                final int partitionBy
        ) {
            this.tableName = tableName;
            this.columnNames = names;
            this.symbolCapacities = symbolCapacities;
            this.ignoreColumnIndexedFlag = false;

            this.columnBits.clear();
            for (int i = 0, size = types.size(); i < size; i++) {
                final TypeAdapter adapter = types.getQuick(i);
                this.columnBits.add(Numbers.encodeLowHighInts(adapter.getType(), adapter.isIndexed() ? 1 : 0));
            }

            this.timestampColumnIndex = timestampColumnIndex;
            this.partitionBy = partitionBy;
        }

        public void setIgnoreColumnIndexedFlag(boolean flag) {
            this.ignoreColumnIndexedFlag = flag;
        }
    }
}
