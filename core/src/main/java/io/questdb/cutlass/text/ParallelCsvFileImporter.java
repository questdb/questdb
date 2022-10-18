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
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cutlass.text.types.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.DirectCharSink;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;


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
    private static final Log LOG = LogFactory.getLog(ParallelCsvFileImporter.class);

    private static final String LOCK_REASON = "parallel import";
    private static final int NO_INDEX = -1;

    private static final int DEFAULT_MIN_CHUNK_SIZE = 300 * 1024 * 1024;
    //holds result of first phase - boundary scanning
    //count of quotes, even new lines, odd new lines, offset to first even newline, offset to first odd newline
    private final LongList chunkStats;
    //holds input for second phase - indexing: offset and start line number for each chunk
    private final LongList indexChunkStats;
    private final LongList partitionKeysAndSizes;
    private final StringSink partitionNameSink;
    private final ObjList<PartitionInfo> partitions;
    //stores 3 values per task : index, lo, hi (lo, hi are indexes in partitionNames)
    private final IntList taskDistribution;
    private final FilesFacade ff;
    private final Path inputFilePath;
    private final Path tmpPath;
    private final RingQueue<TextImportTask> queue;
    private final Sequence pubSeq;
    private final Sequence collectSeq;
    private final int workerCount;
    private final CharSequence inputRoot;
    private final CharSequence inputWorkRoot;
    private final ObjectPool<OtherToTimestampAdapter> otherToTimestampAdapterPool;
    private final CairoSecurityContext securityContext;
    private final DirectCharSink utf8Sink;
    private final TypeManager typeManager;
    private final TextDelimiterScanner textDelimiterScanner;
    private final TextMetadataDetector textMetadataDetector;
    private final CairoEngine cairoEngine;
    private final CairoConfiguration configuration;
    private final TableStructureAdapter targetTableStructure;
    private final TextImportJob localImportJob;
    private int taskCount;
    private int minChunkSize = DEFAULT_MIN_CHUNK_SIZE;
    //path to import directory under, usually $inputWorkRoot/$tableName
    private CharSequence importRoot;
    private long importId;
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
    private int atomicity;
    //input params end
    //index of timestamp column in input file
    private int timestampIndex;
    private boolean targetTableCreated;
    private int targetTableStatus;
    //import status variables
    private byte status = TextImportTask.STATUS_STARTED;
    private byte phase = TextImportTask.PHASE_SETUP;
    private CharSequence errorMessage;
    private final Consumer<TextImportTask> checkStatusRef = this::updateStatus;
    private final Consumer<TextImportTask> collectChunkStatsRef = this::collectChunkStats;
    private final Consumer<TextImportTask> collectStubRef = this::collectStub;
    //incremented in phase 2
    private long linesIndexed;
    //row stats are incremented in phase 3
    private long rowsHandled;
    private long rowsImported;
    private long errors;
    private long phaseErrors;
    private final Consumer<TextImportTask> collectDataImportStatsRef = this::collectDataImportStats;
    private final Consumer<TextImportTask> collectIndexStatsRef = this::collectIndexStats;
    private long startMs;//start time of current phase (in millis)
    private boolean createdWorkDir;
    private ExecutionCircuitBreaker circuitBreaker;
    private PhaseStatusReporter statusReporter;

    public ParallelCsvFileImporter(CairoEngine cairoEngine, int workerCount) {
        if (workerCount < 1) {
            throw TextImportException.instance(TextImportTask.PHASE_SETUP, "Invalid worker count set [value=").put(workerCount).put(']');
        }

        MessageBus bus = cairoEngine.getMessageBus();
        RingQueue<TextImportTask> queue = bus.getTextImportQueue();
        if (queue.getCycle() < 1) {
            throw TextImportException.instance(TextImportTask.PHASE_SETUP, "Parallel import queue size cannot be zero!");
        }

        this.cairoEngine = cairoEngine;
        this.workerCount = workerCount;

        this.queue = queue;
        this.pubSeq = bus.getTextImportPubSeq();
        this.collectSeq = bus.getTextImportColSeq();
        this.localImportJob = new TextImportJob(bus);

        this.securityContext = AllowAllCairoSecurityContext.INSTANCE;
        this.configuration = cairoEngine.getConfiguration();

        this.ff = configuration.getFilesFacade();
        this.inputRoot = configuration.getSqlCopyInputRoot();
        this.inputWorkRoot = configuration.getSqlCopyInputWorkRoot();

        TextConfiguration textConfiguration = configuration.getTextConfiguration();
        this.utf8Sink = new DirectCharSink(textConfiguration.getUtf8SinkSize());
        this.typeManager = new TypeManager(textConfiguration, utf8Sink);
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
    }

    public static void createTable(
            final FilesFacade ff,
            int mkDirMode,
            final CharSequence root,
            final CharSequence tableName,
            TableStructure structure,
            int tableId,
            CairoConfiguration configuration
    ) {
        checkTableName(tableName, configuration);
        try (Path path = new Path()) {
            switch (TableUtils.exists(ff, path, root, tableName, 0, tableName.length())) {
                case TableUtils.TABLE_EXISTS:
                    int errno;
                    if ((errno = ff.rmdir(path)) != 0) {
                        LOG.error().$("remove failed [tableName='").utf8(tableName).$("',path='").utf8(path).$(", error=").$(errno).$(']').$();
                        throw CairoException.critical(errno).put("Table remove failed [tableName=").put(tableName).put("]");
                    }
                case TableUtils.TABLE_DOES_NOT_EXIST:
                    try (MemoryMARW memory = Vm.getMARWInstance()) {
                        TableUtils.createTable(
                                ff,
                                root,
                                mkDirMode,
                                memory,
                                path,
                                tableName,
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
        importId = -1;
        chunkStats.clear();
        indexChunkStats.clear();
        partitionKeysAndSizes.clear();
        partitionNameSink.clear();
        taskDistribution.clear();
        utf8Sink.clear();
        typeManager.clear();
        textMetadataDetector.clear();
        otherToTimestampAdapterPool.clear();
        partitions.clear();
        linesIndexed = 0;
        rowsHandled = 0;
        rowsImported = 0;
        errors = 0;
        phaseErrors = 0;
        inputFileName = null;
        tableName = null;
        timestampColumn = null;
        timestampIndex = -1;
        partitionBy = -1;
        columnDelimiter = -1;
        timestampAdapter = null;
        forceHeader = false;
        status = TextImportTask.STATUS_STARTED;
        phase = TextImportTask.PHASE_SETUP;
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
        this.inputFilePath.close();
        this.tmpPath.close();
        this.utf8Sink.close();
        this.textMetadataDetector.close();
        this.textDelimiterScanner.close();
        this.localImportJob.close();
    }

    public void of(
            CharSequence tableName,
            CharSequence inputFileName,
            long importId,
            int partitionBy,
            byte columnDelimiter,
            CharSequence timestampColumn,
            CharSequence timestampFormat,
            boolean forceHeader,
            ExecutionCircuitBreaker circuitBreaker,
            int atomicity
    ) {
        clear();
        this.circuitBreaker = circuitBreaker;
        this.tableName = tableName;
        this.importRoot = tmpPath.of(inputWorkRoot).concat(tableName).toString();
        this.inputFileName = inputFileName;
        this.timestampColumn = timestampColumn;
        this.partitionBy = partitionBy;
        this.columnDelimiter = columnDelimiter;
        if (timestampFormat != null) {
            DateFormat dateFormat = typeManager.getInputFormatConfiguration().getTimestampFormatFactory().get(timestampFormat);
            this.timestampAdapter = (TimestampAdapter) typeManager.nextTimestampAdapter(
                    false,
                    dateFormat,
                    configuration.getTextConfiguration().getDefaultDateLocale()
            );
        }
        this.forceHeader = forceHeader;
        this.timestampIndex = -1;
        this.status = TextImportTask.STATUS_STARTED;
        this.phase = TextImportTask.PHASE_SETUP;
        this.targetTableStatus = -1;
        this.targetTableCreated = false;
        this.atomicity = Atomicity.isValid(atomicity) ? atomicity : Atomicity.SKIP_ROW;
        this.importId = importId;
        inputFilePath.of(inputRoot).concat(inputFileName).$();
    }

    @TestOnly
    public void of(
            CharSequence tableName,
            CharSequence inputFileName,
            long importId,
            int partitionBy,
            byte columnDelimiter,
            CharSequence timestampColumn,
            CharSequence tsFormat,
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
            CharSequence tableName,
            CharSequence inputFileName,
            long importId,
            int partitionBy,
            byte columnDelimiter,
            CharSequence timestampColumn,
            CharSequence timestampFormat,
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

    public void process() throws TextImportException {
        final long startMs = getCurrentTimeMs();

        long fd = -1;
        try {
            try {
                updateImportStatus(TextImportTask.STATUS_STARTED, Numbers.LONG_NaN, Numbers.LONG_NaN, 0);

                try {
                    fd = TableUtils.openRO(ff, inputFilePath, LOG);
                } catch (CairoException e) {
                    throw TextImportException.instance(TextImportTask.PHASE_SETUP, e.getFlyweightMessage(), e.getErrno());
                }

                long length = ff.length(fd);
                if (length < 1) {
                    throw TextImportException.instance(TextImportTask.PHASE_SETUP, "ignored empty input file [file='").put(inputFilePath).put(']');
                }

                try (TableWriter writer = parseStructure(fd)) {
                    phaseBoundaryCheck(length);
                    phaseIndexing();
                    phasePartitionImport();
                    phaseSymbolTableMerge(writer);
                    phaseUpdateSymbolKeys(writer);
                    phaseBuildSymbolIndex(writer);
                    try {
                        movePartitions();
                        attachPartitions(writer);
                    } catch (Throwable t) {
                        cleanUp(writer);
                        throw t;
                    }
                    updateImportStatus(TextImportTask.STATUS_FINISHED, rowsHandled, rowsImported, errors);
                } catch (Throwable t) {
                    cleanUp();
                    throw t;
                } finally {
                    if (createdWorkDir) {
                        removeWorkDir();
                    }
                }
                // these are the leftovers that also need to be converted
            } catch (CairoException e) {
                throw TextImportException.instance(TextImportTask.PHASE_CLEANUP, e.getFlyweightMessage(), e.getErrno());
            } catch (TextException e) {
                throw TextImportException.instance(TextImportTask.PHASE_CLEANUP, e.getFlyweightMessage());
            } finally {
                if (fd != -1) {
                    ff.close(fd);
                }
            }
        } catch (TextImportException e) {
            LOG.error()
                    .$("could not import [phase=").$(TextImportTask.getPhaseName(e.getPhase()))
                    .$(", ex=").$(e.getFlyweightMessage())
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
            this.statusReporter.report(TextImportTask.NO_PHASE, status, null, rowsHandled, rowsImported, errors);
        }
    }

    public void updatePhaseStatus(byte phase, byte status, @Nullable final CharSequence msg) {
        if (this.statusReporter != null) {
            this.statusReporter.report(phase, status, msg, Numbers.LONG_NaN, Numbers.LONG_NaN, phaseErrors);
        }
    }

    private static void checkTableName(CharSequence tableName, CairoConfiguration configuration) {
        if (!TableUtils.isValidTableName(tableName, configuration.getMaxFileNameLength())) {
            throw CairoException.nonCritical()
                    .put("invalid table name [table=").putAsPrintable(tableName)
                    .put(']');
        }
    }

    //load balances existing partitions between given number of workers using partition sizes
    //returns number of tasks
    static int assignPartitions(ObjList<PartitionInfo> partitions, int workerCount) {
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

    private void attachPartitions(TableWriter writer) throws TextImportException {
        phasePrologue(TextImportTask.PHASE_ATTACH_PARTITIONS);

        // Go descending, attaching last partition is more expensive than others
        for (int i = partitions.size() - 1; i > -1; i--) {
            PartitionInfo partition = partitions.getQuick(i);
            if (partition.importedRows == 0) {
                continue;
            }

            final CharSequence partitionDirName = partition.name;
            try {
                final long timestamp = PartitionBy.parsePartitionDirName(partitionDirName, partitionBy);
                writer.attachPartition(timestamp, partition.importedRows);
            } catch (CairoException e) {
                throw TextImportException.instance(
                                TextImportTask.PHASE_ATTACH_PARTITIONS, "could not attach [partition='")
                        .put(partitionDirName).put("', msg=")
                        .put('[').put(e.getErrno()).put("] ").put(e.getFlyweightMessage()).put(']');
            }
        }

        phaseEpilogue(TextImportTask.PHASE_ATTACH_PARTITIONS);
    }

    private void cleanUp(TableWriter writer) {
        if (targetTableStatus == TableUtils.TABLE_EXISTS && writer != null) {
            writer.truncate();
        }
    }

    private void cleanUp() {
        if (targetTableStatus == TableUtils.TABLE_DOES_NOT_EXIST && targetTableCreated) {
            cairoEngine.remove(securityContext, tmpPath, tableName);
        }
    }

    private int collect(int queuedCount, Consumer<TextImportTask> consumer) {
        int collectedCount = 0;
        while (collectedCount < queuedCount) {
            final long seq = collectSeq.next();
            if (seq > -1) {
                TextImportTask task = queue.get(seq);
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

    private void collectChunkStats(final TextImportTask task) {
        updateStatus(task);
        final TextImportTask.PhaseBoundaryCheck phaseBoundaryCheck = task.getCountQuotesPhase();
        final int chunkOffset = 5 * task.getChunkIndex();
        chunkStats.set(chunkOffset, phaseBoundaryCheck.getQuoteCount());
        chunkStats.set(chunkOffset + 1, phaseBoundaryCheck.getNewLineCountEven());
        chunkStats.set(chunkOffset + 2, phaseBoundaryCheck.getNewLineCountOdd());
        chunkStats.set(chunkOffset + 3, phaseBoundaryCheck.getNewLineOffsetEven());
        chunkStats.set(chunkOffset + 4, phaseBoundaryCheck.getNewLineOffsetOdd());
    }

    private void collectDataImportStats(final TextImportTask task) {
        updateStatus(task);

        final TextImportTask.PhasePartitionImport phase = task.getImportPartitionDataPhase();
        LongList rows = phase.getImportedRows();

        for (int i = 0, n = rows.size(); i < n; i += 2) {
            partitions.get((int) rows.get(i)).importedRows = rows.get(i + 1);
        }
        rowsHandled += phase.getRowsHandled();
        rowsImported += phase.getRowsImported();
        phaseErrors += phase.getErrors();
        errors += phase.getErrors();
    }

    private void collectIndexStats(final TextImportTask task) {
        updateStatus(task);
        final TextImportTask.PhaseIndexing phaseIndexing = task.getBuildPartitionIndexPhase();
        final LongList keys = phaseIndexing.getPartitionKeysAndSizes();
        this.partitionKeysAndSizes.add(keys);
        this.linesIndexed += phaseIndexing.getLineCount();
        this.phaseErrors += phaseIndexing.getErrorCount();
        this.errors += phaseIndexing.getErrorCount();
    }

    private void collectStub(final TextImportTask task) {
        updateStatus(task);
    }

    private void createWorkDir() {
        // First, create the work root dir, if it doesn't exist.
        Path workDirPath = tmpPath.of(inputWorkRoot).slash$();
        if (!ff.exists(workDirPath)) {
            int result = ff.mkdir(workDirPath, configuration.getMkDirMode());
            if (result != 0) {
                throw CairoException.critical(ff.errno()).put("could not create import work root directory [path='").put(workDirPath).put("']");
            }
        }

        // Next, remove and recreate the per-table sub-dir.
        removeWorkDir();
        workDirPath = tmpPath.of(importRoot).slash$();
        int result = ff.mkdir(workDirPath, configuration.getMkDirMode());
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

    private boolean isOneOfMainDirectories(CharSequence p) {
        String path = normalize(p);
        if (path == null) {
            return false;
        }

        return path.equals(normalize(configuration.getConfRoot())) ||
                path.equals(normalize(configuration.getRoot())) ||
                path.equals(normalize(configuration.getDbDirectory())) ||
                path.equals(normalize(configuration.getSnapshotRoot())) ||
                path.equals(normalize(configuration.getBackupRoot()));
    }

    private void logTypeError(int i, int type) {
        LOG.info()
                .$("mis-detected [table=").$(tableName)
                .$(", column=").$(i)
                .$(", type=").$(ColumnType.nameOf(type))
                .$(", workerCount=").$(workerCount)
                .I$();
    }

    private void movePartitions() {
        phasePrologue(TextImportTask.PHASE_MOVE_PARTITIONS);
        final int taskCount = getTaskCount();

        try {
            for (int i = 0; i < taskCount; i++) {
                int index = taskDistribution.getQuick(i * 3);
                int lo = taskDistribution.getQuick(i * 3 + 1);
                int hi = taskDistribution.getQuick(i * 3 + 2);
                final Path srcPath = localImportJob.getTmpPath1().of(importRoot).concat(tableName).put("_").put(index);
                final Path dstPath = localImportJob.getTmpPath2().of(configuration.getRoot()).concat(tableName);
                final int srcPlen = srcPath.length();
                final int dstPlen = dstPath.length();

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

                        ff.iterateDir(srcPath, (long name, int type) -> {
                            if (type == Files.DT_FILE) {
                                srcPath.trimTo(srcPlen).concat(partitionName).concat(name).$();
                                dstPath.trimTo(dstPlen).concat(partitionName).put(configuration.getAttachPartitionSuffix()).concat(name).$();
                                if (ff.copy(srcPath, dstPath) < 0) {
                                    throw TextException.$("could not copy partition file [to='").put(dstPath).put("', errno=").put(ff.errno()).put(']');
                                }
                            }
                        });
                        srcPath.parent();
                    } else if (res != Files.FILES_RENAME_OK) {
                        throw CairoException.critical(ff.errno()).put("could not copy partition file [to=").put(dstPath).put(']');
                    }
                }
            }
        } catch (CairoException e) {
            throw TextImportException.instance(TextImportTask.PHASE_MOVE_PARTITIONS, e.getFlyweightMessage(), e.getErrno());
        } catch (TextException e) {
            throw TextImportException.instance(TextImportTask.PHASE_MOVE_PARTITIONS, e.getFlyweightMessage());
        }
        phaseEpilogue(TextImportTask.PHASE_MOVE_PARTITIONS);
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

    private TableWriter openWriterAndOverrideImportMetadata(
            ObjList<CharSequence> names,
            ObjList<TypeAdapter> types,
            CairoSecurityContext cairoSecurityContext,
            TypeManager typeManager
    ) throws TextException {
        TableWriter writer = cairoEngine.getWriter(cairoSecurityContext, tableName, LOCK_REASON);
        RecordMetadata metadata = writer.getMetadata();

        if (metadata.getColumnCount() < types.size()) {
            writer.close();
            throw TextException.$("column count mismatch [textColumnCount=").put(types.size())
                    .put(", tableColumnCount=").put(metadata.getColumnCount())
                    .put(", table=").put(tableName)
                    .put(']');
        }

        //remap index is only needed to adjust names and types
        //workers will import data into temp tables without remapping
        IntList remapIndex = new IntList();
        remapIndex.ensureCapacity(types.size());
        for (int i = 0, n = types.size(); i < n; i++) {

            final int columnIndex = metadata.getColumnIndexQuiet(names.getQuick(i));
            final int idx = (columnIndex > -1 && columnIndex != i) ? columnIndex : i; // check for strict match ?
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
                        if (detectedAdapter instanceof TimestampCompatibleAdapter) {
                            types.setQuick(i, otherToTimestampAdapterPool.next().of((TimestampCompatibleAdapter) detectedAdapter));
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

        //at this point we've to use target table columns names otherwise partition attach could fail on metadata differences
        //(if header names or synthetic names are different from table's)
        for (int i = 0, n = remapIndex.size(); i < n; i++) {
            names.set(i, metadata.getColumnName(remapIndex.get(i)));
        }

        //add table columns missing in input file
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
                }
            }
        }

        return writer;
    }

    TableWriter parseStructure(long fd) throws TextImportException {
        phasePrologue(TextImportTask.PHASE_ANALYZE_FILE_STRUCTURE);
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

                TableWriter writer = prepareTable(
                        securityContext,
                        textMetadataDetector.getColumnNames(),
                        textMetadataDetector.getColumnTypes(),
                        inputFilePath,
                        typeManager
                );
                phaseEpilogue(TextImportTask.PHASE_ANALYZE_FILE_STRUCTURE);
                return writer;
            } else {
                throw TextException.$("could not read from file '").put(inputFilePath).put("' to analyze structure");
            }
        } catch (CairoException e) {
            throw TextImportException.instance(TextImportTask.PHASE_ANALYZE_FILE_STRUCTURE, e.getFlyweightMessage(), e.getErrno());
        } catch (TextException e) {
            throw TextImportException.instance(TextImportTask.PHASE_ANALYZE_FILE_STRUCTURE, e.getFlyweightMessage());
        } finally {
            Unsafe.free(buf, len, MemoryTag.NATIVE_IMPORT);
        }
    }

    //returns list with N chunk boundaries
    LongList phaseBoundaryCheck(long fileLength) throws TextImportException {
        phasePrologue(TextImportTask.PHASE_BOUNDARY_CHECK);
        assert (workerCount > 0 && minChunkSize > 0);

        if (workerCount == 1) {
            indexChunkStats.setPos(0);
            indexChunkStats.add(0);
            indexChunkStats.add(0);
            indexChunkStats.add(fileLength);
            indexChunkStats.add(0);
            phaseEpilogue(TextImportTask.PHASE_BOUNDARY_CHECK);
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
                    final TextImportTask task = queue.get(seq);
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
        phaseEpilogue(TextImportTask.PHASE_BOUNDARY_CHECK);
        return indexChunkStats;
    }

    private void phaseBuildSymbolIndex(TableWriter writer) throws TextImportException {
        phasePrologue(TextImportTask.PHASE_BUILD_SYMBOL_INDEX);

        final RecordMetadata metadata = writer.getMetadata();
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
                        final TextImportTask task = queue.get(seq);
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

        phaseEpilogue(TextImportTask.PHASE_BUILD_SYMBOL_INDEX);
    }

    private void phaseEpilogue(byte phase) {
        throwErrorIfNotOk();
        long endMs = getCurrentTimeMs();
        LOG.info()
                .$("finished [importId=").$hexPadded(importId)
                .$(", phase=").$(TextImportTask.getPhaseName(phase))
                .$(", file=`").$(inputFilePath)
                .$("`, duration=").$((endMs - startMs) / 1000).$('s')
                .$(", errors=").$(phaseErrors)
                .I$();
        updatePhaseStatus(phase, TextImportTask.STATUS_FINISHED, null);
    }

    void phaseIndexing() throws TextException {
        phasePrologue(TextImportTask.PHASE_INDEXING);

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
                    final TextImportTask task = queue.get(seq);
                    task.setChunkIndex(colIdx);
                    task.setCircuitBreaker(circuitBreaker);
                    task.ofPhaseIndexing(
                            chunkLo,
                            chunkHi,
                            lineNumber,
                            colIdx,
                            inputFileName,
                            importRoot,
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

        phaseEpilogue(TextImportTask.PHASE_INDEXING);
    }

    private void phasePartitionImport() throws TextImportException {
        if (partitions.size() == 0) {
            if (linesIndexed > 0) {
                throw TextImportException.instance(TextImportTask.PHASE_PARTITION_IMPORT,
                        "All rows were skipped. Possible reasons: timestamp format mismatch or rows exceed maximum line length (65k).");
            } else {
                throw TextImportException.instance(TextImportTask.PHASE_PARTITION_IMPORT,
                        "No rows in input file to import.");
            }
        }

        phasePrologue(TextImportTask.PHASE_PARTITION_IMPORT);
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
                    final TextImportTask task = queue.get(seq);
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

        phaseEpilogue(TextImportTask.PHASE_PARTITION_IMPORT);
    }

    private void phasePrologue(byte phase) {
        phaseErrors = 0;
        LOG.info()
                .$("started [importId=").$hexPadded(importId)
                .$(", phase=").$(TextImportTask.getPhaseName(phase))
                .$(", file=`").$(inputFilePath)
                .$("`, workerCount=").$(workerCount).I$();
        updatePhaseStatus(phase, TextImportTask.STATUS_STARTED, null);
        startMs = getCurrentTimeMs();
    }

    private void phaseSymbolTableMerge(final TableWriter writer) throws TextImportException {
        phasePrologue(TextImportTask.PHASE_SYMBOL_TABLE_MERGE);
        final int tmpTableCount = getTaskCount();

        int queuedCount = 0;
        int collectedCount = 0;
        TableWriterMetadata metadata = writer.getMetadata();

        for (int columnIndex = 0, size = metadata.getColumnCount(); columnIndex < size; columnIndex++) {
            if (ColumnType.isSymbol(metadata.getColumnType(columnIndex))) {
                final CharSequence symbolColumnName = metadata.getColumnName(columnIndex);
                int tmpTableSymbolColumnIndex = targetTableStructure.getSymbolColumnIndex(symbolColumnName);

                while (true) {
                    final long seq = pubSeq.next();
                    if (seq > -1) {
                        final TextImportTask task = queue.get(seq);
                        task.setChunkIndex(columnIndex);
                        task.ofPhaseSymbolTableMerge(
                                configuration,
                                importRoot,
                                writer,
                                tableName,
                                symbolColumnName,
                                columnIndex,
                                tmpTableSymbolColumnIndex,
                                tmpTableCount,
                                partitionBy
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

        phaseEpilogue(TextImportTask.PHASE_SYMBOL_TABLE_MERGE);
    }

    private void phaseUpdateSymbolKeys(final TableWriter writer) throws TextImportException {
        phasePrologue(TextImportTask.PHASE_UPDATE_SYMBOL_KEYS);

        final int tmpTableCount = getTaskCount();
        int queuedCount = 0;
        int collectedCount = 0;
        for (int t = 0; t < tmpTableCount; ++t) {
            tmpPath.of(importRoot).concat(tableName).put("_").put(t);

            try (TxReader txFile = new TxReader(ff).ofRO(tmpPath.concat(TXN_FILE_NAME).$(), partitionBy)) {
                txFile.unsafeLoadAll();
                final int partitionCount = txFile.getPartitionCount();

                for (int p = 0; p < partitionCount; p++) {
                    final long partitionSize = txFile.getPartitionSize(p);
                    final long partitionTimestamp = txFile.getPartitionTimestamp(p);
                    TableWriterMetadata metadata = writer.getMetadata();
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
                                    final TextImportTask task = queue.get(seq);
                                    task.setChunkIndex(t);
                                    task.setCircuitBreaker(circuitBreaker);
                                    task.ofPhaseUpdateSymbolKeys(
                                            cairoEngine,
                                            targetTableStructure,
                                            t,
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

        phaseEpilogue(TextImportTask.PHASE_UPDATE_SYMBOL_KEYS);
    }

    TableWriter prepareTable(
            CairoSecurityContext cairoSecurityContext,
            ObjList<CharSequence> names,
            ObjList<TypeAdapter> types,
            Path path,
            TypeManager typeManager
    ) throws TextException {
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

        TableWriter writer = null;

        try {
            targetTableStatus = cairoEngine.getStatus(cairoSecurityContext, path, tableName);
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
                    targetTableStructure.of(tableName, names, types, timestampIndex, partitionBy);
                    createTable(
                            ff,
                            configuration.getMkDirMode(),
                            configuration.getRoot(),
                            tableName,
                            targetTableStructure,
                            (int) cairoEngine.getTableIdGenerator().getNextId(),
                            configuration
                    );
                    targetTableCreated = true;
                    writer = cairoEngine.getWriter(cairoSecurityContext, tableName, LOCK_REASON);
                    partitionBy = writer.getPartitionBy();
                    break;
                case TableUtils.TABLE_EXISTS:
                    writer = openWriterAndOverrideImportMetadata(names, types, cairoSecurityContext, typeManager);

                    if (writer.getRowCount() > 0) {
                        throw TextException.$("target table must be empty [table=").put(tableName).put(']');
                    }

                    CharSequence designatedTimestampColumnName = writer.getDesignatedTimestampColumnName();
                    int designatedTimestampIndex = writer.getMetadata().getTimestampIndex();
                    if (PartitionBy.isPartitioned(partitionBy) && partitionBy != writer.getPartitionBy()) {
                        throw TextException.$("declared partition by unit doesn't match table's");
                    }
                    partitionBy = writer.getPartitionBy();
                    if (!PartitionBy.isPartitioned(partitionBy)) {
                        throw TextException.$("target table is not partitioned");
                    }
                    validate(names, types, designatedTimestampColumnName, designatedTimestampIndex);
                    targetTableStructure.of(tableName, names, types, timestampIndex, partitionBy);
                    break;
                default:
                    throw TextException.$("name is reserved [table=").put(tableName).put(']');
            }

            inputFilePath.of(inputRoot).concat(inputFileName).$();//getStatus might override it
            targetTableStructure.setIgnoreColumnIndexedFlag(true);

            if (timestampAdapter == null && ColumnType.isTimestamp(types.getQuick(timestampIndex).getType())) {
                timestampAdapter = (TimestampAdapter) types.getQuick(timestampIndex);
            }
        } catch (Throwable t) {
            if (writer != null) {
                writer.close();
            }

            throw t;
        }

        return writer;
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

        DateFormat dirFormat = PartitionBy.getPartitionDirFormatMethod(partitionBy);

        for (int i = 0, n = distinctKeys.size(); i < n; i++) {
            long key = distinctKeys.getQuick(i);
            long size = totalSizes.getQuick(i);

            partitionNameSink.clear();
            dirFormat.format(distinctKeys.get(i), null, null, partitionNameSink);
            String dirName = partitionNameSink.toString();

            partitions.add(new PartitionInfo(key, dirName, size));
        }
    }

    private void removeWorkDir() {
        Path workDirPath = tmpPath.of(importRoot).$();
        if (ff.exists(workDirPath)) {
            if (isOneOfMainDirectories(importRoot)) {
                throw TextException.$("could not remove import work directory because it points to one of main directories [path='").put(workDirPath).put("'] .");
            }

            LOG.info().$("removing import work directory [path='").$(workDirPath).$("']").$();

            int errno = ff.rmdir(workDirPath);
            if (errno != 0) {
                throw TextException.$("could not remove import work directory [path='").put(workDirPath).put("', errno=").put(errno).put(']');
            }
        }
    }

    private void stealWork() {
        if (localImportJob.run(0)) {
            return;
        }
        Os.pause();
    }

    private void throwErrorIfNotOk() {
        if (status == TextImportTask.STATUS_FAILED) {
            throw TextImportException.instance(phase, "import failed [phase=")
                    .put(TextImportTask.getPhaseName(phase))
                    .put(", msg=`").put(errorMessage).put("`]");
        } else if (status == TextImportTask.STATUS_CANCELLED) {
            TextImportException ex = TextImportException.instance(phase, "import cancelled [phase=")
                    .put(TextImportTask.getPhaseName(phase))
                    .put(", msg=`").put(errorMessage).put("`]");
            ex.setCancelled(true);
            throw ex;
        }
    }

    private void updateStatus(final TextImportTask task) {
        boolean cancelledOrFailed = status == TextImportTask.STATUS_FAILED || status == TextImportTask.STATUS_CANCELLED;
        if (!cancelledOrFailed && (task.isFailed() || task.isCancelled())) {
            status = task.getStatus();
            phase = task.getPhase();
            errorMessage = task.getErrorMessage();
        }
    }

    void validate(ObjList<CharSequence> names,
                  ObjList<TypeAdapter> types,
                  CharSequence designatedTimestampColumnName,
                  int designatedTimestampIndex) throws TextException {
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

    static class PartitionInfo {
        long key;
        CharSequence name;
        long bytes;

        int taskId;//assigned worker/task id
        long importedRows;//used to detect partitions that need skipping (because e.g. no data was imported for them)

        PartitionInfo(long key, CharSequence name, long bytes) {
            this.key = key;
            this.name = name;
            this.bytes = bytes;
        }

        PartitionInfo(long key, CharSequence name, long bytes, int taskId) {
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
        private final CairoConfiguration configuration;
        private final LongList columnBits = new LongList();
        private CharSequence tableName;
        private ObjList<CharSequence> columnNames;
        private int timestampColumnIndex;
        private int partitionBy;
        private boolean ignoreColumnIndexedFlag;

        public TableStructureAdapter(CairoConfiguration configuration) {
            this.configuration = configuration;
        }

        @Override
        public int getColumnCount() {
            return columnNames.size();
        }

        @Override
        public long getColumnHash(int columnIndex) {
            return configuration.getRandom().nextLong();
        }

        @Override
        public CharSequence getColumnName(int columnIndex) {
            return columnNames.getQuick(columnIndex);
        }

        @Override
        public int getColumnType(int columnIndex) {
            return Numbers.decodeLowInt(columnBits.getQuick(columnIndex));
        }

        @Override
        public long getCommitLag() {
            return configuration.getCommitLag();
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
        public int getPartitionBy() {
            return partitionBy;
        }

        @Override
        public boolean getSymbolCacheFlag(int columnIndex) {
            return false;
        }

        @Override
        public int getSymbolCapacity(int columnIndex) {
            return configuration.getDefaultSymbolCapacity();
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
        public boolean isWallEnabled() {
            return false;
        }

        @Override
        public boolean isIndexed(int columnIndex) {
            return !ignoreColumnIndexedFlag && Numbers.decodeHighInt(columnBits.getQuick(columnIndex)) != 0;
        }

        @Override
        public boolean isSequential(int columnIndex) {
            return false;
        }

        public int getSymbolColumnIndex(CharSequence symbolColumnName) {
            int index = -1;
            for (int i = 0, n = columnNames.size(); i < n; i++) {
                if (getColumnType(i) == ColumnType.SYMBOL) {
                    index++;
                }

                if (symbolColumnName.equals(columnNames.get(i))) {
                    return index;
                }
            }

            return -1;
        }

        public void of(final CharSequence tableName,
                       final ObjList<CharSequence> names,
                       final ObjList<TypeAdapter> types,
                       final int timestampColumnIndex,
                       final int partitionBy
        ) {
            this.tableName = tableName;
            this.columnNames = names;
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
