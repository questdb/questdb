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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.DefaultLifecycleManager;
import io.questdb.cairo.EmptyTxnScoreboardPool;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.SymbolMapReaderImpl;
import io.questdb.cairo.SymbolMapWriter;
import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxnScoreboardPool;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cutlass.text.types.TimestampAdapter;
import io.questdb.cutlass.text.types.TypeAdapter;
import io.questdb.griffin.engine.functions.columns.ColumnUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Decimal256;
import io.questdb.std.DirectLongList;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IOURing;
import io.questdb.std.IOURingFacade;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.SwarUtils;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;
import static io.questdb.cutlass.text.ParallelCsvFileImporter.createTable;

public class CopyImportTask {

    public static final byte NO_PHASE = -1;
    public static final byte PHASE_ANALYZE_FILE_STRUCTURE = 9;
    public static final byte PHASE_ATTACH_PARTITIONS = 8;
    public static final byte PHASE_BOUNDARY_CHECK = 1;
    public static final byte PHASE_BUILD_SYMBOL_INDEX = 6;
    public static final byte PHASE_CLEANUP = 10;
    public static final byte PHASE_INDEXING = 2;
    public static final byte PHASE_MOVE_PARTITIONS = 7;
    public static final byte PHASE_PARTITION_IMPORT = 3;
    public static final byte PHASE_SETUP = 0;
    public static final byte PHASE_SYMBOL_TABLE_MERGE = 4;
    public static final byte PHASE_UPDATE_SYMBOL_KEYS = 5;
    public static final byte STATUS_CANCELLED = 3;
    public static final byte STATUS_FAILED = 2;
    public static final byte STATUS_FINISHED = 1;
    public static final byte STATUS_STARTED = 0;
    private static final TxnScoreboardPool EMPTY_SCOREBOARD_POOL = new EmptyTxnScoreboardPool();
    private static final Log LOG = LogFactory.getLog(CopyImportTask.class);
    private static final long MASK_NEW_LINE = SwarUtils.broadcast((byte) '\n');
    private static final long MASK_QUOTE = SwarUtils.broadcast((byte) '"');
    private static final IntObjHashMap<String> PHASE_NAME_MAP = new IntObjHashMap<>();
    private static final IntObjHashMap<String> STATUS_NAME_MAP = new IntObjHashMap<>();
    private final PhaseBoundaryCheck phaseBoundaryCheck = new PhaseBoundaryCheck();
    private final PhaseBuildSymbolIndex phaseBuildSymbolIndex = new PhaseBuildSymbolIndex();
    private final PhaseIndexing phaseIndexing = new PhaseIndexing();
    private final PhasePartitionImport phasePartitionImport = new PhasePartitionImport();
    private final PhaseSymbolTableMerge phaseSymbolTableMerge = new PhaseSymbolTableMerge();
    private final PhaseUpdateSymbolKeys phaseUpdateSymbolKeys = new PhaseUpdateSymbolKeys();
    private int chunkIndex;
    private @Nullable ExecutionCircuitBreaker circuitBreaker;
    private @Nullable CharSequence errorMessage;
    private byte phase;
    private byte status;

    public static String getPhaseName(byte phase) {
        return PHASE_NAME_MAP.get(phase);
    }

    public static String getStatusName(byte status) {
        return STATUS_NAME_MAP.get(status);
    }

    public void clear() {
        if (phase == PHASE_BOUNDARY_CHECK) {
            phaseBoundaryCheck.clear();
        } else if (phase == PHASE_INDEXING) {
            phaseIndexing.clear();
        } else if (phase == PHASE_PARTITION_IMPORT) {
            phasePartitionImport.clear();
        } else if (phase == PHASE_SYMBOL_TABLE_MERGE) {
            phaseSymbolTableMerge.clear();
        } else if (phase == PHASE_UPDATE_SYMBOL_KEYS) {
            phaseUpdateSymbolKeys.clear();
        } else if (phase == PHASE_BUILD_SYMBOL_INDEX) {
            phaseBuildSymbolIndex.clear();
        } else {
            throw TextException.$("Unexpected phase ").put(phase);
        }
    }

    public PhaseIndexing getBuildPartitionIndexPhase() {
        return phaseIndexing;
    }

    public int getChunkIndex() {
        return chunkIndex;
    }

    public PhaseBoundaryCheck getCountQuotesPhase() {
        return phaseBoundaryCheck;
    }

    public @Nullable CharSequence getErrorMessage() {
        return errorMessage;
    }

    public PhasePartitionImport getImportPartitionDataPhase() {
        return phasePartitionImport;
    }

    public byte getPhase() {
        return phase;
    }

    public byte getStatus() {
        return status;
    }

    public boolean isCancelled() {
        return this.status == STATUS_CANCELLED;
    }

    public boolean isFailed() {
        return this.status == STATUS_FAILED;
    }

    public void ofPhaseBoundaryCheck(final FilesFacade ff, Path path, long chunkStart, long chunkEnd) {
        this.phase = PHASE_BOUNDARY_CHECK;
        this.phaseBoundaryCheck.of(ff, path, chunkStart, chunkEnd);
    }

    public void ofPhaseBuildSymbolIndex(
            CairoEngine cairoEngine,
            TableStructure tableStructure,
            CharSequence root,
            int index,
            RecordMetadata metadata
    ) {
        this.phase = PHASE_BUILD_SYMBOL_INDEX;
        this.phaseBuildSymbolIndex.of(cairoEngine, tableStructure, root, index, metadata);
    }

    public void ofPhaseIndexing(
            long chunkStart,
            long chunkEnd,
            long lineNumber,
            int index,
            CharSequence inputFileName,
            CharSequence importRoot,
            int timestampType,
            int partitionBy,
            byte columnDelimiter,
            int timestampIndex,
            TimestampAdapter adapter,
            boolean ignoreHeader,
            int atomicity
    ) {
        this.phase = PHASE_INDEXING;
        this.phaseIndexing.of(
                chunkStart,
                chunkEnd,
                lineNumber,
                index,
                inputFileName,
                importRoot,
                timestampType,
                partitionBy,
                columnDelimiter,
                timestampIndex,
                adapter,
                ignoreHeader,
                atomicity
        );
    }

    public void ofPhaseSymbolTableMerge(
            CairoConfiguration cfg,
            CharSequence importRoot,
            TableWriter writer,
            TableToken tableToken,
            CharSequence column,
            int commonWriterIndex,
            int tempTableDenseSymbolIndex,
            int tempTableColumnIndex,
            int tmpTableCount,
            int partitionBy,
            int timestampType
    ) {
        this.phase = PHASE_SYMBOL_TABLE_MERGE;
        this.phaseSymbolTableMerge.of(
                cfg,
                importRoot,
                writer,
                tableToken,
                column,
                commonWriterIndex,
                tempTableDenseSymbolIndex,
                tempTableColumnIndex,
                tmpTableCount,
                partitionBy,
                timestampType
        );
    }

    public void ofPhaseUpdateSymbolKeys(
            CairoEngine cairoEngine,
            ParallelCsvFileImporter.TableStructureAdapter tableStructure,
            int tempTableIndex,
            long partitionSize,
            long partitionTimestamp,
            CharSequence root,
            CharSequence columnName,
            int symbolCount
    ) {
        this.phase = PHASE_UPDATE_SYMBOL_KEYS;
        this.phaseUpdateSymbolKeys.of(
                cairoEngine,
                tableStructure,
                tempTableIndex,
                partitionSize,
                partitionTimestamp,
                root,
                columnName,
                symbolCount
        );
    }

    public boolean run(
            TextLexerWrapper lf,
            CsvFileIndexer indexer,
            DirectUtf16Sink utf16Sink,
            DirectUtf8Sink utf8Sink,
            Decimal256 decimal256,
            DirectLongList unmergedIndexes,
            long fileBufAddr,
            long fileBufSize,
            Path p1,
            Path p2
    ) {
        try {
            LOG.debug().$("starting [phase=").$(getPhaseName(phase)).$(",index=").$(chunkIndex).I$();

            this.status = STATUS_STARTED;
            this.errorMessage = null;

            throwIfCancelled();

            if (phase == PHASE_BOUNDARY_CHECK) {
                phaseBoundaryCheck.run(fileBufAddr, fileBufSize);
            } else if (phase == PHASE_INDEXING) {
                phaseIndexing.run(indexer, fileBufAddr, fileBufSize);
            } else if (phase == PHASE_PARTITION_IMPORT) {
                phasePartitionImport.run(lf, fileBufAddr, fileBufSize, utf16Sink, utf8Sink, decimal256, unmergedIndexes, p1, p2);
            } else if (phase == PHASE_SYMBOL_TABLE_MERGE) {
                phaseSymbolTableMerge.run(p1);
            } else if (phase == PHASE_UPDATE_SYMBOL_KEYS) {
                phaseUpdateSymbolKeys.run(p1);
            } else if (phase == PHASE_BUILD_SYMBOL_INDEX) {
                phaseBuildSymbolIndex.run();
            } else {
                throw TextException.$("Unexpected phase ").put(phase);
            }

            LOG.debug().$("finished [phase=").$(getPhaseName(phase)).$(",index=").$(chunkIndex).I$();
        } catch (TextImportException e) {
            this.status = STATUS_CANCELLED;
            this.errorMessage = e.getMessage();
            LOG.error().$("Import cancelled [phase=").$(getPhaseName(e.getPhase())).I$();
            return false;
        } catch (Throwable t) {
            LOG.error()
                    .$("could not import [phase=").$(getPhaseName(phase))
                    .$(", ex=").$(t)
                    .I$();
            this.status = STATUS_FAILED;
            this.errorMessage = t.getMessage();
            return false;
        }

        return true;
    }

    public void setChunkIndex(int chunkIndex) {
        this.chunkIndex = chunkIndex;
    }

    public void setCircuitBreaker(@Nullable ExecutionCircuitBreaker circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
    }

    private TextImportException getCancelException() {
        TextImportException ex = TextImportException.instance(this.phase, "Cancelled");
        ex.setCancelled(true);
        return ex;
    }

    private void throwIfCancelled() throws TextImportException {
        if (circuitBreaker != null && circuitBreaker.checkIfTripped()) {
            throw getCancelException();
        }
    }

    void ofPhasePartitionImport(
            CairoEngine cairoEngine,
            TableStructure targetTableStructure,
            ObjList<TypeAdapter> types,
            int atomicity,
            byte columnDelimiter,
            CharSequence importRoot,
            CharSequence inputFileName,
            int index,
            int lo,
            int hi,
            final ObjList<ParallelCsvFileImporter.PartitionInfo> partitions
    ) {
        this.phase = PHASE_PARTITION_IMPORT;
        this.phasePartitionImport.of(
                cairoEngine,
                targetTableStructure,
                types,
                atomicity,
                columnDelimiter,
                importRoot,
                inputFileName,
                index,
                lo,
                hi,
                partitions
        );
    }

    public static class PhaseBoundaryCheck {
        private long chunkEnd;
        private long chunkStart;
        private FilesFacade ff;
        private long newLineCountEven;
        private long newLineCountOdd;
        private long newLineOffsetEven;
        private long newLineOffsetOdd;
        private Path path;
        private long quoteCount;

        public void clear() {
            this.ff = null;
            this.path = null;
            this.chunkStart = -1;
            this.chunkEnd = -1;
        }

        public long getNewLineCountEven() {
            return newLineCountEven;
        }

        public long getNewLineCountOdd() {
            return newLineCountOdd;
        }

        public long getNewLineOffsetEven() {
            return newLineOffsetEven;
        }

        public long getNewLineOffsetOdd() {
            return newLineOffsetOdd;
        }

        public long getQuoteCount() {
            return quoteCount;
        }

        public void of(final FilesFacade ff, Path path, long chunkStart, long chunkEnd) {
            assert ff != null;
            assert path != null;
            assert chunkStart >= 0 && chunkEnd > chunkStart;

            this.ff = ff;
            this.path = path;
            this.chunkStart = chunkStart;
            this.chunkEnd = chunkEnd;
        }

        public void run(long fileBufPtr, long fileBufSize) throws TextException {
            long offset = chunkStart;

            // output vars
            long quotes = 0;
            long[] nlCount = new long[2];
            long[] nlFirst = new long[]{-1, -1};

            long read;
            long ptr;
            long hi;

            long fd = TableUtils.openRO(ff, path.$(), LOG);
            ff.fadvise(fd, chunkStart, chunkEnd - chunkStart, Files.POSIX_FADV_SEQUENTIAL);
            try {
                do {
                    long leftToRead = Math.min(chunkEnd - offset, fileBufSize);
                    read = (int) ff.read(fd, fileBufPtr, leftToRead, offset);
                    if (read < 1) {
                        break;
                    }
                    hi = fileBufPtr + read;
                    ptr = fileBufPtr;

                    while (ptr < hi) {
                        if (ptr < hi - 7) {
                            long word = Unsafe.getUnsafe().getLong(ptr);
                            long zeroBytesWord = SwarUtils.markZeroBytes(word ^ MASK_NEW_LINE)
                                    | SwarUtils.markZeroBytes(word ^ MASK_QUOTE);
                            if (zeroBytesWord == 0) {
                                ptr += 8;
                                continue;
                            } else {
                                ptr += SwarUtils.indexOfFirstMarkedByte(zeroBytesWord);
                            }
                        }

                        final byte b = Unsafe.getUnsafe().getByte(ptr++);
                        if (b == '"') {
                            quotes++;
                        } else if (b == '\n') {
                            nlCount[(int) (quotes & 1)]++;
                            if (nlFirst[(int) (quotes & 1)] == -1) {
                                nlFirst[(int) (quotes & 1)] = offset + (ptr - fileBufPtr);
                            }
                        }
                    }

                    offset += read;
                } while (offset < chunkEnd);

                if (read < 0 || offset < chunkEnd) {
                    throw TextException
                            .$("could not read import file [path='").put(path)
                            .put("', offset=").put(offset)
                            .put(", errno=").put(ff.errno())
                            .put(']');
                }
            } finally {
                ff.close(fd);
            }

            this.quoteCount = quotes;
            this.newLineCountEven = nlCount[0];
            this.newLineCountOdd = nlCount[1];
            this.newLineOffsetEven = nlFirst[0];
            this.newLineOffsetOdd = nlFirst[1];
        }
    }

    public static class PhaseBuildSymbolIndex {
        private final StringSink tableNameSink = new StringSink();
        private CairoEngine cairoEngine;
        private int index;
        private RecordMetadata metadata;
        private CharSequence root;
        private TableStructure tableStructure;

        public void clear() {
            this.cairoEngine = null;
            this.tableStructure = null;
            this.root = null;
            this.index = -1;
            this.metadata = null;
        }

        public void of(
                CairoEngine cairoEngine,
                TableStructure tableStructure,
                CharSequence root,
                int index, RecordMetadata metadata
        ) {
            this.cairoEngine = cairoEngine;
            this.tableStructure = tableStructure;
            this.root = root;
            this.index = index;
            this.metadata = metadata;
        }

        public void run() {
            final CairoConfiguration configuration = cairoEngine.getConfiguration();

            tableNameSink.clear();
            tableNameSink.put(tableStructure.getTableName()).put('_').put(index);
            String tableName = tableNameSink.toString();
            String dbLogName = configuration.getDbLogName();
            TableToken tableToken = new TableToken(tableName, tableName, dbLogName, cairoEngine.getNextTableId(), false, false, false);

            final int columnCount = metadata.getColumnCount();
            try (
                    TableWriter w = new TableWriter(
                            configuration,
                            tableToken,
                            cairoEngine.getMessageBus(),
                            null,
                            true,
                            DefaultLifecycleManager.INSTANCE,
                            root,
                            cairoEngine.getDdlListener(tableToken),
                            cairoEngine.getCheckpointStatus(),
                            cairoEngine,
                            EMPTY_SCOREBOARD_POOL
                    )
            ) {
                for (int i = 0; i < columnCount; i++) {
                    if (metadata.isColumnIndexed(i)) {
                        w.addIndex(metadata.getColumnName(i), metadata.getIndexValueBlockCapacity(i));
                    }
                }
            }
        }
    }

    public static class PhaseSymbolTableMerge {
        private CairoConfiguration cfg;
        private CharSequence column;
        private int commonWriterIndex;
        private CharSequence importRoot;
        private int partitionBy;
        private TableToken tableToken;
        private int tempTableColumnIndex;
        private int tempTableDenseSymbolIndex;
        private int timestampType;
        private int tmpTableCount;
        private TableWriter writer;

        public void clear() {
            this.cfg = null;
            this.importRoot = null;
            this.writer = null;
            this.tableToken = null;
            this.column = null;
            this.commonWriterIndex = -1;
            this.tempTableDenseSymbolIndex = -1;
            this.tmpTableCount = -1;
            this.partitionBy = -1;
            this.timestampType = ColumnType.NULL;
        }

        public void of(
                CairoConfiguration cfg,
                CharSequence importRoot,
                TableWriter writer,
                TableToken tableToken,
                CharSequence column,
                int commonWriterIndex,
                int tempTableDenseSymbolIndex,
                int tempTableColumnIndex,
                int tmpTableCount,
                int partitionBy,
                int timestampType
        ) {
            this.cfg = cfg;
            this.importRoot = importRoot;
            this.writer = writer;
            this.tableToken = tableToken;
            this.column = column;
            this.commonWriterIndex = commonWriterIndex;
            this.tempTableDenseSymbolIndex = tempTableDenseSymbolIndex;
            this.tempTableColumnIndex = tempTableColumnIndex;
            this.tmpTableCount = tmpTableCount;
            this.partitionBy = partitionBy;
            this.timestampType = timestampType;
        }

        public void run(Path path) {
            final FilesFacade ff = cfg.getFilesFacade();
            path.of(importRoot).concat(tableToken.getTableName());
            int plen = path.size();
            for (int i = 0; i < tmpTableCount; i++) {
                path.trimTo(plen);
                path.putAscii('_').put(i);
                int tableLen = path.size();
                try (
                        TxReader txFile = new TxReader(ff).ofRO(path.concat(TXN_FILE_NAME).$(), timestampType, partitionBy);
                        ColumnVersionReader cvReader = new ColumnVersionReader().ofRO(ff, path.trimTo(tableLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$())
                ) {
                    path.trimTo(tableLen);
                    txFile.unsafeLoadAll();
                    cvReader.readUnsafe();
                    int symbolCount = txFile.getSymbolValueCount(tempTableDenseSymbolIndex);
                    try (
                            SymbolMapReaderImpl reader = new SymbolMapReaderImpl(
                                    cfg,
                                    path,
                                    column,
                                    cvReader.getSymbolTableNameTxn(tempTableColumnIndex),
                                    symbolCount
                            );
                            MemoryCMARW mem = Vm.getSmallCMARWInstance(
                                    ff,
                                    path.concat(column).put(TableUtils.SYMBOL_KEY_REMAP_FILE_SUFFIX).$(),
                                    MemoryTag.MMAP_IMPORT,
                                    cfg.getWriterFileOpenOpts()
                            )
                    ) {
                        // It is possible to skip symbol rewrite when symbols do not clash.
                        // From our benchmarks rewriting symbols take a tiny fraction of time compared to everything else
                        // so that we don't need to optimise this yet.
                        SymbolMapWriter.mergeSymbols(writer.getSymbolMapWriter(commonWriterIndex), reader, mem);
                    }
                }
            }
        }
    }

    public static class PhaseUpdateSymbolKeys {
        CharSequence columnName;
        int tempTableIndex;
        long partitionSize;
        long partitionTimestamp;
        CharSequence root;
        int symbolCount;
        private CairoEngine cairoEngine;
        private ParallelCsvFileImporter.TableStructureAdapter tableStructure;

        public void clear() {
            this.cairoEngine = null;
            this.tableStructure = null;
            this.tempTableIndex = -1;
            this.partitionSize = -1;
            this.partitionTimestamp = -1;
            this.root = null;
            this.columnName = null;
            this.symbolCount = -1;
        }

        public void of(
                CairoEngine cairoEngine,
                ParallelCsvFileImporter.TableStructureAdapter tableStructure,
                int tempTableIndex,
                long partitionSize,
                long partitionTimestamp,
                CharSequence root,
                CharSequence columnName,
                int symbolCount
        ) {
            this.cairoEngine = cairoEngine;
            this.tableStructure = tableStructure;
            this.tempTableIndex = tempTableIndex;
            this.partitionSize = partitionSize;
            this.partitionTimestamp = partitionTimestamp;
            this.root = root;
            this.columnName = columnName;
            this.symbolCount = symbolCount;
        }

        public void run(Path path) {
            final FilesFacade ff = cairoEngine.getConfiguration().getFilesFacade();

            TableToken tableToken = cairoEngine.verifyTableName(tableStructure.getTableName());
            path.of(root).concat(tableToken.getTableName()).put('_').put(tempTableIndex);
            int plen = path.size();

            long columnMemory = 0;
            long columnMemorySize = 0;
            long remapTableMemory = 0;
            long remapTableMemorySize = 0;
            long columnFd = -1;
            long remapFd = -1;
            try (
                    ColumnVersionReader cvReader = new ColumnVersionReader().ofRO(ff, path.trimTo(plen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$())
            ) {
                cvReader.readUnsafe();
                path.trimTo(plen);

                int tempTableColumnIndex = tableStructure.getColumnIndex(columnName);
                long columnNameTxn = cvReader.getColumnNameTxn(partitionTimestamp, tempTableColumnIndex);

                TableUtils.setPathForNativePartition(
                        path.slash(),
                        TableUtils.getTimestampType(tableStructure),
                        tableStructure.getPartitionBy(),
                        partitionTimestamp,
                        -1
                );
                path.concat(columnName).put(TableUtils.FILE_SUFFIX_D);
                if (columnNameTxn != -1) {
                    path.put('.').put(columnNameTxn);
                }
                columnFd = TableUtils.openFileRWOrFail(ff, path.$(), CairoConfiguration.O_NONE);
                columnMemorySize = ff.length(columnFd);
                assert columnMemorySize > 0;

                path.trimTo(plen);
                path.concat(columnName).put(TableUtils.SYMBOL_KEY_REMAP_FILE_SUFFIX);
                remapFd = TableUtils.openFileRWOrFail(ff, path.$(), CairoConfiguration.O_NONE);
                remapTableMemorySize = ff.length(remapFd);

                if (columnMemorySize >= Integer.BYTES && remapTableMemorySize >= Integer.BYTES) {
                    columnMemory = TableUtils.mapRW(ff, columnFd, columnMemorySize, MemoryTag.MMAP_IMPORT);
                    remapTableMemory = TableUtils.mapRW(ff, remapFd, remapTableMemorySize, MemoryTag.MMAP_IMPORT);
                    long columnMemSize = partitionSize * Integer.BYTES;
                    long remapMemSize = (long) symbolCount * Integer.BYTES;
                    ColumnUtils.symbolColumnUpdateKeys(columnMemory, columnMemSize, remapTableMemory, remapMemSize);
                }
            } finally {
                ff.close(columnFd);
                ff.close(remapFd);
                if (columnMemory > 0) {
                    ff.munmap(columnMemory, columnMemorySize, MemoryTag.MMAP_IMPORT);
                }
                if (remapTableMemory > 0) {
                    ff.munmap(remapTableMemory, remapTableMemorySize, MemoryTag.MMAP_IMPORT);
                }
            }
        }
    }

    public class PhaseIndexing {
        //stores partition key and size for all indexed partitions
        private final LongList partitionKeysAndSizes = new LongList();
        private TimestampAdapter adapter;
        private int atomicity;
        private long chunkEnd;
        private long chunkStart;
        private byte columnDelimiter;
        private long errorCount;
        private boolean ignoreHeader;
        private CharSequence importRoot;
        private int index;
        private CharSequence inputFileName;
        private long lineCount;
        private long lineNumber;
        private int partitionBy;
        private int timestampIndex;
        private int timestampType;

        public void clear() {
            this.chunkStart = -1;
            this.chunkEnd = -1;
            this.lineNumber = -1;
            this.lineCount = 0;
            this.errorCount = 0;
            this.index = -1;
            this.inputFileName = null;
            this.importRoot = null;
            this.partitionBy = -1;
            this.timestampType = ColumnType.NULL;
            this.columnDelimiter = (byte) -1;
            this.timestampIndex = -1;
            this.adapter = null;
            this.ignoreHeader = false;
            this.atomicity = -1;
        }

        public long getErrorCount() {
            return errorCount;
        }

        public long getLineCount() {
            return lineCount;
        }

        public LongList getPartitionKeysAndSizes() {
            return partitionKeysAndSizes;
        }

        public void of(
                long chunkStart,
                long chunkEnd,
                long lineNumber,
                int index,
                CharSequence inputFileName,
                CharSequence importRoot,
                int timestampType,
                int partitionBy,
                byte columnDelimiter,
                int timestampIndex,
                TimestampAdapter adapter,
                boolean ignoreHeader,
                int atomicity
        ) {
            assert chunkStart >= 0 && chunkEnd > chunkStart;
            assert lineNumber >= 0;

            this.chunkStart = chunkStart;
            this.chunkEnd = chunkEnd;
            this.lineNumber = lineNumber;
            this.index = index;
            this.inputFileName = inputFileName;
            this.importRoot = importRoot;
            this.timestampType = timestampType;
            this.partitionBy = partitionBy;
            this.columnDelimiter = columnDelimiter;
            this.timestampIndex = timestampIndex;
            this.adapter = adapter;
            this.ignoreHeader = ignoreHeader;
            this.atomicity = atomicity;
        }

        public void run(CsvFileIndexer indexer, long fileBufAddr, long fileBufSize) throws TextException {
            try {
                indexer.of(
                        inputFileName,
                        importRoot,
                        index,
                        timestampType,
                        partitionBy,
                        columnDelimiter,
                        timestampIndex,
                        adapter,
                        ignoreHeader,
                        atomicity,
                        circuitBreaker
                );
                indexer.index(chunkStart, chunkEnd, lineNumber, partitionKeysAndSizes, fileBufAddr, fileBufSize);
                lineCount = indexer.getLineCount();
                errorCount = indexer.getErrorCount();
            } catch (TextException e) {
                if (indexer.isCancelled()) {
                    throw getCancelException();
                } else {
                    throw e;
                }
            } finally {
                indexer.clear();
            }
        }
    }

    public class PhasePartitionImport {
        private final LongList importedRows = new LongList();
        private final LongList offsets = new LongList();
        private final StringSink tableNameSink = new StringSink();
        private int atomicity;
        private byte columnDelimiter;
        private CairoEngine engine;
        private long errors;
        private int hi;
        private CharSequence importRoot;
        private int index;
        private CharSequence inputFileName;
        private int lo;
        private long offset;
        private ObjList<ParallelCsvFileImporter.PartitionInfo> partitions;
        private long rowsHandled;
        private long rowsImported;
        private TableWriter tableWriterRef;
        private TableStructure targetTableStructure;
        private TimestampAdapter timestampAdapter;
        private int timestampIndex;
        private ObjList<TypeAdapter> types;
        private DirectUtf16Sink utf16Sink;
        private DirectUtf8Sink utf8Sink;
        private Decimal256 decimal256;
        private final CsvTextLexer.Listener onFieldsPartitioned = this::onFieldsPartitioned;

        public void clear() {
            this.engine = null;
            this.targetTableStructure = null;
            this.types = null;
            this.atomicity = -1;
            this.columnDelimiter = (byte) -1;
            this.importRoot = null;
            this.inputFileName = null;
            this.index = -1;
            this.partitions = null;
            this.timestampIndex = -1;
            this.timestampAdapter = null;

            this.offset = 0;
            this.importedRows.clear();
            this.tableNameSink.clear();
            this.rowsHandled = 0;
            this.rowsImported = 0;
            this.errors = 0;

            this.utf16Sink = null;
            this.decimal256 = null;
        }

        public long getErrors() {
            return errors;
        }

        public LongList getImportedRows() {
            return importedRows;
        }

        public long getRowsHandled() {
            return rowsHandled;
        }

        public long getRowsImported() {
            return rowsImported;
        }

        public void run(
                TextLexerWrapper lf,
                long fileBufAddr,
                long fileBufSize,
                DirectUtf16Sink utf16Sink,
                DirectUtf8Sink utf8Sink,
                Decimal256 decimal256,
                DirectLongList unmergedIndexes,
                Path path,
                Path tmpPath
        ) throws TextException {
            this.utf16Sink = utf16Sink;
            this.utf8Sink = utf8Sink;
            this.decimal256 = decimal256;

            final CairoConfiguration configuration = engine.getConfiguration();
            final FilesFacade ff = configuration.getFilesFacade();

            tableNameSink.clear();
            tableNameSink.put(targetTableStructure.getTableName()).put('_').put(index);
            String publicTableName = tableNameSink.toString();
            String dbLogName = engine.getConfiguration().getDbLogName();
            TableToken tableToken = new TableToken(
                    publicTableName,
                    publicTableName,
                    dbLogName,
                    engine.getNextTableId(),
                    false,
                    false,
                    false
            );
            createTable(
                    ff,
                    configuration.getMkDirMode(),
                    importRoot,
                    tableToken.getDirName(),
                    publicTableName,
                    targetTableStructure,
                    0,
                    AllowAllSecurityContext.INSTANCE
            );

            try (
                    TableWriter writer = new TableWriter(
                            configuration,
                            tableToken,
                            engine.getMessageBus(),
                            null,
                            true,
                            DefaultLifecycleManager.INSTANCE,
                            importRoot,
                            engine.getDdlListener(tableToken),
                            engine.getCheckpointStatus(),
                            engine,
                            EMPTY_SCOREBOARD_POOL
                    )
            ) {
                tableWriterRef = writer;
                AbstractTextLexer lexer = lf.getLexer(columnDelimiter);
                lexer.setTableName(tableNameSink);
                lexer.setSkipLinesWithExtraValues(false);

                long prevErrors;
                try {
                    for (int i = lo; i < hi; i++) {
                        throwIfCancelled();

                        lexer.clear();
                        prevErrors = errors;

                        final CharSequence name = partitions.getQuick(i).name;
                        path.of(importRoot).concat(name);
                        mergePartitionIndexAndImportData(
                                ff,
                                configuration.getIOURingFacade(),
                                configuration.isIOURingEnabled(),
                                path,
                                lexer,
                                fileBufAddr,
                                fileBufSize,
                                utf16Sink,
                                unmergedIndexes,
                                tmpPath
                        );

                        long newErrors = errors - prevErrors;
                        long imported = atomicity == Atomicity.SKIP_ROW ? lexer.getLineCount() - newErrors : lexer.getLineCount();
                        importedRows.add(i);
                        importedRows.add(imported);
                        rowsHandled += lexer.getLineCount();
                        rowsImported += imported;

                        LOG.info()
                                .$("imported data [temp_table=").$(tableNameSink)
                                .$(", partition=").$(name)
                                .$(", lines=").$(lexer.getLineCount())
                                .$(", errors=").$(newErrors)
                                .I$();
                    }
                } finally {
                    writer.commit();
                }
            }
        }

        private void consumeIOURing(
                FilesFacade ff,
                long sqeMin,
                AbstractTextLexer lexer,
                long fileBufAddr,
                LongList offsets,
                IOURing ring,
                int cc,
                Path tmpPath
        ) {
            int submitted = ring.submit();
            assert submitted == cc;

            long nextCqe = sqeMin;
            int writtenMax = 0;
            // consume submitted tasks
            for (int i = 0; i < submitted; i++) {

                while (!ring.nextCqe()) {
                    Os.pause();
                }

                if (ring.getCqeRes() < 0) {
                    throw TextException
                            .$("could not read from file [path='").put(tmpPath)
                            .put("', errno=").put(ff.errno())
                            .put(", offset=").put(offset)
                            .put("]");
                }

                if (ring.getCqeId() == nextCqe) {
                    // only parse lines in order of submissions
                    nextCqe++;
                    parseLinesAndWrite(lexer, fileBufAddr, offsets, writtenMax);
                    writtenMax++;
                }
            }

            // if reads came out of order, the writtenMax should be less than submitted
            for (int i = writtenMax; i < submitted; i++) {
                parseLinesAndWrite(lexer, fileBufAddr, offsets, i);
            }
        }

        private TableWriter.Row getRow(DirectUtf8Sequence dus, long offset) {
            final long timestamp;
            try {
                timestamp = timestampAdapter.getTimestamp(dus);
            } catch (Throwable e) {
                if (atomicity == Atomicity.SKIP_ALL) {
                    throw TextException.$("could not parse timestamp [offset=").put(offset).put(", msg=").put(e.getMessage()).put(']');
                } else {
                    logError(offset, timestampIndex, dus);
                    return null;
                }
            }
            return tableWriterRef.newRow(timestamp);
        }

        private void importPartitionData(
                final IOURingFacade rf,
                final boolean ioURingEnabled,
                final AbstractTextLexer lexer,
                long address,
                long size,
                long fileBufAddr,
                long fileBufSize,
                DirectUtf16Sink utf8Sink,
                Path tmpPath
        ) throws TextException {
            if (ioURingEnabled && rf.isAvailable()) {
                importPartitionDataURing(
                        rf,
                        lexer,
                        address,
                        size,
                        fileBufAddr,
                        fileBufSize,
                        utf8Sink,
                        tmpPath
                );
            } else {
                importPartitionDataVanilla(
                        lexer,
                        address,
                        size,
                        fileBufAddr,
                        fileBufSize,
                        utf8Sink,
                        tmpPath
                );
            }
        }

        private void importPartitionDataURing(
                final IOURingFacade rf,
                AbstractTextLexer lexer,
                long address,
                long size,
                long fileBufAddr,
                long fileBufSize,
                DirectUtf16Sink utf8Sink,
                Path tmpPath
        ) {
            final CairoConfiguration configuration = engine.getConfiguration();
            final FilesFacade ff = configuration.getFilesFacade();

            offsets.clear();
            lexer.setupBeforeExactLines(onFieldsPartitioned);

            long fd = -1;
            try {
                tmpPath.of(configuration.getSqlCopyInputRoot()).concat(inputFileName);
                utf8Sink.clear();
                fd = TableUtils.openRO(ff, tmpPath.$(), LOG);

                final long len = ff.length(fd);
                if (len == -1) {
                    throw CairoException.critical(ff.errno())
                            .put("could not get length of file [path=").put(tmpPath)
                            .put(']');
                }

                ff.fadvise(fd, 0, len, Files.POSIX_FADV_RANDOM);

                final long MASK = ~((255L) << 56 | (255L) << 48);
                final long count = size / (2 * Long.BYTES);

                int ringCapacity = 32;
                long sqeMin = 0;
                long sqeMax = -1;
                try (IOURing ring = rf.newInstance(ringCapacity)) {
                    long addr = fileBufAddr;
                    long lim = fileBufAddr + fileBufSize;
                    int cc = 0;
                    int bytesToRead;
                    int additionalLines;
                    for (long i = 0; i < count; i++) {
                        throwIfCancelled();

                        final long lengthAndOffset = Unsafe.getUnsafe().getLong(address + i * 2L * Long.BYTES + Long.BYTES);
                        final int lineLength = (int) (lengthAndOffset >>> 48);
                        // the offset is used by the callback to report errors
                        offset = lengthAndOffset & MASK;
                        bytesToRead = lineLength;

                        // schedule reads until we either run out of ring capacity or
                        // our read buffer size

                        if (cc == ringCapacity || (cc > 0 && addr + lineLength > lim)) {
                            // we are out of ring capacity or our buffer is exhausted
                            consumeIOURing(ff, sqeMin, lexer, fileBufAddr, offsets, ring, cc, tmpPath);

                            cc = 0;
                            addr = fileBufAddr;
                            offsets.clear();
                            sqeMin = sqeMax + 1;
                        }
                        if (addr + lineLength > lim) {
                            throw TextException.$("buffer overflow [path='").put(tmpPath)
                                    .put("', lineLength=").put(lineLength)
                                    .put(", fileBufSize=").put(fileBufSize)
                                    .put("]");
                        }

                        // try to coalesce ahead lines into the same read, if they're sequential
                        additionalLines = 0;
                        for (long j = i + 1; j < count; j++) {
                            long nextLengthAndOffset = Unsafe.getUnsafe().getLong(address + j * 2L * Long.BYTES + Long.BYTES);
                            int nextLineLength = (int) (nextLengthAndOffset >>> 48);
                            long nextOffset = nextLengthAndOffset & MASK;

                            // line indexing stops on first EOL char, e.g. \r, but it could be followed by \n
                            long diff = nextOffset - offset - bytesToRead;
                            int nextBytesToRead = ((int) diff) + nextLineLength;

                            if (diff > -1 && diff < 2 && addr + bytesToRead + nextBytesToRead <= lim) {
                                bytesToRead += nextBytesToRead;
                                additionalLines++;
                            } else {
                                break;
                            }
                        }
                        i += additionalLines;

                        sqeMax = ring.enqueueRead(fd, offset, addr, bytesToRead);
                        if (sqeMax == -1) {
                            throw TextException.$("io_uring error [path='").put(tmpPath)
                                    .put("', cqeRes=").put(-ring.getCqeRes())
                                    .put("]");
                        }

                        offsets.add(addr - fileBufAddr, bytesToRead);

                        cc++;
                        addr += bytesToRead;
                    } // for

                    // check if something is enqueued
                    if (cc > 0) {
                        consumeIOURing(ff, sqeMin, lexer, fileBufAddr, offsets, ring, cc, tmpPath);
                    }
                }

            } finally {
                ff.close(fd);
            }
        }

        private void importPartitionDataVanilla(
                AbstractTextLexer lexer,
                long address,
                long size,
                long fileBufAddr,
                long fileBufSize,
                DirectUtf16Sink utf8Sink,
                Path tmpPath
        ) {
            final CairoConfiguration configuration = engine.getConfiguration();
            final FilesFacade ff = configuration.getFilesFacade();

            lexer.setupBeforeExactLines(onFieldsPartitioned);

            long fd = -1;
            try {
                tmpPath.of(configuration.getSqlCopyInputRoot()).concat(inputFileName);
                utf8Sink.clear();
                fd = TableUtils.openRO(ff, tmpPath.$(), LOG);

                final long len = ff.length(fd);
                if (len == -1) {
                    throw CairoException.critical(ff.errno())
                            .put("could not get length of file [path=").put(tmpPath)
                            .put(']');
                }

                ff.fadvise(fd, 0, len, Files.POSIX_FADV_RANDOM);

                final long MASK = ~((255L) << 56 | (255L) << 48);
                final long count = size / (2 * Long.BYTES);
                int bytesToRead;
                int additionalLines;

                for (long i = 0; i < count; i++) {
                    throwIfCancelled();

                    long lengthAndOffset = Unsafe.getUnsafe().getLong(address + i * 2L * Long.BYTES + Long.BYTES);
                    int lineLength = (int) (lengthAndOffset >>> 48);
                    offset = lengthAndOffset & MASK;
                    bytesToRead = lineLength;

                    // try to coalesce ahead lines into the same read, if they're sequential
                    additionalLines = 0;
                    for (long j = i + 1; j < count; j++) {
                        long nextLengthAndOffset = Unsafe.getUnsafe().getLong(address + j * 2L * Long.BYTES + Long.BYTES);
                        int nextLineLength = (int) (nextLengthAndOffset >>> 48);
                        long nextOffset = nextLengthAndOffset & MASK;

                        // line indexing stops on first EOL char, e.g. \r, but it could be followed by \n
                        long diff = nextOffset - offset - bytesToRead;
                        int nextBytesToRead = (int) (diff + nextLineLength);
                        if (diff > -1 && diff < 2 && bytesToRead + nextBytesToRead <= fileBufSize) {
                            bytesToRead += nextBytesToRead;
                            additionalLines++;
                        } else {
                            break;
                        }
                    }
                    i += additionalLines;

                    if (bytesToRead > fileBufSize) {
                        throw TextException
                                .$("buffer overflow [path='").put(tmpPath)
                                .put("', bytesToRead=").put(bytesToRead)
                                .put(", fileBufSize=").put(fileBufSize)
                                .put("]");
                    }

                    long n = ff.read(fd, fileBufAddr, bytesToRead, offset);
                    if (n > 0) {
                        // at this phase there is no way for lines to be split across buffers
                        lexer.parse(fileBufAddr, fileBufAddr + n);
                    } else {
                        throw TextException
                                .$("could not read from file [path='").put(tmpPath)
                                .put("', errno=").put(ff.errno())
                                .put(", offset=").put(offset)
                                .put("]");
                    }
                }
            } finally {
                ff.close(fd);
            }
        }

        private void logError(long offset, int column, final DirectUtf8Sequence dus) {
            LOG.error()
                    .$("type syntax [type=").$(ColumnType.nameOf(types.getQuick(column).getType()))
                    .$(", offset=").$(offset)
                    .$(", column=").$(column)
                    .$(", value='").$(dus)
                    .$("']").$();
        }

        private void mergePartitionIndexAndImportData(
                final FilesFacade ff,
                final IOURingFacade rf,
                boolean ioURingEnabled,
                Path partitionPath,
                final AbstractTextLexer lexer,
                long fileBufAddr,
                long fileBufSize,
                DirectUtf16Sink utf8Sink,
                DirectLongList unmergedIndexes,
                Path tmpPath
        ) throws TextException {
            unmergedIndexes.clear();
            partitionPath.slash$();
            int partitionLen = partitionPath.size();

            long mergedIndexSize = -1;
            long mergeIndexAddr = 0;
            long fd = -1;
            try {
                mergedIndexSize = openIndexChunks(ff, partitionPath, unmergedIndexes, partitionLen);

                if (unmergedIndexes.size() > 2) { // there's more than 1 chunk so we've to merge
                    partitionPath.trimTo(partitionLen);
                    partitionPath.concat(CsvFileIndexer.INDEX_FILE_NAME);

                    fd = TableUtils.openFileRWOrFail(ff, partitionPath.$(), CairoConfiguration.O_NONE);
                    mergeIndexAddr = TableUtils.mapRW(ff, fd, mergedIndexSize, MemoryTag.MMAP_IMPORT);

                    Vect.mergeLongIndexesAsc(unmergedIndexes.getAddress(), (int) unmergedIndexes.size() / 2, mergeIndexAddr);
                    // release chunk memory because it's been copied to merge area
                    unmap(ff, unmergedIndexes);

                    importPartitionData(
                            rf,
                            ioURingEnabled,
                            lexer,
                            mergeIndexAddr,
                            mergedIndexSize,
                            fileBufAddr,
                            fileBufSize,
                            utf8Sink,
                            tmpPath
                    );
                } else { // we can use the single chunk as is
                    importPartitionData(
                            rf,
                            ioURingEnabled,
                            lexer,
                            unmergedIndexes.get(0),
                            mergedIndexSize,
                            fileBufAddr,
                            fileBufSize,
                            utf8Sink,
                            tmpPath
                    );
                }
            } finally {
                ff.close(fd);
                if (mergeIndexAddr != 0) {
                    ff.munmap(mergeIndexAddr, mergedIndexSize, MemoryTag.MMAP_IMPORT);
                }
                unmap(ff, unmergedIndexes);
            }
        }

        private boolean onField(
                long offset,
                final DirectUtf8Sequence dus,
                TableWriter.Row w,
                int fieldIndex
        ) throws TextException {
            TypeAdapter type = this.types.getQuick(fieldIndex);
            try {
                type.write(w, fieldIndex, dus, utf16Sink, utf8Sink, decimal256);
            } catch (NumericException | Utf8Exception | ImplicitCastException ignore) {
                errors++;
                logError(offset, fieldIndex, dus);
                switch (atomicity) {
                    case Atomicity.SKIP_ALL:
                        w.cancel();
                        tableWriterRef.rollback();
                        throw TextException.$("bad syntax [line offset=").put(offset).put(",column=").put(fieldIndex).put(']');
                    case Atomicity.SKIP_ROW:
                        w.cancel();
                        return true;
                    default: // SKIP column
                        break;
                }
            } catch (Exception e) {
                throw TextException.$("unexpected error [line offset=").put(offset).put(",column=").put(fieldIndex).put(",message=").put(e.getMessage()).put(']');
            }
            return false;
        }

        private void onFieldsPartitioned(long line, ObjList<DirectUtf8String> values, int valuesLength) {
            assert tableWriterRef != null;
            DirectUtf8Sequence dus = values.getQuick(timestampIndex);
            final TableWriter.Row w = getRow(dus, offset);
            if (w == null) {
                return;
            }
            for (int i = 0; i < valuesLength; i++) {
                dus = values.getQuick(i);
                if (i == timestampIndex || dus.size() == 0) {
                    continue;
                }
                if (onField(offset, dus, w, i)) {
                    return;
                }
            }
            w.append();
        }

        private long openIndexChunks(FilesFacade ff, Path partitionPath, DirectLongList mergeIndexes, int partitionLen) {
            long mergedIndexSize = 0;
            long chunk = ff.findFirst(partitionPath.$());
            if (chunk > 0) {
                try {
                    do {
                        // chunk loop
                        long chunkName = ff.findName(chunk);
                        long chunkType = ff.findType(chunk);
                        if (chunkType == Files.DT_FILE) {
                            partitionPath.trimTo(partitionLen);
                            partitionPath.concat(chunkName);

                            long fd = TableUtils.openRO(ff, partitionPath.$(), LOG);
                            long size = 0;
                            long address = -1;

                            try {
                                size = ff.length(fd);
                                if (size < 1) {
                                    throw TextException.$("index chunk is empty [path='").put(partitionPath).put(']');
                                }
                                address = TableUtils.mapRO(ff, fd, size, MemoryTag.MMAP_IMPORT);
                                mergeIndexes.add(address);
                                mergeIndexes.add(size / CsvFileIndexer.INDEX_ENTRY_SIZE);
                                mergedIndexSize += size;
                            } catch (Throwable t) {
                                if (address != -1) { //release mem if it can't be added to mergeIndexes
                                    ff.munmap(address, size, MemoryTag.MMAP_IMPORT);
                                }
                                throw t;
                            } finally {
                                ff.close(fd);
                            }
                        }
                    } while (ff.findNext(chunk) > 0);
                } finally {
                    ff.findClose(chunk);
                }
            }
            return mergedIndexSize;
        }

        private void parseLinesAndWrite(AbstractTextLexer lexer, long fileBufAddr, LongList offsets, int j) {
            final long lo = fileBufAddr + offsets.getQuick(j * 2);
            final long hi = lo + offsets.getQuick(j * 2 + 1);
            lexer.parse(lo, hi);
        }

        private void unmap(FilesFacade ff, DirectLongList mergeIndexes) {
            for (long i = 0, sz = mergeIndexes.size() / 2; i < sz; i++) {
                final long addr = mergeIndexes.get(2 * i);
                final long size = mergeIndexes.get(2 * i + 1) * CsvFileIndexer.INDEX_ENTRY_SIZE;
                ff.munmap(addr, size, MemoryTag.MMAP_IMPORT);
            }
            mergeIndexes.clear();
        }

        void of(
                CairoEngine cairoEngine,
                TableStructure targetTableStructure,
                ObjList<TypeAdapter> types,
                int atomicity,
                byte columnDelimiter,
                CharSequence importRoot,
                CharSequence inputFileName,
                int index,
                int lo,
                int hi,
                final ObjList<ParallelCsvFileImporter.PartitionInfo> partitions
        ) {
            this.engine = cairoEngine;
            this.targetTableStructure = targetTableStructure;
            this.types = types;
            this.atomicity = atomicity;
            this.columnDelimiter = columnDelimiter;
            this.importRoot = importRoot;
            this.inputFileName = inputFileName;
            this.index = index;
            this.lo = lo;
            this.hi = hi;
            this.partitions = partitions;
            this.timestampIndex = targetTableStructure.getTimestampIndex();
            this.timestampAdapter = (timestampIndex > -1 && timestampIndex < types.size()) ? (TimestampAdapter) types.getQuick(timestampIndex) : null;
            this.errors = 0;
            this.importedRows.clear();
        }
    }

    static {
        PHASE_NAME_MAP.put(PHASE_SETUP, "setup");
        PHASE_NAME_MAP.put(PHASE_BOUNDARY_CHECK, "boundary_check");
        PHASE_NAME_MAP.put(PHASE_INDEXING, "indexing");
        PHASE_NAME_MAP.put(PHASE_PARTITION_IMPORT, "partition_import");
        PHASE_NAME_MAP.put(PHASE_SYMBOL_TABLE_MERGE, "symbol_table_merge");
        PHASE_NAME_MAP.put(PHASE_UPDATE_SYMBOL_KEYS, "update_symbol_keys");
        PHASE_NAME_MAP.put(PHASE_BUILD_SYMBOL_INDEX, "build_symbol_index");
        PHASE_NAME_MAP.put(PHASE_MOVE_PARTITIONS, "move_partitions");
        PHASE_NAME_MAP.put(PHASE_ATTACH_PARTITIONS, "attach_partitions");
        PHASE_NAME_MAP.put(PHASE_ANALYZE_FILE_STRUCTURE, "analyze_file_structure");
        PHASE_NAME_MAP.put(PHASE_CLEANUP, "cleanup");

        STATUS_NAME_MAP.put(STATUS_STARTED, "started");
        STATUS_NAME_MAP.put(STATUS_FINISHED, "finished");
        STATUS_NAME_MAP.put(STATUS_FAILED, "failed");
        STATUS_NAME_MAP.put(STATUS_CANCELLED, "cancelled");
    }
}
