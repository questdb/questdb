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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cutlass.text.types.TimestampAdapter;
import io.questdb.cutlass.text.types.TypeAdapter;
import io.questdb.griffin.engine.functions.columns.ColumnUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.DirectCharSink;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import static io.questdb.cutlass.text.ParallelCsvFileImporter.createTable;

public class TextImportTask {

    private static final Log LOG = LogFactory.getLog(TextImportTask.class);

    public static final byte PHASE_BOUNDARY_CHECK = 1;
    public static final byte PHASE_INDEXING = 2;
    public static final byte PHASE_PARTITION_IMPORT = 3;
    public static final byte PHASE_SYMBOL_TABLE_MERGE = 4;
    public static final byte PHASE_UPDATE_SYMBOL_KEYS = 5;
    public static final byte PHASE_BUILD_SYMBOL_INDEX = 6;

    private static final IntObjHashMap<String> PHASE_NAME_MAP = new IntObjHashMap<>();

    static {
        PHASE_NAME_MAP.put(PHASE_BOUNDARY_CHECK, "BOUNDARY_CHECK");
        PHASE_NAME_MAP.put(PHASE_INDEXING, "INDEXING");
        PHASE_NAME_MAP.put(PHASE_PARTITION_IMPORT, "PARTITION_IMPORT");
        PHASE_NAME_MAP.put(PHASE_SYMBOL_TABLE_MERGE, "SYMBOL_TABLE_MERGE");
        PHASE_NAME_MAP.put(PHASE_UPDATE_SYMBOL_KEYS, "UPDATE_SYMBOL_KEYS");
        PHASE_NAME_MAP.put(PHASE_BUILD_SYMBOL_INDEX, "BUILD_SYMBOL_INDEX");
    }

    public static final byte STATUS_OK = 0;
    public static final byte STATUS_ERROR = 1;
    public static final byte STATUS_CANCEL = 2;

    private byte phase;
    private int index;
    private SqlExecutionCircuitBreaker circuitBreaker;

    private final CountQuotesStage countQuotesStage = new CountQuotesStage();
    private final BuildPartitionIndexStage buildPartitionIndexStage = new BuildPartitionIndexStage();
    private final ImportPartitionDataStage importPartitionDataStage = new ImportPartitionDataStage();
    private final MergeSymbolTablesStage mergeSymbolTablesStage = new MergeSymbolTablesStage();
    private final UpdateSymbolColumnKeysStage updateSymbolColumnKeysStage = new UpdateSymbolColumnKeysStage();
    private final BuildSymbolColumnIndexStage buildSymbolColumnIndexStage = new BuildSymbolColumnIndexStage();

    private byte status;
    private CharSequence errorMessage;

    public static String getPhaseName(byte phase) {
        return PHASE_NAME_MAP.get(phase);
    }

    public BuildPartitionIndexStage getBuildPartitionIndexStage() {
        return buildPartitionIndexStage;
    }

    public ImportPartitionDataStage getImportPartitionDataStage() {
        return importPartitionDataStage;
    }

    public CountQuotesStage getCountQuotesStage() {
        return countQuotesStage;
    }

    public CharSequence getErrorMessage() {
        return errorMessage;
    }

    public int getIndex() {
        return index;
    }

    public byte getPhase() {
        return phase;
    }

    public boolean isFailed() {
        return this.status == STATUS_ERROR;
    }

    public boolean isCancelled() {
        return this.status == STATUS_CANCEL;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public void setCircuitBreaker(SqlExecutionCircuitBreaker circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
    }

    public void ofBuildPartitionIndexStage(long chunkStart,
                                           long chunkEnd,
                                           long lineNumber,
                                           int index,
                                           CairoConfiguration configuration,
                                           CharSequence inputFileName,
                                           CharSequence importRoot,
                                           int partitionBy,
                                           byte columnDelimiter,
                                           int timestampIndex,
                                           TimestampAdapter adapter,
                                           boolean ignoreHeader,
                                           int atomicity) {
        this.phase = PHASE_INDEXING;
        this.buildPartitionIndexStage.of(chunkStart,
                chunkEnd,
                lineNumber,
                index,
                configuration,
                inputFileName,
                importRoot,
                partitionBy,
                columnDelimiter,
                timestampIndex,
                adapter,
                ignoreHeader,
                atomicity);
    }

    public void ofBuildSymbolColumnIndexStage(CairoEngine cairoEngine, TableStructure tableStructure, CharSequence root, int index, RecordMetadata metadata) {
        this.phase = PHASE_BUILD_SYMBOL_INDEX;
        this.buildSymbolColumnIndexStage.of(cairoEngine, tableStructure, root, index, metadata);
    }

    public void ofCountQuotesStage(final FilesFacade ff, Path path, long chunkStart, long chunkEnd, int bufferLength) {
        this.phase = PHASE_BOUNDARY_CHECK;
        this.countQuotesStage.of(ff, path, chunkStart, chunkEnd, bufferLength);
    }

    void ofImportPartitionDataStage(CairoEngine cairoEngine,
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
        this.importPartitionDataStage.of(cairoEngine, targetTableStructure, types, atomicity, columnDelimiter, importRoot, inputFileName, index, lo, hi, partitions);
    }

    public void ofMergeSymbolTablesStage(CairoConfiguration cfg,
                                         CharSequence importRoot,
                                         TableWriter writer,
                                         CharSequence table,
                                         CharSequence column,
                                         int columnIndex,
                                         int symbolColumnIndex,
                                         int tmpTableCount,
                                         int partitionBy
    ) {
        this.phase = PHASE_SYMBOL_TABLE_MERGE;
        this.mergeSymbolTablesStage.of(cfg, importRoot, writer, table, column, columnIndex, symbolColumnIndex, tmpTableCount, partitionBy);
    }

    public void ofUpdateSymbolColumnKeysStage(CairoEngine cairoEngine,
                                              TableStructure tableStructure,
                                              int index,
                                              long partitionSize,
                                              long partitionTimestamp,
                                              CharSequence root,
                                              CharSequence columnName,
                                              int symbolCount
    ) {
        this.phase = PHASE_UPDATE_SYMBOL_KEYS;
        this.updateSymbolColumnKeysStage.of(cairoEngine, tableStructure, index, partitionSize, partitionTimestamp, root, columnName, symbolCount);
    }

    public boolean run(TextLexer lexer, CsvFileIndexer indexer, DirectCharSink utf8Sink, DirectLongList mergeIndexes, long fileBufAddr, long fileBufSize, Path p1, Path p2) {
        try {
            status = STATUS_OK;
            if (circuitBreaker != null && circuitBreaker.checkIfTripped()) {
                status = STATUS_CANCEL;
                this.errorMessage = "Task is cancelled";
                return false;
            }
            if (phase == PHASE_BOUNDARY_CHECK) {
                countQuotesStage.run(fileBufAddr, fileBufSize);
            } else if (phase == PHASE_INDEXING) {
                buildPartitionIndexStage.run(indexer, fileBufAddr, fileBufSize);
            } else if (phase == PHASE_PARTITION_IMPORT) {
                importPartitionDataStage.run(lexer, fileBufAddr, fileBufSize, utf8Sink, mergeIndexes, p1, p2);
            } else if (phase == PHASE_SYMBOL_TABLE_MERGE) {
                mergeSymbolTablesStage.run(p1);
            } else if (phase == PHASE_UPDATE_SYMBOL_KEYS) {
                updateSymbolColumnKeysStage.run(p1);
            } else if (phase == PHASE_BUILD_SYMBOL_INDEX) {
                buildSymbolColumnIndexStage.run();
            } else {
                throw TextException.$("Unexpected phase ").put(phase);
            }
        } catch (Throwable t) {
            LOG.error().$("Import error in ").$(getPhaseName(phase)).$(" phase. ").$(t).$();
            this.status = STATUS_ERROR;
            this.errorMessage = t.getMessage();
            return false;
        }

        return true;
    }

    public void clear() {
        if (phase == PHASE_BOUNDARY_CHECK) {
            countQuotesStage.clear();
        } else if (phase == PHASE_INDEXING) {
            buildPartitionIndexStage.clear();
        } else if (phase == PHASE_PARTITION_IMPORT) {
            importPartitionDataStage.clear();
        } else if (phase == PHASE_SYMBOL_TABLE_MERGE) {
            mergeSymbolTablesStage.clear();
        } else if (phase == PHASE_UPDATE_SYMBOL_KEYS) {
            updateSymbolColumnKeysStage.clear();
        } else if (phase == PHASE_BUILD_SYMBOL_INDEX) {
            buildSymbolColumnIndexStage.clear();
        } else {
            throw TextException.$("Unexpected phase ").put(phase);
        }
    }

    public static class CountQuotesStage {
        private long quoteCount;
        private long newLineCountEven;
        private long newLineCountOdd;
        private long newLineOffsetEven;
        private long newLineOffsetOdd;

        private long chunkStart;
        private long chunkEnd;
        private Path path;
        private FilesFacade ff;

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

        public void of(final FilesFacade ff, Path path, long chunkStart, long chunkEnd, int bufferLength) {
            assert ff != null;
            assert path != null;
            assert chunkStart >= 0 && chunkEnd > chunkStart;
            assert bufferLength > 0;

            this.ff = ff;
            this.path = path;
            this.chunkStart = chunkStart;
            this.chunkEnd = chunkEnd;
        }

        public void run(long fileBufPtr, long fileBufSize) throws TextException {
            long offset = chunkStart;

            //output vars
            long quotes = 0;
            long[] nlCount = new long[2];
            long[] nlFirst = new long[]{-1, -1};

            long read;
            long ptr;
            long hi;

            long fd = TableUtils.openRO(ff, path, LOG);
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
                        final byte c = Unsafe.getUnsafe().getByte(ptr++);
                        if (c == '"') {
                            quotes++;
                        } else if (c == '\n') {
                            nlCount[(int) (quotes & 1)]++;
                            if (nlFirst[(int) (quotes & 1)] == -1) {
                                nlFirst[(int) (quotes & 1)] = offset + (ptr - fileBufPtr);
                            }
                        }
                    }

                    offset += read;
                } while (offset < chunkEnd);

                if (read < 0 || offset < chunkEnd) {
                    throw TextException.$("Could not read import file [path='").put(path).put("',offset=").put(offset).put(",errno=").put(ff.errno()).put("]");
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

        public void clear() {
            this.ff = null;
            this.path = null;
            this.chunkStart = -1;
            this.chunkEnd = -1;
        }
    }

    public static class BuildPartitionIndexStage {
        private final LongList partitionKeysAndSizes = new LongList();//stores partition key and size for all indexed partitions
        private long chunkStart;
        private long chunkEnd;
        private long lineNumber;
        private CharSequence inputFileName;
        private CharSequence importRoot;
        private int index;
        private int partitionBy;
        private byte columnDelimiter;
        private int timestampIndex;
        private TimestampAdapter adapter;
        private boolean ignoreHeader;
        private CairoConfiguration configuration;
        private int atomicity;

        public LongList getPartitionKeysAndSizes() {
            return partitionKeysAndSizes;
        }

        public void of(long chunkStart,
                       long chunkEnd,
                       long lineNumber,
                       int index,
                       CairoConfiguration configuration,
                       CharSequence inputFileName,
                       CharSequence importRoot,
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
            this.configuration = configuration;
            this.inputFileName = inputFileName;
            this.importRoot = importRoot;
            this.partitionBy = partitionBy;
            this.columnDelimiter = columnDelimiter;
            this.timestampIndex = timestampIndex;
            this.adapter = adapter;
            this.ignoreHeader = ignoreHeader;
            this.atomicity = atomicity;
        }

        public void run(CsvFileIndexer indexer, long fileBufAddr, long fileBufSize) throws TextException {
            try {
                indexer.of(inputFileName, importRoot, index, partitionBy, columnDelimiter, timestampIndex, adapter, ignoreHeader, atomicity);
                indexer.index(chunkStart, chunkEnd, lineNumber, partitionKeysAndSizes, fileBufAddr, fileBufSize);
            } finally {
                indexer.clear();
            }
        }

        public void clear() {
            this.chunkStart = -1;
            this.chunkEnd = -1;
            this.lineNumber = -1;

            this.index = -1;
            this.configuration = null;
            this.inputFileName = null;
            this.importRoot = null;
            this.partitionBy = -1;
            this.columnDelimiter = (byte) -1;
            this.timestampIndex = -1;
            this.adapter = null;
            this.ignoreHeader = false;
            this.atomicity = -1;
        }
    }

    public static class ImportPartitionDataStage {
        private static final Log LOG = LogFactory.getLog(ImportPartitionDataStage.class);//todo: use shared instance
        private final StringSink tableNameSink = new StringSink();
        private CairoEngine cairoEngine;
        private TableStructure targetTableStructure;
        private ObjList<TypeAdapter> types;
        private int atomicity;
        private byte columnDelimiter;
        private CharSequence importRoot;
        private CharSequence inputFileName;

        private int index;
        private int lo;
        private int hi;
        private ObjList<ParallelCsvFileImporter.PartitionInfo> partitions;

        private TableWriter tableWriterRef;
        private int timestampIndex;
        private TimestampAdapter timestampAdapter;

        private long offset;
        private int errors;
        private final LongList importedRows = new LongList();

        private DirectCharSink utf8Sink;

        void of(CairoEngine cairoEngine,
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
            this.cairoEngine = cairoEngine;
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

        public void run(TextLexer lexer, long fileBufAddr, long fileBufSize, DirectCharSink sink, DirectLongList longList, Path path, Path tmpPath) throws TextException {

            this.utf8Sink = sink;
            tableNameSink.clear();
            tableNameSink.put(targetTableStructure.getTableName()).put('_').put(index);

            final CairoConfiguration configuration = cairoEngine.getConfiguration();
            final FilesFacade ff = configuration.getFilesFacade();
            createTable(ff, configuration.getMkDirMode(), importRoot, tableNameSink, targetTableStructure, 0, configuration);

            try (TableWriter writer = new TableWriter(configuration,
                    tableNameSink,
                    cairoEngine.getMessageBus(),
                    null,
                    true,
                    DefaultLifecycleManager.INSTANCE,
                    importRoot,
                    cairoEngine.getMetrics())) {

                tableWriterRef = writer;

                lexer.setTableName(tableNameSink);
                lexer.of(columnDelimiter);
                lexer.setSkipLinesWithExtraValues(false);

                try {
                    for (int i = lo; i < hi; i++) {
                        lexer.clear();
                        errors = 0;

                        final CharSequence name = partitions.get(i).name;
                        path.of(importRoot).concat(name);
                        mergePartitionIndexAndImportData(ff, path, lexer, fileBufAddr, fileBufSize, sink, longList, tmpPath);

                        long imported = atomicity == Atomicity.SKIP_ROW ? lexer.getLineCount() - errors : lexer.getLineCount();
                        importedRows.add(i);
                        importedRows.add(imported);

                        LOG.info().$("Imported data [temp_table=").$(tableNameSink).$(",partition=").$(name).$(",lines=").$(lexer.getLineCount()).$(",errors=").$(errors).$("]").$();
                    }
                } finally {
                    writer.commit(CommitMode.SYNC);
                }
            }
        }

        public LongList getImportedRows() {
            return importedRows;
        }

        public void clear() {
            this.cairoEngine = null;
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

            this.errors = 0;
            this.offset = 0;
            this.importedRows.clear();
            this.tableNameSink.clear();

            this.utf8Sink = null;
        }

        private void logError(long offset, int column, final DirectByteCharSequence dbcs) {
            LOG.error()
                    .$("type syntax [type=").$(ColumnType.nameOf(types.getQuick(column).getType()))
                    .$(", offset=").$(offset)
                    .$(", column=").$(column)
                    .$(", value='").$(dbcs)
                    .I$();
        }

        private void importPartitionData(final TextLexer lexer, long address, long size, long fileBufAddr, long fileBufSize, DirectCharSink utf8Sink, Path path) throws TextException {
            final CairoConfiguration configuration = cairoEngine.getConfiguration();
            final FilesFacade ff = configuration.getFilesFacade();

            long fd = -1;
            try {
                path.of(configuration.getInputRoot()).concat(inputFileName).$();
                utf8Sink.clear();
                fd = TableUtils.openRO(ff, path, LOG);

                long MASK = ~((255L) << 56 | (255L) << 48);
                final long count = size / (2 * Long.BYTES);
                long bytesToRead;
                int additionalLines;

                for (long i = 0; i < count; i++) {
                    long lengthAndOffset = Unsafe.getUnsafe().getLong(address + i * 2L * Long.BYTES + Long.BYTES);
                    long lineLength = lengthAndOffset >>> 48;
                    offset = lengthAndOffset & MASK;
                    bytesToRead = lineLength;
                    additionalLines = 0;

                    for (long j = i + 1; j < count; j++) {
                        long nextLengthAndOffset = Unsafe.getUnsafe().getLong(address + j * 2L * Long.BYTES + Long.BYTES);
                        long nextLineLength = nextLengthAndOffset >>> 48;
                        long nextOffset = nextLengthAndOffset & MASK;

                        //line indexing stops on first EOL char, e.g. \r, but it could be followed by \n
                        long diff = nextOffset - offset - bytesToRead;
                        if (diff > -1 && diff < 2 && bytesToRead + nextLineLength < fileBufSize) {
                            bytesToRead += diff + nextLineLength;
                            additionalLines++;
                        } else {
                            break;
                        }
                    }

                    i += additionalLines;

                    long n = ff.read(fd, fileBufAddr, bytesToRead, offset);
                    if (n > 0) {
                        lexer.parse(fileBufAddr, fileBufAddr + n, Integer.MAX_VALUE, this::onFieldsPartitioned);
                    } else {
                        throw TextException.$("Can't read from file path='").put(path).put("', errno=").put(ff.errno()).put(",offset=").put(offset);
                    }
                }

                lexer.parseLast();
            } finally {
                if (fd > -1) {
                    ff.close(fd);
                }
            }
        }

        private void mergePartitionIndexAndImportData(
                final FilesFacade ff,
                Path partitionPath,
                final TextLexer lexer,
                long fileBufAddr,
                long fileBufSize,
                DirectCharSink utf8Sink,
                DirectLongList mergeIndexes,
                Path tmpPath
        ) throws TextException {
            mergeIndexes.clear();
            partitionPath.slash$();
            int partitionLen = partitionPath.length();

            long mergedIndexSize = -1;
            long address = -1;
            long fd = -1;

            try {
                mergedIndexSize = openIndexChunks(ff, partitionPath, mergeIndexes, partitionLen);

                partitionPath.trimTo(partitionLen);
                partitionPath.concat(CsvFileIndexer.INDEX_FILE_NAME).$();

                fd = TableUtils.openFileRWOrFail(ff, partitionPath, CairoConfiguration.O_NONE);
                address = TableUtils.mapRW(ff, fd, mergedIndexSize, MemoryTag.MMAP_PARALLEL_IMPORT);

                long mergedAddr = getMergedIndexes(ff, address, mergeIndexes);

                importPartitionData(lexer, mergedAddr, mergedIndexSize, fileBufAddr, fileBufSize, utf8Sink, tmpPath);
            } finally {
                if (fd > -1) {
                    ff.close(fd);
                }
                if (address != -1) {
                    ff.munmap(address, mergedIndexSize, MemoryTag.MMAP_PARALLEL_IMPORT);
                }
                unmap(ff, mergeIndexes);
            }
        }

        private long getMergedIndexes(FilesFacade ff, long address, DirectLongList mergeIndexes) {
            try {
                return Vect.mergeLongIndexesAscExt(mergeIndexes.getAddress(), (int) mergeIndexes.size() / 2, address);
            } finally {
                //release chunk memory early
                //if there's only one chunk we didn't have to merge, can use it and have to release after loading data
                if (mergeIndexes.size() > 2) {
                    unmap(ff, mergeIndexes);
                }
            }
        }

        private void unmap(FilesFacade ff, DirectLongList mergeIndexes) {
            for (long i = 0, sz = mergeIndexes.size() / 2; i < sz; i++) {
                final long addr = mergeIndexes.get(2 * i);
                final long size = mergeIndexes.get(2 * i + 1) * CsvFileIndexer.INDEX_ENTRY_SIZE;
                ff.munmap(addr, size, MemoryTag.MMAP_PARALLEL_IMPORT);
            }
            mergeIndexes.clear();
        }

        private boolean onField(long offset, final DirectByteCharSequence dbcs, TableWriter.Row w, int fieldIndex, DirectCharSink utf8Sink)
                throws TextException {
            TypeAdapter type = this.types.getQuick(fieldIndex);
            try {
                type.write(w, fieldIndex, dbcs, utf8Sink);
            } catch (Exception ignore) {
                errors++;
                logError(offset, fieldIndex, dbcs);
                switch (atomicity) {
                    case Atomicity.SKIP_ALL:
                        tableWriterRef.rollback();
                        throw TextException.$("bad syntax [line offset=").put(offset).put(",column=").put(fieldIndex).put(']');
                    case Atomicity.SKIP_ROW:
                        w.cancel();
                        return true;
                    default: // SKIP column
                        break;
                }
            }
            return false;
        }

        private void onFieldsPartitioned(long line, ObjList<DirectByteCharSequence> values, int valuesLength) {
            assert tableWriterRef != null;
            DirectByteCharSequence dbcs = values.getQuick(timestampIndex);
            final TableWriter.Row w = getRow(dbcs, offset);
            if (w == null) {
                return;
            }
            for (int i = 0; i < valuesLength; i++) {
                dbcs = values.getQuick(i);
                if (i == timestampIndex || dbcs.length() == 0) {
                    continue;
                }
                if (onField(offset, dbcs, w, i, utf8Sink)) return;
            }
            w.append();
        }

        private TableWriter.Row getRow(DirectByteCharSequence dbcs, long offset) {
            try {
                return tableWriterRef.newRow(timestampAdapter.getTimestamp(dbcs));
            } catch (Exception e) {
                if (atomicity == Atomicity.SKIP_ALL) {
                    throw TextException.$("Can't parse timestamp on line starting at offset=").put(offset).put(". ").put(e.getMessage());
                } else {
                    logError(offset, timestampIndex, dbcs);
                    return null;
                }
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

                            long fd = TableUtils.openRO(ff, partitionPath, LOG);
                            long size = 0;
                            long address = -1;

                            try {
                                size = ff.length(fd);
                                address = TableUtils.mapRO(ff, fd, size, MemoryTag.MMAP_PARALLEL_IMPORT);
                                mergeIndexes.add(address);
                                mergeIndexes.add(size / CsvFileIndexer.INDEX_ENTRY_SIZE);
                                mergedIndexSize += size;
                            } catch (Throwable t) {
                                if (address != -1) { //release mem if it can't be added to mergeIndexes
                                    ff.munmap(address, size, MemoryTag.MMAP_PARALLEL_IMPORT);
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
    }

    public static class MergeSymbolTablesStage {
        private CairoConfiguration cfg;
        private CharSequence importRoot;
        private TableWriter writer;
        private CharSequence table;
        private CharSequence column;
        private int columnIndex;
        private int symbolColumnIndex;
        private int tmpTableCount;
        private int partitionBy;

        public void of(CairoConfiguration cfg,
                       CharSequence importRoot,
                       TableWriter writer,
                       CharSequence table,
                       CharSequence column,
                       int columnIndex,
                       int symbolColumnIndex,
                       int tmpTableCount,
                       int partitionBy
        ) {
            this.cfg = cfg;
            this.importRoot = importRoot;
            this.writer = writer;
            this.table = table;
            this.column = column;
            this.columnIndex = columnIndex;
            this.symbolColumnIndex = symbolColumnIndex;
            this.tmpTableCount = tmpTableCount;
            this.partitionBy = partitionBy;
        }

        public void run(Path path) {
            final FilesFacade ff = cfg.getFilesFacade();
            path.of(importRoot).concat(table);
            int plen = path.length();
            for (int i = 0; i < tmpTableCount; i++) {
                path.trimTo(plen);
                path.put("_").put(i);
                try (TxReader txFile = new TxReader(ff).ofRO(path, partitionBy)) {
                    txFile.unsafeLoadAll();
                    int symbolCount = txFile.getSymbolValueCount(symbolColumnIndex);
                    try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(cfg, path, column, TableUtils.COLUMN_NAME_TXN_NONE, symbolCount)) {
                        try (MemoryCMARW mem = Vm.getSmallCMARWInstance(
                                ff,
                                path.concat(column).put(TableUtils.SYMBOL_KEY_REMAP_FILE_SUFFIX).$(),
                                MemoryTag.MMAP_PARALLEL_IMPORT,
                                cfg.getWriterFileOpenOpts()
                        )) {
                            SymbolMapWriter.mergeSymbols(writer.getSymbolMapWriter(columnIndex), reader, mem);//TODO: use result !
                        }
                    }
                }
            }
        }

        public void clear() {
            this.cfg = null;
            this.importRoot = null;
            this.writer = null;
            this.table = null;
            this.column = null;
            this.columnIndex = -1;
            this.symbolColumnIndex = -1;
            this.tmpTableCount = -1;
            this.partitionBy = -1;
        }
    }

    public static class UpdateSymbolColumnKeysStage {
        int index;
        long partitionSize;
        long partitionTimestamp;
        CharSequence root;
        CharSequence columnName;
        int symbolCount;
        private CairoEngine cairoEngine;
        private TableStructure tableStructure;

        public void of(CairoEngine cairoEngine,
                       TableStructure tableStructure,
                       int index,
                       long partitionSize,
                       long partitionTimestamp,
                       CharSequence root,
                       CharSequence columnName,
                       int symbolCount
        ) {
            this.cairoEngine = cairoEngine;
            this.tableStructure = tableStructure;
            this.index = index;
            this.partitionSize = partitionSize;
            this.partitionTimestamp = partitionTimestamp;
            this.root = root;
            this.columnName = columnName;
            this.symbolCount = symbolCount;
        }

        public void run(Path path) {
            final FilesFacade ff = cairoEngine.getConfiguration().getFilesFacade();
            path.of(root).concat(tableStructure.getTableName()).put("_").put(index);
            int plen = path.length();
            PartitionBy.setSinkForPartition(path.slash(), tableStructure.getPartitionBy(), partitionTimestamp, false);
            path.concat(columnName).put(TableUtils.FILE_SUFFIX_D);

            long columnMemory = 0;
            long columnMemorySize = 0;
            long remapTableMemory = 0;
            long remapTableMemorySize = 0;
            long columnFd = -1;
            long remapFd = -1;
            try {
                columnFd = TableUtils.openFileRWOrFail(ff, path.$(), CairoConfiguration.O_NONE);
                columnMemorySize = ff.length(columnFd);

                path.trimTo(plen);
                path.concat(columnName).put(TableUtils.SYMBOL_KEY_REMAP_FILE_SUFFIX);
                remapFd = TableUtils.openFileRWOrFail(ff, path.$(), CairoConfiguration.O_NONE);
                remapTableMemorySize = ff.length(remapFd);

                if (columnMemorySize >= Integer.BYTES && remapTableMemorySize >= Integer.BYTES) {
                    columnMemory = TableUtils.mapRW(ff, columnFd, columnMemorySize, MemoryTag.MMAP_PARALLEL_IMPORT);
                    remapTableMemory = TableUtils.mapRW(ff, remapFd, remapTableMemorySize, MemoryTag.MMAP_PARALLEL_IMPORT);
                    long columnMemSize = partitionSize * Integer.BYTES;
                    long remapMemSize = (long) symbolCount * Integer.BYTES;
                    ColumnUtils.symbolColumnUpdateKeys(columnMemory, columnMemSize, remapTableMemory, remapMemSize);
                }
            } finally {
                if (columnFd != -1) {
                    ff.close(columnFd);
                }
                if (remapFd != -1) {
                    ff.close(remapFd);
                }
                if (columnMemory > 0) {
                    ff.munmap(columnMemory, columnMemorySize, MemoryTag.MMAP_PARALLEL_IMPORT);
                }
                if (remapTableMemory > 0) {
                    ff.munmap(remapTableMemory, remapTableMemorySize, MemoryTag.MMAP_PARALLEL_IMPORT);
                }
            }
        }

        public void clear() {
            this.cairoEngine = null;
            this.tableStructure = null;
            this.index = -1;
            this.partitionSize = -1;
            this.partitionTimestamp = -1;
            this.root = null;
            this.columnName = null;
            this.symbolCount = -1;
        }
    }

    public static class BuildSymbolColumnIndexStage {
        private final StringSink tableNameSink = new StringSink();
        private CairoEngine cairoEngine;
        private TableStructure tableStructure;
        private CharSequence root;
        private int index;
        private RecordMetadata metadata;

        public void of(CairoEngine cairoEngine, TableStructure tableStructure, CharSequence root, int index, RecordMetadata metadata) {
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
            final int columnCount = metadata.getColumnCount();
            try (TableWriter w = new TableWriter(configuration,
                    tableNameSink,
                    cairoEngine.getMessageBus(),
                    null,
                    true,
                    DefaultLifecycleManager.INSTANCE,
                    root,
                    cairoEngine.getMetrics())) {
                for (int i = 0; i < columnCount; i++) {
                    if (metadata.isColumnIndexed(i)) {
                        w.addIndex(metadata.getColumnName(i), metadata.getIndexValueBlockCapacity(i));
                    }
                }
            }
        }

        public void clear() {
            this.cairoEngine = null;
            this.tableStructure = null;
            this.root = null;
            this.index = -1;
            this.metadata = null;
        }
    }
}
