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
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cutlass.text.types.TimestampAdapter;
import io.questdb.cutlass.text.types.TypeAdapter;
import io.questdb.griffin.engine.functions.columns.ColumnUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.std.*;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.DirectCharSink;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import static io.questdb.cutlass.text.FileIndexer.createTable;

public class TextImportTask {

    private static final Log LOG = LogFactory.getLog(TextImportTask.class);

    public static final byte PHASE_BOUNDARY_CHECK = 1;
    public static final byte PHASE_INDEXING = 2;
    public static final byte PHASE_PARTITION_IMPORT = 3;
    public static final byte PHASE_SYMBOL_TABLE_MERGE = 4;
    public static final byte PHASE_UPDATE_SYMBOL_KEYS = 5;
    public static final byte PHASE_BUILD_INDEX = 6;

    public static final byte STATUS_OK = 0;
    public static final byte STATUS_ERROR = 1;

    private byte phase;
    private int index;

    private final CountQuotesStage countQuotesStage = new CountQuotesStage();
    private final BuildPartitionIndexStage buildPartitionIndexStage = new BuildPartitionIndexStage();
    private final ImportPartitionDataStage importPartitionDataStage = new ImportPartitionDataStage();
    private final MergeSymbolTablesStage mergeSymbolTablesStage = new MergeSymbolTablesStage();
    private final UpdateSymbolColumnKeysStage updateSymbolColumnKeysStage = new UpdateSymbolColumnKeysStage();
    private final BuildSymbolColumnIndexStage buildSymbolColumnIndexStage = new BuildSymbolColumnIndexStage();

    private byte status;
    private CharSequence errorMessage;

    public BuildPartitionIndexStage getBuildPartitionIndexStage() {
        return buildPartitionIndexStage;
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

    public boolean isError() {
        return this.status != STATUS_OK;
    }

    public void setIndex(int index) {
        this.index = index;
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
                                           int bufferLen) {
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
                bufferLen);
    }

    public void ofBuildSymbolColumnIndexStage(CairoEngine cairoEngine, TableStructure tableStructure, CharSequence root, int index, RecordMetadata metadata) {
        this.phase = PHASE_BUILD_INDEX;
        this.buildSymbolColumnIndexStage.of(cairoEngine, tableStructure, root, index, metadata);
    }

    public void ofCountQuotesStage(final FilesFacade ff, Path path, long chunkStart, long chunkEnd, int bufferLength) {
        this.phase = PHASE_BOUNDARY_CHECK;
        this.countQuotesStage.of(ff, path, chunkStart, chunkEnd, bufferLength);
    }

    public void ofImportPartitionDataStage(CairoEngine cairoEngine,
                                           TableStructure targetTableStructure,
                                           ObjList<TypeAdapter> types,
                                           int atomicity,
                                           byte columnDelimiter,
                                           CharSequence importRoot,
                                           CharSequence inputFileName,
                                           int index,
                                           long lo,
                                           long hi,
                                           final ObjList<CharSequence> partitionNames,
                                           int maxLineLength
    ) {
        this.phase = PHASE_PARTITION_IMPORT;
        this.importPartitionDataStage.of(cairoEngine, targetTableStructure, types, atomicity, columnDelimiter, importRoot, inputFileName, index, lo, hi, partitionNames, maxLineLength);
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

    public boolean run() {
        try {
            status = STATUS_OK;

            if (phase == PHASE_BOUNDARY_CHECK) {
                countQuotesStage.run();
            } else if (phase == PHASE_INDEXING) {
                buildPartitionIndexStage.run();
            } else if (phase == PHASE_PARTITION_IMPORT) {
                importPartitionDataStage.run();
            } else if (phase == PHASE_SYMBOL_TABLE_MERGE) {
                mergeSymbolTablesStage.run();
            } else if (phase == PHASE_UPDATE_SYMBOL_KEYS) {
                updateSymbolColumnKeysStage.run();
            } else if (phase == PHASE_BUILD_INDEX) {
                buildSymbolColumnIndexStage.run();
            } else {
                throw TextException.$("Unexpected phase ").put(phase);
            }

        } catch (Throwable t) {
            LOG.error().$(t).$();
            this.status = STATUS_ERROR;
            this.errorMessage = t.getMessage();
            return false;
        }

        return true;
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
        private int bufferLength;

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
            this.bufferLength = bufferLength;
        }

        public void run() throws TextException {

            long offset = chunkStart;

            //output vars
            long quotes = 0;
            long[] nlCount = new long[2];
            long[] nlFirst = new long[]{-1, -1};

            long read;
            long ptr;
            long hi;

            long fd = ff.openRO(path);
            if (fd < 0) {
                throw TextException.$("Can't open file path='").put(path).put("', errno=").put(ff.errno());
            }
            long fileBufferPtr = -1;
            try {
                fileBufferPtr = Unsafe.malloc(bufferLength, MemoryTag.NATIVE_DEFAULT);


                do {
                    long leftToRead = Math.min(chunkEnd - offset, bufferLength);
                    read = (int) ff.read(fd, fileBufferPtr, leftToRead, offset);
                    if (read < 1) {
                        break;
                    }
                    hi = fileBufferPtr + read;
                    ptr = fileBufferPtr;

                    while (ptr < hi) {
                        final byte c = Unsafe.getUnsafe().getByte(ptr++);
                        if (c == '"') {
                            quotes++;
                        } else if (c == '\n') {
                            nlCount[(int) (quotes & 1)]++;
                            if (nlFirst[(int) (quotes & 1)] == -1) {
                                nlFirst[(int) (quotes & 1)] = offset + (ptr - fileBufferPtr);
                            }
                        }
                    }

                    offset += read;
                } while (offset < chunkEnd);

                if (read < 0 || offset < chunkEnd) {
                    throw CairoException.instance(ff.errno()).put("could not read import file [path='").put(path).put("',read bytes=").put(read).put(",offset=").put(offset).put("]");
                }
            } finally {
                ff.close(fd);
                if (fileBufferPtr > 0) {
                    Unsafe.free(fileBufferPtr, bufferLength, MemoryTag.NATIVE_DEFAULT);
                }
            }

            this.quoteCount = quotes;
            this.newLineCountEven = nlCount[0];
            this.newLineCountOdd = nlCount[1];
            this.newLineOffsetEven = nlFirst[0];
            this.newLineOffsetOdd = nlFirst[1];
        }
    }

    public static class BuildPartitionIndexStage {
        private final LongList partitionKeys = new LongList();
        private long chunkStart;
        private long chunkEnd;
        private long lineNumber;
        private long maxLineLength;
        private CharSequence inputFileName;
        private CharSequence importRoot;
        private int index;
        private int partitionBy;
        private byte columnDelimiter;
        private int timestampIndex;
        private TimestampAdapter adapter;
        private boolean ignoreHeader;
        private CairoConfiguration configuration;
        private int bufferLen;

        public long getMaxLineLength() {
            return maxLineLength;
        }

        public LongList getPartitionKeys() {
            return partitionKeys;
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
                       int bufferLen) {
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
            this.bufferLen = bufferLen;
        }

        public void run() {
            try (FileSplitter splitter = new FileSplitter(configuration)) {
                splitter.setBufferLength(bufferLen);
                splitter.of(inputFileName, importRoot, index, partitionBy, columnDelimiter, timestampIndex, adapter, ignoreHeader);
                splitter.index(chunkStart, chunkEnd, lineNumber, partitionKeys);
                this.maxLineLength = splitter.getMaxLineLength();
            }
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
        private long lo;
        private long hi;
        private ObjList<CharSequence> partitionNames;
        private int maxLineLength;

        private TableWriter tableWriterRef;
        private int timestampIndex;
        private TimestampAdapter timestampAdapter;

        public void of(CairoEngine cairoEngine,
                       TableStructure targetTableStructure,
                       ObjList<TypeAdapter> types,
                       int atomicity,
                       byte columnDelimiter,
                       CharSequence importRoot,
                       CharSequence inputFileName,
                       int index,
                       long lo,
                       long hi,
                       final ObjList<CharSequence> partitionNames,
                       int maxLineLength
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
            this.partitionNames = partitionNames;
            this.maxLineLength = maxLineLength;
            this.timestampIndex = targetTableStructure.getTimestampIndex();
            this.timestampAdapter = (timestampIndex > -1 && timestampIndex < types.size()) ? (TimestampAdapter) types.getQuick(timestampIndex) : null;
        }

        public void run() {
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

                final TextConfiguration textConfiguration = configuration.getTextConfiguration();
                try (TextLexer lexer = new TextLexer(textConfiguration)) {
                    lexer.setTableName(tableNameSink);
                    lexer.of(columnDelimiter);
                    lexer.setSkipLinesWithExtraValues(false);
                    try {
                        lexer.restart(false);
                        for (int i = (int) lo; i < hi; i++) {
                            final CharSequence name = partitionNames.get(i);
                            try (Path path = new Path().of(importRoot).concat(name)) {
                                mergePartitionIndexAndImportData(ff, path, lexer, types, maxLineLength);
                            }
                        }
                    } finally {
                        lexer.parseLast();
                        writer.commit(CommitMode.SYNC);
                    }
                }
            }
        }

        private void logError(long line, int i, final DirectByteCharSequence dbcs) {
            LogRecord logRecord = LOG.error().$("type syntax [type=").$(ColumnType.nameOf(types.getQuick(i).getType())).$("]\t");
            logRecord.$('[').$(line).$(':').$(i).$("] -> ").$(dbcs).$();
        }

        private void importPartitionData(final TextLexer lexer, final ObjList<TypeAdapter> types, long address, long size, int len) {
            final CairoConfiguration configuration = cairoEngine.getConfiguration();
            final FilesFacade ff = configuration.getFilesFacade();

            long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                long fd;
                try (Path path = new Path().of(configuration.getInputRoot()).concat(inputFileName)) {
                    fd = ff.openRO(path.$());
                }
                int utf8SinkSize = cairoEngine.getConfiguration().getTextConfiguration().getUtf8SinkSize();
                try(DirectCharSink utf8Sink = new DirectCharSink(utf8SinkSize)) {
                    final long count = size / (2 * Long.BYTES);
                    for (long i = 0; i < count; i++) {
                        final long offset = Unsafe.getUnsafe().getLong(address + i * 2L * Long.BYTES + Long.BYTES);
                        long n = ff.read(fd, buf, len, offset);
                        if (n > 0) {
                            lexer.parse(buf, buf + n, 0,
                                    (long line, ObjList<DirectByteCharSequence> fields, int hi) ->
                                            this.onFieldsPartitioned(line, fields, hi, utf8Sink));
                        }
                    }
                } finally {
                    ff.close(fd);
                }
            } finally {
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
            }
        }

        private void mergePartitionIndexAndImportData(final FilesFacade ff,
                                                      final Path partitionPath,
                                                      final TextLexer lexer,
                                                      final ObjList<TypeAdapter> types,
                                                      int maxLineLength) {
            try (DirectLongList mergeIndexes = new DirectLongList(64, MemoryTag.NATIVE_DEFAULT)) {
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
                    importPartitionData(lexer, types, merged, mergedIndexSize, maxLineLength);
                } finally {
                    if (address != -1) {
                        ff.munmap(address, mergedIndexSize, MemoryTag.MMAP_DEFAULT);
                    }
                    for (long i = 0, sz = mergeIndexes.size() / 2; i < sz; i++) {
                        final long addr = mergeIndexes.get(2 * i);
                        final long size = mergeIndexes.get(2 * i + 1) * FileSplitter.INDEX_ENTRY_SIZE;
                        ff.munmap(addr, size, MemoryTag.MMAP_DEFAULT);
                    }
                }
            }
        }

        private boolean onField(long line, final DirectByteCharSequence dbcs, TableWriter.Row w, int i, DirectCharSink utf8Sink) {
            TypeAdapter type = this.types.getQuick(i);
            try {
                switch (ColumnType.tagOf(type.getType())) {
                    case ColumnType.STRING:
                    case ColumnType.SYMBOL:
                    case ColumnType.TIMESTAMP:
                    case ColumnType.DATE:
                        type.write(w, i, dbcs, utf8Sink);
                        break;
                    default:
                        type.write(w, i, dbcs);
                }
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

        private void onFieldsPartitioned(long line, final ObjList<DirectByteCharSequence> values, int valuesLength, DirectCharSink utf8Sink) {
            assert tableWriterRef != null;
            DirectByteCharSequence dbcs = values.getQuick(timestampIndex);
            try {
                final TableWriter.Row w = tableWriterRef.newRow(timestampAdapter.getTimestamp(dbcs));
                for (int i = 0; i < valuesLength; i++) {
                    dbcs = values.getQuick(i);
                    if (i == timestampIndex || dbcs.length() == 0) {
                        continue;
                    }
                    if (onField(line, dbcs, w, i, utf8Sink)) return;
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

        public void run() {
            final FilesFacade ff = cfg.getFilesFacade();
            try (Path path = new Path()) {
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
                                    MemoryTag.MMAP_DEFAULT,
                                    cfg.getWriterFileOpenOpts()
                            )) {
                                SymbolMapWriter.mergeSymbols(writer.getSymbolMapWriter(columnIndex), reader, mem);
                            }
                        }
                    }
                }
            }
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

        public void run() {
            final FilesFacade ff = cairoEngine.getConfiguration().getFilesFacade();
            Path path = Path.getThreadLocal(root);
            path.concat(tableStructure.getTableName()).put("_").put(index);
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
                    columnMemory = TableUtils.mapRW(ff, columnFd, columnMemorySize, MemoryTag.MMAP_DEFAULT);
                    remapTableMemory = TableUtils.mapRW(ff, remapFd, remapTableMemorySize, MemoryTag.MMAP_DEFAULT);
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
                    ff.munmap(columnMemory, columnMemorySize, MemoryTag.MMAP_DEFAULT);
                }
                if (remapTableMemory > 0) {
                    ff.munmap(remapTableMemory, remapTableMemorySize, MemoryTag.MMAP_DEFAULT);
                }
            }
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
    }
}
