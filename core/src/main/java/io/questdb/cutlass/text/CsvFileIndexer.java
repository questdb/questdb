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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cairo.vm.MemoryPMARImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cutlass.text.types.TimestampAdapter;
import io.questdb.cutlass.text.types.TypeManager;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Decimal256;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.SwarUtils;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;


/**
 * Class is responsible for scanning whole/chunk of input csv file and building per-partition index files .
 * Example path : workDir/targetTable/2022-06/0_1
 * These indexes are chunked to limit memory usage and are later merged into index.m .
 * It's a simplified version of text lexer that only:
 * - parses
 * - (if needed) buffers using a small buffer
 * the timestamp field .
 */
public class CsvFileIndexer implements Closeable, Mutable {
    public static final long INDEX_ENTRY_SIZE = 2 * Long.BYTES;
    public static final CharSequence INDEX_FILE_NAME = "index.m";
    private static final Log LOG = LogFactory.getLog(CsvFileIndexer.class);
    private static final long MASK_CR = SwarUtils.broadcast((byte) '\r');
    private static final long MASK_NEW_LINE = SwarUtils.broadcast((byte) '\n');
    private static final long MASK_QUOTE = SwarUtils.broadcast((byte) '"');
    // A guess at how long could a timestamp string be, including long day, month name, etc.
    // since we're only interested in timestamp field/col there's no point buffering whole line
    // we'll copy field part to buffer only if current field is designated timestamp
    private static final int MAX_TIMESTAMP_LENGTH = 100;
    private final CairoConfiguration configuration;
    private final int dirMode;
    private final FilesFacade ff;
    private final int fieldRollBufLen;
    private final CharSequence inputRoot;
    private final long maxIndexChunkSize;
    final private ObjList<IndexOutputFile> outputFileDenseList = new ObjList<>();
    // maps partitionFloors to output file descriptors
    final private LongObjHashMap<IndexOutputFile> outputFileLookupMap = new LongObjHashMap<>();
    // timestamp field of current line
    final private DirectUtf8String timestampField;
    // used for timestamp parsing
    private final TypeManager typeManager;
    // used for timestamp parsing
    private final DirectUtf16Sink utf16Sink;
    private final DirectUtf8Sink utf8Sink;
    private boolean cancelled = false;
    private @Nullable ExecutionCircuitBreaker circuitBreaker;
    private byte columnDelimiter;
    private long columnDelimiterMask;
    private boolean delayedOutQuote;
    private boolean eol;
    private int errorCount = 0;
    private boolean failOnTsError;
    // input file descriptor (cached between initial boundary scan & indexing phases)
    private long fd = -1;
    private long fieldHi;
    private int fieldIndex;
    // these two are pointers either into file read buffer or roll buffer
    private long fieldLo;
    private long fieldRollBufCur;
    private long fieldRollBufPtr;
    // if set to true then ignore first line of input file
    private boolean header;
    private CharSequence importRoot;
    private boolean inQuote;
    private int index;
    private CharSequence inputFileName;
    // fields taken & adjusted  from textLexer
    private long lastLineStart;
    private long lastQuotePos = -1;
    private long lineCount;
    private long lineNumber;
    // file offset of current start of buffered block
    private long offset;
    private DateFormat partitionDirFormatMethod;
    // used to map timestamp to output file
    private TimestampDriver.TimestampFloorMethod partitionFloorMethod;
    // work dir path
    private Path path;
    private boolean rollBufferUnusable = false;
    private long sortBufferLength;
    private long sortBufferPtr;
    // adapter used to parse timestamp column
    private TimestampAdapter timestampAdapter;
    private TimestampDriver timestampDriver;
    // position of timestamp column in csv (0-based)
    private int timestampIndex;
    private long timestampValue;
    private boolean useFieldRollBuf = false;

    public CsvFileIndexer(CairoConfiguration configuration) {
        try {
            this.configuration = configuration;
            final TextConfiguration textConfiguration = configuration.getTextConfiguration();
            int utf8SinkSize = textConfiguration.getUtf8SinkSize();
            this.utf16Sink = new DirectUtf16Sink(utf8SinkSize);
            this.utf8Sink = new DirectUtf8Sink(utf8SinkSize);
            this.typeManager = new TypeManager(textConfiguration, utf16Sink, utf8Sink, new Decimal256());
            this.ff = configuration.getFilesFacade();
            this.dirMode = configuration.getMkDirMode();
            this.inputRoot = configuration.getSqlCopyInputRoot();
            this.maxIndexChunkSize = configuration.getSqlCopyMaxIndexChunkSize();
            this.fieldRollBufLen = MAX_TIMESTAMP_LENGTH;
            this.fieldRollBufPtr = Unsafe.malloc(fieldRollBufLen, MemoryTag.NATIVE_IMPORT);
            this.fieldRollBufCur = fieldRollBufPtr;
            this.timestampField = new DirectUtf8String();
            this.failOnTsError = false;
            this.path = new Path();
            this.sortBufferPtr = -1;
            this.sortBufferLength = 0;
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    @Override
    public final void clear() {
        fieldLo = 0;
        eol = false;
        fieldIndex = 0;
        inQuote = false;
        delayedOutQuote = false;
        lineNumber = 0;
        lineCount = 0;
        fieldRollBufCur = fieldRollBufPtr;
        useFieldRollBuf = false;
        rollBufferUnusable = false;
        header = false;
        errorCount = 0;
        offset = -1;
        Misc.clear(timestampField);
        lastQuotePos = -1;
        timestampValue = Long.MIN_VALUE;

        inputFileName = null;
        importRoot = null;
        timestampAdapter = null;
        timestampIndex = -1;
        partitionFloorMethod = null;
        partitionDirFormatMethod = null;
        columnDelimiter = -1;
        columnDelimiterMask = 0;

        closeOutputFiles();
        closeSortBuffer();

        if (ff != null && ff.close(fd)) {
            fd = -1;
        }

        failOnTsError = false;
        if (path != null) {
            path.trimTo(0);
        }
        circuitBreaker = null;
        cancelled = false;
    }

    @Override
    public void close() {
        fieldRollBufPtr = Unsafe.free(fieldRollBufPtr, fieldRollBufLen, MemoryTag.NATIVE_IMPORT);
        path = Misc.free(path);
        Misc.clear(typeManager);
        Misc.free(utf16Sink);
        Misc.free(utf8Sink);
        clear();
    }

    public int getErrorCount() {
        return errorCount;
    }

    public long getLineCount() {
        return lineCount;
    }

    public void index(
            long chunkLo,
            long chunkHi,
            long lineNumber,
            LongList partitionKeysAndSizes,
            long fileBufAddr,
            long fileBufSize
    ) {
        assert chunkHi > 0;
        assert chunkLo >= 0 && chunkLo < chunkHi;

        openInputFile();

        this.offset = chunkLo;
        long read;

        this.lastLineStart = offset;
        this.lineNumber = lineNumber;

        try {
            do {
                if (circuitBreaker != null && circuitBreaker.checkIfTripped()) {
                    this.cancelled = true;
                    throw TextException.$("Cancelled");
                }
                long leftToRead = Math.min(chunkHi - offset, fileBufSize);
                read = (int) ff.read(fd, fileBufAddr, leftToRead, offset);
                if (read < 1) {
                    break;
                }
                parse(fileBufAddr, fileBufAddr + read);
                offset += read;
            } while (offset < chunkHi);

            if (read < 0 || offset < chunkHi) {
                throw TextException
                        .$("could not read file [path='").put(path)
                        .put("', offset=").put(offset)
                        .put(", errno=").put(ff.errno())
                        .put(']');
            } else {
                parseLast();
            }

            collectPartitionStats(partitionKeysAndSizes);
            sortAndCloseOutputFiles();
        } finally {
            closeOutputFiles();//close without sorting if there's an error
            closeSortBuffer();
        }

        this.lineCount = this.lineNumber - lineNumber;
        LOG.info()
                .$("finished chunk [chunkLo=").$(chunkLo)
                .$(", chunkHi=").$(chunkHi)
                .$(", lines=").$(lineCount)
                .$(", errors=").$(errorCount)
                .I$();
    }

    public void indexLine(long ptr, long lo) throws TextException {
        // this is fine because Long.MIN_VALUE is treated as null and would be rejected by partitioned tables
        if (timestampValue == Long.MIN_VALUE) {
            return;
        }

        long lineStartOffset = lastLineStart;
        long length = offset + ptr - lo - lastLineStart;
        if (length >= (1L << 16)) {
            LOG.error().$("row exceeds maximum line length (65k) for parallel import [line=").$(lineNumber)
                    .$(", length=").$(length).I$();
            errorCount++;
            return;
        }

        // second long stores:
        //  length as 16 bits unsigned number followed by
        //  offset as 48-bits unsigned number
        //  allowing for importing 256TB big files with rows up to 65kB long
        long lengthAndOffset = (length << 48 | lineStartOffset);
        long partitionKey = partitionFloorMethod.floor(timestampValue);
        long mapKey = timestampDriver.toHours(partitionKey); //remove trailing zeros to avoid excessive collisions in hashmap

        final IndexOutputFile target;
        int keyIndex = outputFileLookupMap.keyIndex(mapKey);
        if (keyIndex > -1) {
            target = prepareTargetFile(partitionKey);
            outputFileDenseList.add(target);
            outputFileLookupMap.putAt(keyIndex, mapKey, target);
        } else {
            target = outputFileLookupMap.valueAt(keyIndex);
        }

        if (target.indexChunkSize == maxIndexChunkSize) {
            target.nextChunk(ff, getPartitionIndexPrefix(partitionKey));
        }

        target.putEntry(timestampValue, lengthAndOffset, length);
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public void of(
            CharSequence inputFileName,
            CharSequence importRoot,
            int index,
            int timestampType,
            int partitionBy,
            byte columnDelimiter,
            int timestampIndex,
            TimestampAdapter adapter,
            boolean ignoreHeader,
            int atomicity,
            @Nullable ExecutionCircuitBreaker circuitBreaker
    ) {
        this.inputFileName = inputFileName;
        this.importRoot = importRoot;
        this.timestampDriver = ColumnType.getTimestampDriver(timestampType);
        this.partitionFloorMethod = timestampDriver.getPartitionFloorMethod(partitionBy);
        this.partitionDirFormatMethod = timestampDriver.getPartitionDirFormatMethod(partitionBy);
        this.offset = 0;
        this.columnDelimiter = columnDelimiter;
        this.columnDelimiterMask = SwarUtils.broadcast(columnDelimiter);
        if (timestampIndex < 0) {
            throw TextException.$("Timestamp index is not set [value=").put(timestampIndex).put(']');
        }
        this.timestampIndex = timestampIndex;
        this.timestampAdapter = adapter;
        this.header = ignoreHeader;
        this.index = index;
        this.failOnTsError = (atomicity == Atomicity.SKIP_ALL);
        this.timestampValue = Long.MIN_VALUE;
        this.circuitBreaker = circuitBreaker;
    }

    public void parseLast() {
        if (useFieldRollBuf) {
            if (inQuote && lastQuotePos < fieldHi) {
                errorCount++;
                LOG.info().$("quote is missing [table=").$("tableName").$(']').$();
            } else {
                this.fieldHi++;
                stashField(fieldIndex, 0);
                triggerLine(0);
            }
        }
    }

    public void sort(long srcFd, long srcSize) {
        if (srcSize < 1) {
            return;
        }

        long srcAddress = -1;

        try {
            srcAddress = TableUtils.mapRW(ff, srcFd, srcSize, MemoryTag.MMAP_IMPORT);

            if (sortBufferPtr == -1) {
                sortBufferPtr = Unsafe.malloc(maxIndexChunkSize, MemoryTag.NATIVE_IMPORT);
                sortBufferLength = maxIndexChunkSize;
            }

            Vect.radixSortLongIndexAscInPlace(srcAddress, srcSize / INDEX_ENTRY_SIZE, sortBufferPtr);
        } finally {
            if (srcAddress != -1) {
                ff.munmap(srcAddress, srcSize, MemoryTag.MMAP_IMPORT);
            }
        }
    }

    private void checkEol(long lo) {
        if (eol) {
            uneol(lo);
        }
    }

    private void clearRollBuffer(long ptr) {
        useFieldRollBuf = false;
        fieldRollBufCur = fieldRollBufPtr;
        this.fieldLo = this.fieldHi = ptr;
    }

    private void closeOutputFiles() {
        Misc.freeObjListAndClear(outputFileDenseList);
        Misc.clear(outputFileLookupMap);
    }

    private void closeSortBuffer() {
        if (sortBufferPtr != -1) {
            Unsafe.free(sortBufferPtr, sortBufferLength, MemoryTag.NATIVE_IMPORT);
            sortBufferPtr = -1;
            sortBufferLength = 0;
        }
    }

    private void collectPartitionStats(LongList partitionKeysAndSizes) {
        partitionKeysAndSizes.setPos(0);
        for (int i = 0, n = outputFileDenseList.size(); i < n; i++) {
            final IndexOutputFile value = outputFileDenseList.getQuick(i);
            partitionKeysAndSizes.add(value.partitionKey, value.dataSize);
        }
    }

    private void eol(long ptr, byte c) {
        if (c == '\n' || c == '\r') {
            eol = true;
            rollBufferUnusable = false;
            clearRollBuffer(ptr);
            fieldIndex = 0;
            lineNumber++;
        }
    }

    private boolean fitsInBuffer(int requiredLength) {
        if (requiredLength > fieldRollBufLen) {
            LOG.info()
                    .$("timestamp column value too long [path=").$(inputFileName)
                    .$(", line=").$(lineNumber)
                    .$(", requiredLen=").$(requiredLength)
                    .$(", rollLimit=").$(fieldRollBufLen)
                    .$(']').$();
            errorCount++;
            rollBufferUnusable = true;
            return false;
        }

        return true;
    }

    private Path getPartitionIndexDir(long partitionKey) {
        path.of(importRoot).slash();
        partitionDirFormatMethod.format(partitionKey, EN_LOCALE, null, path);
        return path;
    }

    private Path getPartitionIndexPrefix(long partitionKey) {
        return getPartitionIndexDir(partitionKey).slash().put(index);
    }

    private void onColumnDelimiter(long lo, long ptr) {
        checkEol(lo);

        if (inQuote) {
            return;
        }
        stashField(fieldIndex++, ptr);
    }

    private void onLineEnd(long ptr, long lo) {
        if (inQuote) {
            return;
        }

        if (eol) {
            this.fieldLo = this.fieldHi;
            return;
        }

        stashField(fieldIndex, ptr);
        indexLine(ptr, lo);
        triggerLine(ptr);
    }

    private void onQuote() {
        if (inQuote) {
            delayedOutQuote = !delayedOutQuote;
            lastQuotePos = this.fieldHi;
        } else if (fieldHi - fieldLo == 1) {
            inQuote = true;
            this.fieldLo = this.fieldHi;
        }
    }

    private void parse(long lo, long hi) {
        this.fieldHi = useFieldRollBuf ? fieldRollBufCur : (this.fieldLo = lo);
        long ptr = lo;

        while (ptr < hi) {
            if (!rollBufferUnusable && !useFieldRollBuf && !delayedOutQuote && ptr < hi - 7) {
                long word = Unsafe.getUnsafe().getLong(ptr);
                long zeroBytesWord = SwarUtils.markZeroBytes(word ^ MASK_NEW_LINE)
                        | SwarUtils.markZeroBytes(word ^ MASK_CR)
                        | SwarUtils.markZeroBytes(word ^ MASK_QUOTE)
                        | SwarUtils.markZeroBytes(word ^ columnDelimiterMask);
                if (zeroBytesWord == 0) {
                    ptr += 7;
                    this.fieldHi += 7;
                    continue;
                } else {
                    int firstIndex = SwarUtils.indexOfFirstMarkedByte(zeroBytesWord);
                    ptr += firstIndex;
                    this.fieldHi += firstIndex;
                }
            }

            final byte b = Unsafe.getUnsafe().getByte(ptr++);
            if (rollBufferUnusable) {
                eol(ptr, b);
                continue;
            }

            if (useFieldRollBuf) {
                putToRollBuf(b);
                if (rollBufferUnusable) {
                    continue;
                }
            }

            this.fieldHi++;

            if (delayedOutQuote && b != '"') {
                inQuote = delayedOutQuote = false;
            }

            if (b == columnDelimiter) {
                onColumnDelimiter(lo, ptr);
            } else if (b == '"') {
                checkEol(lo);
                onQuote();
            } else if (b == '\n' || b == '\r') {
                onLineEnd(ptr, lo);
            } else {
                checkEol(lo);
            }
        }

        if (useFieldRollBuf) {
            return;
        }

        if (eol) {
            this.fieldLo = 0;
        } else if (fieldIndex == timestampIndex) {
            rollField(hi);
        }
    }

    private void parseTimestamp() {
        try {
            timestampValue = timestampAdapter.getTimestamp(timestampField);
        } catch (Exception e) {
            if (failOnTsError) {
                throw TextException.$("could not parse timestamp [line=").put(lineNumber).put(", column=").put(timestampIndex).put(']');
            } else {
                LOG.error().$("could not parse timestamp [line=").$(lineNumber).$(", column=").$(timestampIndex).I$();
                errorCount++;
            }
        }
    }

    @NotNull
    private IndexOutputFile prepareTargetFile(long partitionKey) {
        getPartitionIndexDir(partitionKey);
        path.slash();

        if (!ff.exists(path.$())) {
            int result = ff.mkdir(path.$(), dirMode);
            if (result != 0 && !ff.exists(path.$())) {//ignore because other worker might've created it
                throw TextException.$("Couldn't create partition dir [path='").put(path).put("']");
            }
        }

        path.put(index);

        return new IndexOutputFile(ff, path, partitionKey);
    }

    private void putToRollBuf(byte c) {
        if (fitsInBuffer((int) (fieldRollBufCur - fieldRollBufPtr + 1L))) {
            Unsafe.getUnsafe().putByte(fieldRollBufCur++, c);
        }
    }

    // roll timestamp field if it's split over read buffer boundaries
    private void rollField(long hi) {
        // lastLineStart is an offset from 'lo'
        // 'lo' is the address of incoming buffer
        int length = (int) (hi - fieldLo);
        if (length > 0 && fitsInBuffer(length)) {
            assert fieldLo + length <= hi;
            Vect.memcpy(fieldRollBufPtr, fieldLo, length);
            fieldRollBufCur = fieldRollBufPtr + length;
            shift(fieldLo - fieldRollBufPtr);
            useFieldRollBuf = true;
        }
    }

    private void shift(long d) {
        this.fieldLo -= d;
        this.fieldHi -= d;
        if (lastQuotePos > -1) {
            this.lastQuotePos -= d;
        }
    }

    private void sortAndCloseOutputFiles() {
        for (int i = 0, n = outputFileDenseList.size(); i < n; i++) {
            outputFileDenseList.getQuick(i).sortAndClose();
        }
        this.outputFileDenseList.clear();
        this.outputFileLookupMap.clear();
    }

    private void stashField(int fieldIndex, long ptr) {
        if (fieldIndex == timestampIndex && !header) {
            if (lastQuotePos > -1) {
                timestampField.of(fieldLo, lastQuotePos - 1);
            } else {
                timestampField.of(fieldLo, fieldHi - 1);
            }

            parseTimestamp();

            if (useFieldRollBuf) {
                clearRollBuffer(ptr);
            }
        }

        this.lastQuotePos = -1;
        this.fieldLo = this.fieldHi;
    }

    private void triggerLine(long ptr) {
        eol = true;
        fieldIndex = 0;
        if (useFieldRollBuf) {
            clearRollBuffer(ptr);
        }

        if (header) {
            header = false;
            return;
        }

        lineNumber++;
        timestampValue = Long.MIN_VALUE;
    }

    private void uneol(long lo) {
        eol = false;
        this.lastLineStart = this.offset + (this.fieldLo - lo);
    }

    void openInputFile() {
        if (fd > -1) {
            return;
        }

        path.of(inputRoot).slash().concat(inputFileName);
        this.fd = TableUtils.openRO(ff, path.$(), LOG);

        long len = ff.length(fd);
        if (len == -1) {
            throw CairoException.critical(ff.errno()).put(
                            "could not get length of file [path=").put(path)
                    .put(']');
        }
        ff.fadvise(fd, 0, len, Files.POSIX_FADV_SEQUENTIAL);
    }

    class IndexOutputFile implements Closeable {
        final MemoryPMARImpl memory;
        final long partitionKey;
        int chunkNumber;
        long dataSize; // partition data size in bytes
        long indexChunkSize;

        IndexOutputFile(FilesFacade ff, Path path, long partitionKey) {
            this.partitionKey = partitionKey;
            this.indexChunkSize = 0;
            this.chunkNumber = 0;
            this.dataSize = 0;
            this.memory = new MemoryPMARImpl(configuration);
            nextChunk(ff, path);
        }

        @Override
        public void close() {
            if (memory.isOpen()) {
                memory.close(true, Vm.TRUNCATE_TO_POINTER);
            }
        }

        public void nextChunk(FilesFacade ff, Path path) {
            if (memory.isOpen()) {
                sortAndClose();
            }

            chunkNumber++; //start with file name like $workerIndex_$chunkIndex, e.g. 1_1
            indexChunkSize = 0;
            path.put('_').put(chunkNumber);

            LPSZ lpsz = path.$();
            if (ff.exists(lpsz)) {
                throw TextException.$("index file already exists [path=").put(path).put(']');
            } else {
                LOG.debug().$("created import index file [path='").$(path).$("']").$();
            }

            this.memory.of(ff, lpsz, ff.getMapPageSize(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
        }

        private void sortAndClose() {
            if (memory.isOpen()) {
                CsvFileIndexer.this.sort(memory.getFd(), indexChunkSize);
                memory.close(true, Vm.TRUNCATE_TO_POINTER);
            }
        }

        void putEntry(long timestamp, long offset, long length) {
            memory.putLong128(timestamp, offset);
            indexChunkSize += INDEX_ENTRY_SIZE;
            dataSize += length;
        }
    }
}
