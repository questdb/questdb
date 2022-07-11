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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.MemoryPMARImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cutlass.text.types.TimestampAdapter;
import io.questdb.cutlass.text.types.TypeManager;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.DirectCharSink;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;


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
    public static final CharSequence INDEX_FILE_NAME = "index.m";

    public static final long INDEX_ENTRY_SIZE = 2 * Long.BYTES;

    private static final Log LOG = LogFactory.getLog(CsvFileIndexer.class);

    //used for timestamp parsing 
    private final TypeManager typeManager;

    //used for timestamp parsing
    private final DirectCharSink utf8Sink;

    //work dir path
    private final Path path;

    private final CharSequence inputRoot;
    private CharSequence importRoot;

    private final FilesFacade ff;
    private final int dirMode;
    private CharSequence inputFileName;

    //input file descriptor (cached between initial boundary scan & indexing phases)
    private long fd = -1;

    private final long maxIndexChunkSize;

    //file offset of current start of buffered block
    private long offset;

    //position of timestamp column in csv (0-based) 
    private int timestampIndex;

    //adapter used to parse timestamp column
    private TimestampAdapter timestampAdapter;

    //used to map timestamp to output file  
    private PartitionBy.PartitionFloorMethod partitionFloorMethod;
    private DateFormat partitionDirFormatMethod;

    //A guess at how long could a timestamp string be, including long day, month name, etc.
    //since we're only interested in timestamp field/col there's no point buffering whole line 
    //we'll copy field part to buffer only if current field is designated timestamp
    private static final int MAX_TIMESTAMP_LENGTH = 100;

    //maps partitionFloors to output file descriptors
    final private LongObjHashMap<IndexOutputFile> outputFiles;

    //timestamp field of current line
    final private DirectByteCharSequence timestampField;
    private long timestampValue;

    //fields taken & adjusted  from textLexer
    private long lastLineStart;

    private long fieldRollBufCur;
    private final int fieldRollBufLen;
    private long fieldRollBufPtr;

    //if set to true then ignore first line of input file 
    private boolean header;
    private long lastQuotePos = -1;
    private long errorCount = 0;

    private int fieldIndex;
    private long lineCount;
    private boolean eol;

    private boolean useFieldRollBuf = false;
    private boolean rollBufferUnusable = false;

    private byte columnDelimiter;
    private boolean inQuote;
    private boolean delayedOutQuote;

    //these two are pointers either into file read buffer or roll buffer   
    private long fieldLo;
    private long fieldHi;
    private int index;

    private boolean failOnTsError;

    private long sortBufferPtr;
    private long sortBufferLength;

    public CsvFileIndexer(CairoConfiguration configuration) {
        final TextConfiguration textConfiguration = configuration.getTextConfiguration();
        this.utf8Sink = new DirectCharSink(textConfiguration.getUtf8SinkSize());
        this.typeManager = new TypeManager(textConfiguration, utf8Sink);
        this.ff = configuration.getFilesFacade();
        this.dirMode = configuration.getMkDirMode();
        this.inputRoot = configuration.getInputRoot();
        this.maxIndexChunkSize = configuration.getMaxImportIndexChunkSize();
        this.fieldRollBufLen = MAX_TIMESTAMP_LENGTH;
        this.fieldRollBufPtr = Unsafe.malloc(fieldRollBufLen, MemoryTag.NATIVE_DEFAULT);
        this.fieldRollBufCur = fieldRollBufPtr;

        this.timestampField = new DirectByteCharSequence();
        this.failOnTsError = false;
        this.path = new Path();
        this.outputFiles = new LongObjHashMap<>();
        this.sortBufferPtr = -1;
        this.sortBufferLength = 0;
    }

    public void of(
            CharSequence inputFileName,
            CharSequence importRoot,
            int index,
            int partitionBy,
            byte columnDelimiter,
            int timestampIndex,
            TimestampAdapter adapter,
            boolean ignoreHeader,
            int atomicity
    ) {
        this.inputFileName = inputFileName;
        this.importRoot = importRoot;
        this.partitionFloorMethod = PartitionBy.getPartitionFloorMethod(partitionBy);
        this.partitionDirFormatMethod = PartitionBy.getPartitionDirFormatMethod(partitionBy);
        this.offset = 0;
        this.columnDelimiter = columnDelimiter;
        if (timestampIndex < 0) {
            throw TextException.$("Timestamp index is not set [value=").put(timestampIndex).put(']');
        }
        this.timestampIndex = timestampIndex;
        this.timestampAdapter = adapter;
        this.header = ignoreHeader;
        this.index = index;
        this.failOnTsError = (atomicity == Atomicity.SKIP_ALL);
        this.timestampValue = Long.MIN_VALUE;
    }

    public void indexLine(long ptr, long lo) throws TextException {
        //this is fine because Long.MIN_VALUE is treated as null and would be rejected by partitioned tables 
        if (timestampValue == Long.MIN_VALUE) {
            return;
        }

        long lineStartOffset = lastLineStart;
        long length = offset + ptr - lo - lastLineStart;
        if (length >= (1L << 16)) {
            throw TextException.$("Row exceeds maximum length for parallel import ").put(1 << 16);
        }

        //second long stores: 
        // length as 16 bits unsigned number followed by 
        // offset as 48-bits unsigned number 
        // allowing for importing 256TB big files with rows up to 65kB long  
        long lengthAndOffset = (length << 48 | lineStartOffset);
        long partitionKey = partitionFloorMethod.floor(timestampValue);
        long mapKey = partitionKey / Timestamps.HOUR_MICROS; //remove trailing zeros to avoid excessive collisions in hashmap 

        final IndexOutputFile target;
        int keyIndex = outputFiles.keyIndex(mapKey);
        if (keyIndex > -1) {
            target = prepareTargetFile(partitionKey);
            outputFiles.putAt(keyIndex, mapKey, target);
        } else {
            target = outputFiles.valueAt(keyIndex);
        }

        if (target.indexChunkSize == maxIndexChunkSize) {
            target.nextChunk(ff, getPartitionIndexPrefix(partitionKey));
        }

        target.putEntry(timestampValue, lengthAndOffset, length);
    }

    private void parseTimestamp() {
        try {
            timestampValue = timestampAdapter.getTimestamp(timestampField);
        } catch (Exception e) {
            if (failOnTsError) {
                throw TextException.$("could not parse timestamp [line=").put(lineCount).put(", column=").put(timestampIndex).put(']');
            } else {
                LOG.error().$("could not parse timestamp [line=").$(lineCount).$(", column=").$(timestampIndex).I$();
                errorCount++;
            }
        }
    }

    class IndexOutputFile {
        final MemoryPMARImpl memory = new MemoryPMARImpl();
        long indexChunkSize;
        long dataSize;//partition data size in bytes 
        int chunkNumber;
        long partitionKey;

        IndexOutputFile(FilesFacade ff, Path path, long partitionKey) {
            this.partitionKey = partitionKey;
            this.indexChunkSize = 0;
            this.chunkNumber = 0;
            this.dataSize = 0;

            nextChunk(ff, path);
        }

        public void nextChunk(FilesFacade ff, Path path) {
            if (memory.isOpen()) {
                sortAndClose();
            }

            chunkNumber++; //start with file name like $workerIndex_$chunkIndex, e.g. 1_1
            indexChunkSize = 0;
            path.put('_').put(chunkNumber).$();

            if (ff.exists(path)) {
                throw TextException.$("index file already exists [path=").put(path).put(']');
            } else {
                LOG.debug().$("created import index file ").$(path).$();
            }

            this.memory.of(ff, path, ff.getPageSize(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
        }

        void putEntry(long timestamp, long offset, long length) {
            memory.putLong128(timestamp, offset);
            indexChunkSize += INDEX_ENTRY_SIZE;
            dataSize += length;
        }

        private void sortAndClose() {
            if (memory.isOpen()) {
                CsvFileIndexer.this.sort(memory.getFd(), indexChunkSize);
                memory.close(true, Vm.TRUNCATE_TO_POINTER);
            }
        }

        public void close() {
            if (memory.isOpen()) {
                memory.close(true, Vm.TRUNCATE_TO_POINTER);
            }
        }
    }

    @NotNull
    private IndexOutputFile prepareTargetFile(long partitionKey) {
        getPartitionIndexDir(partitionKey);
        path.slash$();

        if (!ff.exists(path)) {
            int result = ff.mkdir(path, dirMode);
            if (result != 0 && !ff.exists(path)) {//ignore because other worker might've created it 
                LOG.error().$("Couldn't create partition dir=").$(path).$();
            }
        }

        path.chop$().put(index);

        return new IndexOutputFile(ff, path, partitionKey);
    }

    private Path getPartitionIndexDir(long partitionKey) {
        path.of(importRoot).slash();
        partitionDirFormatMethod.format(partitionKey, null, null, path);
        return path;
    }

    private Path getPartitionIndexPrefix(long partitionKey) {
        return getPartitionIndexDir(partitionKey).slash().put(index);
    }

    @Override
    public final void clear() {
        this.fieldLo = 0;
        this.eol = false;
        this.fieldIndex = 0;
        this.inQuote = false;
        this.delayedOutQuote = false;
        this.lineCount = 0;
        this.fieldRollBufCur = fieldRollBufPtr;
        this.useFieldRollBuf = false;
        this.rollBufferUnusable = false;
        this.header = false;
        this.errorCount = 0;
        this.offset = -1;
        this.timestampField.clear();
        this.lastQuotePos = -1;
        this.timestampValue = Long.MIN_VALUE;

        this.inputFileName = null;
        this.importRoot = null;
        this.timestampAdapter = null;
        this.timestampIndex = -1;
        this.partitionFloorMethod = null;
        this.partitionDirFormatMethod = null;
        this.columnDelimiter = -1;

        closeOutputFiles();
        closeSortBuffer();

        if (fd > -1) {
            ff.close(fd);
            fd = -1;
        }

        this.failOnTsError = false;
        this.path.trimTo(0);
    }

    private void sortAndCloseOutputFiles() {
        this.outputFiles.forEach((key, value) -> value.sortAndClose());
        this.outputFiles.clear();
    }

    private void closeOutputFiles() {
        this.outputFiles.forEach((key, value) -> value.close());
        this.outputFiles.clear();
    }

    @Override
    public void close() {
        if (fieldRollBufPtr != 0) {
            Unsafe.free(fieldRollBufPtr, fieldRollBufLen, MemoryTag.NATIVE_DEFAULT);
            fieldRollBufPtr = 0;
        }

        this.path.close();
        this.typeManager.clear();
        this.utf8Sink.close();

        clear();
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

    private void eol(long ptr, byte c) {
        if (c == '\n' || c == '\r') {
            eol = true;
            rollBufferUnusable = false;
            clearRollBuffer(ptr);
            fieldIndex = 0;
            lineCount++;
        }
    }

    private boolean fitsInBuffer(int requiredLength) {
        if (requiredLength > fieldRollBufLen) {
            LOG.info()
                    .$("timestamp column value too long [path=").$(inputFileName)
                    .$(", line=").$(lineCount)
                    .$(", requiredLen=").$(requiredLength)
                    .$(", rollLimit=").$(fieldRollBufLen)
                    .$(']').$();
            errorCount++;
            rollBufferUnusable = true;
            return false;
        }

        return true;
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
            final byte c = Unsafe.getUnsafe().getByte(ptr++);

            if (rollBufferUnusable) {
                eol(ptr, c);
                continue;
            }

            if (useFieldRollBuf) {
                putToRollBuf(c);
                if (rollBufferUnusable) {
                    continue;
                }
            }

            this.fieldHi++;

            if (delayedOutQuote && c != '"') {
                inQuote = delayedOutQuote = false;
            }

            if (c == columnDelimiter) {
                onColumnDelimiter(lo, ptr);
            } else if (c == '"') {
                checkEol(lo);
                onQuote();
            } else if (c == '\n' || c == '\r') {
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

    private void putToRollBuf(byte c) {
        if (fitsInBuffer((int) (fieldRollBufCur - fieldRollBufPtr + 1L))) {
            Unsafe.getUnsafe().putByte(fieldRollBufCur++, c);
        }
    }

    //roll timestamp field if it's split over  read buffer boundaries  
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

    private void stashField(int fieldIndex, long ptr) {
        if (fieldIndex == timestampIndex && !header) {
            if (lastQuotePos > -1) {
                timestampField.of(this.fieldLo, lastQuotePos - 1);
            } else {
                timestampField.of(this.fieldLo, this.fieldHi - 1);
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

        lineCount++;
        timestampValue = Long.MIN_VALUE;
    }

    private void uneol(long lo) {
        eol = false;
        this.lastLineStart = this.offset + (this.fieldLo - lo);
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
        this.lineCount = lineNumber;

        try {
            do {
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

        LOG.info()
                .$("finished chunk [chunkLo=").$(chunkLo)
                .$(", chunkHi=").$(chunkHi)
                .$(", lines=").$(lineCount - lineNumber)
                .$(", errors=").$(errorCount)
                .I$();
    }

    private void closeSortBuffer() {
        if (sortBufferPtr != -1) {
            Unsafe.free(sortBufferPtr, sortBufferLength, MemoryTag.NATIVE_DEFAULT);
            sortBufferPtr = -1;
            sortBufferLength = 0;
        }
    }

    private void collectPartitionStats(LongList partitionKeysAndSizes) {
        partitionKeysAndSizes.setPos(0);
        outputFiles.forEach((key, value) -> {
            partitionKeysAndSizes.add(value.partitionKey);
            partitionKeysAndSizes.add(value.dataSize);
        });
    }

    void openInputFile() {
        if (fd > -1) {
            return;
        }

        path.of(inputRoot).slash().concat(inputFileName).$();
        this.fd = TableUtils.openRO(ff, path, LOG);
    }

    public void sort(final long srcFd, long srcSize) {
        if (srcSize < 1) {
            return;
        }

        long srcAddress = -1;

        try {
            srcAddress = TableUtils.mapRW(ff, srcFd, srcSize, MemoryTag.MMAP_DEFAULT);

            if (sortBufferPtr == -1) {
                sortBufferPtr = Unsafe.malloc(maxIndexChunkSize, MemoryTag.NATIVE_DEFAULT);
                sortBufferLength = maxIndexChunkSize;
            }

            Vect.radixSortLongIndexAscInPlace(srcAddress, srcSize / INDEX_ENTRY_SIZE, sortBufferPtr);
        } finally {
            if (srcAddress != -1) {
                ff.munmap(srcAddress, srcSize, MemoryTag.MMAP_DEFAULT);
            }
        }
    }
}
