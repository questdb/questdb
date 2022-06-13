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
import io.questdb.cairo.CairoException;
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
public class FileSplitter implements Closeable, Mutable {
    public static final CharSequence INDEX_FILE_NAME = "index.m";
    public static final CharSequence INDEX_CHUNKS_DIR_NAME = "chunks";

    public static final long INDEX_ENTRY_SIZE = 2 * Long.BYTES;

    private static final Log LOG = LogFactory.getLog(FileSplitter.class);

    //used for timestamp parsing 
    private final TypeManager typeManager;

    //used for timestamp parsing
    private final DirectCharSink utf8Sink;

    //work dir path
    private final Path path = new Path();

    private final CharSequence inputRoot;
    private CharSequence importRoot;

    private final FilesFacade ff;
    private final int dirMode;
    private CharSequence inputFileName;

    //input file descriptor (cached between initial boundary scan & indexing phases)
    private long fd = -1;
    //input file buffer (used in multiple phases)
    private long fileBufferPtr = -1;
    private int bufferLength;

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
    final private LongObjHashMap<IndexOutputFile> outputFiles = new LongObjHashMap<>();

    //timestamp field of current line
    final private DirectByteCharSequence timestampField;

    //fields taken & adjusted  from textLexer
    private final int fieldRollBufLimit;
    private long lastLineStart;

    private long fieldRollBufCur;
    private final int fieldRollBufLen;
    private long fieldRollBufPtr;

    //if set to true then ignore first line of input file 
    private boolean header;
    private long lastQuotePos = -1;
    private long errorCount = 0;
    private int maxLineLength;

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

    public FileSplitter(CairoConfiguration configuration) {
        final TextConfiguration textConfiguration = configuration.getTextConfiguration();
        this.utf8Sink = new DirectCharSink(textConfiguration.getUtf8SinkSize());
        this.typeManager = new TypeManager(textConfiguration, utf8Sink);
        this.ff = configuration.getFilesFacade();
        this.dirMode = configuration.getMkDirMode();

        this.inputRoot = configuration.getInputRoot();
        this.maxIndexChunkSize = configuration.getMaxImportIndexChunkSize();

        this.bufferLength = configuration.getSqlCopyBufferSize();

        this.fieldRollBufLen = MAX_TIMESTAMP_LENGTH;
        this.fieldRollBufLimit = MAX_TIMESTAMP_LENGTH;
        this.fieldRollBufPtr = Unsafe.malloc(fieldRollBufLen, MemoryTag.NATIVE_DEFAULT);
        this.fieldRollBufCur = fieldRollBufPtr;

        this.timestampField = new DirectByteCharSequence();
    }

    public int getMaxLineLength() {
        return maxLineLength;
    }

    public void of(CharSequence inputFileName, CharSequence importRoot, int index, int partitionBy, byte columnDelimiter, int timestampIndex, TimestampAdapter adapter, boolean ignoreHeader) {
        this.inputFileName = inputFileName;
        this.importRoot = importRoot;
        this.partitionFloorMethod = PartitionBy.getPartitionFloorMethod(partitionBy);
        this.partitionDirFormatMethod = PartitionBy.getPartitionDirFormatMethod(partitionBy);
        this.offset = 0;
        this.columnDelimiter = columnDelimiter;
        this.timestampIndex = timestampIndex;
        this.timestampAdapter = adapter;
        this.header = ignoreHeader;
        this.index = index;
    }

    public void onTimestampField() {
        long timestamp;
        try {
            timestamp = timestampAdapter.getTimestamp(timestampField);
        } catch (Exception e) {
            LOG.error().$("can't parse timestamp on line ").$(lineCount).$(" column ").$(timestampIndex).$();
            errorCount++;
            return;
        }

        long lineStartOffset = lastLineStart;
        //long lineEndOffset = offset + lastLineStart;

        long partitionKey = partitionFloorMethod.floor(timestamp);
        long mapKey = partitionKey / Timestamps.HOUR_MICROS; //remove trailing zeros to avoid excessive collisions in hashmap 

        IndexOutputFile target = outputFiles.get(mapKey);
        if (target == null) {
            target = prepareTargetFile(partitionKey);
            outputFiles.put(mapKey, target);
        }

        target.putEntry(timestamp, lineStartOffset);

        if (target.size == maxIndexChunkSize) {
            target.nextChunk(ff, getPartitionIndexPrefix(partitionKey));
        }
    }

    static class IndexOutputFile {
        MemoryPMARImpl memory;
        long size;
        int chunkNumber;
        long partitionKey;

        IndexOutputFile(FilesFacade ff, Path path, long partitionKey) {
            this.partitionKey = partitionKey;
            this.size = 0;
            this.chunkNumber = 0;

            nextChunk(ff, path);
        }

        public void nextChunk(FilesFacade ff, Path path) {
            if (memory != null) {
                sortAndClose(ff);
            }

            //start with file name like $workerIndex_$chunkIndex, e.g. 1_1
            chunkNumber++;
            size = 0;

            path.put('_').put(chunkNumber).$();

            if (ff.exists(path)) {
                throw CairoException.instance(-1).put("index file already exists [path=").put(path).put(']');
            } else {
                LOG.debug().$("created import index file ").$(path).$();
            }

            if (this.memory == null) {
                this.memory = new MemoryPMARImpl(ff, path, ff.getPageSize(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
            } else {
                this.memory.of(ff, path, ff.getPageSize(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
            }
        }

        void putEntry(long timestamp, long offset) {
            memory.putLong(timestamp);
            memory.putLong(offset);
            size += INDEX_ENTRY_SIZE;
        }

        private void sortAndClose(FilesFacade ff) {
            if (memory != null) {
                sort(ff, memory.getFd(), size);

                memory.close(true, Vm.TRUNCATE_TO_POINTER);
            }
        }

        public void close(FilesFacade ff) {
            sortAndClose(ff);
            memory = null;
        }
    }

    @NotNull
    private IndexOutputFile prepareTargetFile(long partitionKey) {
        getPartitionIndexDir(partitionKey);
        path.slash$();

        if (!ff.exists(path)) {
            int result = ff.mkdir(path, dirMode);
            if (result != 0) {//TODO: maybe we can ignore it
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

        this.inputFileName = null;

        closeOutputFiles();

        if (fileBufferPtr > -1) {
            Unsafe.free(fileBufferPtr, bufferLength, MemoryTag.NATIVE_DEFAULT);
            fileBufferPtr = -1;
        }

        if (fd > -1) {
            boolean closed = ff.close(fd);
            if (!closed) {
                LOG.error().$("Couldn't close file fd=").$(fd).$();
            }

            fd = -1;
        }
    }

    private void closeOutputFiles() {
        this.outputFiles.forEach((key, value) -> value.close(ff));
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

    public long getErrorCount() {
        return errorCount;
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
        if (c == '\n' || c == '\r') {//what if we're inside a quote ? TODO: fix !!!
            eol = true;
            rollBufferUnusable = false;
            clearRollBuffer(ptr);
            fieldIndex = 0;
            lineCount++;
        }
    }

    private boolean fitsInBuffer(int requiredLength) {
        if (requiredLength > fieldRollBufLimit) {
            LOG.info()
                    .$("timestamp column value too long [path=").$(inputFileName)
                    .$(", line=").$(lineCount)
                    .$(", requiredLen=").$(requiredLength)
                    .$(", rollLimit=").$(fieldRollBufLimit)
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
        triggerLine(ptr);
        this.maxLineLength = (int) Math.max(maxLineLength, offset + ptr - lo - lastLineStart);
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
            rollField(lo, hi);
            useFieldRollBuf = true;
        }
    }

    private void putToRollBuf(byte c) {
        if (fieldRollBufCur - fieldRollBufPtr == fieldRollBufLen) {
            if (fitsInBuffer(fieldRollBufLen + 1)) {
                Unsafe.getUnsafe().putByte(fieldRollBufCur++, c);
            }
        } else {
            Unsafe.getUnsafe().putByte(fieldRollBufCur++, c);
        }
    }

    //roll timestamp field if it's split over  read buffer boundaries  
    private void rollField(long lo, long hi) {
        // lastLineStart is an offset from 'lo'
        // 'lo' is the address of incoming buffer
        int length = (int) (hi - fieldLo);
        if (length < fieldRollBufLen || fitsInBuffer(length)) {
            assert fieldLo + length <= hi;
            Vect.memcpy(fieldRollBufPtr, fieldLo, length);
            fieldRollBufCur = fieldRollBufPtr + length;
            //TODO: do we need this ? if we're rolling then we don't have timestamp yet
            shift(fieldLo - fieldRollBufPtr);
        }
    }

    private void shift(long d) {
        timestampField.shl(d);
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

            onTimestampField();

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
    }

    private void uneol(long lo) {
        eol = false;
        this.lastLineStart = this.offset + (this.fieldLo - lo);
    }

    public void index(long chunkLo, long chunkHi, long lineNumber,  LongList partitionKeys) {
        assert chunkHi > 0;
        assert chunkLo >= 0 && chunkLo < chunkHi;

        openInputFile();
        prepareBuffer();

        this.offset = chunkLo;
        long read;

        this.lastLineStart = offset;
        this.lineCount = lineNumber;
        this.maxLineLength = 0;

        try {
            do {
                long leftToRead = Math.min(chunkHi - offset, bufferLength);
                read = (int) ff.read(fd, fileBufferPtr, leftToRead, offset);
                if (read < 1) {
                    break;
                }
                parse(fileBufferPtr, fileBufferPtr + read);
                offset += read;
            } while (offset < chunkHi);

            if (read < 0 || offset < chunkHi) {
                throw CairoException.instance(ff.errno()).put("could not read file");
            } else {
                parseLast();
            }

            collectPartitionKeys(partitionKeys);
        } finally {
            closeOutputFiles();
        }

        LOG.info().$("Finished indexing chunk [start=").$(chunkLo).$(",end=").$(chunkHi)
                .$(",lines=").$(lineCount - lineNumber).$(",errors=").$(errorCount).$(']').$();
    }

    private void collectPartitionKeys(LongList partitionKeys) {
        partitionKeys.setPos(0);
        outputFiles.forEach((key, value) -> partitionKeys.add(value.partitionKey));
    }

    void openInputFile() {
        if (fd > -1) {
            return;
        }

        path.of(inputRoot).slash().concat(inputFileName).$();

        long fd = ff.openRO(path);
        if (fd < 0)
            throw CairoException.instance(ff.errno()).put("could not open read-only [file=").put(path).put(']');

        this.fd = fd;
    }

    void prepareBuffer() {
        if (fileBufferPtr < 0) {
            fileBufferPtr = Unsafe.malloc(bufferLength, MemoryTag.NATIVE_DEFAULT);
        }
    }

    public void countQuotes(long chunkStart, long chunkEnd, LongList chunkStats, int chunkIndex) throws TextException {
        long offset = chunkStart;

        //output vars
        long quotes = 0;
        long[] nlCount = new long[2];
        long[] nlFirst = new long[]{-1, -1};

        long read;
        long ptr;
        long hi;

        openInputFile();
        prepareBuffer();

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
            throw TextException.$("could not read import file [errno=").put(ff.errno()).put(']');
        }

        chunkStats.set(chunkIndex, quotes);
        chunkStats.set(chunkIndex + 1, nlCount[0]);
        chunkStats.set(chunkIndex + 2, nlCount[1]);
        chunkStats.set(chunkIndex + 3, nlFirst[0]);
        chunkStats.set(chunkIndex + 4, nlFirst[1]);

        LOG.info().$("Finished checking boundaries in chunk [no=").$(chunkIndex / 5).$(']').$();
    }

    public static void sort(FilesFacade ff, final long srcFd, long srcSize) {
        //int plen = path.length();
        //long srcFd = -1;
        long srcAddress = -1;

        //long dstFd = -1;
        long bufferPtr = -1;

        try {
            //srcFd = TableUtils.openFileRWOrFail(ff, path.$(), CairoConfiguration.O_NONE);
            //final long srcSize = ff.length(srcFd);
            srcAddress = TableUtils.mapRW(ff, srcFd, srcSize, MemoryTag.MMAP_DEFAULT);

//            dstFd = TableUtils.openFileRWOrFail(ff, path.chop$().put(".s").$(), CairoConfiguration.O_NONE);
//            final long dstAddress = TableUtils.mapRW(ff, dstFd, srcSize, MemoryTag.MMAP_DEFAULT);

            bufferPtr = Unsafe.malloc(srcSize, MemoryTag.NATIVE_DEFAULT);

            Vect.radixSortLongIndexAscInPlace(srcAddress, srcSize / INDEX_ENTRY_SIZE, bufferPtr);
        } finally {
            if (srcAddress != -1) {
                ff.munmap(srcAddress, srcSize, MemoryTag.MMAP_DEFAULT);
            }

            //srcFc belongs to outside object
//            if (srcFd != -1) {
//                ff.close(srcFd);
//                //ff.remove(path);
//                //path.trimTo(plen);
//            }

            if (bufferPtr != -1) {
                Unsafe.free(bufferPtr, srcSize, MemoryTag.MMAP_DEFAULT);
            }

//            if (dstFd != -1) {
//                ff.fsync(dstFd);
//                ff.close(dstFd);
//            }
        }
    }

    public boolean isHeader() {
        return header;
    }

    public byte getColumnDelimiter() {
        return columnDelimiter;
    }

    public void setColumnDelimiter(byte delimiter) {
        this.columnDelimiter = delimiter;
    }

    public void setTimestampIndex(int tsIndex) {
        this.timestampIndex = tsIndex;
    }

    public void setBufferLength(int length) {
        this.bufferLength = length;
    }

    public CharSequence getImportRoot() {
        return this.importRoot;
    }

    public CharSequence getInputFileName() {
        return this.inputFileName;
    }
}
