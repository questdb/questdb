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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.vm.MemoryPMARImpl;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cutlass.text.types.TimestampAdapter;
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
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.DirectCharSink;
import io.questdb.std.str.Path;

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

    private static final Log LOG = LogFactory.getLog(FileSplitter.class);
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    private final LongList stats = new LongList();

    private RingQueue<TextImportTask> queue;
    private Sequence pubSeq;
    private Sequence subSeq;
    private int workerCount;

    //TODO: rework
    private final DateLocale defaultDateLocale;

    //used for timestamp parsing 
    private final TypeManager typeManager;

    //used for timestamp parsing
    private final DirectCharSink utf8Sink;

    private final int bufferLength;

    private final FilesFacade ff;

    //work dir path
    private final Path path;
    private final int plen;
    private final int dirMode;

    //file offset of current start of buffered block
    private long offset;

    //position of timestamp column in csv 
    private int timestampIndex;

    //adapter used to parse timestamp column
    private TimestampAdapter timestampAdapter;

    //used to map timestamp to output file  
    private PartitionBy.PartitionFloorMethod partitionFloorMethod;
    private DateFormat partitionDirFormatMethod;

    private CharSequence inputFileName;

    //A guess at how long could a timestamp string be, including long day, month name, etc.
    //since we're only interested in timestamp field/col there's no point buffering whole line 
    //we'll copy field part to buffer only if current field is designated timestamp
    private static final int MAX_TIMESTAMP_LENGTH = 100;

    //maps partitionFloors to output file descriptors
    final private LongObjHashMap<MemoryMA> outputFiles = new LongObjHashMap<>();

    //timestamp field of current line
    final private DirectByteCharSequence timestampField;

    //fields taken & adjusted  from textLexer
    private final int fieldRollBufLimit;
    private boolean ignoreEolOnce;
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

    //line start offset of line with rolled timestamp field relative to start of file  
    private long rolledFieldLineOffset = -1;
    private boolean useFieldRollBuf = false;
    private boolean rollBufferUnusable = false;

    private byte columnDelimiter;
    private boolean inQuote;
    private boolean delayedOutQuote;

    //these two are pointers either into file read buffer or roll buffer   
    private long fieldLo;
    private long fieldHi;

    public FileSplitter(SqlExecutionContext sqlExecutionContext) {
        MessageBus bus = sqlExecutionContext.getMessageBus();
        this.workerCount = sqlExecutionContext.getWorkerCount();
        this.queue = bus.getTextImportQueue();
        this.pubSeq = bus.getTextImportPubSeq();
        this.subSeq = bus.getTextImportSubSeq();

        CairoEngine engine = sqlExecutionContext.getCairoEngine();
        final TextConfiguration textConfiguration = engine.getConfiguration().getTextConfiguration();
        this.defaultDateLocale = textConfiguration.getDefaultDateLocale();
        this.utf8Sink = new DirectCharSink(textConfiguration.getUtf8SinkSize());
        this.typeManager = new TypeManager(textConfiguration, utf8Sink);
        this.ff = engine.getConfiguration().getFilesFacade();
        this.bufferLength = engine.getConfiguration().getSqlCopyBufferSize();

        this.fieldRollBufLen = MAX_TIMESTAMP_LENGTH;
        this.fieldRollBufLimit = MAX_TIMESTAMP_LENGTH;
        this.fieldRollBufPtr = Unsafe.malloc(fieldRollBufLen, MemoryTag.NATIVE_DEFAULT);
        this.fieldRollBufCur = fieldRollBufPtr;

        this.timestampField = new DirectByteCharSequence();

        this.path = new Path().of(engine.getConfiguration().getInputWorkRoot());
        this.plen = path.length();
        this.dirMode = engine.getConfiguration().getMkDirMode();
    }

    //timestampIndex - zero-based index of timestamp column 
    public void split(CharSequence inputFileName, long fd, int partitionBy, byte columnDelimiter, int timestampIndex, DateFormat format, boolean ignoreHeader)
            throws TextException, SqlException {

        this.inputFileName = inputFileName;
        this.partitionFloorMethod = PartitionBy.getPartitionFloorMethod(partitionBy);
        this.partitionDirFormatMethod = PartitionBy.getPartitionDirFormatMethod(partitionBy);
        this.offset = 0;
        this.columnDelimiter = columnDelimiter;
        this.timestampIndex = timestampIndex;
        this.timestampAdapter = (TimestampAdapter) this.typeManager.nextTimestampAdapter(false, format, defaultDateLocale);
        this.header = ignoreHeader;

        createWorkDir(inputFileName);
        LOG.info().$("Started indexing file ").$(inputFileName).$();

        long buffer = Unsafe.malloc(bufferLength, MemoryTag.NATIVE_DEFAULT);
        try {
            long fileLength = ff.length(fd);
            long read = ff.read(fd, buffer, bufferLength, 0);

            while (read > 0) {
                parse(buffer, buffer + read);
                offset += read;
                read = (int) ff.read(fd, buffer, bufferLength, offset);
            }

            if (read < 0 || offset < fileLength) {
                throw SqlException.$(/*model.getFileName().position*/1, "could not read file [errno=").put(ff.errno()).put(']');
            } else {
                parseLast();
            }

            LOG.info().$("Finished indexing file ").$(inputFileName).$();

        } finally {
            Unsafe.free(buffer, bufferLength, MemoryTag.NATIVE_DEFAULT);
            clear();
        }
    }

    //TODO: we'll' need to lock dir or acquire table lock to make sure there are no two parallel user-issued imports of the same file 
    private void createWorkDir(CharSequence inputFileName) {
        path.trimTo(plen).slash().concat(inputFileName).slash$().$();

        if (ff.exists(path)) {
            int errno = ff.rmdir(path);
            if (errno != 0) {
                throw CairoException.instance(errno).put("Can't remove import work dir ").put(path).put(" errno=").put(errno);
            }
        }

        int errno = ff.mkdir(path, dirMode);
        if (errno != 0) {
            throw CairoException.instance(errno).put("Can't create import work dir ").put(path).put(" errno=").put(errno);
        }

        LOG.info().$("created import dir ").$(path).$();

        path.trimTo(plen);
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
        this.rolledFieldLineOffset = -1;
        this.rollBufferUnusable = false;
        this.header = false;
        this.errorCount = 0;
        this.offset = -1;
        this.timestampField.clear();
        this.lastQuotePos = -1;

        this.inputFileName = null;

        outputFiles.forEach((key, value) -> {
            value.close();//TODO: truncate ?
        });

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
        rolledFieldLineOffset = -1;
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

    private void ignoreEolOnce() {
        eol = true;
        fieldIndex = 0;
        ignoreEolOnce = false;
    }

    private void onColumnDelimiter(long lo, long ptr) {
        checkEol(lo);

        if (inQuote || ignoreEolOnce) {
            return;
        }
        stashField(fieldIndex++, ptr);
    }

    private void onLineEnd(long ptr) {
        if (inQuote) {
            return;
        }

        if (eol) {
            this.fieldLo = this.fieldHi;
            return;
        }

        stashField(fieldIndex, ptr);

        if (ignoreEolOnce) {
            ignoreEolOnce();
            return;
        }

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
                onQuote();
            } else if (c == '\n' || c == '\r') {
                onLineEnd(ptr);
            } else {
                checkEol(lo);
            }
        }

        if (useFieldRollBuf) {
            return;
        }

        if (eol) {
            this.fieldLo = 0;
        } else if (fieldIndex == timestampIndex) {//we only need to buffer index field
            rollField(lo, hi);
            useFieldRollBuf = true;
            rolledFieldLineOffset = this.offset + this.lastLineStart;
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
                lastQuotePos = -1;
            } else {
                timestampField.of(this.fieldLo, this.fieldHi - 1);
            }

            onTimestampField();

            if (useFieldRollBuf) {
                clearRollBuffer(ptr);
            }
        }

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
        this.lastLineStart = this.fieldLo - lo;
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

        long lineStartOffset;
        if (useFieldRollBuf) {
            lineStartOffset = rolledFieldLineOffset;
        } else {
            lineStartOffset = offset + lastLineStart;
        }
        //long lineEndOffset = offset + lastLineStart;

        long floor = partitionFloorMethod.floor(timestamp);

        MemoryMA target = outputFiles.get(floor);
        if (target == null) {
            path.trimTo(plen);
            path.slash().concat(inputFileName).slash();
            partitionDirFormatMethod.format(floor, null, null, path);
            path.put("_idx").$();

            if (ff.exists(path)) {
                //TODO: change exception type?
                throw CairoException.instance(-1).put("index file already exists [path=").put(path).put(']');
            } else {
                LOG.info().$("created import index file ").$(path).$();
            }

            target = new MemoryPMARImpl(ff, path, ff.getPageSize(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
            outputFiles.put(floor, target);
            path.trimTo(plen);
        }

        target.putLong(timestamp);
        target.putLong(lineStartOffset);
        //target.putLong(lineEndOffset);
    }

    protected void process() {
        int fileSize = 100;
        final long chunkSize = (fileSize + workerCount - 1) / workerCount;
        final int taskCount = (int) ((fileSize + chunkSize - 1) / chunkSize);

        //todo: splitting part
        int queuedCount = 0;
        doneLatch.reset();

        stats.setPos(taskCount * 4); // quotesEven, quotesOdd, newlineOffsetEven, newlineOffsetOdd
        stats.zero(0);

        for (int i = 0; i < taskCount; ++i) {
            final long chunkLo = i * chunkSize;
            final long chunkHi = Long.min(chunkLo + chunkSize, fileSize);

            final long seq = pubSeq.next();
            if (seq < 0) {
                // process locally
            } else {
                queue.get(seq).of(4 * i, chunkLo, chunkHi, stats, doneLatch);
                pubSeq.done(seq);
                queuedCount++;
            }
        }

        // process our own queue
        // this should fix deadlock with 1 worker configuration
        while (doneLatch.getCount() > -queuedCount) {
            long seq = subSeq.next();
            if (seq > -1) {
                queue.get(seq).run();
                subSeq.done(seq);
            }
        }

        doneLatch.await(queuedCount);

        queuedCount = 0;
        doneLatch.reset();
        //todo: parsing part
    }
}
