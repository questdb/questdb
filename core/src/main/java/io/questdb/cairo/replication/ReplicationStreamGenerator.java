package io.questdb.cairo.replication;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

public class ReplicationStreamGenerator implements Closeable {
    private static final ReplicationStreamGeneratorResult RETRY_RESULT = new ReplicationStreamGeneratorResult(true, null);
    private ObjList<ReplicationStreamGeneratorResult> frameCache = new ObjList<>();
    private IntList symbolCounts = new IntList();
    private int tableId;
    private int nConcurrentFrames;
    private PageFrameCursor cursor;
    private RecordMetadata metaData;
    private int columnCount;
    private PageFrame sourceFrame;
    private int atColumnIndex;
    private int nFrames;
    private boolean symbolDataFrame;
    private boolean finishedBlock;

    public ReplicationStreamGenerator() {
    }

    public void of(int tableId, int nConcurrentFrames, PageFrameCursor cursor, RecordMetadata metaData, IntList initialSymbolCounts) {
        assert null == this.cursor; // We should have been cleared
        this.tableId = tableId;
        this.nConcurrentFrames = nConcurrentFrames;
        this.cursor = cursor;
        this.metaData = metaData;
        this.nFrames = 0;
        this.atColumnIndex = 0;
        columnCount = metaData.getColumnCount();
        symbolDataFrame = true;
        finishedBlock = false;

        while (frameCache.size() < nConcurrentFrames) {
            ReplicationStreamGeneratorFrame frame = new ReplicationStreamGeneratorFrame();
            ReplicationStreamGeneratorResult result = new ReplicationStreamGeneratorResult(false, frame);
            frameCache.add(result);
        }
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            if (metaData.getColumnType(columnIndex) == ColumnType.SYMBOL) {
                symbolCounts.extendAndSet(columnIndex, initialSymbolCounts.get(columnIndex));
            } else {
                symbolCounts.extendAndSet(columnIndex, -1);
            }
        }
    }

    public ReplicationStreamGeneratorResult nextDataFrame() {
        assert null != this.cursor; // We should have been assigned
        ReplicationStreamGeneratorResult result = getNextFree();
        if (null != result) {
            ReplicationStreamGeneratorFrame frame = result.getFrame();
            if (null != sourceFrame) {
                generateDataFrame(frame);
            } else {
                if (!finishedBlock) {
                    sourceFrame = cursor.next();
                    if (null != sourceFrame) {
                        atColumnIndex = 0;
                        generateDataFrame(frame);
                    } else {
                        generateEndOfBlock(frame);
                        finishedBlock = true;
                    }
                } else {
                    return null;
                }
            }
            frame.setNotReady();
            return result;
        }
        return RETRY_RESULT;
    }

    public ReplicationStreamGeneratorResult generateCommitBlockFrame() {
        ReplicationStreamGeneratorResult result = getNextFree();
        if (null != result) {
            ReplicationStreamGeneratorFrame frame = result.getFrame();
            frame.of(TableReplicationStreamHeaderSupport.CB_HEADER_SIZE, 0, 0, 0);
            frame.frameHeaderLength = TableReplicationStreamHeaderSupport.CB_HEADER_SIZE;
            updateGenericHeader(frame, TableReplicationStreamHeaderSupport.FRAME_TYPE_COMMIT_BLOCK, frame.frameHeaderLength);
            frame.setNotReady();
            return result;
        }
        return RETRY_RESULT;
    }

    private ReplicationStreamGeneratorResult getNextFree() {
        ReplicationStreamGeneratorResult result = frameCache.get(nFrames % nConcurrentFrames);
        ReplicationStreamGeneratorFrame frame = result.getFrame();
        if (frame.isReady()) {
            nFrames++;
            return result;
        }
        return null;
    }

    private void generateDataFrame(ReplicationStreamGeneratorFrame frame) {
        if (symbolDataFrame) {
            int symbolCount = symbolCounts.getQuick(atColumnIndex);
            if (symbolCount >= 0) {
                SymbolMapReader symReader = cursor.getSymbolMapReader(atColumnIndex);
                int newSymbolCount = symReader.size();
                if (newSymbolCount > symbolCount) {
                    generateSymbolDataFrame(frame, symReader, symbolCount, newSymbolCount);
                    symbolDataFrame = false;
                    symbolCounts.set(atColumnIndex, newSymbolCount);
                    return;
                }
            }
        } else {
            symbolDataFrame = true;
        }
        frame.of(TableReplicationStreamHeaderSupport.DF_HEADER_SIZE, sourceFrame.getPageAddress(atColumnIndex), (int) sourceFrame.getPageSize(atColumnIndex),
                atColumnIndex % nConcurrentFrames);
        long frameSize = frame.frameHeaderLength + frame.frameDataLength;
        Unsafe.getUnsafe().putLong(frame.frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_DF_FIRST_TIMESTAMP, sourceFrame.getFirstTimestamp());
        Unsafe.getUnsafe().putInt(frame.frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_DF_COLUMN_INDEX, atColumnIndex);
        // TODO:
        Unsafe.getUnsafe().putLong(frame.frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_DF_DATA_OFFSET, 0);
        updateGenericHeader(frame, TableReplicationStreamHeaderSupport.FRAME_TYPE_DATA_FRAME, frameSize);
        atColumnIndex++;
        if (atColumnIndex == columnCount) {
            sourceFrame = null;
        }
    }

    private void generateSymbolDataFrame(ReplicationStreamGeneratorFrame frame, SymbolMapReader symReader, int symbolCount, int newSymbolCount) {
        long addressFrameStart = symReader.symbolCharsAddressOf(symbolCount);
        long dataOffset = addressFrameStart - symReader.symbolCharsAddressOf(0);
        int dataSize = (int) (symReader.symbolCharsAddressOf(newSymbolCount) - addressFrameStart);
        frame.of(TableReplicationStreamHeaderSupport.SFF_HEADER_SIZE, addressFrameStart, dataSize, atColumnIndex % nConcurrentFrames);
        long frameSize = frame.frameHeaderLength + frame.frameDataLength;
        Unsafe.getUnsafe().putInt(frame.frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_SFF_COLUMN_INDEX, atColumnIndex);
        Unsafe.getUnsafe().putLong(frame.frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_SFF_DATA_OFFSET, dataOffset);
        updateGenericHeader(frame, TableReplicationStreamHeaderSupport.FRAME_TYPE_SYMBOL_STRINGS_FRAME, frameSize);
    }

    private void generateEndOfBlock(ReplicationStreamGeneratorFrame frame) {
        frame.of(TableReplicationStreamHeaderSupport.EOB_HEADER_SIZE, 0, 0, 0);
        long frameSize = frame.frameHeaderLength;
        Unsafe.getUnsafe().putInt(frame.frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_EOB_N_FRAMES_SENT, nFrames - 1);
        updateGenericHeader(frame, TableReplicationStreamHeaderSupport.FRAME_TYPE_END_OF_BLOCK, frameSize);
    }

    private void updateGenericHeader(ReplicationStreamGeneratorFrame frame, byte frameType, long frameSize) {
        assert frameSize <= Integer.MAX_VALUE;
        Unsafe.getUnsafe().putInt(frame.frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_FRAME_SIZE, (int) (frameSize));
        Unsafe.getUnsafe().putByte(frame.frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_FRAME_TYPE, frameType);
        Unsafe.getUnsafe().putInt(frame.frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_MASTER_TABLE_ID, tableId);
    }

    public void clear() {
        if (null != cursor) {
            cursor.close();
            cursor = null;
        }
    }

    @Override
    public void close() {
        if (null != frameCache) {
            Misc.freeObjList(frameCache);
            clear();
            symbolCounts = null;
            frameCache = null;
        }
    }

    public static class ReplicationStreamGeneratorResult implements Closeable {
        private final boolean retry;
        private ReplicationStreamGeneratorFrame frame;

        private ReplicationStreamGeneratorResult(boolean retry, ReplicationStreamGeneratorFrame frame) {
            super();
            this.retry = retry;
            this.frame = frame;
        }

        public boolean isRetry() {
            return retry;
        }

        public ReplicationStreamGeneratorFrame getFrame() {
            return frame;
        }

        @Override
        public void close() {
            if (null != frame) {
                frame.close();
                frame = null;
            }
        }
    }

    public static class ReplicationStreamGeneratorFrame {
        private final AtomicBoolean ready = new AtomicBoolean(true);
        private long frameHeaderAddress;
        private int frameHeaderLength;
        private long frameDataAddress;
        private int frameDataLength;
        private int threadId;

        private ReplicationStreamGeneratorFrame() {
            frameHeaderAddress = Unsafe.malloc(TableReplicationStreamHeaderSupport.MAX_HEADER_SIZE);
        }

        private ReplicationStreamGeneratorFrame of(int frameHeaderLength, long frameDataAddress, int frameDataLength, int threadId) {
            assert ready.get();
            this.frameHeaderLength = frameHeaderLength;
            this.frameDataAddress = frameDataAddress;
            this.frameDataLength = frameDataLength;
            this.threadId = threadId;
            return this;
        }

        /**
         * Frames with the same threadId need to be applied in the same concurrent context
         * @return threadId
         */
        public int getThreadId() {
            return threadId;
        }

        public int getFrameHeaderLength() {
            return frameHeaderLength;
        }

        public long getFrameHeaderAddress() {
            return frameHeaderAddress;
        }

        public int getFrameDataLength() {
            return frameDataLength;
        }

        public long getFrameDataAddress() {
            return frameDataAddress;
        }

        public void complete() {
            assert !ready.get();
            ready.set(true);
        }

        public void cancel() {
            complete();
        }

        private boolean isReady() {
            return ready.get();
        }

        private void setNotReady() {
            assert ready.get();
            ready.set(false);
        }

        private void close() {
            if (0 != frameHeaderAddress) {
                Unsafe.free(frameHeaderAddress, TableReplicationStreamHeaderSupport.MAX_HEADER_SIZE);
                frameHeaderAddress = 0;
            }
        }
    }
}
