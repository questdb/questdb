package io.questdb.cairo.replication;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

public class ReplicationStreamGenerator implements Closeable {
    private static final ReplicationStreamGeneratorResult RETRY_RESULT = new ReplicationStreamGeneratorResult(true, null);
    private ObjList<ReplicationStreamGeneratorResult> resultByThread = new ObjList<>();
    private IntList symbolCounts = new IntList();
    private int tableId;
    private int nConsumerThreads;
    private PageFrameCursor cursor;
    private RecordMetadata metaData;
    private int columnCount;
    private PageFrame sourceFrame;
    private int atColumnIndex;
    private int nFrames;

    public ReplicationStreamGenerator(CairoConfiguration configuration) {
    }

    public void of(int tableId, int nConsumerThreads, PageFrameCursor cursor, RecordMetadata metaData, IntList initialSymbolCounts) {
        assert null == this.cursor; // We should have been cleared
        this.tableId = tableId;
        this.nConsumerThreads = nConsumerThreads;
        this.cursor = cursor;
        this.metaData = metaData;
        this.nFrames = 0;
        this.atColumnIndex = 0;
        columnCount = metaData.getColumnCount();

        while (resultByThread.size() < nConsumerThreads) {
            ReplicationStreamGeneratorFrame frame = new ReplicationStreamGeneratorFrame(resultByThread.size());
            ReplicationStreamGeneratorResult result = new ReplicationStreamGeneratorResult(false, frame);
            resultByThread.add(result);
        }
        // TODO: symbols
    }

    public ReplicationStreamGeneratorResult nextDataFrame() {
        ReplicationStreamGeneratorResult result = resultByThread.get(atColumnIndex % nConsumerThreads);
        ReplicationStreamGeneratorFrame frame = result.getFrame();
        if (frame.isReady()) {
            if (null != sourceFrame) {
                generateDataFrame(frame);
            } else {
                sourceFrame = cursor.next();
                if (null != sourceFrame) {
                    atColumnIndex = 0;
                    generateDataFrame(frame);
                } else {
                    if (nFrames > 0) {
                        generateEndOfBlock(frame);
                    } else {
                        return null;
                    }
                }
            }
            frame.setNotReady();
            return result;
        }
        return RETRY_RESULT;
    }

    public ReplicationStreamGeneratorResult generateCommitBlockFrame() {
        ReplicationStreamGeneratorResult result = resultByThread.get(atColumnIndex % nConsumerThreads);
        ReplicationStreamGeneratorFrame frame = result.getFrame();
        if (frame.isReady()) {
            frame.frameDataSize = 0;
            frame.frameDataAddress = 0;
            frame.frameHeaderSize = TableReplicationStreamHeaderSupport.CB_HEADER_SIZE;
            updateGenericHeader(frame, TableReplicationStreamHeaderSupport.FRAME_TYPE_COMMIT_BLOCK, frame.frameHeaderSize);
            frame.setNotReady();
            return result;
        }
        return RETRY_RESULT;
    }

    private void generateDataFrame(ReplicationStreamGeneratorFrame frame) {
        frame.frameDataSize = sourceFrame.getPageSize(atColumnIndex);
        frame.frameDataAddress = sourceFrame.getPageAddress(atColumnIndex);
        frame.frameHeaderSize = TableReplicationStreamHeaderSupport.DF_HEADER_SIZE;
        long frameSize = frame.frameHeaderSize + frame.frameDataSize;
        Unsafe.getUnsafe().putLong(frame.frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_DF_FIRST_TIMESTAMP, sourceFrame.getFirstTimestamp());
        Unsafe.getUnsafe().putInt(frame.frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_DF_COLUMN_INDEX, atColumnIndex);
        // TODO:
        Unsafe.getUnsafe().putLong(frame.frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_DF_DATA_OFFSET, 0);
        updateGenericHeader(frame, TableReplicationStreamHeaderSupport.FRAME_TYPE_DATA_FRAME, frameSize);
        nFrames++;
        atColumnIndex++;
        if (atColumnIndex == columnCount) {
            sourceFrame = null;
        }
    }

    private void generateEndOfBlock(ReplicationStreamGeneratorFrame frame) {
        frame.frameDataSize = 0;
        frame.frameDataAddress = 0;
        frame.frameHeaderSize = TableReplicationStreamHeaderSupport.EOB_HEADER_SIZE;
        long frameSize = frame.frameHeaderSize;
        Unsafe.getUnsafe().putInt(frame.frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_EOB_N_FRAMES_SENT, nFrames);
        updateGenericHeader(frame, TableReplicationStreamHeaderSupport.FRAME_TYPE_END_OF_BLOCK, frameSize);
        nFrames = 0;
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
        if (null != resultByThread) {
            Misc.freeObjList(resultByThread);
            clear();
            symbolCounts = null;
            resultByThread = null;
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
        private final int threadId;
        private final AtomicBoolean ready = new AtomicBoolean(true);
        private long frameHeaderSize;
        private long frameHeaderAddress;
        private long frameDataSize;
        private long frameDataAddress;

        private ReplicationStreamGeneratorFrame(int threadId) {
            this.threadId = threadId;
            frameHeaderAddress = Unsafe.malloc(TableReplicationStreamHeaderSupport.MAX_HEADER_SIZE);
        }

        public int getThreadId() {
            return threadId;
        }

        public long getFrameHeaderSize() {
            return frameHeaderSize;
        }

        public long getFrameHeaderAddress() {
            return frameHeaderAddress;
        }

        public long getFrameDataSize() {
            return frameDataSize;
        }

        public long getFrameDataAddress() {
            return frameDataAddress;
        }

        public void complete() {
            assert !ready.get();
            ready.set(true);
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
