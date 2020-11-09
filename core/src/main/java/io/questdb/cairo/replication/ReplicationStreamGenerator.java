package io.questdb.cairo.replication;

import java.io.Closeable;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.IntList;
import io.questdb.std.Unsafe;

public class ReplicationStreamGenerator implements Closeable {
    private final ReplicationStreamFrameMeta frameMeta = new ReplicationStreamFrameMetaImpl();
    private IntList symbolCounts = new IntList();
    private int tableId;
    private PageFrameCursor cursor;
    private RecordMetadata metaData;
    private int columnCount;
    private PageFrame sourceFrame;
    private int atColumnIndex;
    private int nFrames;

    private long frameHeaderSize;
    private long frameHeaderAddress;
    private long frameDataSize;
    private long frameDataAddress;

    public ReplicationStreamGenerator(CairoConfiguration configuration) {
        frameHeaderAddress = Unsafe.malloc(TableReplicationStreamHeaderSupport.MAX_HEADER_SIZE);
    }

    public void of(int tableId, PageFrameCursor cursor, RecordMetadata metaData, IntList initialSymbolCounts) {
        clear();
        this.tableId = tableId;
        this.cursor = cursor;
        this.metaData = metaData;
        this.nFrames = 0;
        columnCount = metaData.getColumnCount();
        // symbolCounts
    }

    public ReplicationStreamFrameMeta next() {
        if (null != sourceFrame) {
            generateDataFrame();
        } else {
            sourceFrame = cursor.next();
            if (null != sourceFrame) {
                atColumnIndex = 0;
                generateDataFrame();
            } else {
                if (nFrames > 0) {
                    generateEndOfBlock();
                } else {
                    return null;
                }
            }
        }
        return frameMeta;
    }

    public ReplicationStreamFrameMeta generateCommitBlockFrame() {
        frameDataSize = 0;
        frameDataAddress = 0;
        frameHeaderSize = TableReplicationStreamHeaderSupport.CB_HEADER_SIZE;
        updateGenericHeader(TableReplicationStreamHeaderSupport.FRAME_TYPE_COMMIT_BLOCK, frameHeaderSize);
        return frameMeta;
    }

    private void generateDataFrame() {
        frameDataSize = sourceFrame.getPageSize(atColumnIndex);
        frameDataAddress = sourceFrame.getPageAddress(atColumnIndex);
        frameHeaderSize = TableReplicationStreamHeaderSupport.DF_HEADER_SIZE;
        long frameSize = frameHeaderSize + frameDataSize;
        Unsafe.getUnsafe().putLong(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_DF_FIRST_TIMESTAMP, sourceFrame.getFirstTimestamp());
        Unsafe.getUnsafe().putInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_DF_COLUMN_INDEX, atColumnIndex);
        // TODO:
        Unsafe.getUnsafe().putLong(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_DF_DATA_OFFSET, Long.MIN_VALUE);
        updateGenericHeader(TableReplicationStreamHeaderSupport.FRAME_TYPE_DATA_FRAME, frameSize);
        nFrames++;
        atColumnIndex++;
        if (atColumnIndex == columnCount) {
            sourceFrame = null;
        }
    }

    private void generateEndOfBlock() {
        frameDataSize = 0;
        frameDataAddress = 0;
        frameHeaderSize = TableReplicationStreamHeaderSupport.EOB_HEADER_SIZE;
        long frameSize = frameHeaderSize;
        Unsafe.getUnsafe().putInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_EOB_N_FRAMES_SENT, nFrames);
        updateGenericHeader(TableReplicationStreamHeaderSupport.FRAME_TYPE_END_OF_BLOCK, frameSize);
        nFrames = 0;
    }

    private void updateGenericHeader(byte frameType, long frameSize) {
        assert frameSize <= Integer.MAX_VALUE;
        Unsafe.getUnsafe().putInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_FRAME_SIZE, (int) (frameSize));
        Unsafe.getUnsafe().putByte(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_FRAME_TYPE, frameType);
        Unsafe.getUnsafe().putInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_MASTER_TABLE_ID, tableId);
    }

    public void clear() {
        if (null != cursor) {
            cursor.close();
            cursor = null;
        }
    }

    @Override
    public void close() {
        if (0 != frameHeaderAddress) {
            clear();
            symbolCounts = null;
            Unsafe.free(frameHeaderAddress, TableReplicationStreamHeaderSupport.MAX_HEADER_SIZE);
            frameHeaderAddress = 0;
        }
    }

    interface ReplicationStreamFrameMeta {
        long getFrameHeaderSize();

        long getFrameHeaderAddress();

        long getFrameDataSize();

        long getFrameDataAddress();
    }

    private class ReplicationStreamFrameMetaImpl implements ReplicationStreamFrameMeta {

        @Override
        public long getFrameHeaderSize() {
            return frameHeaderSize;
        }

        @Override
        public long getFrameHeaderAddress() {
            return frameHeaderAddress;
        }

        @Override
        public long getFrameDataSize() {
            return frameDataSize;
        }

        @Override
        public long getFrameDataAddress() {
            return frameDataAddress;
        }

    }
}
