package io.questdb.cairo.replication;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TablePageFrameCursor;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.IntList;
import io.questdb.std.Unsafe;

public class ReplicationStreamGenerator implements Closeable {
    private final ReplicationStreamFrameMeta frameMeta = new ReplicationStreamFrameMetaImpl();
    private IntList symbolCounts = new IntList();
    private int tableId;
    private TablePageFrameCursor cursor;
    private RecordMetadata metaData;
    private int columnCount;
    private PageFrame sourceFrame;
    private int frameSequenceId;
    private int atColumnIndex;

    private long frameHeaderSize;
    private long frameHeaderAddress;
    private long frameDataSize;
    private long frameDataAddress;

    public ReplicationStreamGenerator(CairoConfiguration configuration) {
        frameHeaderAddress = Unsafe.malloc(TableReplicationStreamHeaderSupport.MAX_HEADER_SIZE);
    }

    public void of(int tableId, int frameSequenceId, TablePageFrameCursor cursor, RecordMetadata metaData, IntList initialSymbolCounts) {
        clear();
        this.tableId = tableId;
        this.frameSequenceId = frameSequenceId;
        this.cursor = cursor;
        this.metaData = metaData;
        columnCount = metaData.getColumnCount();
        // symbolCounts
    }

    public ReplicationStreamFrameMeta next() {
        long frameSize;
        if (null != sourceFrame) {
            // Generate data frame (DF)
            frameDataSize = sourceFrame.getPageSize(atColumnIndex);
            frameDataAddress = sourceFrame.getPageAddress(atColumnIndex);
            frameHeaderSize = TableReplicationStreamHeaderSupport.DF_HEADER_SIZE;
            frameSize = frameHeaderSize + frameDataSize;
            Unsafe.getUnsafe().putByte(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_FRAME_TYPE, TableReplicationStreamHeaderSupport.FRAME_TYPE_DATA_FRAME);
            Unsafe.getUnsafe().putInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_MASTER_TABLE_ID, tableId);
            Unsafe.getUnsafe().putInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_FRAME_SEQUENCE_ID, frameSequenceId);
            Unsafe.getUnsafe().putInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_DF_COLUMN_INDEX, atColumnIndex);

            atColumnIndex++;
            if (atColumnIndex == columnCount) {
                frameSequenceId++;
                sourceFrame = null;
            }
        } else {
            sourceFrame = cursor.next();
            if (null == sourceFrame) {
                return null;
            }
            // Generate start of stream frame (SOSF)
            atColumnIndex = 0;
            frameHeaderSize = frameSize = TableReplicationStreamHeaderSupport.BMF_HEADER_SIZE;
            frameDataSize = 0;
            Unsafe.getUnsafe().putByte(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_FRAME_TYPE, TableReplicationStreamHeaderSupport.FRAME_TYPE_BLOCK_META_FRAME);
            Unsafe.getUnsafe().putInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_MASTER_TABLE_ID, tableId);
            Unsafe.getUnsafe().putInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_FRAME_SEQUENCE_ID, frameSequenceId);
            Unsafe.getUnsafe().putLong(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_BMF_BLOCK_FIRST_TIMESTAMP, sourceFrame.getFirstTimestamp());

        }
        assert frameSize <= Integer.MAX_VALUE;
        Unsafe.getUnsafe().putInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_FRAME_SIZE, (int) (frameSize));
        return frameMeta;
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
