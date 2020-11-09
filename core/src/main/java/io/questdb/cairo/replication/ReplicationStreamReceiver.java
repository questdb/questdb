package io.questdb.cairo.replication;

import java.io.Closeable;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.replication.ReplicationSlaveManager.SlaveWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

public class ReplicationStreamReceiver implements Closeable {
    enum IOContextResult {
        NEEDS_READ, NEEDS_BACKOFF_RETRY
    }

    private final FilesFacade ff;
    private final ReplicationSlaveManager recvMgr;
    private final IntList tableIds = new IntList();
    private final IntObjHashMap<TableDetails> tableDetailsByMasterTableId = new IntObjHashMap<>();
    private final ObjList<TableDetails> tableDetailsCache = new ObjList<>();
    private long fd = -1;
    private long frameHeaderAddress;
    private long frameHeaderReadOffset;
    private long frameHeaderReadRemaining;

    private byte frameType;
    private long frameDataNBytesRemaining;
    private SlaveWriter slaveWriter;

    private long frameFirstTimestamp;
    private long frameMappingAddress;
    private long frameMappingSize;
    private long frameMappingOffset;

    private int dataFrameColumnIndex;
    private long dataFrameColumnOffset;

    public ReplicationStreamReceiver(CairoConfiguration configuration, ReplicationSlaveManager recvMgr) {
        this.ff = configuration.getFilesFacade();
        this.recvMgr = recvMgr;
        frameHeaderAddress = Unsafe.malloc(TableReplicationStreamHeaderSupport.MAX_HEADER_SIZE);
        fd = -1;
    }

    public void of(long fd) {
        this.fd = fd;
        resetReading();
    }

    private void resetReading() {
        frameHeaderReadOffset = 0;
        frameHeaderReadRemaining = TableReplicationStreamHeaderSupport.MIN_HEADER_SIZE;
        frameType = TableReplicationStreamHeaderSupport.FRAME_TYPE_UNKNOWN;
        frameMappingAddress = 0;
        slaveWriter = null;
    }

    IOContextResult handleIO() {
        while (frameHeaderReadRemaining > 0) {
            long nRead = ff.read(fd, frameHeaderAddress, frameHeaderReadRemaining, frameHeaderReadOffset);
            if (nRead == -1) {
                // TODO Disconnected while reading header
                throw new RuntimeException();
            }
            frameHeaderReadOffset += nRead;
            frameHeaderReadRemaining -= nRead;
            if (frameHeaderReadRemaining > 0) {
                return IOContextResult.NEEDS_READ;
            }

            if (frameType == TableReplicationStreamHeaderSupport.FRAME_TYPE_UNKNOWN) {
                // decode the generic header
                decodeGenericHeader();
                if (frameHeaderReadRemaining > 0)
                    continue;
            }

            switch (frameType) {
                case TableReplicationStreamHeaderSupport.FRAME_TYPE_DATA_FRAME: {
                    handleDataFrameHeader();
                    break;
                }

                case TableReplicationStreamHeaderSupport.FRAME_TYPE_END_OF_BLOCK: {
                    handleEndOfBlockHeader();
                    resetReading();
                    return IOContextResult.NEEDS_READ;
                }

                default:
                    assert false;
            }
        }

        long nRead = ff.read(fd, frameMappingAddress, frameDataNBytesRemaining, frameMappingOffset);
        if (nRead == -1) {
            // TODO Disconnected mid stream
            throw new RuntimeException();
        }
        frameDataNBytesRemaining -= nRead;
        if (frameDataNBytesRemaining == 0) {
            slaveWriter.unmap(dataFrameColumnIndex, frameMappingAddress, frameMappingSize);
            slaveWriter = null;
            resetReading();
        }
        return IOContextResult.NEEDS_READ;
    }

    private void handleEndOfBlockHeader() {
        if (frameDataNBytesRemaining != 0) {
            // TODO Received junk in the header
            throw new RuntimeException();
        }
        int nFrames = Unsafe.getUnsafe().getInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_EOB_N_FRAMES_SENT);
        slaveWriter.markBlockNFrames(nFrames);
    }

    private void handleDataFrameHeader() {
        // TODO deal with column top
        frameFirstTimestamp = Unsafe.getUnsafe().getLong(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_DF_FIRST_TIMESTAMP);
        dataFrameColumnIndex = Unsafe.getUnsafe().getInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_DF_COLUMN_INDEX);
        dataFrameColumnOffset = Unsafe.getUnsafe().getLong(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_DF_DATA_OFFSET);

        frameMappingAddress = slaveWriter.mapColumnData(frameFirstTimestamp, dataFrameColumnIndex, dataFrameColumnOffset, frameDataNBytesRemaining);
        frameMappingSize = frameDataNBytesRemaining;
        frameMappingOffset = 0;
    }

    private void decodeGenericHeader() {
        assert frameHeaderReadRemaining == 0;
        frameType = Unsafe.getUnsafe().getByte(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_FRAME_TYPE);
        if (frameType > TableReplicationStreamHeaderSupport.FRAME_TYPE_MAX_ID || frameType < TableReplicationStreamHeaderSupport.FRAME_TYPE_MIN_ID) {
            // TODO Received junk frame type
            throw new RuntimeException();
        }
        frameHeaderReadRemaining = TableReplicationStreamHeaderSupport.getFrameHeaderSize(frameType) - frameHeaderReadOffset;
        frameDataNBytesRemaining = Unsafe.getUnsafe().getInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_FRAME_SIZE) - frameHeaderReadOffset
                - frameHeaderReadRemaining;
        if (frameDataNBytesRemaining < 0) {
            // TODO Received junk in the header
            throw new RuntimeException();
        }
        int masterTableId = Unsafe.getUnsafe().getInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_MASTER_TABLE_ID);
        slaveWriter = getSlaveWriter(masterTableId);
    }

    private SlaveWriter getSlaveWriter(int masterTableId) {
        SlaveWriter slaveWriter;
        TableDetails tableDetails = tableDetailsByMasterTableId.get(masterTableId);
        if (null == tableDetails) {
            slaveWriter = recvMgr.getSlaveWriter(masterTableId);
            tableDetails = newTableDetails().of(masterTableId, slaveWriter);
            tableDetailsByMasterTableId.put(masterTableId, tableDetails);
            tableIds.add(masterTableId);
        } else {
            slaveWriter = tableDetails.getSlaveWriter();
        }
        return slaveWriter;
    }

    public void clear() {
        if (fd != -1) {
            for (int id = 0, sz = tableIds.size(); id < sz; id++) {
                releaseTableDetails(tableDetailsByMasterTableId.get(tableIds.get(id)));
            }
            tableIds.clear();
            tableDetailsByMasterTableId.clear();
            fd = -1;
        }
    }

    @Override
    public void close() {
        if (0 != frameHeaderAddress) {
            clear();
            Unsafe.free(frameHeaderAddress, TableReplicationStreamHeaderSupport.MAX_HEADER_SIZE);
            frameHeaderAddress = 0;
        }
    }

    private TableDetails newTableDetails() {
        int sz = tableDetailsCache.size();
        if (sz > 0) {
            sz--;
            TableDetails tableDetails = tableDetailsCache.get(sz);
            tableDetailsCache.remove(sz);
            return tableDetails;
        }
        return new TableDetails();
    }

    private void releaseTableDetails(TableDetails tableDetails) {
        tableDetails.clear();
        tableDetailsCache.add(tableDetails);
    }

    private class TableDetails implements Closeable {
        private int masterTableId;
        private SlaveWriter slaveWriter;

        private TableDetails of(int masterTableId, SlaveWriter slaveWriter) {
            assert this.slaveWriter == null;
            this.masterTableId = masterTableId;
            this.slaveWriter = slaveWriter;
            return this;
        }

        private SlaveWriter getSlaveWriter() {
            return slaveWriter;
        }

        private void clear() {
            if (null != slaveWriter) {
                recvMgr.releaseSlaveWriter(masterTableId, slaveWriter);
                slaveWriter = null;
            }
        }

        @Override
        public void close() {
            clear();
        }

    }
}
