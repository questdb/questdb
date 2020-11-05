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

    private SlaveWriter dataFrameSlaveWriter;
    private long dataFrameColumnAddress;
    private long dataFrameColumnSize;
    private long dataFrameColumnOffset;
    private int dataFrameColumnIndex;

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
        dataFrameColumnAddress = 0;
        dataFrameColumnIndex = -1;
    }

    IOContextResult handleIO() {
        while (frameHeaderReadRemaining > 0) {
            long nRead = ff.read(fd, frameHeaderAddress, frameHeaderReadRemaining, frameHeaderReadOffset);
            if (nRead == -1) {
                // TODO Disconnected mid stream
                throw new RuntimeException();
            }
            frameHeaderReadOffset += nRead;
            frameHeaderReadRemaining -= nRead;
            if (frameHeaderReadRemaining > 0) {
                return IOContextResult.NEEDS_READ;
            }

            if (frameType == TableReplicationStreamHeaderSupport.FRAME_TYPE_UNKNOWN) {
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
                    // TODO Received junk frame
                    throw new RuntimeException();
                }
                if (frameHeaderReadRemaining > 0)
                    continue;
            }

            switch (frameType) {
                case TableReplicationStreamHeaderSupport.FRAME_TYPE_DATA_FRAME: {
                    int masterTableId = Unsafe.getUnsafe().getInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_MASTER_TABLE_ID);
                    SlaveWriter slaveWriter = getSlaveWriter(masterTableId);
                    int frameSequenceId = Unsafe.getUnsafe().getInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_FRAME_SEQUENCE_ID);
                    if (slaveWriter.getFrameSequenceId() != frameSequenceId) {
                        // Out of sequence stream, need to wait until the previous stream completes
                        // TODO: Add a timeout for this condition
                        return IOContextResult.NEEDS_BACKOFF_RETRY;
                    }
                    dataFrameColumnIndex = Unsafe.getUnsafe().getInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_DF_COLUMN_INDEX);
                    dataFrameSlaveWriter = slaveWriter;
                    dataFrameColumnSize = frameDataNBytesRemaining;
                    dataFrameColumnAddress = slaveWriter.mapColumnAppend(dataFrameColumnIndex, dataFrameColumnSize);
                    dataFrameColumnOffset = 0;
                    break;
                }

                case TableReplicationStreamHeaderSupport.FRAME_TYPE_BLOCK_META_FRAME: {
                    if (frameDataNBytesRemaining != 0) {
                        // TODO Received junk SOS frame
                        throw new RuntimeException();
                    }
                    int masterTableId = Unsafe.getUnsafe().getInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_MASTER_TABLE_ID);
                    SlaveWriter slaveWriter = getSlaveWriter(masterTableId);
                    int frameSequenceId = Unsafe.getUnsafe().getInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_FRAME_SEQUENCE_ID);
                    if (slaveWriter.getFrameSequenceId() != frameSequenceId) {
                        // Out of sequence stream, need to wait until the previous stream completes
                        // TODO: Add a timeout for this condition
                        return IOContextResult.NEEDS_BACKOFF_RETRY;
                    }
                    long timestampLo = Unsafe.getUnsafe().getLong(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_BMF_BLOCK_FIRST_TIMESTAMP);
                    slaveWriter.startPageFrame(timestampLo);
                    resetReading();
                    break;
                }

                default:
                    assert false;
            }
        }

        switch (frameType) {
            case TableReplicationStreamHeaderSupport.FRAME_TYPE_DATA_FRAME: {
                long nRead = ff.read(fd, dataFrameColumnAddress, frameDataNBytesRemaining, dataFrameColumnOffset);
                if (nRead == -1) {
                    // TODO Disconnected mid stream
                    throw new RuntimeException();
                }
                frameDataNBytesRemaining -= nRead;
                if (frameDataNBytesRemaining == 0) {
                    dataFrameSlaveWriter.unmapColumnAppend(dataFrameColumnIndex, dataFrameColumnAddress, dataFrameColumnSize);
                    dataFrameSlaveWriter = null;
                    resetReading();
                }
                break;
            }

            default:
                assert false;
        }

        return IOContextResult.NEEDS_READ;
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
