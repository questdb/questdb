package io.questdb.cairo.replication;

import java.io.Closeable;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NetworkFacade;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.Unsafe;

public class ReplicationStreamReceiver implements Closeable {
    private static final Log LOG = LogFactory.getLog(ReplicationStreamReceiver.class);
    private final NetworkFacade nf;
    private IntObjHashMap<SlaveWriter> slaveWriteByMasterTableId;
    private long fd = -1;
    private Runnable disconnectedCallback;
    private long frameHeaderAddress;
    private int frameHeaderOffset;
    private int frameHeaderRemaining;

    private byte frameType;
    private int frameDataNBytesRemaining;
    private int masterTableId;
    private SlaveWriter slaveWriter;

    private long frameFirstTimestamp;
    private long frameMappingAddress;
    private long frameMappingSize;
    private long frameMappingOffset;

    private int dataFrameColumnIndex;
    private long dataFrameColumnOffset;

    private boolean readyToCommit;
    private int nCommits;

    public ReplicationStreamReceiver(NetworkFacade nf) {
        this.nf = nf;
        frameHeaderAddress = Unsafe.malloc(TableReplicationStreamHeaderSupport.MAX_HEADER_SIZE);
        fd = -1;
    }

    public void of(long fd, IntObjHashMap<SlaveWriter> slaveWriteByMasterTableId, Runnable disconnectedCallback) {
        this.fd = fd;
        this.slaveWriteByMasterTableId = slaveWriteByMasterTableId;
        this.disconnectedCallback = disconnectedCallback;
        readyToCommit = false;
        nCommits = 0;
        resetReading();
    }

    private void resetReading() {
        frameHeaderOffset = 0;
        frameHeaderRemaining = TableReplicationStreamHeaderSupport.MIN_HEADER_SIZE;
        frameType = TableReplicationStreamHeaderSupport.FRAME_TYPE_UNKNOWN;
        frameMappingAddress = 0;
        slaveWriter = null;
    }

    boolean handleIO() {
        if (!readyToCommit) {
            return handleRead();
        } else {
            return handleWrite();
        }
    }

    private boolean handleRead() {
        assert frameHeaderRemaining > 0 || frameDataNBytesRemaining > 0;
        while (frameHeaderRemaining > 0) {
            int nRead = nf.recv(fd, frameHeaderAddress + frameHeaderOffset, frameHeaderRemaining);
            if (nRead == -1) {
                LOG.info().$("peer disconnected when reading frame header [fd=").$(fd).$(']').$();
                disconnect();
                return true;
            }
            frameHeaderOffset += nRead;
            frameHeaderRemaining -= nRead;
            if (frameHeaderRemaining > 0) {
                return nRead > 0;
            }

            if (frameType == TableReplicationStreamHeaderSupport.FRAME_TYPE_UNKNOWN) {
                // decode the generic header
                if (!decodeGenericHeader()) {
                    disconnect();
                    return true;
                }
                if (frameHeaderRemaining > 0)
                    continue;
            }

            switch (frameType) {
                case TableReplicationStreamHeaderSupport.FRAME_TYPE_DATA_FRAME: {
                    handleDataFrameHeader();
                    break;
                }

                case TableReplicationStreamHeaderSupport.FRAME_TYPE_SYMBOL_STRINGS_FRAME: {
                    handleSymbolDataFrameHeader();
                    break;
                }

                case TableReplicationStreamHeaderSupport.FRAME_TYPE_END_OF_BLOCK: {
                    if (!handleEndOfBlockHeader()) {
                        disconnect();
                    }
                    resetReading();
                    return true;
                }

                case TableReplicationStreamHeaderSupport.FRAME_TYPE_COMMIT_BLOCK: {
                    if (!handleCommitBlock()) {
                        disconnect();
                    }
                    return true;
                }

                default:
                    assert false;
            }
        }

        int nRead = nf.recv(fd, frameMappingAddress + frameMappingOffset, frameDataNBytesRemaining);
        if (nRead == -1) {
            LOG.info().$("peer disconnected when reading frame data [fd=").$(fd).$(']').$();
            disconnect();
            return true;
        }
        frameDataNBytesRemaining -= nRead;
        if (frameDataNBytesRemaining == 0) {
            if (slaveWriter.completeFrame()) {
                handleReadyToCommit();
            }
            slaveWriter = null;
            resetReading();
        }
        return nRead > 0;
    }

    private boolean handleWrite() {
        int nWritten = nf.send(fd, frameHeaderAddress + frameHeaderOffset, frameHeaderRemaining);
        if (nWritten == -1) {
            LOG.info().$("peer disconnected when writiing frame [fd=").$(fd).$(']').$();
            disconnect();
        }
        frameHeaderRemaining -= nWritten;
        frameHeaderOffset += nWritten;
        if (frameHeaderRemaining == 0) {
            readyToCommit = false;
            resetReading();
            return nWritten > 0;
        }

        return true;
    }

    private boolean handleEndOfBlockHeader() {
        if (frameDataNBytesRemaining != 0) {
            LOG.info().$("invalid end of block frame, disconnecting [fd=").$(fd).$(", frameDataNBytesRemaining=").$(frameDataNBytesRemaining).$(']').$();
            return false;
        }
        int nFrames = Unsafe.getUnsafe().getInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_EOB_N_FRAMES_SENT);
        if (slaveWriter.markBlockNFrames(nFrames)) {
            handleReadyToCommit();
        }
        return true;
    }

    private boolean handleCommitBlock() {
        if (frameDataNBytesRemaining != 0) {
            LOG.info().$("invalid commit block frame, disconnecting [fd=").$(fd).$(", frameDataNBytesRemaining=").$(frameDataNBytesRemaining).$(']').$();
            return false;
        }
        slaveWriter.commit();
        resetReading();
        nCommits++;
        return true;
    }

    private void handleDataFrameHeader() {
        // TODO deal with column top
        frameFirstTimestamp = Unsafe.getUnsafe().getLong(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_DF_FIRST_TIMESTAMP);
        dataFrameColumnIndex = Unsafe.getUnsafe().getInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_DF_COLUMN_INDEX);
        dataFrameColumnOffset = Unsafe.getUnsafe().getLong(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_DF_DATA_OFFSET);

        frameMappingAddress = slaveWriter.getDataMap(frameFirstTimestamp, dataFrameColumnIndex, dataFrameColumnOffset, frameDataNBytesRemaining);
        frameMappingSize = frameDataNBytesRemaining;
        frameMappingOffset = 0;
    }

    private void handleSymbolDataFrameHeader() {
        dataFrameColumnIndex = Unsafe.getUnsafe().getInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_SFF_COLUMN_INDEX);
        dataFrameColumnOffset = Unsafe.getUnsafe().getLong(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_SFF_DATA_OFFSET);

        frameMappingAddress = slaveWriter.getSymbolDataMap(dataFrameColumnIndex, dataFrameColumnOffset, frameDataNBytesRemaining);
        frameMappingSize = frameDataNBytesRemaining;
        frameMappingOffset = 0;
    }

    private boolean decodeGenericHeader() {
        assert frameHeaderRemaining == 0;
        frameType = Unsafe.getUnsafe().getByte(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_FRAME_TYPE);
        if (frameType > TableReplicationStreamHeaderSupport.FRAME_TYPE_MAX_ID || frameType < TableReplicationStreamHeaderSupport.FRAME_TYPE_MIN_ID) {
            LOG.info().$("peer sent invalid frame header, disconnecting [frameType=").$(frameType).$(']').$();
            return false;
        }
        frameHeaderRemaining = TableReplicationStreamHeaderSupport.getFrameHeaderSize(frameType) - frameHeaderOffset;
        frameDataNBytesRemaining = Unsafe.getUnsafe().getInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_FRAME_SIZE) - frameHeaderOffset
                - frameHeaderRemaining;
        if (frameDataNBytesRemaining < 0) {
            LOG.info().$("peer sent invalid frame header, disconnecting [frameDataNBytesRemaining=").$(frameDataNBytesRemaining).$(']').$();
            return false;
        }
        masterTableId = Unsafe.getUnsafe().getInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_MASTER_TABLE_ID);
        slaveWriter = getSlaveWriter(masterTableId);
        return true;
    }

    private void handleReadyToCommit() {
        frameHeaderRemaining = TableReplicationStreamHeaderSupport.SCR_HEADER_SIZE;
        Unsafe.getUnsafe().putInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_FRAME_SIZE, frameHeaderRemaining);
        Unsafe.getUnsafe().putByte(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_FRAME_TYPE, TableReplicationStreamHeaderSupport.FRAME_TYPE_SLAVE_COMMIT_READY);
        Unsafe.getUnsafe().putInt(frameHeaderAddress + TableReplicationStreamHeaderSupport.OFFSET_MASTER_TABLE_ID, masterTableId);
        frameHeaderOffset = 0;
        readyToCommit = true;
    }

    private SlaveWriter getSlaveWriter(int masterTableId) {
        return slaveWriteByMasterTableId.get(masterTableId);
    }

    private void disconnect() {
        fd = -1;
        disconnectedCallback.run();
    }

    public int getnCommits() {
        return nCommits;
    }

    public void clear() {
        if (fd != -1) {
            slaveWriteByMasterTableId = null;
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
}
