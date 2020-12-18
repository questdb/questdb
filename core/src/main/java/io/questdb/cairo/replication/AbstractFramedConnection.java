package io.questdb.cairo.replication;

import java.io.Closeable;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NetworkFacade;
import io.questdb.std.Unsafe;

abstract class AbstractFramedConnection implements Closeable {
    private static final Log LOG = LogFactory.getLog(AbstractFramedConnection.class);
    private static final int BUFSZ = 1024;
    protected final NetworkFacade nf;
    protected long fd;
    protected long bufferAddress;
    protected int bufferSize;
    protected int bufferOffset;
    protected int bufferLen;
    protected boolean reading;
    private byte frameType;

    protected AbstractFramedConnection(NetworkFacade nf) {
        this.nf = nf;
    }

    protected final boolean init(long fd) {
        this.fd = fd;
        assert BUFSZ >= TableReplicationStreamHeaderSupport.MAX_HEADER_SIZE;
        bufferSize = BUFSZ;
        bufferAddress = Unsafe.malloc(bufferSize);
        if (bufferAddress == 0) {
            return false;
        }
        return true;
    }

    protected boolean handleIO() {
        if (fd >= 0) {
            if (reading) {
                return handleRead();
            } else {
                return handleWrite();
            }
        }
        return false;
    }

    private boolean handleRead() {
        boolean busy = false;
        while (true) {
            int len = bufferLen - bufferOffset;
            assert len > 0;
            int rc = nf.recv(fd, bufferAddress + bufferOffset, len);
            if (rc > 0) {
                bufferOffset += rc;
                if (bufferOffset > TableReplicationStreamHeaderSupport.MIN_HEADER_SIZE) {
                    if (frameType == TableReplicationStreamHeaderSupport.FRAME_TYPE_UNKNOWN) {
                        frameType = Unsafe.getUnsafe().getByte(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_FRAME_TYPE);
                        bufferLen = Unsafe.getUnsafe().getInt(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_FRAME_SIZE);
                        if (bufferLen <= 0) {
                            LOG.info().$("invalid frame length [fd=").$(fd).$(", frameType=").$(frameType).$(", bufferLen=").$(bufferLen).$(']').$();
                            close();
                            return true;
                        }
                        checkBufferCapacity(bufferLen);
                    }

                    if (bufferOffset < bufferLen) {
                        // Wait for full header
                        return true;
                    }

                    if (bufferOffset > bufferLen) {
                        // All frames handled by this object are request response, so we cannot have read the beginning of another frame
                        LOG.info().$("received data after the end of the current frame [fd=").$(fd).$(", frameType=").$(frameType).$(']').$();
                        close();
                        return true;
                    }

                    busy = true;
                    if (handleFrame(frameType)) {
                        return true;
                    }

                    LOG.info().$("peer sent invalid frame header, disconnecting [fd=").$(fd).$(", frameType=").$(frameType).$(']').$();
                    close();
                    return true;
                } else {
                    return true;
                }
            }

            if (rc == 0) {
                return busy;
            }

            close();
            return true;
        }
    }

    protected abstract boolean handleFrame(byte frameType);

    private boolean handleWrite() {
        int len = bufferLen - bufferOffset;
        assert len > 0;
        int rc = nf.send(fd, bufferAddress + bufferOffset, len);
        if (rc > 0) {
            bufferOffset += rc;
            if (bufferOffset == bufferLen) {
                onFinishedWriting();
            }
            return true;
        }

        if (rc == 0) {
            return false;
        }

        close();
        return true;
    }

    protected void onFinishedWriting() {
        resetReading();
    }

    protected final void resetReading() {
        bufferOffset = 0;
        bufferLen = bufferSize;
        frameType = TableReplicationStreamHeaderSupport.FRAME_TYPE_UNKNOWN;
        reading = true;
    }

    protected final void resetWriting(byte frameType, int frameLen) {
        bufferOffset = 0;
        bufferLen = frameLen;
        checkBufferCapacity(bufferLen);
        Unsafe.getUnsafe().putInt(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_FRAME_SIZE, bufferLen);
        Unsafe.getUnsafe().putByte(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_FRAME_TYPE, frameType);
        reading = false;
    }

    protected final void checkBufferCapacity(int minSz) {
        if (bufferSize < minSz) {
            bufferAddress = Unsafe.realloc(bufferAddress, bufferSize, minSz);
            bufferSize = minSz;
        }
    }

    protected final boolean isDisconnected() {
        return fd < 0;
    }

    protected void onClosed() {
    }

    @Override
    public final void close() {
        if (fd != -1) {
            LOG.info().$("closing peer connection [fd=").$(fd).$(']').$();
            Unsafe.free(bufferAddress, bufferSize);
            bufferAddress = 0;
            nf.close(fd, LOG);
            fd = -1;
            onClosed();
        }
    }
}