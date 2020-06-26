package io.questdb.cutlass.line.tcp;

import io.questdb.cutlass.line.tcp.LineTcpMeasurementScheduler.LineTcpMeasurementEvent;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.IOContext;
import io.questdb.network.IODispatcher;
import io.questdb.network.IOOperation;
import io.questdb.network.NetworkFacade;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectByteCharSequence;

class LineTcpConnectionContext implements IOContext, Mutable {
    private static final Log LOG = LogFactory.getLog(LineTcpConnectionContext.class);
    private final NetworkFacade nf;
    private final LineTcpMeasurementScheduler scheduler;
    private long fd;
    private IODispatcher<LineTcpConnectionContext> dispatcher;
    private long recvBufStart;
    private long recvBufEnd;
    private long recvBufPos;
    private boolean peerDisconnected;
    private final DirectByteCharSequence byteCharSequence = new DirectByteCharSequence();

    LineTcpConnectionContext(LineTcpReceiverConfiguration configuration, LineTcpMeasurementScheduler scheduler) {
        nf = configuration.getNetworkFacade();
        this.scheduler = scheduler;
        recvBufStart = Unsafe.malloc(configuration.getMsgBufferSize());
        recvBufEnd = recvBufStart + configuration.getMsgBufferSize();
    }

    // returns true if busy
    boolean handleIO() {
        try {
            LineTcpMeasurementEvent event = scheduler.getNewEvent();
            if (null == event) {
                // Waiting for writer threads to drain queue, request callback as soon as possible
                dispatcher.registerChannel(this, IOOperation.READ);
                dispatcher.registerChannel(this, IOOperation.WRITE);
                return true;
            }

            // Read as much data as possible
            int len = (int) (recvBufEnd - recvBufPos);
            if (len > 0 && !peerDisconnected) {
                int nRead = nf.recv(fd, recvBufPos, len);
                if (nRead < 0) {
                    if (recvBufPos != recvBufStart) {
                        LOG.info().$('[').$(fd).$("] disconnected with partial measurement, ").$(recvBufPos - recvBufStart).$(" unprocessed bytes").$();
                    }
                    peerDisconnected = true;
                } else {
                    recvBufPos += nRead;
                }
            }

            // Process as much data as possible
            long recvBufLineStart = recvBufStart;
            do {
                long recvBufLineNext = event.parseLine(recvBufLineStart, recvBufPos);
                if (recvBufLineNext == -1) {
                    break;
                }
                if (!event.isError()) {
                    scheduler.commitNewEvent(event);
                    event = scheduler.getNewEvent();
                } else {
                    LOG.error().$('[').$(fd).$("] failed to parse measurement, code ").$(event.getErrorCode()).$(" at ").$(event.getErrorPosition()).$(" in ")
                            .$(byteCharSequence.of(recvBufLineStart, recvBufLineNext - 1)).$();
                }
                recvBufLineStart = recvBufLineNext;
            } while (recvBufLineStart != recvBufPos && null != event);

            // Compact input buffer
            if (recvBufLineStart != recvBufStart) {
                len = (int) (recvBufPos - recvBufLineStart);
                if (len > 0) {
                    Unsafe.getUnsafe().copyMemory(recvBufLineStart, recvBufStart, len);
                }
                recvBufPos = recvBufStart + len;
            }

            // Check if we are waiting for writer threads
            if (null == event) {
                // Waiting for writer threads to drain queue, request callback as soon as possible
                dispatcher.registerChannel(this, IOOperation.READ);
                dispatcher.registerChannel(this, IOOperation.WRITE);
                return true;
            }

            // Check for buffer overflow
            if (recvBufPos == recvBufEnd) {
                LOG.error().$('[').$(fd).$("] buffer overflow [msgBufferSize=").$(recvBufEnd - recvBufStart).$(']').$();
                dispatcher.disconnect(this);
                return false;
            }

            if (peerDisconnected) {
                // Peer disconnected, we have now finished disconnect our end
                dispatcher.disconnect(this);
                return false;
            }

            dispatcher.registerChannel(this, IOOperation.READ);
            return true;
        } catch (RuntimeException ex) {
            LOG.error().$('[').$(fd).$("] Failed to process line data").$(ex).$();
            dispatcher.disconnect(this);
            return false;
        }
    }

    @Override
    public void clear() {
        recvBufPos = recvBufStart;
        peerDisconnected = false;
    }

    @Override
    public void close() {
        this.fd = -1;
        Unsafe.free(recvBufStart, recvBufEnd - recvBufStart);
        recvBufStart = recvBufEnd = recvBufPos = 0;
    }

    @Override
    public long getFd() {
        return fd;
    }

    @Override
    public boolean invalid() {
        return fd == -1;
    }

    @Override
    public IODispatcher<LineTcpConnectionContext> getDispatcher() {
        return dispatcher;
    }

    LineTcpConnectionContext of(long clientFd, IODispatcher<LineTcpConnectionContext> dispatcher) {
        this.fd = clientFd;
        this.dispatcher = dispatcher;
        clear();
        return this;
    }
}