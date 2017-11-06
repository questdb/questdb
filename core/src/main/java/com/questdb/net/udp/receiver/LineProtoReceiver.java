package com.questdb.net.udp.receiver;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.CairoException;
import com.questdb.cairo.TableWriter;
import com.questdb.cairo.pool.ResourcePool;
import com.questdb.misc.Misc;
import com.questdb.misc.Net;
import com.questdb.misc.Os;
import com.questdb.mp.Job;
import com.questdb.parser.lp.CairoLineProtoParser;
import com.questdb.parser.lp.LineProtoLexer;
import com.questdb.std.str.DirectByteCharSequence;

import java.io.Closeable;

public class LineProtoReceiver implements Closeable, Job {
    private final int msgCount;
    private final DirectByteCharSequence byteSequence = new DirectByteCharSequence();
    private final LineProtoLexer lexer;
    private final CairoLineProtoParser parser;
    private long fd = -1;
    private long msgVec = 0;

    public LineProtoReceiver(ReceiverConfiguration receiverCfg, CairoConfiguration cairoCfg, ResourcePool<TableWriter> writerPool) {
        fd = Net.socketUdp();
        if (fd < 0) {
            throw CairoException.instance(Os.errno()).put("Cannot open UDP socket");
        }

        if (!Net.bind(fd, receiverCfg.getBindIPv4Address(), receiverCfg.getPort())) {
            close();
            throw CairoException.instance(Os.errno()).put("Cannot bind to ").put(receiverCfg.getBindIPv4Address()).put(':').put(receiverCfg.getPort());
        }

        if (!Net.join(fd, receiverCfg.getBindIPv4Address(), receiverCfg.getGroupIPv4Address())) {
            close();
            throw CairoException.instance(Os.errno()).put("Cannot join multicast group ").put(receiverCfg.getGroupIPv4Address()).put(" [bindTo=").put(receiverCfg.getBindIPv4Address()).put(']');
        }

        this.msgCount = receiverCfg.getMsgCount();

        if (Net.setRcvBuf(fd, receiverCfg.getReceiveBufferSize()) != 0) {
            System.out.println("err");
        }
        msgVec = Net.msgHeaders(receiverCfg.getMsgBufferSize(), msgCount);
        lexer = new LineProtoLexer(receiverCfg.getMsgBufferSize());
        parser = new CairoLineProtoParser(cairoCfg, writerPool);
        lexer.withParser(parser);
    }

    @Override
    public void close() {
        parser.commitAll();
        if (fd > -1) {
            Net.close(fd);
            if (msgVec != 0) {
                Net.freeMsgHeaders(msgVec);
            }
            Misc.free(parser);
            fd = -1;
        }
    }

    @Override
    public boolean run() {
        int count = Net.recvmmsg(fd, msgVec, msgCount);
        if (count > 0) {
            long p = msgVec;
            for (int i = 0; i < count; i++) {
                long buf = Net.getMMsgBuf(p);
                byteSequence.of(buf, buf + Net.getMMsgBufLen(p));
                lexer.parse(byteSequence);
                lexer.parseLast();
                p += Net.MMSGHDR_SIZE;
            }
            return true;
        }
        return false;
    }
}
