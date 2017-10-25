package com.questdb.net.udp.receiver;

import com.questdb.cairo.CairoException;
import com.questdb.misc.Net;
import com.questdb.misc.Os;
import com.questdb.misc.Unsafe;
import com.questdb.std.str.DirectByteCharSequence;

import java.io.Closeable;

public class LineProtoReceiver implements Closeable {
    private final int msgSize = 2048;
    private final int msgCount = 4096;
    private long fd = -1;
    private long msgVec;

    public LineProtoReceiver(CharSequence bindIPv4Address, CharSequence groupIPv4Address, int port) {
        fd = Net.socketUdp();
        if (fd < 0) {
            throw CairoException.instance(Os.errno()).put("Cannot open UDP socket");
        }

        if (!Net.bind(fd, bindIPv4Address, port)) {
            throw CairoException.instance(Os.errno()).put("Cannot bind to ").put(bindIPv4Address).put(':').put(port);
        }

        if (!Net.join(fd, bindIPv4Address, groupIPv4Address)) {
            throw CairoException.instance(Os.errno()).put("Cannot join multicast group ").put(groupIPv4Address).put(" [bindTo=").put(bindIPv4Address).put(']');
        }

        msgVec = Net.msgHeaders(msgSize, msgCount);

        DirectByteCharSequence charSequence = new DirectByteCharSequence();

        int count = Net.recvmmsg(fd, msgVec, msgCount);
        if (count > 0) {
            long p = msgVec;
            for (int i = 0; i < count; i++) {
                int size = Unsafe.getUnsafe().getInt(p + Net.MMSGHDR_BUFFER_LENGTH_OFFSET);
                long addr = Unsafe.getUnsafe().getLong(Unsafe.getUnsafe().getLong(p + Net.MMSGHDR_BUFFER_ADDRESS_OFFSET));
                charSequence.of(addr, addr + size);
//                System.out.println("Received: " + Long.toHexString(addr));
                System.out.print(charSequence);
                p += Net.MMSGHDR_SIZE;
            }
        } else {
            System.out.println("count: " + count);
            System.out.println("errno: " + Os.errno());
            System.out.println(Os.errno() == Net.EWOULDBLOCK);
        }
    }

    @Override
    public void close() {
        if (fd > -1) {
            Net.close(fd);
            Net.freeMsgHeaders(msgVec);
            fd = -1;
        }
    }
}
