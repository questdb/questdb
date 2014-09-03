package com.nfsdb.journal.net;

import java.net.SocketAddress;
import java.nio.channels.ByteChannel;

public class SocketChannelHolder {
    public final ByteChannel byteChannel;
    public final SocketAddress socketAddress;

    public SocketChannelHolder(ByteChannel byteChannel, SocketAddress socketAddress) {
        this.byteChannel = byteChannel;
        this.socketAddress = socketAddress;
    }
}
