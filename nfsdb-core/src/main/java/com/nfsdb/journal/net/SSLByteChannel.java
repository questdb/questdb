package com.nfsdb.journal.net;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;

public class SSLByteChannel implements ByteChannel {
    public SSLByteChannel(SSLContext sslc, ByteChannel delegate, boolean client) {
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return 0;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return 0;
    }

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    public void close() throws IOException {

    }
}
