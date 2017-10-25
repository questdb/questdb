package com.questdb.net.udp.receiver;

import com.questdb.misc.Os;
import org.junit.Ignore;
import org.junit.Test;

public class LineProtoReceiverTest {
    @Test
    @Ignore
    public void testSimpleReceive() throws Exception {
        LineProtoReceiver receiver = new LineProtoReceiver("0.0.0.0", "234.5.6.7", 4567);
        receiver.close();
    }

    static {
        Os.init();
    }
}