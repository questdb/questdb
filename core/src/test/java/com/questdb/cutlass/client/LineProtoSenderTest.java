package com.questdb.cutlass.client;

import com.questdb.std.Os;
import org.junit.Test;

public class LineProtoSenderTest {

    @Test
    public void testSimple() throws Exception {
        try (LineProtoSender sender = new LineProtoSender("234.5.6.7", 4567, 110)) {
            sender.metric("weather").tag("location", "london").tag("by", "quest").field("temp", 3400).$(System.currentTimeMillis());
            sender.metric("weather2").tag("location", "london").tag("by", "quest").field("temp", 3400).$(System.currentTimeMillis());
            sender.metric("weather3").tag("location", "london").tag("by", "quest").field("temp", 3400).$(System.currentTimeMillis());
            sender.flush();
        }
    }

    static {
        Os.init();
    }
}