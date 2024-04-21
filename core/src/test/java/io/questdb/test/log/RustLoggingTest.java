package io.questdb.test.log;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.RustLogAdaptor;
import io.questdb.log.RustLogging;
import io.questdb.std.str.GcUtf8String;
import org.junit.Test;

public class RustLoggingTest {

    private static final Log LOG = LogFactory.getLog(RustLoggingTest.class);

    @Test
    public void testAdaptors() {
        GcUtf8String testStr = new GcUtf8String("test щось");
        RustLogAdaptor.advisoryUtf8(LOG, testStr.lo(), testStr.hi());
        RustLogAdaptor.criticalUtf8(LOG, testStr.lo(), testStr.hi());
        RustLogAdaptor.debugUtf8(LOG, testStr.lo(), testStr.hi());
        RustLogAdaptor.errorUtf8(LOG, testStr.lo(), testStr.hi());
        RustLogAdaptor.infoUtf8(LOG, testStr.lo(), testStr.hi());
    }

    @Test
    public void testLogging() {
        // All info-level log messages (and above) should appear
        // in stdout when running this test.
        // If they don't, temporarily uncomment the following line:
        //     LogFactory.configureSync();
        RustLogging.logMsg(RustLogging.LEVEL_INFO, "a::b::c", "test_msg1");
        RustLogging.logMsg(RustLogging.LEVEL_WARN, "def", "test_msg2");
        RustLogging.logMsg(RustLogging.LEVEL_INFO, "a::b::c", "test_msg3");
        RustLogging.logMsg(RustLogging.LEVEL_ERROR, "def", "test_msg4");
        RustLogging.logMsg(RustLogging.LEVEL_INFO, "a::b::c", "test_msg5");
        RustLogging.logMsg(RustLogging.LEVEL_DEBUG, "ghi", "test_msg6");
        RustLogging.logMsg(RustLogging.LEVEL_TRACE, "a::b::c", "test_msg7");
        RustLogging.logMsg(RustLogging.LEVEL_ERROR, "ghi", "test_msg8");
    }

    @Test
    public void testLoggingUtf8() {
        // This is expected to produce garbage output.

        // 4 two-byte UTF-8 characters in both UTF-8 and UTF-16.
        // >>> 'щось'.encode('utf-8')
        // b'\xd1\x89\xd0\xbe\xd1\x81\xd1\x8c'
        // >>> len(_)
        // 8
        // >>> 'щось'.encode('utf-16be')
        // b'\x04I\x04>\x04A\x04L'
        // >>> len(_)
        // 8
        RustLogging.logMsg(RustLogging.LEVEL_ERROR, "x", "щось");
    }
}
