package io.questdb.log;

import io.questdb.std.Qdb;

public class RustLogging {
    public static int LEVEL_DEBUG = 4;
    public static int LEVEL_ERROR = 1;
    public static int LEVEL_INFO = 3;
    public static int LEVEL_TRACE = 5;
    public static int LEVEL_WARN = 2;

    @SuppressWarnings("EmptyMethod")
    public static void init() {
    }

    // Java_io_questdb_log_RustLogging_logMsg
    public static native void logMsg(int level, String target, String msg);

    // Java_io_questdb_log_RustLogging_installRustLogger
    private static native void installRustLogger(int maxLevel);

    static {
        Qdb.init();

        // To ensure that the Log and LogFactory classes are loaded
        // before the JNI code performs lookups on them.
        LogFactory.init();

        RustLogAdaptor.init();

        // Log debug messages and above.
        installRustLogger(3);  // LEVEL_INFO
    }
}
