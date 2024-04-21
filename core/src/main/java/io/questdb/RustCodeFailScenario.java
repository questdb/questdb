package io.questdb;

import io.questdb.std.Qdb;
import io.questdb.std.QuietCloseable;

public class RustCodeFailScenario implements QuietCloseable {
    private long impl;

    public RustCodeFailScenario() {
        this.impl = setup();
    }

    // Java_io_questdb_RustCodeFailScenario_hasFailpoints
    public static native boolean hasFailpoints();

    // Java_io_questdb_RustCodeFailScenario_removeFailpoint
    public static native void removeFailpoint(final String failpoint);

    // Java_io_questdb_RustCodeFailScenario_setFailpoint
    public static native void setFailpoint(final String failpoint, final String action);

    @Override
    public void close() {
        if (this.impl != 0) {
            teardown(this.impl);
        }
        this.impl = 0;
    }

    // Java_io_questdb_RustCodeFailScenario_setup
    private static native long setup();

    // Java_io_questdb_RustCodeFailScenario_teardown
    private static native void teardown(long impl);

    static {
        Qdb.init();
    }
}
