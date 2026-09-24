package io.questdb.test.cairo.crash;

import org.junit.Test;

import java.util.List;

/**
 * Concrete runner that exercises the harness self-check.  Surefire skips abstract classes,
 * so this thin subclass is required to make the {@code @Test} discoverable.
 */
public class CrashConsistencySelfCheckTest extends AbstractCrashConsistencyTest {

    @Test
    public void testHarnessSelfCheck() throws Exception {
        runWithCrashFacade(() -> {
            execute("create table t (ts timestamp, s string) timestamp(ts) partition by none");
            execute("insert into t values (0, 'hello-world-0000')");
            markDurableBaseline();
            crashAndReopen();
            assertSyncDurable("t", "s", List.of("hello-world-0000"));
        });
    }

}
