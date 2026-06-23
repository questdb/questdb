package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import org.junit.Assert;
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

    @Test
    public void testForEachCrashPointDrivesWorkloadToCompletion() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        try {
            runWithCrashFacade(() -> {
                int trips = forEachCrashPoint(
                        () -> {
                            execute("drop table if exists fc");
                            execute("create table fc (ts timestamp, s string) timestamp(ts) partition by none");
                        },
                        () -> {
                            for (int i = 0; i < 3; i++) {
                                execute("insert into fc values (" + (i * 1_000_000L) + ", 'v" + i + "')");
                            }
                        },
                        () -> assertNoSilentCorruption("fc", "s", java.util.List.of("v0", "v1", "v2")),
                        64
                );
                Assert.assertTrue("expected at least one crash point to fire in SYNC mode (guards against SYNC degrading to NOSYNC)", trips >= 1);
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        }
    }
}
