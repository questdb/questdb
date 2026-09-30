/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/


package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * A failed sync on a cleanup path poisons the engine, but it must not skip the frees that follow it. In
 * production the halt callback stops the process inside the poison call. An embedded or test engine returns
 * from it, and a skipped free then leaks the writer's descriptors, mappings and lock on an engine that already
 * refuses writes. assertMemoryLeak is the cleanup bar: a stranded mapping shows as a non-zero tag balance, and
 * a stranded descriptor trips the fd tracking of {@link TestFilesFacadeImpl}.
 */
public class DataSyncFailureCleanupTest extends AbstractCairoTest {

    @Test
    public void testParquetToNativeReleasesDecoderWhenPartitionDirFsyncFails() throws Exception {
        Assume.assumeFalse("the native partition dir takes no fsync on Windows", Os.isWindows());
        // The partition dir fsync is skipped under NOSYNC, so pin a mode that takes it.
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        final FsyncAndCloseFailureFacade ff = new FsyncAndCloseFailureFacade("produceNativeFromParquet");
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 1), ('2024-01-02T00:00:00.000000Z', 2)");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");

            ff.isArmed = true;
            try {
                execute("ALTER TABLE t CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                Assert.fail("a failed partition dir fsync must stay fail-stop");
            } catch (CairoError e) {
                Assert.assertTrue(CairoException.isDataSyncFailure(e));
                Assert.assertTrue(engine.isDurabilityFailed());
            } finally {
                ff.isArmed = false;
                engine.releaseAllWriters();
                engine.resetDurabilityFailure();
            }
            Assert.assertEquals("the injected fsync failure must have been reached", 1, ff.failureCount);
        });
    }

    @Test
    public void testWalWriterOpenReleasesResourcesWhenSegmentDirFsyncFails() throws Exception {
        Assume.assumeFalse("the WAL segment dir takes no fsync on Windows", Os.isWindows());
        // The segment dir fsync is skipped under NOSYNC, so pin a mode that takes it.
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        final FsyncAndCloseFailureFacade ff = new FsyncAndCloseFailureFacade("openNewSegment");
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE w (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken tt = engine.verifyTableName("w");

            // The constructor opens the first segment, so the first armed segment fsync is the constructor's.
            ff.isArmed = true;
            try (WalWriter ignore = engine.getWalWriter(tt)) {
                Assert.fail("a failed segment dir fsync must stay fail-stop");
            } catch (CairoError e) {
                Assert.assertTrue(CairoException.isDataSyncFailure(e));
                Assert.assertTrue(engine.isDurabilityFailed());
            } finally {
                ff.isArmed = false;
                engine.resetDurabilityFailure();
            }
            Assert.assertEquals("the injected fsync failure must have been reached", 1, ff.failureCount);

            // The table still takes writes once the poison is cleared.
            execute("INSERT INTO w VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            assertQuery("SELECT count() FROM w")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1\n");
        });
    }

    private static boolean isCalledFrom(String methodName) {
        for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
            if (methodName.equals(frame.getMethodName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Fails the next {@code fsyncAndClose} called from {@code caller} once armed, with a genuine data-sync
     * failure, then behaves normally.
     */
    private static final class FsyncAndCloseFailureFacade extends TestFilesFacadeImpl {
        private final String caller;
        int failureCount;
        boolean isArmed;

        FsyncAndCloseFailureFacade(String caller) {
            this.caller = caller;
        }

        @Override
        public void fsyncAndClose(long fd) {
            if (isArmed && isCalledFrom(caller)) {
                isArmed = false;
                failureCount++;
                // FilesFacadeImpl closes the fd before it throws. So must this fake, or the leak is its own.
                super.close(fd);
                throw CairoException.dataSyncFailure(5, "fsyncAndClose").put("injected fsync failure");
            }
            super.fsyncAndClose(fd);
        }
    }
}
