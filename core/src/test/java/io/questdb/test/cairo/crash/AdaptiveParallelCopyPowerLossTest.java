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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cutlass.text.CopyImportTask;
import io.questdb.cutlass.text.ParallelCsvFileImporter;
import io.questdb.griffin.SqlException;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A parallel COPY that reported success on an adaptive instance must survive a power loss, into a new table as
 * well as into an existing empty one. The importer writes every partition outside the WAL, so nothing can redo
 * it after the crash: the import must be durable before COPY reports it done. Nothing is written back by the
 * kernel here: only the product's own barriers make bytes durable. Outside ADAPTIVE the import promises nothing
 * new, and the importer makes no extra flush there.
 */
@RunWith(Parameterized.class)
public class AdaptiveParallelCopyPowerLossTest extends AbstractAdaptiveCrashTest {
    private static final String CSV = """
            ts,s,v
            2024-02-01T00:00:00.000000Z,c1,101
            2024-02-01T01:00:00.000000Z,c2,102
            2024-02-02T00:00:00.000000Z,c3,103
            2024-02-03T00:00:00.000000Z,c4,104
            """;
    private static final String IMPORTED = """
            ts\ts\tv
            2024-02-01T00:00:00.000000Z\tc1\t101
            2024-02-01T01:00:00.000000Z\tc2\t102
            2024-02-02T00:00:00.000000Z\tc3\t103
            2024-02-03T00:00:00.000000Z\tc4\t104
            """;
    private final String commitMode;
    private final boolean newTable;
    private final boolean syncfsFileSystemWide;

    public AdaptiveParallelCopyPowerLossTest(String commitMode, boolean newTable, boolean syncfsFileSystemWide) {
        this.commitMode = commitMode;
        this.newTable = newTable;
        this.syncfsFileSystemWide = syncfsFileSystemWide;
    }

    @Parameterized.Parameters(name = "mode={0}, newTable={1}, syncfsFileSystemWide={2}")
    public static Collection<Object[]> data() {
        final List<Object[]> params = new ArrayList<>();
        for (String mode : new String[]{"adaptive", "nosync", "async"}) {
            for (boolean syncfsFileSystemWide : new boolean[]{true, false}) {
                params.add(new Object[]{mode, true, syncfsFileSystemWide});
                params.add(new Object[]{mode, false, syncfsFileSystemWide});
            }
        }
        return params;
    }

    @Test
    public void testFailedDeviceFlushStopsTheEngine() throws Exception {
        // syncfs reports a write-back error of any file on the filesystem, so a failed device flush may belong to
        // another table: in adaptive commit mode the engine must stop, whichever flush that makes the imported
        // partitions live and durable fails.
        Assume.assumeTrue("the import flushes nothing outside adaptive", "adaptive".equals(commitMode));
        Assume.assumeTrue("an existing table is flushed by the writer's save point, which stops the engine itself", newTable);
        assumeCrashHarnessSupported();
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, "true");
        inputRoot = temp.newFolder().getAbsolutePath();
        inputWorkRoot = temp.newFolder().getAbsolutePath();
        Files.writeString(java.nio.file.Path.of(inputRoot, "t.csv"), CSV, StandardCharsets.UTF_8);
        final AtomicBoolean attaching = new AtomicBoolean();
        final AtomicInteger failAt = new AtomicInteger();
        final AtomicInteger flushes = new AtomicInteger();
        final StringSink failedFlush = new StringSink();
        crashFf = new CrashFaultFilesFacade() {
            @Override
            public void barrierFsync(long fd) {
                failIfDue("barrierFsync", fd);
                super.barrierFsync(fd);
            }

            @Override
            public void fdatasync(long fd) {
                failIfDue("fdatasync", fd);
                super.fdatasync(fd);
            }

            @Override
            public void fsync(long fd) {
                failIfDue("fsync", fd);
                super.fsync(fd);
            }

            @Override
            public void fsyncAndClose(long fd) {
                try {
                    failIfDue("fsyncAndClose", fd);
                } catch (CairoException e) {
                    close(fd);
                    throw e;
                }
                super.fsyncAndClose(fd);
            }

            @Override
            public void fsyncDurable(long fd) {
                failIfDue("fsyncDurable", fd);
                super.fsyncDurable(fd);
            }

            @Override
            public boolean isSyncfsFileSystemWide() {
                return syncfsFileSystemWide;
            }

            @Override
            public void msync(long addr, long len, boolean async) {
                failIfDue("msync", -1);
                super.msync(addr, len, async);
            }

            @Override
            public void syncfs(long fd) {
                failIfDue("syncfs", fd);
                super.syncfs(fd);
            }

            private void failIfDue(String operation, long fd) {
                if (attaching.get() && failAt.get() > 0 && flushes.incrementAndGet() == failAt.get()) {
                    failedFlush.clear();
                    failedFlush.put(operation).put(' ').put(fd > -1 ? pathOf(fd) : "");
                    throw CairoException.dataSyncFailure(5, operation).put("injected [operation=").put(operation).put(']');
                }
            }
        };
        crashFf.setDbRoot(root);
        assertMemoryLeak(crashFf, () -> {
            execute("CREATE TABLE other (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            // Fail the first device flush from the attach on, then the second, and so on, until an import completes
            // without reaching the failing one.
            for (int i = 1; ; i++) {
                attaching.set(false);
                flushes.set(0);
                failAt.set(i);
                Throwable failure = null;
                try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 1)) {
                    importer.of("t", "t.csv", 1, PartitionBy.DAY, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                    importer.setStatusReporter((phase, status, msg, rowsHandled, rowsImported, errors) -> {
                        if (phase == CopyImportTask.PHASE_ATTACH_PARTITIONS && status == CopyImportTask.STATUS_STARTED) {
                            attaching.set(true);
                        }
                    });
                    importer.process(AllowAllSecurityContext.INSTANCE);
                } catch (Throwable th) {
                    failure = th;
                }
                failAt.set(0);
                if (flushes.get() < i) {
                    Assert.assertNull("round " + i, failure);
                    Assert.assertTrue("no device flush from the attach on", i > 1);
                    break;
                }
                final String context = "failed flush " + i + " [" + failedFlush + ']';
                Assert.assertNotNull(context, failure);
                Assert.assertTrue(context, engine.isDurabilityFailed());
                try {
                    execute("INSERT INTO other VALUES ('2024-01-01', 1)");
                    Assert.fail(context + ": a write was accepted after the engine stopped");
                } catch (CairoException | CairoError expected) {
                    // The engine refuses writes.
                } finally {
                    engine.resetDurabilityFailure();
                }
                Assert.assertEquals(context + ": the created table is dropped", "", rows(engine));
            }
        });
    }

    @Test
    public void testImportSurvivesPowerLoss() throws Exception {
        assumeCrashHarnessSupported();
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, "true");
        // A power loss takes no close-time save point.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        inputRoot = temp.newFolder().getAbsolutePath();
        inputWorkRoot = temp.newFolder().getAbsolutePath();
        Files.writeString(java.nio.file.Path.of(inputRoot, "t.csv"), CSV, StandardCharsets.UTF_8);
        final AtomicInteger syncfsCalls = new AtomicInteger();
        crashFf = new CrashFaultFilesFacade() {
            @Override
            public boolean isSyncfsFileSystemWide() {
                return syncfsFileSystemWide;
            }

            @Override
            public void syncfs(long fd) {
                syncfsCalls.incrementAndGet();
                super.syncfs(fd);
            }
        };
        crashFf.setDbRoot(root);
        assertMemoryLeak(crashFf, () -> {
            if (!newTable) {
                execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                drainWalQueue();
            }
            markDurableBaseline();
            syncfsCalls.set(0);
            try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 1)) {
                importer.of("t", "t.csv", 1, PartitionBy.DAY, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                importer.process(AllowAllSecurityContext.INSTANCE);
            }
            final int importSyncfsCalls = syncfsCalls.get();
            final String imported = rows(engine);
            Assert.assertEquals(IMPORTED, imported);

            engine.releaseAllReaders();
            engine.releaseAllWriters();
            engine.releaseAllWalWriters();
            engine.releaseInactive();
            crashFf.crash(root);
            try (CairoEngine restarted = new CairoEngine(configuration)) {
                TestUtils.drainWalQueue(restarted);
                final String recovered = rows(restarted);
                if ("adaptive".equals(commitMode)) {
                    Assert.assertEquals("a finished COPY must survive a power loss", imported, recovered);
                } else {
                    // No promise: the rows that come back are some of the imported ones.
                    for (String line : recovered.split("\n")) {
                        Assert.assertTrue("unexpected row after the crash: " + line, imported.contains(line));
                    }
                    Assert.assertEquals("no flush outside adaptive", 0, importSyncfsCalls);
                }
            }
        });
    }

    private static String rows(CairoEngine e) {
        final StringSink sink = new StringSink();
        try {
            e.print("SELECT * FROM t", sink);
        } catch (SqlException | CairoException ex) {
            // The table did not survive.
            return "";
        }
        return sink.toString();
    }
}
