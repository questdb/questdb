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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cutlass.text.ParallelCsvFileImporter;
import io.questdb.griffin.SqlException;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
    private final String commitMode;
    private final boolean newTable;

    public AdaptiveParallelCopyPowerLossTest(String commitMode, boolean newTable) {
        this.commitMode = commitMode;
        this.newTable = newTable;
    }

    @Parameterized.Parameters(name = "mode={0}, newTable={1}")
    public static Collection<Object[]> data() {
        final List<Object[]> params = new ArrayList<>();
        for (String mode : new String[]{"adaptive", "nosync", "async"}) {
            params.add(new Object[]{mode, true});
            params.add(new Object[]{mode, false});
        }
        return params;
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
            Assert.assertEquals(
                    """
                            ts\ts\tv
                            2024-02-01T00:00:00.000000Z\tc1\t101
                            2024-02-01T01:00:00.000000Z\tc2\t102
                            2024-02-02T00:00:00.000000Z\tc3\t103
                            2024-02-03T00:00:00.000000Z\tc4\t104
                            """,
                    imported
            );

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
