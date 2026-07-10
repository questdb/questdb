/*+*****************************************************************************
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

package io.questdb.test.cairo.o3;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

/**
 * Validates the donor-link zero-copy split mode: a mid-partition suffix child holds only a 16-byte
 * {@code _dlink} pointer (no column files) and reads its rows from the donor version dir, giving
 * results identical to the hardlink split path, and folds/reopens correctly.
 */
public class DonorLinkPartitionTest extends AbstractO3Test {

    @Before
    public void setUpDonorLink() {
        partitionTopEnabled = true;
        donorLinkSplitEnabled = true;
        // Keep mid-partition split pieces from being folded by the post-commit squash so the linked
        // child persists long enough to observe/read it.
        o3MidPartitionMaxSplits = 20;
        o3LastPartitionMaxSplits = 20;
    }

    @Test
    public void testMidSplitCreatesLinkFileAndReadsCorrectly() throws Exception {
        executeWithPool(
                0, (engine, compiler, executionContext, timestampTypeName) -> {
                    // x spans 2020-02-03T13 .. 2020-02-05T13 (two full mid partitions + a partial last).
                    engine.execute(
                            "create table x as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " -x j," +
                                    " rnd_str(5,16,2) as str," +
                                    " timestamp_sequence('2020-02-03T13', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(60*24*2)" +
                                    ") timestamp (ts) partition by DAY",
                            executionContext
                    );
                    // z lands inside the MID partition 2020-02-04 (not the last) -> its suffix child stays
                    // a non-active mid piece, so it is never opened for append and keeps its _dlink.
                    engine.execute(
                            "create table z as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " timestamp_sequence('2020-02-04T18:01', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(50))",
                            executionContext
                    );
                    engine.execute("create table y as (select * from x union all select * from z)", executionContext);
                    engine.execute("insert into x select * from z", executionContext);

                    // The mid suffix child holds only a _dlink pointer, no column files.
                    assertHasDonorLinkChild(engine, "x");

                    // Reading the linked child resolves the donor bytes: identical to the union.
                    TestUtils.assertEquals(compiler, executionContext, "y order by ts", "x");

                    // Reopen everything from disk (registry + reader redirect rebuilt from _txn + link files).
                    engine.releaseAllReaders();
                    engine.releaseAllWriters();
                    Assert.assertTrue("link child must survive reopen", hasDonorLinkChild(engine, "x"));
                    TestUtils.assertEquals(compiler, executionContext, "y order by ts", "x");

                    // Force-fold the split family; the linked source reads via the donor path. Data
                    // stays identical whether or not the pieces are physically folded.
                    engine.releaseAllReaders();
                    engine.execute("alter table x squash partitions", executionContext);
                    TestUtils.assertEquals(compiler, executionContext, "y order by ts", "x");
                    engine.releaseAllReaders();
                    engine.releaseAllWriters();
                    TestUtils.assertEquals(compiler, executionContext, "y order by ts", "x");
                }
        );
    }

    @Test
    public void testDropAbovePartitionsMaterializesLinkChildOnActivation() throws Exception {
        executeWithPool(
                0, (engine, compiler, executionContext, timestampTypeName) -> {
                    engine.execute(
                            "create table x as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " -x j," +
                                    " timestamp_sequence('2020-02-03T13', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(60*24*2)" +
                                    ") timestamp (ts) partition by DAY",
                            executionContext
                    );
                    engine.execute(
                            "create table z as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " timestamp_sequence('2020-02-04T18:01', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(50))",
                            executionContext
                    );
                    engine.execute("insert into x select * from z", executionContext);
                    assertHasDonorLinkChild(engine, "x");

                    // y mirrors x with only the 2020-02-04 partition retained (the mid link child + its donor).
                    engine.execute(
                            "create table y as (select * from x where ts in '2020-02-04') timestamp(ts) partition by DAY",
                            executionContext
                    );

                    // Drop the last partition so the link child's logical partition becomes the last.
                    engine.execute("alter table x drop partition list '2020-02-05'", executionContext);
                    engine.execute("alter table x drop partition list '2020-02-03'", executionContext);

                    // In-order append past the child materializes it (open for append).
                    engine.execute(
                            "create table w as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " -x j," +
                                    " timestamp_sequence('2020-02-04T23:30', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(20))",
                            executionContext
                    );
                    engine.execute("insert into y select * from w", executionContext);
                    engine.execute("insert into x select * from w", executionContext);

                    TestUtils.assertEquals(compiler, executionContext, "y order by ts", "x");
                    engine.releaseAllReaders();
                    engine.releaseAllWriters();
                    TestUtils.assertEquals(compiler, executionContext, "y order by ts", "x");
                }
        );
    }

    private static void assertHasDonorLinkChild(CairoEngine engine, String tableName) {
        final TableToken token = engine.verifyTableName(tableName);
        final File tableDir = new File(engine.getConfiguration().getDbRoot(), token.getDirName());
        final File[] parts = tableDir.listFiles();
        Assert.assertNotNull(parts);
        boolean found = false;
        for (File part : parts) {
            if (!part.isDirectory()) {
                continue;
            }
            final File dlink = new File(part, "_dlink");
            if (dlink.exists()) {
                found = true;
                Assert.assertEquals("_dlink must be 16 bytes", 16L, dlink.length());
                final File[] files = part.listFiles();
                Assert.assertNotNull(files);
                for (File f : files) {
                    final String n = f.getName();
                    Assert.assertFalse(
                            "a donor-link child dir must hold no column files, found: " + n,
                            n.endsWith(".d") || n.endsWith(".i") || n.endsWith(".k") || n.endsWith(".v")
                    );
                }
            }
        }
        Assert.assertTrue("expected a donor-link child (_dlink) in table " + tableName + "\n" + dumpTree(tableDir), found);
    }

    private static String dumpTree(File tableDir) {
        final StringBuilder sb = new StringBuilder("tree of ").append(tableDir).append(":\n");
        final File[] parts = tableDir.listFiles();
        if (parts != null) {
            for (File part : parts) {
                sb.append("  ").append(part.getName()).append(part.isDirectory() ? "/" : "").append('\n');
                if (part.isDirectory()) {
                    final File[] files = part.listFiles();
                    if (files != null) {
                        for (File f : files) {
                            sb.append("      ").append(f.getName()).append(" (").append(f.length()).append(")\n");
                        }
                    }
                }
            }
        }
        return sb.toString();
    }

    private static boolean hasDonorLinkChild(CairoEngine engine, String tableName) {
        final TableToken token = engine.verifyTableName(tableName);
        final File tableDir = new File(engine.getConfiguration().getDbRoot(), token.getDirName());
        final File[] parts = tableDir.listFiles();
        if (parts == null) {
            return false;
        }
        for (File part : parts) {
            if (part.isDirectory() && new File(part, "_dlink").exists()) {
                return true;
            }
        }
        return false;
    }
}
