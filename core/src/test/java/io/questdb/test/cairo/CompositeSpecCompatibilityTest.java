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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.CompositeDetachedArtifact;
import io.questdb.cairo.TableReader;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * {@code checkSpecCompatible}: the gate that must pass before a foreign artifact's manifest values may
 * be re-interned into a receiving table.
 * <p>
 * Equal VALUES are not sufficient. {@code "BTC"} under an {@code IDENTITY} dimension and under a
 * {@code TRUNCATE(3)} dimension are different cells, and a {@code HASH} bucket number is meaningless
 * without the same bucket count -- so the dimensions the values are OF must agree too.
 * <p>
 * Each refusal below is driven by a REAL artifact detached from a table whose spec differs in exactly
 * one way, so a rule that silently stopped firing would fail here rather than pass by omission. The
 * compatible control is what stops the whole class passing vacuously because nothing ever matches.
 */
public class CompositeSpecCompatibilityTest extends AbstractCairoTest {

    @Test(timeout = 120_000)
    public void testIdenticalSpecIsAccepted() throws Exception {
        assertMemoryLeak(() -> {
            create("src", "exch SYMBOL", "exch");
            create("dst", "exch SYMBOL", "exch");
            detach("src");
            assertCompatible("src", "dst");
        });
    }

    @Test(timeout = 120_000)
    public void testDifferentIdentityColumnNameIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            create("src", "exch SYMBOL", "exch");
            create("dst", "venue SYMBOL", "venue");
            detach("src");
            assertRefused("src", "dst", "source column differs");
        });
    }

    @Test(timeout = 120_000)
    public void testDifferentDimensionCountIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            create("src", "exch SYMBOL", "exch");
            execute("CREATE TABLE dst (ts TIMESTAMP, exch SYMBOL, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch, sym LAYOUT PLAIN WAL");
            detach("src");
            assertRefused("src", "dst", "dimension count differs");
        });
    }

    @Test(timeout = 120_000)
    public void testDifferentKindIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            create("src", "exch SYMBOL", "exch");
            execute("CREATE TABLE dst (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, truncate(exch, 2) LAYOUT PLAIN WAL");
            detach("src");
            assertRefused("src", "dst", "kind differs");
        });
    }

    @Test(timeout = 120_000)
    public void testDifferentTruncateParamIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, truncate(exch, 3) LAYOUT PLAIN WAL");
            seed("src");
            execute("CREATE TABLE dst (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, truncate(exch, 2) LAYOUT PLAIN WAL");
            detach("src");
            assertRefused("src", "dst", "param differs");
        });
    }

    private void create(String name, String dimCol, String dimExpr) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, " + dimCol + ", px DOUBLE) TIMESTAMP(ts) "
                + "PARTITION BY DAY, " + dimExpr + " LAYOUT PLAIN WAL");
        seed(name);
    }

    private void seed(String name) throws Exception {
        execute("INSERT INTO " + name + " SELECT"
                + " timestamp_sequence('2023-05-01T00:00:00.000000Z', 3600000000L) ts,"
                + " CASE WHEN x % 2 = 0 THEN 'BTC' ELSE 'ETH' END,"
                + " x::double"
                + " FROM long_sequence(4)");
        execute("INSERT INTO " + name + " SELECT"
                + " timestamp_sequence('2023-05-02T00:00:00.000000Z', 3600000000L) ts,"
                + " 'BTC', x::double"
                + " FROM long_sequence(1)");
        drainWalQueue();
        engine.releaseInactive();
    }

    private void detach(String name) throws Exception {
        execute("ALTER TABLE " + name + " DETACH PARTITION LIST '2023-05-01'");
        drainWalQueue();
    }

    private void assertCompatible(String srcTable, String dstTable) throws Exception {
        run(srcTable, dstTable);
    }

    private void assertRefused(String srcTable, String dstTable, String expect) throws Exception {
        try {
            run(srcTable, dstTable);
            Assert.fail("an incompatible spec must be refused: " + expect);
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expect);
        }
    }

    private void run(String srcTable, String dstTable) throws Exception {
        final java.nio.file.Path artifact = findArtifact(srcTable, "2023-05-01.detached");
        Assert.assertNotNull("artifact for " + srcTable + " not found", artifact);
        try (TableReader dst = engine.getReader(engine.verifyTableName(dstTable));
             Path p = new Path()) {
            p.of(artifact.toString());
            CompositeDetachedArtifact.checkSpecCompatible(
                    configuration.getFilesFacade(), configuration, p,
                    dst.getMetadata().getPartitionSpec(), dst.getMetadata(), dstTable);
        }
    }

    private static java.nio.file.Path findArtifact(String table, String child) throws Exception {
        final java.nio.file.Path root = Paths.get(configuration.getDbRoot());
        try (Stream<java.nio.file.Path> walk = Files.walk(root, 2)) {
            for (java.nio.file.Path p : walk.filter(Files::isDirectory).toList()) {
                if (p.getFileName().toString().equals(child)
                        && p.getParent().getFileName().toString().startsWith(table + "~")) {
                    return p;
                }
            }
        }
        return null;
    }
}
