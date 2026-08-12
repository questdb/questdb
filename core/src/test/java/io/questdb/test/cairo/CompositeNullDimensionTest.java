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

package io.questdb.test.cairo;

import io.questdb.cairo.TableToken;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * A NULL value in a composite partition dimension is ORDINARY DATA, exactly as it is on the
 * equivalent plain (day-only) twin: the row must be stored, routed to its own dedicated cell, and
 * returned by every query the plain twin returns it from.
 * <p>
 * Before the fix this hung WAL apply <b>forever</b> (not a crash -- an indefinite spin inside
 * {@code TableWriter#o3ConsumePartitionUpdates}). Every test here is therefore
 * {@code @Test(timeout = 30_000)} so a regression FAILS CI rather than wedging it: a plain
 * assertion-based test would never even get to its assertions.
 * <p>
 * The NULL cell's on-disk segment token is {@code %NULL}. It is injective against every possible
 * real SYMBOL value because {@link io.questdb.cairo.TableUtils#putPathSafe} escapes a literal
 * {@code '%'} to {@code "%25"} -- so no real value can ever render to a name containing a BARE
 * {@code '%'}. {@link #testLiteralPercentNullValueDoesNotCollideWithNullCell} nails that down
 * empirically.
 */
public class CompositeNullDimensionTest extends AbstractCairoTest {

    /**
     * The original minimal repro: NULL interleaved with non-null in ONE commit. Pre-fix this timed
     * out at 30s inside {@code drainWalQueue()}; the identical all-non-null shape completed in 53ms.
     */
    @Test(timeout = 30_000)
    public void testNullMixedWithNonNullInOneCommitMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBothAndDrain(
                    "('2023-01-01T00:00:00.000000Z','BTC',1.0)," +
                            "('2023-01-01T00:00:01.000000Z',NULL,2.0)," +
                            "('2023-01-01T00:00:02.000000Z','BTC',3.0)," +
                            "('2023-01-01T00:00:03.000000Z',NULL,4.0)");

            assertWalTableNotSuspended("c");
            assertWalTableNotSuspended("p");
            engine.releaseInactive();

            assertTwinEquivalence();

            // NULL routes to its OWN cell, distinct from every non-null value's cell.
            TableToken tableToken = engine.verifyTableName("c");
            FilesFacade ff = configuration.getFilesFacade();
            Assert.assertEquals(setOf("exch=BTC", "exch=%NULL"), listCellDirNames(ff, tableToken, "2023-01-01"));

            assertQuery("select count() from table_partitions('c')").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
        });
    }

    /**
     * Every row in the commit is NULL -- the single-cellKey ("no regrouping needed") dispatch path,
     * where the ONE cell is the NULL cell.
     */
    @Test(timeout = 30_000)
    public void testAllNullCommitMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBothAndDrain(
                    "('2023-01-01T00:00:00.000000Z',NULL,1.0)," +
                            "('2023-01-01T00:00:01.000000Z',NULL,2.0)," +
                            "('2023-01-01T00:00:02.000000Z',NULL,3.0)");

            assertWalTableNotSuspended("c");
            engine.releaseInactive();

            assertTwinEquivalence();

            TableToken tableToken = engine.verifyTableName("c");
            FilesFacade ff = configuration.getFilesFacade();
            Assert.assertEquals(setOf("exch=%NULL"), listCellDirNames(ff, tableToken, "2023-01-01"));
        });
    }

    /**
     * NULL arrives in a LATER commit than the non-null rows -- i.e. the NULL cell is created against
     * a day that already exists and already has a populated sibling cell, and then extended by a
     * third commit (the NULL cell itself is the one being re-opened).
     */
    @Test(timeout = 30_000)
    public void testNullInLaterCommitMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBothAndDrain(
                    "('2023-01-01T00:00:00.000000Z','BTC',1.0)," +
                            "('2023-01-01T00:00:01.000000Z','ETH',2.0)");
            assertWalTableNotSuspended("c");

            insertIntoBothAndDrain("('2023-01-01T00:00:02.000000Z',NULL,3.0)");
            assertWalTableNotSuspended("c");

            // and again -- extend the now already-populated NULL cell.
            insertIntoBothAndDrain("('2023-01-01T00:00:03.000000Z',NULL,4.0)");
            assertWalTableNotSuspended("c");

            engine.releaseInactive();
            assertTwinEquivalence();

            TableToken tableToken = engine.verifyTableName("c");
            FilesFacade ff = configuration.getFilesFacade();
            Assert.assertEquals(setOf("exch=BTC", "exch=ETH", "exch=%NULL"), listCellDirNames(ff, tableToken, "2023-01-01"));
        });
    }

    /**
     * TWO dimensions, only ONE of them NULL on a given row: proves the NULL token is a per-DIMENSION
     * segment, not a whole-cell one, and that a partially-NULL tuple still interns to its own
     * distinct cell.
     */
    @Test(timeout = 30_000)
    public void testTwoDimensionsOnlyOneNullMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, side symbol, px double) timestamp(ts) partition by day, exch, side wal");
            execute("create table p (ts timestamp, exch symbol, side symbol, px double) timestamp(ts) partition by day wal");

            final String rows = " values " +
                    "('2023-01-01T00:00:00.000000Z','BTC','BUY',1.0)," +
                    "('2023-01-01T00:00:01.000000Z',NULL,'BUY',2.0)," +
                    "('2023-01-01T00:00:02.000000Z','BTC',NULL,3.0)," +
                    "('2023-01-01T00:00:03.000000Z',NULL,NULL,4.0)";
            execute("insert into c" + rows);
            execute("insert into p" + rows);
            drainWalQueue();

            assertWalTableNotSuspended("c");
            assertWalTableNotSuspended("p");
            engine.releaseInactive();

            assertSqlCursors(
                    "select ts, exch, side, px from p order by ts",
                    "select ts, exch, side, px from c order by ts");
            assertSqlCursors(
                    "select ts, exch, side, px from p where exch is null order by ts",
                    "select ts, exch, side, px from c where exch is null order by ts");
            assertSqlCursors(
                    "select ts, exch, side, px from p where side is null order by ts",
                    "select ts, exch, side, px from c where side is null order by ts");
            assertSqlCursors(
                    "select ts, exch, side, px from p where exch = 'BTC' order by ts",
                    "select ts, exch, side, px from c where exch = 'BTC' order by ts");
            assertSqlCursors("select count() from p", "select count() from c");

            // Four distinct (exch, side) tuples -> four distinct nested cells, each rendering the
            // NULL token per-dimension.
            TableToken tableToken = engine.verifyTableName("c");
            FilesFacade ff = configuration.getFilesFacade();
            Assert.assertEquals(
                    setOf("exch=BTC", "exch=%NULL"),
                    listCellDirNames(ff, tableToken, "2023-01-01"));
            Assert.assertEquals(
                    setOf("side=BUY", "side=%NULL"),
                    listCellDirNames(ff, tableToken, "2023-01-01" + Files.SEPARATOR + "exch=BTC"));
            Assert.assertEquals(
                    setOf("side=BUY", "side=%NULL"),
                    listCellDirNames(ff, tableToken, "2023-01-01" + Files.SEPARATOR + "exch=%NULL"));
        });
    }

    /**
     * Directory naming, PLAIN layout: the bare token, with no {@code <col>=} prefix.
     */
    @Test(timeout = 30_000)
    public void testNullCellDirectoryNamePlainLayout() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch layout plain wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            insertIntoBothAndDrain(
                    "('2023-01-01T00:00:00.000000Z','BTC',1.0)," +
                            "('2023-01-01T00:00:01.000000Z',NULL,2.0)");

            assertWalTableNotSuspended("c");
            engine.releaseInactive();

            assertTwinEquivalence();

            TableToken tableToken = engine.verifyTableName("c");
            FilesFacade ff = configuration.getFilesFacade();
            Assert.assertEquals(setOf("BTC", "%NULL"), listCellDirNames(ff, tableToken, "2023-01-01"));
        });
    }

    /**
     * Injectivity of the reserved token: a table containing the LITERAL symbol value {@code %NULL}
     * alongside a genuine NULL must produce TWO distinct cells. {@code putPathSafe} escapes the
     * literal's {@code '%'} to {@code "%25"}, so the literal renders {@code exch=%25NULL} and can
     * never be confused with the reserved {@code exch=%NULL}.
     */
    @Test(timeout = 30_000)
    public void testLiteralPercentNullValueDoesNotCollideWithNullCell() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBothAndDrain(
                    "('2023-01-01T00:00:00.000000Z','%NULL',1.0)," +
                            "('2023-01-01T00:00:01.000000Z',NULL,2.0)," +
                            "('2023-01-01T00:00:02.000000Z','%NULL',3.0)");

            assertWalTableNotSuspended("c");
            engine.releaseInactive();

            assertTwinEquivalence();

            TableToken tableToken = engine.verifyTableName("c");
            FilesFacade ff = configuration.getFilesFacade();
            Assert.assertEquals(setOf("exch=%25NULL", "exch=%NULL"), listCellDirNames(ff, tableToken, "2023-01-01"));

            // The two rows must NOT have merged into one cell -- literal and NULL are separate.
            assertSqlCursors(
                    "select ts, exch, px from p where exch = '%NULL' order by ts",
                    "select ts, exch, px from c where exch = '%NULL' order by ts");
            assertQuery("select count() from c where exch = '%NULL'").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
            assertQuery("select count() from c where exch is null").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
        });
    }

    private void assertTwinEquivalence() throws SqlException {
        assertSqlCursors("select ts, exch, px from p order by ts, exch", "select ts, exch, px from c order by ts, exch");
        assertSqlCursors("select count() from p", "select count() from c");
        assertSqlCursors("select ts, exch, px from p where exch is null order by ts", "select ts, exch, px from c where exch is null order by ts");
        // QuestDB's `sym = null` is a real NULL match (not standard SQL's always-false): it is also the
        // shape that reaches IDENTITY cell pruning (SqlCodeGenerator#resolveDimensionCellPruneSet) with
        // a VALUE_IS_NULL ordinal and drops the predicate as row-exact -- so it must match the twin.
        assertSqlCursors("select ts, exch, px from p where exch = null order by ts", "select ts, exch, px from c where exch = null order by ts");
        assertSqlCursors("select ts, exch, px from p where exch != null order by ts", "select ts, exch, px from c where exch != null order by ts");
        assertSqlCursors("select ts, exch, px from p where exch = 'BTC' order by ts", "select ts, exch, px from c where exch = 'BTC' order by ts");
        assertSqlCursors("select exch, count() from p order by exch", "select exch, count() from c order by exch");
        assertSqlCursors(
                "select ts, exch, px from p latest on ts partition by exch order by exch",
                "select ts, exch, px from c latest on ts partition by exch order by exch");
    }

    private void assertWalTableNotSuspended(String tableName) {
        Assert.assertFalse(
                tableName + " must not be suspended",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName)));
    }

    private void createTwins() throws SqlException {
        execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
        execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");
    }

    private void insertIntoBothAndDrain(String valuesTuples) throws SqlException {
        execute("insert into c values " + valuesTuples);
        execute("insert into p values " + valuesTuples);
        drainWalQueue();
    }

    /**
     * Lists the immediate child directory names of {@code <dbRoot>/<tableToken>/<relDirPath>},
     * stripping each entry's trailing {@code .<nameTxn>} version suffix. Mirrors {@code
     * CompositeRoutingEndToEndTest}'s identically-named helper.
     */
    private static Set<String> listCellDirNames(FilesFacade ff, TableToken tableToken, String relDirPath) {
        Set<String> names = new HashSet<>();
        try (Path path = new Path()) {
            path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(relDirPath).$();
            long pFind = ff.findFirst(path.$());
            Assert.assertTrue("expected directory to exist: " + path, pFind > 0L);
            try {
                StringSink nameSink = new StringSink();
                do {
                    nameSink.clear();
                    long name = ff.findName(pFind);
                    Utf8s.utf8ToUtf16Z(name, nameSink);
                    int type = ff.findType(pFind);
                    if (type == Files.DT_DIR && !Chars.equals(nameSink, ".") && !Chars.equals(nameSink, "..")) {
                        String entry = nameSink.toString();
                        int dot = entry.lastIndexOf('.');
                        names.add(dot > -1 ? entry.substring(0, dot) : entry);
                    }
                } while (ff.findNext(pFind) > 0);
            } finally {
                ff.findClose(pFind);
            }
        }
        return names;
    }

    private static Set<String> setOf(String... values) {
        Set<String> set = new HashSet<>();
        for (String v : values) {
            set.add(v);
        }
        return set;
    }
}
