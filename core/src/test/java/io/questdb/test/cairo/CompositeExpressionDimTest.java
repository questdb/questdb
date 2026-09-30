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

import io.questdb.cairo.PartitionDimension;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.CreateTableOperationBuilderImpl;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Composite partitioning, Plan 4e: SQL grammar for {@code (expr) AS alias} composite dimensions
 * (e.g. {@code partition by day, (upper(region)) AS r}), the DDL-time safe-subset / string-
 * coercibility gate on {@code CreateTableOperationBuilderImpl#resolveExpressionDimension} (Task 1),
 * and the per-row {@code Function}-eval bridge that turns Task 1's clean-throw placeholder into
 * real ingest-time evaluation (Task 2) -- {@code TableWriter#resolveExpressionDimensionOrdinal}/
 * {@code #ensureCompositeExpressionFunctionsCompiled}, plus the write- and read-side cell-segment
 * reverse-render this required to make INSERT/SELECT actually complete (the same "byte-identical to
 * TRUNCATE" reverse lookup the overall plan describes for Task 3, landed here because Task 2's
 * eval bridge has no observable effect without it -- see {@code TableWriter#renderDimensionSegment}
 * and {@code TableReader#renderCellSegment}/{@code valueOfDimensionKey}).
 * <p>
 * Still NOT supported after Task 2 (a deliberate, bounded scope -- see {@code
 * TableWriter#isSupportedExpressionSourceColumnType}): an EXPRESSION referencing a var-size
 * (VARCHAR/STRING/BINARY) source column, or an exotic fixed type (GEOHASH, LONG256, LONG128, UUID,
 * DECIMAL, INTERVAL, ARRAY) -- {@link #testInsertOnVarSizeSourceColumnThrowsCleanErrorNotAioobe()}
 * locks in that this remaining gap still fails CLEAN, never with an uncontrolled crash. Also still
 * not supported: {@code TableReader#keyOfDimensionValue}'s EXPRESSION case (forward value-&gt;key
 * lookup, used only by partition-pruning optimizations that do not yet apply to any composite
 * dimension kind) -- unreached by ordinary SELECT/filter queries, left for Task 3 proper.
 */
public class CompositeExpressionDimTest extends AbstractCairoTest {

    @Test
    public void testInsertEvaluatesConcatOfTwoSymbolColumnsAndRoutes() throws Exception {
        // Second case (task brief: "a HASH-of-expr or multi-token expr as a second case"): a
        // multi-token expression over TWO symbol columns via the concatenation operator, not just a
        // single-function-of-a-single-column -- proves the Function-eval bridge composes correctly
        // (concat(...)'s own StrFunction-family arg evaluation, each arg itself a compiled
        // SymbolColumn/SymbolFunction leaf reading a DIFFERENT column index off the same row).
        assertMemoryLeak(() -> {
            execute("create table c2 (ts timestamp, region symbol, cls symbol, x double) timestamp(ts) " +
                    "partition by day, (region || '_' || cls) AS r wal");
            execute("insert into c2 values " +
                    "('2020-01-01T00:00:00.000000Z','us','a',1.0), " +
                    "('2020-01-01T01:00:00.000000Z','us','a',2.0), " +
                    "('2020-01-01T02:00:00.000000Z','eu','b',3.0)");
            drainWalQueue();

            assertWalTableNotSuspended("c2");
            engine.releaseInactive();

            TableToken tableToken = engine.verifyTableName("c2");
            FilesFacade ff = configuration.getFilesFacade();
            Assert.assertEquals(setOf("r=us_a", "r=eu_b"), listCellDirNames(ff, tableToken, "2020-01-01"));

            assertQuery("select count() from c2").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
            assertQuery("select count() from c2 where region || '_' || cls = 'us_a'")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
            assertQuery("select count() from c2 where region || '_' || cls = 'eu_b'")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
            assertQuery("select name from table_partitions('c2') order by name").noLeakCheck().expectSize().returns(
                    "name\n2020-01-01/r=eu_b\n2020-01-01/r=us_a\n");
        });
    }

    /**
     * Main acceptance case (task brief): {@code partition by day, (upper(region)) AS r}; inserting
     * region {@code us}/{@code US}/{@code eu}/{@code Eu} must evaluate {@code upper()} per row and
     * route {@code us}/{@code US} to ONE cell ({@code US}) and {@code eu}/{@code Eu} to a SECOND,
     * distinct cell ({@code EU}) -- 2 physical cell directories, not 4. Verified against an
     * equivalent plain table {@code p} with {@code r} as a real, precomputed {@code varchar} column
     * populated by the SAME {@code upper(region)} expression client-side: full ordered scan and
     * per-derived-value filters must match exactly.
     */
    @Test
    public void testInsertEvaluatesUpperExpressionAndRoutesMatchingPrecomputedTwin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, region symbol, x double) timestamp(ts) " +
                    "partition by day, (upper(region)) AS r wal");
            execute("create table p (ts timestamp, region symbol, x double, r varchar) timestamp(ts) " +
                    "partition by day wal");

            execute("insert into c values " +
                    "('2020-01-01T00:00:00.000000Z','us',1.0), " +
                    "('2020-01-01T01:00:00.000000Z','US',2.0), " +
                    "('2020-01-01T02:00:00.000000Z','eu',3.0), " +
                    "('2020-01-01T03:00:00.000000Z','Eu',4.0)");
            execute("insert into p values " +
                    "('2020-01-01T00:00:00.000000Z','us',1.0,'US'), " +
                    "('2020-01-01T01:00:00.000000Z','US',2.0,'US'), " +
                    "('2020-01-01T02:00:00.000000Z','eu',3.0,'EU'), " +
                    "('2020-01-01T03:00:00.000000Z','Eu',4.0,'EU')");
            drainWalQueue();

            assertWalTableNotSuspended("c");
            assertWalTableNotSuspended("p");
            engine.releaseInactive(); // cold reopen -- no pooled reader/writer may mask a fresh self-detect

            // PHYSICAL: exactly 2 cell directories (US, EU) under the single day -- not 4, proving
            // us/US and eu/Eu each collapsed into ONE cell via real per-row upper() evaluation, not
            // one cell per distinct raw region value.
            TableToken tableToken = engine.verifyTableName("c");
            FilesFacade ff = configuration.getFilesFacade();
            Assert.assertEquals(setOf("r=US", "r=EU"), listCellDirNames(ff, tableToken, "2020-01-01"));

            // CATALOGUE: table_partitions() shows the cell names, Hive-rendered via the dimension's
            // alias ("r"), not a source column name (EXPRESSION has none).
            assertQuery("select count() from table_partitions('c')").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
            assertQuery("select name from table_partitions('c') order by name").noLeakCheck().expectSize().returns(
                    "name\n2020-01-01/r=EU\n2020-01-01/r=US\n");

            // LOGICAL: full ordered scan and table-wide count match the precomputed-column twin.
            assertSqlCursors(
                    "select ts, region, x from p order by ts",
                    "select ts, region, x from c order by ts");
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n4\n");

            // Per-derived-value filters (finer than the table-wide count(), catches a cross-cell
            // swap): c's filter re-derives the same expression over the real `region` column (there
            // is no queryable `r` column on c -- the alias is a routing label, not a materialized
            // column), p's filter reads its real, precomputed `r` column directly.
            assertSqlCursors(
                    "select ts, region, x from p where r = 'US' order by ts",
                    "select ts, region, x from c where upper(region) = 'US' order by ts");
            assertSqlCursors(
                    "select ts, region, x from p where r = 'EU' order by ts",
                    "select ts, region, x from c where upper(region) = 'EU' order by ts");
            assertQuery("select count() from c where upper(region) = 'US'").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
            assertQuery("select count() from c where upper(region) = 'EU'").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");

            // FRESH-JVM proxy: a second cold reopen (no pooled reader/writer state survives) still
            // reads back correctly -- not just the first post-drain query.
            engine.releaseInactive();
            assertSqlCursors(
                    "select ts, region, x from p order by ts",
                    "select ts, region, x from c order by ts");
        });
    }

    /**
     * The remaining bounded gap after Task 2 (composite-partitioning Plan 4e): an EXPRESSION
     * dimension referencing a var-size (VARCHAR/STRING/BINARY) source column -- {@code
     * TableWriter#isSupportedExpressionSourceColumnType} does not (yet) allow the {@link
     * CompositeExpressionRecord} adapter to expose one. Renamed/repurposed from the original Task 1
     * AIOOBE-safety test (which asserted the OLD, now-superseded behaviour that EVERY EXPRESSION
     * insert failed): the canonical {@code upper(region)}-over-SYMBOL case now succeeds (see {@link
     * #testInsertEvaluatesUpperExpressionAndRoutesMatchingPrecomputedTwin()}), but this narrower,
     * still-unsupported shape must keep failing CLEAN -- a diagnosable {@code CairoException}, never
     * an uncontrolled {@code ArrayIndexOutOfBoundsException} -- exactly the guarantee Task 1
     * established and Task 2 must not regress for the cases it does not yet cover.
     */
    @Test
    public void testInsertOnVarSizeSourceColumnThrowsCleanErrorNotAioobe() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, region symbol, note varchar, x double) timestamp(ts) " +
                    "partition by day, (upper(note)) AS r wal");
            execute("insert into c values ('2020-01-01T00:00:00.000000Z', 'us', 'hello', 1.0)");
            drainWalQueue();

            Assert.assertTrue(
                    "table must be suspended by the clean var-size-source-column guard, not crash unnoticed",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c"))
            );
            assertQuery(
                    "select suspended, " +
                            "errorMessage like '%composite partitioning does not yet support an EXPRESSION dimension referencing column%' clearMessage, " +
                            "errorMessage like '%ArrayIndexOutOfBoundsException%' isAioobe " +
                            "from wal_tables() where name = 'c'"
            )
                    .noLeakCheck().noRandomAccess()
                    .returns("suspended\tclearMessage\tisAioobe\ntrue\ttrue\tfalse\n");
        });
    }

    @Test
    public void testNonDeterministicExpressionRejected() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("create table c (ts timestamp, region symbol, x double) timestamp(ts) " +
                        "partition by day, (region || rnd_str()) AS r wal");
                Assert.fail("expected a nondeterministic partition dimension expression to be rejected");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "deterministic");
            }

            // now()/sysdate()/timestamp_sequence() etc. are equally rejected, not just the rnd_* family
            // -- the exact-name half of the deny-list, not just the rnd_ prefix check.
            try {
                execute("create table c2 (ts timestamp, region symbol, x double) timestamp(ts) " +
                        "partition by day, (now()) AS r wal");
                Assert.fail("expected now() in a partition dimension expression to be rejected");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "deterministic");
            }

            Assert.assertNull("rejected CREATE must not leave a table behind", engine.getTableTokenIfExists("c"));
            Assert.assertNull("rejected CREATE must not leave a table behind", engine.getTableTokenIfExists("c2"));
        });
    }

    @Test
    public void testNonStringExpressionWithoutCastRejected() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("create table c (ts timestamp, region symbol, x double) timestamp(ts) " +
                        "partition by day, (x) AS r wal");
                Assert.fail("expected a non-string partition dimension expression to be rejected");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "string-coercible");
            }
            Assert.assertNull("rejected CREATE must not leave a table behind", engine.getTableTokenIfExists("c"));
        });
    }

    /**
     * Parser-only introspection (mirrors {@code CompositePartitionParseTest#compileCreateTableModel}):
     * proves the grammar itself captures the expression node and its alias, isolated from the
     * resolve-time safe-subset gate and table creation.
     */
    @Test
    public void testParserCapturesAsAlias() throws Exception {
        assertMemoryLeak(() -> {
            final String sql = "create table c (ts timestamp, region symbol, x double) timestamp(ts) " +
                    "partition by day, (upper(region)) AS r wal";
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                ExecutionModel model = compiler.generateExecutionModel(sql, sqlExecutionContext);
                Assert.assertEquals(ExecutionModel.CREATE_TABLE, model.getModelType());
                CreateTableOperationBuilderImpl builder = (CreateTableOperationBuilderImpl) model;
                Assert.assertEquals(1, builder.getPartitionDimensionExprCount());
                TestUtils.assertEquals("upper", builder.getPartitionDimensionExpr(0).token);
                TestUtils.assertEquals("r", builder.getPartitionDimensionAlias(0));
            }
        });
    }

    @Test
    public void testPersistsExpressionDimensionAcrossReopen() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, region symbol, x double) timestamp(ts) " +
                    "partition by day, (upper(region)) AS r wal");
            engine.releaseInactive(); // force re-read of _meta from disk

            try (TableMetadata m = engine.getTableMetadata(engine.verifyTableName("c"))) {
                Assert.assertTrue(m.getPartitionSpec().isComposite());
                Assert.assertEquals(1, m.getPartitionSpec().getDimensionCount());
                PartitionDimension dim = m.getPartitionSpec().getDimension(0);
                Assert.assertEquals(PartitionDimension.KIND_EXPRESSION, dim.getKind());
                Assert.assertEquals(-1, dim.getColumnIndex());
                Assert.assertEquals(0, dim.getParam());
                Assert.assertEquals("r", dim.getAlias());
                Assert.assertEquals("upper(region)", dim.getExprText());
            }
        });
    }

    @Test
    public void testPlainIdentityHashTruncateStillWorkUnaliased() throws Exception {
        // Regression: the AS-alias capture must be a true no-op for the pre-existing IDENTITY/HASH/
        // TRUNCATE grammar (no AS present at all).
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exchange symbol, sym symbol, x double) timestamp(ts) " +
                    "partition by day, exchange, hash(sym, 16), truncate(sym, 3) wal");
            engine.releaseInactive();

            try (TableMetadata m = engine.getTableMetadata(engine.verifyTableName("c"))) {
                Assert.assertTrue(m.getPartitionSpec().isComposite());
                Assert.assertEquals(3, m.getPartitionSpec().getDimensionCount());
                Assert.assertEquals(PartitionDimension.KIND_IDENTITY, m.getPartitionSpec().getDimension(0).getKind());
                Assert.assertEquals(PartitionDimension.KIND_HASH, m.getPartitionSpec().getDimension(1).getKind());
                Assert.assertEquals(PartitionDimension.KIND_TRUNCATE, m.getPartitionSpec().getDimension(2).getKind());
            }
        });
    }

    @Test
    public void testShowCreateRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, region symbol, x double) timestamp(ts) " +
                    "partition by day, (upper(region)) AS r wal");

            printSql("SHOW CREATE TABLE c;");
            String ddl = sink.toString().replace("ddl\n", "");
            TestUtils.assertContains(ddl, "(upper(region)) AS r");

            execute("drop table c;");
            execute(ddl); // re-create from the emitted DDL
            printSql("SHOW CREATE TABLE c;");
            TestUtils.assertEquals(sink.toString().replace("ddl\n", ""), ddl);
        });
    }

    private void assertWalTableNotSuspended(String tableName) {
        Assert.assertFalse(
                tableName + " must not be suspended",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName)));
    }

    private static Set<String> setOf(String... values) {
        Set<String> set = new HashSet<>();
        for (String v : values) {
            set.add(v);
        }
        return set;
    }

    /**
     * Lists the immediate child directory names of {@code <dbRoot>/<tableToken>/<dayDirName>},
     * stripping each entry's trailing {@code .<nameTxn>} version suffix (e.g. {@code "r=US.3"} ->
     * {@code "r=US"}) so the result is comparable regardless of the exact nameTxn a real commit
     * happened to assign. Mirrors {@code CompositeRoutingEndToEndTest}'s identically-named helper
     * (itself mirroring {@code ShowPartitionsRecordCursorFactory#scanDetachedAndAttachablePartitions}'s
     * own {@code ff.findFirst/findName/findType/findNext/findClose} idiom) -- lifted here rather than
     * widening that class's visibility, per this codebase's own established precedent for this need.
     */
    private static Set<String> listCellDirNames(FilesFacade ff, TableToken tableToken, String dayDirName) {
        Set<String> names = new HashSet<>();
        try (Path path = new Path()) {
            path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(dayDirName).$();
            long pFind = ff.findFirst(path.$());
            Assert.assertTrue("expected day directory to exist: " + path, pFind > 0L);
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
}
