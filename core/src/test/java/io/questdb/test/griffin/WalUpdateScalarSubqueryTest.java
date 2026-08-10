package io.questdb.test.griffin;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Regression pin for how a WAL-applied {@code UPDATE} or {@code ALTER} resolves the table names in
 * its own SQL. The invariant, in one sentence: the statement's declared target resolves to the
 * writer's table however its name has moved since sequencing, and every other name in the statement
 * resolves normally.
 * <p>
 * A WAL {@code UPDATE} is replicated as SQL text and re-executed on every node, so a statement that
 * reads a <em>second</em> table can compute a different answer per node and silently diverge the
 * cluster: the apply path pins the RNG seed, the clock and the bind variables precisely because
 * replay must be identical, but nothing pins another table's watermark, and sequencing is
 * per-table. Such a statement is therefore rejected when the client compiles it, exactly as
 * {@code UPDATE ... FROM} joins already were. Only the statement's own target may be named, however
 * many times it is named.
 * <p>
 * The same reasoning applies to a cursor function, which reaches node-local state without naming a
 * table in the model - a catalogue listing, a partition list, a process metric, a local file - so a
 * WAL {@code UPDATE} may not instantiate one at all. That check is keyed on the type of the
 * function the compiler actually built, not on its name or its factory, because one name can be
 * both: {@code sleep(D)} is a cursor while {@code sleep(l)} is a plain boolean. Keying on the type
 * also makes it indifferent to where the function was written - a FROM source, a projected column
 * or a predicate operand all instantiate it - which is what the earlier per-position checks could
 * not manage. It denies by default, so the deterministic generators {@code long_sequence} and
 * {@code generate_series} are refused with the rest. It is keyed on the compiler having
 * materialised a cursor, not on a function factory having been called, because {@code SHOW} builds
 * its cursor inline in the optimiser and several {@code SHOW} kinds name nothing the model walk
 * could look at.
 * <p>
 * The predicate cases run twice, once against a non-WAL table and once against a WAL one. A
 * predicate that names only the target asserts the identical result in both modes - going through
 * the WAL must not change what a statement means - while a predicate that names a second table
 * asserts the real result on the non-WAL table, where nothing is replicated and nothing changes,
 * and rejection on the WAL one. The two predicate shapes that carry no sub-query (constant, plain
 * column) are the controls: they exercise the fixture and the expectations, so a failure in the
 * sub-query cases cannot be blamed on the harness. The cases that only exist on the apply path -
 * rename, name reuse, a sub-query table that disappears - are WAL-only and say so with
 * {@link Assume}.
 * <p>
 * The apply-path cases that name a second table cannot be sequenced through the compiler any more,
 * so they sequence a <em>legacy segment</em> instead - see
 * {@link #sequenceLegacySegmentUpdate(String, String)}. That is not a contrivance: a statement
 * sequenced by a build from before the rejection existed, still unapplied when the node is upgraded,
 * is exactly this, and it has to keep applying sanely.
 * <p>
 * History, for context only. The remap used to be unconditional: every table reference in a
 * WAL-applied statement was redirected onto the table being updated. With {@code t} holding 5 rows
 * and {@code bounds} holding 3, {@code v >= (SELECT count() FROM bounds)} behaved as {@code v >= 5}
 * instead of {@code v >= 3}; the statement reported success, nothing was logged, and the wrong rows
 * were written. Scoping the remap to the declared target name fixed that, and this class exists so
 * that it stays fixed.
 */
@RunWith(Parameterized.class)
public class WalUpdateScalarSubqueryTest extends AbstractCairoTest {
    private static final String CURSOR_FUNCTION_REJECTED = "UPDATE statements that read a cursor function are not supported for WAL tables";
    private static final String FIVE_ROWS_UNCHANGED = """
            2020-06-01T00:00:00.000000Z\t1
            2020-06-02T00:00:00.000000Z\t2
            2020-06-03T00:00:00.000000Z\t3
            2020-06-04T00:00:00.000000Z\t4
            2020-06-05T00:00:00.000000Z\t5
            """;
    private static final String UPDATED_FROM_FIVE = """
            2020-06-01T00:00:00.000000Z\t1
            2020-06-02T00:00:00.000000Z\t2
            2020-06-03T00:00:00.000000Z\t3
            2020-06-04T00:00:00.000000Z\t4
            2020-06-05T00:00:00.000000Z\t99
            """;
    private static final String UPDATED_FROM_THREE = """
            2020-06-01T00:00:00.000000Z\t1
            2020-06-02T00:00:00.000000Z\t2
            2020-06-03T00:00:00.000000Z\t99
            2020-06-04T00:00:00.000000Z\t99
            2020-06-05T00:00:00.000000Z\t99
            """;
    private static final String UPDATED_FROM_TWO = """
            2020-06-01T00:00:00.000000Z\t1
            2020-06-02T00:00:00.000000Z\t99
            2020-06-03T00:00:00.000000Z\t99
            2020-06-04T00:00:00.000000Z\t99
            2020-06-05T00:00:00.000000Z\t99
            """;
    private final boolean walEnabled;

    public WalUpdateScalarSubqueryTest(boolean walEnabled) {
        this.walEnabled = walEnabled;
    }

    @Parameterized.Parameters(name = "wal={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{false}, {true}});
    }

    // CONTROL: a constant timestamp bound updates the same rows in both modes.
    @Test
    public void testUpdateWithConstantTimestampPredicate() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable(4);
            update("UPDATE t SET v = 99 WHERE ts >= '2020-06-03T00:00:00.000000Z'");
            assertTable("""
                    2020-06-01T00:00:00.000000Z\t1
                    2020-06-02T00:00:00.000000Z\t2
                    2020-06-03T00:00:00.000000Z\t99
                    2020-06-04T00:00:00.000000Z\t99
                    """);
        });
    }

    // CONTROL: a plain column predicate updates the same rows in both modes.
    @Test
    public void testUpdateWithPlainColumnPredicate() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable(4);
            update("UPDATE t SET v = 99 WHERE v >= 3");
            assertTable("""
                    2020-06-01T00:00:00.000000Z\t1
                    2020-06-02T00:00:00.000000Z\t2
                    2020-06-03T00:00:00.000000Z\t99
                    2020-06-04T00:00:00.000000Z\t99
                    """);
        });
    }

    // The scalar sub-query the whole class is named for. bounds holds 3 rows, so on a non-WAL table
    // the predicate is `v >= 3` and rows 3..5 are updated. On a WAL table the statement never gets
    // sequenced: it would be shipped as SQL and re-executed per node against a bounds nothing keeps
    // aligned, so the client is told so synchronously and the target is left untouched.
    @Test
    public void testUpdateWithIntScalarSubqueryPredicate() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable(5);
            createBoundsTable(3);
            assertCrossTableUpdate(
                    "UPDATE t SET v = 99 WHERE v >= (SELECT count() FROM bounds)",
                    "bounds",
                    """
                            2020-06-01T00:00:00.000000Z\t1
                            2020-06-02T00:00:00.000000Z\t2
                            2020-06-03T00:00:00.000000Z\t99
                            2020-06-04T00:00:00.000000Z\t99
                            2020-06-05T00:00:00.000000Z\t99
                            """,
                    FIVE_ROWS_UNCHANGED
            );
        });
    }

    // Same rejection through a timestamp bound rather than an int one, so the shape that used to
    // suspend the table (bounds remapped onto t, max(b) naming a column t does not have) is still
    // pinned. max(b) is 2020-06-03, so on a non-WAL table rows 3 and 4 are updated.
    @Test
    public void testUpdateWithTimestampScalarSubqueryPredicate() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable(4);
            execute("CREATE TABLE bounds (b TIMESTAMP)");
            execute("INSERT INTO bounds VALUES ('2020-06-01T00:00:00.000000Z'),"
                    + "('2020-06-03T00:00:00.000000Z')");
            assertCrossTableUpdate(
                    "UPDATE t SET v = 99 WHERE ts >= (SELECT max(b) FROM bounds)",
                    "bounds",
                    """
                            2020-06-01T00:00:00.000000Z\t1
                            2020-06-02T00:00:00.000000Z\t2
                            2020-06-03T00:00:00.000000Z\t99
                            2020-06-04T00:00:00.000000Z\t99
                            """,
                    """
                            2020-06-01T00:00:00.000000Z\t1
                            2020-06-02T00:00:00.000000Z\t2
                            2020-06-03T00:00:00.000000Z\t3
                            2020-06-04T00:00:00.000000Z\t4
                            """
            );
        });
    }

    // A second table hidden one level further down still has to be found: the guard walks the whole
    // model tree, not just the sub-query directly under WHERE.
    @Test
    public void testUpdateWithNestedSubqueryPredicate() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable(5);
            createBoundsTable(3);
            assertCrossTableUpdate(
                    "UPDATE t SET v = 99 WHERE v >= (SELECT max(c) FROM (SELECT count() c FROM bounds))",
                    "bounds",
                    """
                            2020-06-01T00:00:00.000000Z\t1
                            2020-06-02T00:00:00.000000Z\t2
                            2020-06-03T00:00:00.000000Z\t99
                            2020-06-04T00:00:00.000000Z\t99
                            2020-06-05T00:00:00.000000Z\t99
                            """,
                    FIVE_ROWS_UNCHANGED
            );
        });
    }

    // A UNION branch is a model the nested-model chain does not reach, so it is walked separately.
    // Only the second branch names a second table, and the first names the target, so nothing but
    // the union walk can find bounds here. On a non-WAL table t holds 5 rows and bounds 3, so max(c)
    // is 5 and only the last row is updated.
    @Test
    public void testUpdateWithUnionSubqueryPredicate() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable(5);
            createBoundsTable(3);
            assertCrossTableUpdate(
                    "UPDATE t SET v = 99 WHERE v >= (SELECT max(c) FROM "
                            + "(SELECT count() c FROM t UNION ALL SELECT count() c FROM bounds))",
                    "bounds",
                    """
                            2020-06-01T00:00:00.000000Z\t1
                            2020-06-02T00:00:00.000000Z\t2
                            2020-06-03T00:00:00.000000Z\t3
                            2020-06-04T00:00:00.000000Z\t4
                            2020-06-05T00:00:00.000000Z\t99
                            """,
                    FIVE_ROWS_UNCHANGED
            );
        });
    }

    // A join *inside* a sub-query is the one shape neither existing check would have caught on its
    // own: containsJoin() only walks the statement's own nested-model chain, so it does not see this
    // join, and the sub-query's leading table here is the target, so the leading-table comparison
    // passes. Only descending into the sub-query's join models finds picks. On a non-WAL table the
    // join matches v = 2 and v = 3, so count() is 2 and rows 2..5 are updated.
    @Test
    public void testUpdateWithJoinInsideSubqueryPredicate() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable(5);
            execute("CREATE TABLE picks (v INT)");
            execute("INSERT INTO picks VALUES (2),(3)");
            assertCrossTableUpdate(
                    "UPDATE t SET v = 99 WHERE v >= (SELECT count() FROM t JOIN picks ON t.v = picks.v)",
                    "picks",
                    """
                            2020-06-01T00:00:00.000000Z\t1
                            2020-06-02T00:00:00.000000Z\t99
                            2020-06-03T00:00:00.000000Z\t99
                            2020-06-04T00:00:00.000000Z\t99
                            2020-06-05T00:00:00.000000Z\t99
                            """,
                    FIVE_ROWS_UNCHANGED
            );
        });
    }

    // A CTE names its table inside the WITH clause, which the parser inlines as a nested model where
    // the CTE is referenced (SqlParser#parseSelectFrom), so the walk reaches it the same way.
    @Test
    public void testUpdateWithCteSubqueryPredicate() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable(5);
            createBoundsTable(3);
            assertCrossTableUpdate(
                    "WITH lim AS (SELECT count() c FROM bounds) "
                            + "UPDATE t SET v = 99 WHERE v >= (SELECT c FROM lim)",
                    "bounds",
                    """
                            2020-06-01T00:00:00.000000Z\t1
                            2020-06-02T00:00:00.000000Z\t2
                            2020-06-03T00:00:00.000000Z\t99
                            2020-06-04T00:00:00.000000Z\t99
                            2020-06-05T00:00:00.000000Z\t99
                            """,
                    FIVE_ROWS_UNCHANGED
            );
        });
    }

    // A cursor function is the other way node-local state gets into an UPDATE, and it is invisible
    // to the model walk: parseSelectFrom's FUNCTION branch stores the call itself in tableNameExpr,
    // so there is no LITERAL to compare against the target. table_partitions resolves its argument
    // through the execution context (TablePartitionsFunctionFactory#newInstance -> getTableToken,
    // then ShowPartitionsRecordCursorFactory -> getReader), the two hooks this change stopped
    // remapping, so on the apply path it would read the real bounds at whatever watermark the
    // applying node happens to hold. bounds is not partitioned, so on a non-WAL table it has exactly
    // one partition and the predicate is `v >= 1`, updating every row.
    @Test
    public void testUpdateWithTableFunctionSubqueryPredicate() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable(5);
            createBoundsTable(3);
            assertForeignSourceUpdate(
                    "UPDATE t SET v = 99 WHERE v >= (SELECT count() FROM table_partitions('bounds'))",
                    CURSOR_FUNCTION_REJECTED,
                    """
                            2020-06-01T00:00:00.000000Z\t99
                            2020-06-02T00:00:00.000000Z\t99
                            2020-06-03T00:00:00.000000Z\t99
                            2020-06-04T00:00:00.000000Z\t99
                            2020-06-05T00:00:00.000000Z\t99
                            """,
                    FIVE_ROWS_UNCHANGED
            );
        });
    }

    // The same function one position over, and the position that defeated a guard keyed on where the
    // function stands: a cursor function may also be written as a projected *column*, and
    // SqlOptimiser then rewrites it into a cross join against the model that carries it
    // (rewriteSelect0 -> isCursor(qc.getAst().token) -> addCursorFunctionAsCrossJoin). At the top
    // level that synthesised join is caught by generateUpdate's containsJoin() backstop, but nested
    // one sub-query down it lands inside the sub-query's own model, which containsJoin() does not
    // walk. Keying on the instantiated function's type instead makes the position irrelevant: the
    // function is still instantiated, so it is still seen. bounds is not partitioned, so on a
    // non-WAL table it has exactly one partition and the predicate is `v >= 1`.
    @Test
    public void testUpdateWithTableFunctionInNestedProjectionIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable(5);
            createBoundsTable(3);
            assertForeignSourceUpdate(
                    "UPDATE t SET v = 99 WHERE v >= (SELECT count() FROM (SELECT table_partitions('bounds')))",
                    CURSOR_FUNCTION_REJECTED,
                    """
                            2020-06-01T00:00:00.000000Z\t99
                            2020-06-02T00:00:00.000000Z\t99
                            2020-06-03T00:00:00.000000Z\t99
                            2020-06-04T00:00:00.000000Z\t99
                            2020-06-05T00:00:00.000000Z\t99
                            """,
                    FIVE_ROWS_UNCHANGED
            );
        });
    }

    // The same position reached without a call syntax: a bare literal whose name happens to be a
    // cursor function is turned into that function by the optimiser all the same, because
    // replaceIfCursor and rewriteSelect0 both test isCursor(token) and neither looks at the node
    // type. pg_class enumerates the tables this process knows about - node-local state, not
    // something sequencing keeps aligned - so it is the same divergence with a different spelling.
    // WAL-only: the non-WAL answer is a count of whatever tables the fixture happens to hold, which
    // pins nothing.
    @Test
    public void testUpdateWithCatalogueFunctionAsBareProjectionIsRejected() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            createTargetTable(5);
            assertQuery("UPDATE t SET v = 99 WHERE v >= (SELECT count() FROM (SELECT pg_class))")
                    .noLeakCheck()
                    .fails(0, CURSOR_FUNCTION_REJECTED);
            assertTable(FIVE_ROWS_UNCHANGED);
        });
    }

    // The third position, and the one that showed a per-position guard could never be finished: here
    // the cursor is neither the FROM source nor a projected column but an *operand*, consumed by
    // InSymbolCursorFunctionFactory's `in(KC)`. all_tables() lists the tables this process knows
    // about, so which rows match depends on what else the node happens to hold. The type check does
    // not care where the function stood.
    @Test
    public void testUpdateWithCursorFunctionAsPredicateOperandIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable(5);
            execute("ALTER TABLE t ADD COLUMN s SYMBOL");
            assertForeignSourceUpdate(
                    "UPDATE t SET v = 99 WHERE s IN all_tables()",
                    CURSOR_FUNCTION_REJECTED,
                    FIVE_ROWS_UNCHANGED,
                    FIVE_ROWS_UNCHANGED
            );
        });
    }

    // The 41 cursor factories a guard keyed on FunctionFactory#isCursor() could not see: only six
    // factories override it, while ~47 produce a CURSOR-typed function, and every catalogue and
    // process-state cursor is in the difference. Keying on the instantiated type sees all of them,
    // so one test can sweep a representative sample rather than chase them one at a time. Each reads
    // this process's own state - the tables it knows, the columns of one of them, the queries and
    // readers currently in flight, its own memory counters - none of which sequencing aligns across
    // nodes. WAL-only: the non-WAL answers are counts of whatever the fixture happens to hold.
    @Test
    public void testUpdateWithCatalogueCursorFunctionSourcesAreRejected() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            createTargetTable(5);
            createBoundsTable(3);
            final String[] sources = {
                    "all_tables()",
                    "tables()",
                    "table_columns('bounds')",
                    "query_activity()",
                    "reader_pool()",
                    "memory_metrics()",
                    "wal_transactions('t')",
            };
            for (String source : sources) {
                assertQuery("UPDATE t SET v = 99 WHERE v >= (SELECT count() FROM " + source + ')')
                        .noLeakCheck()
                        .fails(0, CURSOR_FUNCTION_REJECTED);
            }
            assertTable(FIVE_ROWS_UNCHANGED);
        });
    }

    // The second way the compiler materialises a cursor: SHOW builds its factory inline in
    // SqlOptimiser#parseFunctionAndEnumerateColumns and hands it to the model, so it never reaches a
    // function factory and the type check has to be told about it there. SHOW TABLES is backed by
    // the very AllTablesCursorFactory that all_tables() returns, reached by other syntax, so this is
    // the same node-local listing as testUpdateWithCatalogueCursorFunctionSourcesAreRejected with a
    // different spelling. The fixture holds exactly t and bounds, so on a non-WAL table the
    // predicate is `v >= 2`.
    @Test
    public void testUpdateWithShowTablesSubqueryIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable(5);
            createBoundsTable(3);
            assertForeignSourceUpdate(
                    "UPDATE t SET v = 99 WHERE v >= (SELECT count() FROM (SHOW TABLES))",
                    CURSOR_FUNCTION_REJECTED,
                    UPDATED_FROM_TWO,
                    FIVE_ROWS_UNCHANGED
            );
        });
    }

    // The same cursor reached from further away, so that the check is pinned as position-independent
    // here too: nested one model deeper, and standing in a UNION branch. Neither is on the path the
    // model's table-name walk takes, and SHOW TABLES names no table for it to find in any case.
    // WAL-only: the non-WAL answers depend on how many tables the fixture holds at that moment.
    @Test
    public void testUpdateWithShowTablesNestedSubqueryIsRejected() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            createTargetTable(5);
            final String[] shapes = {
                    "UPDATE t SET v = 99 WHERE v >= (SELECT count() FROM (SELECT * FROM (SHOW TABLES)))",
                    "UPDATE t SET v = 99 WHERE v >= (SELECT count() FROM (SHOW TABLES) UNION ALL SELECT count() FROM (SHOW TABLES))",
            };
            for (String shape : shapes) {
                assertQuery(shape)
                        .noLeakCheck()
                        .fails(0, CURSOR_FUNCTION_REJECTED);
            }
            assertTable(FIVE_ROWS_UNCHANGED);
        });
    }

    // The sharpest of the SHOW cases, because the only table it names is the target itself: the
    // model's table-name walk looks at SHOW PARTITIONS FROM t, sees t, and is right to wave it
    // through. What makes it divergent is not which table it reads but what it reports about it -
    // diskSize, location, isReadOnly and isParquet are this node's physical layout, which nothing
    // synchronises even for a table that is otherwise perfectly in step. Only the type check can
    // see that. WAL-only: on-disk sizes are not a stable expectation to assert against.
    @Test
    public void testUpdateWithShowPartitionsSubqueryIsRejected() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            createTargetTable(5);
            assertQuery("UPDATE t SET v = 99 WHERE v >= (SELECT count() FROM (SHOW PARTITIONS FROM t) WHERE diskSize > 1000)")
                    .noLeakCheck()
                    .fails(0, CURSOR_FUNCTION_REJECTED);
            assertTable(FIVE_ROWS_UNCHANGED);
        });
    }

    // The same shape without the node-local column, so that the non-WAL arm has something stable to
    // assert: t is partitioned by day over five distinct days, so count() over its partitions is 5
    // wherever the statement runs, and on a non-WAL table the predicate is `v >= 5`. It is still
    // refused on WAL - the count of partitions is not itself synchronised either, since a node may
    // have squashed or converted partitions the others have not.
    @Test
    public void testUpdateWithShowPartitionsCountSubqueryIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable(5);
            assertForeignSourceUpdate(
                    "UPDATE t SET v = 99 WHERE v >= (SELECT count() FROM (SHOW PARTITIONS FROM t))",
                    CURSOR_FUNCTION_REJECTED,
                    UPDATED_FROM_FIVE,
                    FIVE_ROWS_UNCHANGED
            );
        });
    }

    // Server configuration, which legitimately differs from node to node - it is not replicated and
    // is not meant to be - so a statement whose result depends on it cannot be replayed. It also
    // names nothing at all: SHOW PARAMETERS sets no table name expression, so the model's
    // table-name walk has nothing to look at. WAL-only: the parameter count is a property of the
    // build, not something worth pinning here.
    @Test
    public void testUpdateWithShowParametersSubqueryIsRejected() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            createTargetTable(5);
            assertQuery("UPDATE t SET v = 99 WHERE v >= (SELECT count() FROM (SHOW PARAMETERS))")
                    .noLeakCheck()
                    .fails(0, CURSOR_FUNCTION_REJECTED);
            assertTable(FIVE_ROWS_UNCHANGED);
        });
    }

    // A SHOW whose argument really is a second table. The model's table-name walk deliberately skips
    // SHOW models, because a SHOW argument is not always a table - SHOW USER names a user - so this
    // pins that the skip costs no coverage: the cursor the SHOW materialises catches it anyway.
    // WAL-only: the non-WAL answer is the column count of bounds, which pins nothing about this.
    @Test
    public void testUpdateWithShowNamingSecondTableIsRejected() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            createTargetTable(5);
            createBoundsTable(3);
            assertQuery("UPDATE t SET v = 99 WHERE v >= (SELECT count() FROM (SHOW COLUMNS FROM bounds))")
                    .noLeakCheck()
                    .fails(0, CURSOR_FUNCTION_REJECTED);
            assertTable(FIVE_ROWS_UNCHANGED);
        });
    }

    // Deny-by-default, pinned as a decision rather than left as an accident. long_sequence and
    // generate_series really are deterministic - they generate their rows from their own arguments -
    // so this is a false positive, and it is the price of not keeping a list of names in step with
    // the ~47 factories that produce a cursor. The error costs are asymmetric: a false positive is a
    // synchronous compile error the user sees at once, a false negative is silent, permanent
    // divergence between replicas. count() over long_sequence(3) is 3, so on a non-WAL table rows
    // 3..5 are updated.
    @Test
    public void testUpdateWithLongSequenceSubqueryPredicateIsRejectedOnWal() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable(5);
            assertForeignSourceUpdate(
                    "UPDATE t SET v = 99 WHERE v >= (SELECT count() FROM long_sequence(3))",
                    CURSOR_FUNCTION_REJECTED,
                    UPDATED_FROM_THREE,
                    FIVE_ROWS_UNCHANGED
            );
        });
    }

    // The second generator, for the same reason. generate_series(1, 3, 1) yields 3 rows, so on a
    // non-WAL table the predicate is `v >= 3` exactly as above.
    @Test
    public void testUpdateWithGenerateSeriesSubqueryPredicateIsRejectedOnWal() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable(5);
            assertForeignSourceUpdate(
                    "UPDATE t SET v = 99 WHERE v >= (SELECT count() FROM generate_series(1, 3, 1))",
                    CURSOR_FUNCTION_REJECTED,
                    UPDATED_FROM_THREE,
                    FIVE_ROWS_UNCHANGED
            );
        });
    }

    // The reach of deny-by-default that is not obvious from the SQL text: SqlParser#parseQueryModel
    // synthesises long_sequence(1) as the source of a FROM-less SELECT, so a constant sub-query
    // instantiates a cursor function without naming one. It is rejected for that reason and for no
    // other - the value is a constant and could not diverge - so this is the sharpest edge of the
    // rule and is pinned here so that a later reader sees it was measured, not missed.
    @Test
    public void testUpdateWithConstantSubqueryPredicateIsRejectedOnWal() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable(5);
            assertForeignSourceUpdate(
                    "UPDATE t SET v = 99 WHERE v >= (SELECT 3)",
                    CURSOR_FUNCTION_REJECTED,
                    UPDATED_FROM_THREE,
                    FIVE_ROWS_UNCHANGED
            );
        });
    }

    // The trap the rule is keyed to avoid, and the reason it tests the instantiated function rather
    // than the factory or the name. `sleep` resolves to two different factories: sleep(D) yields a
    // cursor, sleep(l) - TestSleepFunctionFactory - yields a plain boolean, and the boolean one is
    // what `sleep(1)` binds to. Keying on the name or the factory class would reject it and break
    // IODispatcherTest's `update tab set b=false where b=true and sleep(1)`, which is a live query
    // cancellation test on a WAL table.
    @Test
    public void testUpdateWithScalarSleepPredicateIsAllowed() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable(5);
            update("UPDATE t SET v = 99 WHERE v >= 3 AND sleep(1)");
            assertTable(UPDATED_FROM_THREE);
        });
    }

    // A cursor function's argument list is not a blind spot either: a sub-query hidden in there names
    // bounds just as effectively as one in the WHERE clause, and it is the model walk - not the
    // cursor rule - that has to find it, because the walk runs first and its message is the one that
    // names the table. WAL-only because the point is which of the two messages is raised.
    @Test
    public void testUpdateWithSubqueryInsideTableFunctionArgumentIsRejected() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            createTargetTable(5);
            createBoundsTable(3);
            assertQuery("UPDATE t SET v = 99 WHERE v >= "
                    + "(SELECT count() FROM long_sequence((SELECT count() FROM bounds)))")
                    .noLeakCheck()
                    .fails(0, "UPDATE statements that reference another table are not supported for WAL tables "
                            + "[table=bounds]");
            assertTable(FIVE_ROWS_UNCHANGED);
        });
    }

    // The SET clause would be a second way into the model, and it writes the foreign value straight
    // into the target, so it would be the more dangerous half -- except that it is not a way in at
    // all. SqlParser#parseUpdateClause parses a SET value with expr(lexer, (IQueryModel) null, ...),
    // and a null model makes ExpressionTreeBuilder#onNode reject any sub-query outright, whether or
    // not the target is a WAL table. Pinned here so that the guard's silence about SET clauses reads
    // as "nothing can get through" rather than "nobody checked": lift this parser restriction and
    // this test goes red, which is the day the SET clause needs covering too.
    @Test
    public void testUpdateWithSubqueryInSetClauseIsRejectedByParser() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable(5);
            execute("CREATE TABLE picks (v INT)");
            execute("INSERT INTO picks VALUES (7)");
            assertQuery("UPDATE t SET v = (SELECT max(v) FROM picks) WHERE v >= 4")
                    .noLeakCheck()
                    .fails(18, "query is not allowed here");
            assertTable(FIVE_ROWS_UNCHANGED);
        });
    }

    // The other half of the rule, and the one a careless guard breaks: the target's own name may
    // occur as often as the statement likes. max(v) is 5, so only the last row matches, in both
    // modes. Rejecting this would be a regression - an UPDATE already reads and writes the same
    // table, and reading it again in a sub-query is deterministic on every node for the same reason
    // the UPDATE itself is: the apply job holds the target's writer and applies seqTxn in order.
    @Test
    public void testUpdateWithSubqueryOverTargetIsAllowed() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable(5);
            update("UPDATE t SET v = 99 WHERE v >= (SELECT max(v) FROM t)");
            assertTable("""
                    2020-06-01T00:00:00.000000Z\t1
                    2020-06-02T00:00:00.000000Z\t2
                    2020-06-03T00:00:00.000000Z\t3
                    2020-06-04T00:00:00.000000Z\t4
                    2020-06-05T00:00:00.000000Z\t99
                    """);
        });
    }

    // Names are compared the way the rest of the resolution compares them, case-insensitively, so a
    // differently-spelled reference to the target is still the target and is still allowed. The name
    // registry is itself case-insensitive (TableNameRegistryRW builds its map with
    // ConcurrentHashMap<>(false)), so anything else would reject a statement that names one table.
    @Test
    public void testUpdateWithMixedCaseSubqueryOverTargetIsAllowed() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable(5);
            update("UPDATE \"t\" SET v = 99 WHERE v >= (SELECT max(v) FROM T)");
            assertTable("""
                    2020-06-01T00:00:00.000000Z\t1
                    2020-06-02T00:00:00.000000Z\t2
                    2020-06-03T00:00:00.000000Z\t3
                    2020-06-04T00:00:00.000000Z\t4
                    2020-06-05T00:00:00.000000Z\t99
                    """);
        });
    }

    // The target's own name is allowed to occur more than once, and every occurrence is the target:
    // an UPDATE already reads and writes the same table, and a sub-query may name it again. Scoping
    // the remap by name must not turn that second occurrence into an ordinary lookup.
    //
    // The blue/green swap is what gives the shape teeth, and it is not decoration. Without it the
    // name `t` answers with the writer's token down every route - the scoped remap, the old
    // unconditional remap, even a build that declares no target at all and falls through to the
    // registry - so no assertion on the result could tell them apart. Once the name has been handed
    // to a replacement table, the registry answers `t` with the replacement, whose max(v) is 7: a
    // sub-query resolved that way matches no row of the original and the update writes nothing.
    // Only a resolution that goes to the writer's token sees max(v) = 5 and updates the last row.
    @Test
    public void testUpdateWithSubqueryOverTargetAppliesToOriginalTableAfterNameReused() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            createTargetTable(5);
            update("UPDATE t SET v = 99 WHERE v >= (SELECT max(v) FROM t)");
            swapInFreshTableUnderOldName();
            drainWalQueue();
            assertNamed("t_old", """
                    2020-06-01T00:00:00.000000Z\t1
                    2020-06-02T00:00:00.000000Z\t2
                    2020-06-03T00:00:00.000000Z\t3
                    2020-06-04T00:00:00.000000Z\t4
                    2020-06-05T00:00:00.000000Z\t99
                    """);
            // the replacement inherited only the name, never the pending write
            assertNamed("t", "2020-07-01T00:00:00.000000Z\t7\n");
        });
    }

    // The match is case-insensitive, as it must be: the name registry itself is case-insensitive
    // (TableNameRegistryRW builds its map as ConcurrentHashMap<>(false)), so `T` and `t` are one
    // table and both occurrences here are the target. The rename is what gives the case teeth --
    // once the target has been renamed no spelling of the old name resolves through the registry,
    // so a case-sensitive comparison would send the sub-query's `T` to the engine, get "table does
    // not exist" for a name that is in fact the target, and suspend the table.
    @Test
    public void testUpdateWithMixedCaseSubqueryOverTargetAppliesAfterTargetRenamed() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            createTargetTable(5);
            update("UPDATE t SET v = 99 WHERE v >= (SELECT max(v) FROM T)");
            execute("RENAME TABLE t TO t_renamed");
            assertNamed("t_renamed", """
                    2020-06-01T00:00:00.000000Z\t1
                    2020-06-02T00:00:00.000000Z\t2
                    2020-06-03T00:00:00.000000Z\t3
                    2020-06-04T00:00:00.000000Z\t4
                    2020-06-05T00:00:00.000000Z\t99
                    """);
        });
    }

    // Quoting is stripped before the target name is declared, so a quoted reference and a bare one
    // denote the same target. Same rename as above for the same reason: if the quotes survived into
    // the declared name, nothing in the statement would match it, and the target would be looked up
    // through a registry that no longer answers for it.
    @Test
    public void testUpdateWithQuotedTargetAppliesAfterTargetRenamed() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            createTargetTable(5);
            update("UPDATE \"t\" SET v = 99 WHERE v >= (SELECT max(v) FROM \"t\")");
            execute("RENAME TABLE t TO t_renamed");
            assertNamed("t_renamed", """
                    2020-06-01T00:00:00.000000Z\t1
                    2020-06-02T00:00:00.000000Z\t2
                    2020-06-03T00:00:00.000000Z\t3
                    2020-06-04T00:00:00.000000Z\t4
                    2020-06-05T00:00:00.000000Z\t99
                    """);
        });
    }

    // The scoped remap must still tolerate a rename: the stored SQL names the target as it was when
    // the statement was sequenced, so after a rename that name no longer resolves and the target can
    // only be recognised by falling back to the writer's token.
    @Test
    public void testUpdateAppliesAfterTargetRenamed() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            createTargetTable(3);
            // sequenced but not yet applied, then renamed underneath
            update("UPDATE t SET v = 99 WHERE v >= 2");
            execute("RENAME TABLE t TO t_renamed");
            drainWalQueue();
            assertQuery("SELECT ts, v FROM t_renamed")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2020-06-01T00:00:00.000000Z\t1
                            2020-06-02T00:00:00.000000Z\t99
                            2020-06-03T00:00:00.000000Z\t99
                            """);
        });
    }

    // Both halves of the contract at once, on a legacy segment: the target is recognised through a
    // name that no longer resolves, while the sub-query's own table still resolves to itself. A
    // pre-upgrade segment is the only way a statement of this shape reaches apply now, and it still
    // has to land on the right rows of the right table.
    @Test
    public void testUpdateWithSubqueryAppliesAfterTargetRenamed() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            createTargetTable(3);
            execute("CREATE TABLE bounds (b TIMESTAMP)");
            execute("INSERT INTO bounds VALUES ('2020-06-02T00:00:00.000000Z')");
            sequenceLegacySegmentUpdate("UPDATE t SET v = 99 WHERE ts >= (SELECT max(b) FROM bounds)", "v");
            execute("RENAME TABLE t TO t_renamed");
            drainWalQueue();
            assertQuery("SELECT ts, v FROM t_renamed")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2020-06-01T00:00:00.000000Z\t1
                            2020-06-02T00:00:00.000000Z\t99
                            2020-06-03T00:00:00.000000Z\t99
                            """);
        });
    }

    // ALTER travels the same WAL apply path as UPDATE and is re-compiled the same way, so it needs
    // the same target-name scoping. DROP PARTITION is used because it is non-structural -- structural
    // ALTERs (ADD/DROP/RENAME COLUMN) are applied as metadata changes and never re-compiled as SQL,
    // so they would not exercise this at all.
    @Test
    public void testAlterAppliesAfterTargetRenamed() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            createTargetTable(3);
            execute("ALTER TABLE t DROP PARTITION LIST '2020-06-01'");
            execute("RENAME TABLE t TO t_renamed");
            drainWalQueue();
            assertNamed("t_renamed", """
                    2020-06-02T00:00:00.000000Z\t2
                    2020-06-03T00:00:00.000000Z\t3
                    """);
        });
    }

    // ALTER twin of the blue/green case: the pending ALTER belongs to the table it was issued
    // against, not to whatever now answers to that name.
    //
    // DROP PARTITION WHERE is the shape that can tell the two apart. A WAL-applied ALTER is executed
    // against the writer the apply job already holds (OperationExecutor.executeAlter ->
    // tableWriter.apply(alterOp, seqTxn)), so for a shape whose payload is fixed by the SQL text --
    // DROP PARTITION LIST, SET PARAM, SET TTL, ATTACH PARTITION -- the resolved token never reaches
    // the outcome and no test of it can detect mis-resolution. WHERE is different: the partition
    // list is computed during re-compilation by filterPartitions() over a reader opened on the
    // *resolved* token, and only then handed to the writer. Resolve the wrong table and the writer
    // is told to drop the wrong set of partitions.
    //
    // The replacement is built and drained before the swap so that it owns a partition the predicate
    // also matches by the time the ALTER is applied. Without that, mis-resolution merely raises
    // "no partitions matched WHERE clause" and the assertion would be pinning an error message
    // rather than the silent wrong-partition drop that is the actual hazard.
    @Test
    public void testAlterAppliesToOriginalTableAfterNameReused() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            createTargetTable(3);
            execute("CREATE TABLE t_new (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t_new VALUES ('2020-06-01T00:00:00.000000Z',6),('2020-07-01T00:00:00.000000Z',7)");
            drainWalQueue();
            // sequenced but not yet applied, then the name is handed to the replacement
            execute("ALTER TABLE t DROP PARTITION WHERE ts < '2020-06-03'");
            execute("RENAME TABLE t TO t_old");
            execute("RENAME TABLE t_new TO t");
            drainWalQueue();
            // resolved against the original: 2020-06-01 and 2020-06-02 match, 2020-06-03 survives.
            // Resolved against the replacement, only 2020-06-01 matches and the writer drops that
            // one partition instead, leaving 2020-06-02 behind.
            assertNamed("t_old", "2020-06-03T00:00:00.000000Z\t3\n");
            // the replacement must keep every partition it was created with
            assertNamed("t", """
                    2020-06-01T00:00:00.000000Z\t6
                    2020-07-01T00:00:00.000000Z\t7
                    """);
        });
    }

    // Mat-view twin of the two ALTER cases above. ALTER MATERIALIZED VIEW travels through the WAL
    // as SQL and is re-compiled at apply time exactly as ALTER TABLE is -- ADD INDEX, DROP INDEX and
    // SYMBOL CAPACITY are the non-structural mat-view alters, so they are the ones that get
    // re-compiled -- and it therefore has to declare its target too.
    //
    // A mat view cannot be renamed (checkMatViewModification rejects RENAME TABLE on one), but the
    // rename guard is not the only way a name stops denoting the same object: DROP frees it and
    // CREATE hands it to a different view. With no target declared, the re-compilation resolves the
    // stored name through the registry and builds the operation from the replacement's metadata --
    // its tableId, its column layout, and an index block size estimated from its reader -- and then
    // applies it to the writer of the original view.
    //
    // Opening the replacement's directory during the apply is the discriminator: nothing in an ALTER
    // of the original view has any reason to touch it, while a re-compilation that resolved the name
    // through the registry reads the replacement's metadata to build the operation and then opens a
    // reader on it for the ADD INDEX size estimate. Exactly one tick is driven so the window cannot
    // include the mat view refresh job, which reads the replacement legitimately.
    //
    // What the correct path does here is refuse, not succeed, and that is worth stating because it
    // bounds what this test can assert. The replacement took the name by DROP + CREATE, so the
    // original view is dropped; resolution goes to the writer's token as it must, and the target's
    // status is then reported as "does not exist" because the registry no longer answers with that
    // token - the ALTER dies with "materialized view does not exist" and is lost along with the view
    // it belonged to. So neither "the index was created on the original" nor "the original's data
    // files were read" is available as a positive companion: the compilation stops before either.
    // The companion asserted instead is the last thing that does happen on the original - the apply
    // reading the stored SQL out of its WAL - which fails if the tick never engaged this view's
    // transaction at all and leaves the negative assertion vacuously true.
    @Test
    public void testAlterMatViewAppliesToOriginalViewAfterNameReused() throws Exception {
        Assume.assumeTrue(walEnabled);
        final AtomicBoolean swapPending = new AtomicBoolean();
        final AtomicBoolean replacementRead = new AtomicBoolean();
        final AtomicBoolean originalSqlRead = new AtomicBoolean();
        final StringSink originalDirName = new StringSink();
        final StringSink replacementDirName = new StringSink();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, WalUtils.EVENT_FILE_NAME) && swapPending.compareAndSet(true, false)) {
                    TestUtils.unchecked(() -> {
                        execute("DROP MATERIALIZED VIEW mv");
                        execute("CREATE MATERIALIZED VIEW mv AS (SELECT sym, avg(price) price, ts FROM base SAMPLE BY 1d) PARTITION BY DAY");
                    });
                    // armed only now, so the CREATE's own I/O is not counted
                    replacementDirName.put(engine.verifyTableName("mv").getDirName());
                } else if (replacementDirName.length() > 0) {
                    if (Utf8s.containsAscii(name, replacementDirName)) {
                        replacementRead.set(true);
                    } else if (Utf8s.endsWithAscii(name, WalUtils.EVENT_FILE_NAME) && Utf8s.containsAscii(name, originalDirName)) {
                        originalSqlRead.set(true);
                    }
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT sym, last(price) price, ts FROM base SAMPLE BY 1h) PARTITION BY DAY");
            execute("INSERT INTO base VALUES ('gbpusd', 1.3, '2024-09-10T12:05:00.000000Z')");
            drainWalAndMatViewQueues();
            originalDirName.put(engine.verifyTableName("mv").getDirName());

            execute("ALTER MATERIALIZED VIEW mv ALTER COLUMN sym ADD INDEX");
            swapPending.set(true);
            tickWalQueue(1);

            Assert.assertFalse("the swap must have fired during apply, or the test proves nothing", swapPending.get());
            Assert.assertTrue(
                    "the apply must have gone on to read the stored ALTER out of the original view's "
                            + "WAL after the swap; without that there was no re-compilation to get right",
                    originalSqlRead.get()
            );
            Assert.assertFalse(
                    "the mat-view ALTER must be re-compiled against the view whose writer applies it, "
                            + "not whatever now answers to its name",
                    replacementRead.get()
            );
        });
    }

    // Blue/green swap: the old name is handed to a NEW table while an UPDATE against the original
    // is still sequenced but unapplied. The statement belongs to the table it was issued against --
    // now called t_old -- and must not follow the name onto the replacement.
    //
    // The window is not narrow: any WAL lag holds statements unapplied, and a suspended table holds
    // them indefinitely.
    @Test
    public void testUpdateAppliesToOriginalTableAfterNameReused() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            createTargetTable(3);
            update("UPDATE t SET v = 99 WHERE v >= 2");
            swapInFreshTableUnderOldName();
            drainWalQueue();
            assertNamed("t_old", """
                    2020-06-01T00:00:00.000000Z\t1
                    2020-06-02T00:00:00.000000Z\t99
                    2020-06-03T00:00:00.000000Z\t99
                    """);
            // the replacement inherited only the name, never the pending write
            assertNamed("t", "2020-07-01T00:00:00.000000Z\t7\n");
        });
    }

    // Both rules at once, again on a legacy segment: the target must be recognised through a name
    // that now denotes a different table, while the sub-query's own table still resolves to itself.
    @Test
    public void testUpdateWithSubqueryAppliesToOriginalTableAfterNameReused() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            createTargetTable(3);
            execute("CREATE TABLE bounds (b TIMESTAMP)");
            execute("INSERT INTO bounds VALUES ('2020-06-02T00:00:00.000000Z')");
            sequenceLegacySegmentUpdate("UPDATE t SET v = 99 WHERE ts >= (SELECT max(b) FROM bounds)", "v");
            swapInFreshTableUnderOldName();
            drainWalQueue();
            assertNamed("t_old", """
                    2020-06-01T00:00:00.000000Z\t1
                    2020-06-02T00:00:00.000000Z\t99
                    2020-06-03T00:00:00.000000Z\t99
                    """);
            assertNamed("t", "2020-07-01T00:00:00.000000Z\t7\n");
        });
    }

    // The sub-query's table can disappear between sequencing and apply, and refreshing the target's
    // token cannot bring it back. WAL apply must reach a terminal, operator-visible state -- the
    // table suspended -- instead of ejecting and re-notifying itself forever.
    //
    // This is the scenario the rejection makes rare rather than impossible, so it is the one the
    // suspend-instead-of-spin fix protects: the segment was sequenced by a build from before the
    // rejection existed and is still unapplied when the node comes back up on a newer build. An
    // upgrade that turned such a segment into an infinite retry would be worse than the divergence
    // the rejection prevents, which is why this must keep passing.
    //
    // The tick count is bounded on purpose: the failure mode this pins is an unbounded retry, so
    // drainWalQueue() would never return and the test would die on the fork timeout rather than fail.
    @Test
    public void testUpdateWithDroppedSubqueryTableSuspendsTable() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            createTargetTable(5);
            createBoundsTable(3);
            final TableToken target = engine.verifyTableName("t");
            // sequenced but not yet applied, then the sub-query's table is dropped underneath
            sequenceLegacySegmentUpdate("UPDATE t SET v = 99 WHERE v >= (SELECT count() FROM bounds)", "v");
            execute("DROP TABLE bounds");
            tickWalQueue(8);
            Assert.assertTrue(
                    "WAL apply must suspend t so the stall is visible, not retry the txn forever",
                    engine.getTableSequencerAPI().isSuspended(target)
            );
        });
    }

    // The name a tableDoesNotExist carries must not outlive it. SqlException is a per-carrier
    // flyweight, so a name left behind by an earlier throw would let WAL apply mistake a missing
    // sub-query table for its own target and go back to retrying forever.
    //
    // Under Maven this is a contract pin rather than a live reproduction: core/pom.xml runs tests
    // with -ea, and SqlException.position() replaces the flyweight with a fresh instance when
    // assertions are on, so no factory call here ever reuses an instance. The test below is the one
    // that reaches past that and exercises the reuse a shipped server actually does.
    @Test
    public void testTableDoesNotExistNameDoesNotOutliveTheException() {
        TestUtils.assertEquals("bounds", SqlException.tableDoesNotExist(0, "bounds").getTableName());
        Assert.assertEquals(0, SqlException.$(0, "unrelated failure").getTableName().length());
        Assert.assertEquals(0, SqlException.walRecoverable(0).getTableName().length());
    }

    // The same invariant where it is load-bearing: in a shipped server assertions are off, so
    // position() hands back the carrier's one reused instance and every throw on that carrier shares
    // the tableName sink. Nothing in a -ea test run can observe that, which is why this loads its own
    // copy of SqlException with assertions disabled for it and drives the real flyweight.
    //
    // Two constructions are pinned, and each one is what the corresponding assertion fails without.
    // tableDoesNotExist clears the sink immediately before writing it, so a second throw cannot
    // append to the first ('bounds' then 't' would read back as 'boundst', and WAL apply would
    // compare that against the statement's target). getTableName() is gated on the error code, so an
    // exception that names no table cannot hand back the name the previous one left behind - which
    // matters because that name is the sole input to ApplyWal2TableJob's suspend-or-retry decision.
    @Test
    public void testTableNameCannotOutliveTheExceptionWithAssertionsDisabled() throws Exception {
        final Class<?> sqlException = loadSqlExceptionWithAssertionsDisabled();
        final Method tableDoesNotExist = sqlException.getMethod("tableDoesNotExist", int.class, CharSequence.class);
        final Method walRecoverable = sqlException.getMethod("walRecoverable", int.class);
        final Method getTableName = sqlException.getMethod("getTableName");

        final Object first = tableDoesNotExist.invoke(null, 0, "bounds");
        TestUtils.assertEquals("bounds", (CharSequence) getTableName.invoke(first));

        final Object second = tableDoesNotExist.invoke(null, 0, "t");
        Assert.assertSame("the copy must reuse one flyweight per carrier, or this proves nothing", first, second);
        TestUtils.assertEquals("t", (CharSequence) getTableName.invoke(second));

        final Object third = walRecoverable.invoke(null, 0);
        Assert.assertSame("the copy must reuse one flyweight per carrier, or this proves nothing", first, third);
        Assert.assertEquals(0, ((CharSequence) getTableName.invoke(third)).length());
    }

    // Guard for the recovery the fix must not break: when the target is renamed after the apply job
    // has taken the writer, the stored SQL names a table the registry no longer knows, compilation
    // fails with "table does not exist" for the target's own name, and WAL apply must refresh the
    // token and retry rather than suspend. The rename fires from inside the read of the WAL event
    // file that carries the SQL, which is the last I/O before compilation, so the window is exact.
    @Test
    public void testUpdateRecoversWhenTargetRenamedDuringApply() throws Exception {
        Assume.assumeTrue(walEnabled);
        final AtomicBoolean renamePending = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, WalUtils.EVENT_FILE_NAME) && renamePending.compareAndSet(true, false)) {
                    TestUtils.unchecked(() -> execute("RENAME TABLE t TO t_renamed"));
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            createTargetTable(3);
            update("UPDATE t SET v = 99 WHERE v >= 2");
            renamePending.set(true);
            drainWalQueue();
            Assert.assertFalse("the rename must have fired during apply, or the test proves nothing", renamePending.get());
            final TableToken renamed = engine.verifyTableName("t_renamed");
            Assert.assertFalse(
                    "a genuine target rename must recover, not suspend",
                    engine.getTableSequencerAPI().isSuspended(renamed)
            );
            assertNamed("t_renamed", """
                    2020-06-01T00:00:00.000000Z\t1
                    2020-06-02T00:00:00.000000Z\t99
                    2020-06-03T00:00:00.000000Z\t99
                    """);
        });
    }

    // The discriminating guard for the fix: this is the one shape where the target's *own* name
    // raises "table does not exist". ALTER resolves its target through tableExistsOrFail, which
    // consults the registry; the apply job takes the writer, the old name is then handed to a
    // replacement table, and the registry stops answering with the writer's token. That is exactly
    // what the token-refresh retry exists for, so the fix must let it through - lumping it in with
    // the sub-query failures would suspend a table that recovers on its own.
    @Test
    public void testAlterRecoversWhenTargetNameReusedDuringApply() throws Exception {
        Assume.assumeTrue(walEnabled);
        final AtomicBoolean swapPending = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, WalUtils.EVENT_FILE_NAME) && swapPending.compareAndSet(true, false)) {
                    TestUtils.unchecked(WalUpdateScalarSubqueryTest.this::swapInFreshTableUnderOldName);
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            createTargetTable(3);
            execute("ALTER TABLE t DROP PARTITION LIST '2020-06-01'");
            swapPending.set(true);
            drainWalQueue();
            Assert.assertFalse("the swap must have fired during apply, or the test proves nothing", swapPending.get());
            final TableToken original = engine.verifyTableName("t_old");
            Assert.assertFalse(
                    "the target's own name failing to resolve is what the retry is for; it must not suspend",
                    engine.getTableSequencerAPI().isSuspended(original)
            );
            assertNamed("t_old", """
                    2020-06-02T00:00:00.000000Z\t2
                    2020-06-03T00:00:00.000000Z\t3
                    """);
            assertNamed("t", "2020-07-01T00:00:00.000000Z\t7\n");
        });
    }

    // Renaming the sub-query's table is the same failure as dropping it - the name simply stops
    // resolving - and the finding names both triggers, so both are pinned. Legacy segment, for the
    // same reason as the dropped-table case above.
    @Test
    public void testUpdateWithRenamedSubqueryTableSuspendsTable() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            createTargetTable(5);
            execute("CREATE TABLE bounds (b TIMESTAMP)");
            execute("INSERT INTO bounds VALUES ('2020-01-01T00:00:00.000000Z')");
            final TableToken target = engine.verifyTableName("t");
            sequenceLegacySegmentUpdate("UPDATE t SET v = 99 WHERE v >= (SELECT count() FROM bounds)", "v");
            execute("RENAME TABLE bounds TO bounds_v2");
            tickWalQueue(8);
            Assert.assertTrue(
                    "WAL apply must suspend t so the stall is visible, not retry the txn forever",
                    engine.getTableSequencerAPI().isSuspended(target)
            );
        });
    }

    // Characterization of a sharp edge the scoped remap opens up, not an endorsement of it. Now
    // that a sub-query resolves its own table, it also opens a real reader on it, and a reader lock
    // is reachable: DROP, RENAME and TRUNCATE of a *non-WAL* table each hold one for the duration of
    // the DDL (CairoEngine.lockAll for DROP/RENAME, SqlCompilerImpl's non-WAL TRUNCATE arm). None of
    // that was reachable before, because the sub-query's table was never opened.
    //
    // Both halves are asserted on purpose. The first pins that the collision is loud: the table is
    // suspended and wal_tables() names the locked table, rather than the apply silently stopping.
    // The second pins that nothing was lost and that `bounds` really is the table being read --
    // unlock, RESUME WAL, and the acknowledged transaction applies with the rows it always meant.
    // A test asserting only suspension would still pass if the transaction had been dropped.
    //
    // The lock is taken and released explicitly around the drain, so there is no timing assumption.
    // The idiom is CheckpointTest#testCheckpointPrepareFailsOnLockedTableReader. Legacy segment, for
    // the same reason as the dropped-table case above.
    @Test
    public void testUpdateWithLockedSubqueryTableSuspendsTableAndResumes() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            createTargetTable(5);
            createBoundsTable(3);
            final TableToken target = engine.verifyTableName("t");
            final TableToken bounds = engine.verifyTableName("bounds");
            // sequenced but not yet applied, then the sub-query's table is reader-locked underneath
            sequenceLegacySegmentUpdate("UPDATE t SET v = 99 WHERE v >= (SELECT count() FROM bounds)", "v");
            Assert.assertTrue("could not lock readers on bounds", engine.lockReadersByTableToken(bounds));
            try {
                drainWalQueue();
                Assert.assertTrue(
                        "a locked sub-query table must suspend t, not stop applying it silently",
                        engine.getTableSequencerAPI().isSuspended(target)
                );
                printSql("SELECT errorMessage FROM wal_tables() WHERE name = 't'");
                TestUtils.assertContains(sink, "table is locked: bounds");
            } finally {
                engine.unlockReaders(bounds);
            }
            execute("ALTER TABLE t RESUME WAL");
            assertTable("""
                    2020-06-01T00:00:00.000000Z\t1
                    2020-06-02T00:00:00.000000Z\t2
                    2020-06-03T00:00:00.000000Z\t99
                    2020-06-04T00:00:00.000000Z\t99
                    2020-06-05T00:00:00.000000Z\t99
                    """);
        });
    }

    // The join half of the same policy, and the older one: the PG-style UPDATE ... FROM shape
    // (SqlParser.parseDmlUpdate turns the FROM list into join models, UpdateTest#testUpdateWithSymbolJoin
    // covers it non-WAL) reaches a second table through a join model rather than a nested sub-query,
    // and SqlCompilerImpl#generateUpdate has always rejected it for WAL tables. The sub-query
    // rejection above is that restriction extended to the other route in, so the two are now
    // consistent; the join message is kept separate because it is the more specific of the two.
    @Test
    public void testUpdateFromJoinIsRejectedBeforeReachingWalApply() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            createTargetTable(3);
            execute("CREATE TABLE picks (v INT)");
            execute("INSERT INTO picks VALUES (2)");
            assertQuery("UPDATE t SET v = 99 FROM picks WHERE t.v = picks.v")
                    .noLeakCheck()
                    .fails(0, "UPDATE statements with join are not supported yet for WAL tables");
            assertTable("""
                    2020-06-01T00:00:00.000000Z\t1
                    2020-06-02T00:00:00.000000Z\t2
                    2020-06-03T00:00:00.000000Z\t3
                    """);
        });
    }

    /**
     * Drains the WAL apply queue before every query assertion, so no test has to remember to. This
     * is the hook {@link io.questdb.test.AbstractCairoTest#assertQuery(CharSequence)} calls, and
     * {@code UpdateTest} overrides it exactly this way.
     */
    @Override
    protected void prepareForQueryAssertion() {
        if (walEnabled) {
            drainWalQueue();
        }
    }

    /**
     * Loads a second copy of {@link SqlException} with assertions disabled for it, so that
     * {@code position()} returns the carrier-local flyweight instead of the fresh instance its
     * {@code assert} allocates under {@code -ea}. Only that class is defined here; everything it
     * refers to still comes from the parent loader.
     */
    private static Class<?> loadSqlExceptionWithAssertionsDisabled() throws Exception {
        final String className = SqlException.class.getName();
        final byte[] bytes;
        try (InputStream is = SqlException.class.getResourceAsStream("SqlException.class")) {
            Assert.assertNotNull("SqlException.class must be readable as a resource", is);
            bytes = is.readAllBytes();
        }
        final ClassLoader loader = new ClassLoader(SqlException.class.getClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (!className.equals(name)) {
                    return super.loadClass(name, resolve);
                }
                Class<?> loaded = findLoadedClass(name);
                if (loaded == null) {
                    loaded = defineClass(name, bytes, 0, bytes.length);
                }
                if (resolve) {
                    resolveClass(loaded);
                }
                return loaded;
            }
        };
        loader.setClassAssertionStatus(className, false);
        return loader.loadClass(className);
    }

    /**
     * Asserts the two halves of the cross-table rule for one statement. Non-WAL is unaffected -
     * nothing is replicated, nothing is re-executed - so it must go on producing
     * {@code expectedRowsWhenAllowed}. On WAL the statement must be refused at compile time, with
     * the target left exactly as it was: rejection is only worth anything if it happens before the
     * transaction is acknowledged and sequenced.
     */
    private void assertCrossTableUpdate(
            String updateSql,
            String foreignTable,
            String expectedRowsWhenAllowed,
            String expectedRowsWhenRejected
    ) throws Exception {
        assertForeignSourceUpdate(
                updateSql,
                "UPDATE statements that reference another table are not supported for WAL tables [table="
                        + foreignTable + ']',
                expectedRowsWhenAllowed,
                expectedRowsWhenRejected
        );
    }

    /**
     * As {@link #assertCrossTableUpdate(String, String, String, String)}, but for a statement whose
     * foreign source is not a plain table name and so is refused with a different message.
     */
    private void assertForeignSourceUpdate(
            String updateSql,
            String expectedMessage,
            String expectedRowsWhenAllowed,
            String expectedRowsWhenRejected
    ) throws Exception {
        if (walEnabled) {
            assertQuery(updateSql)
                    .noLeakCheck()
                    .fails(0, expectedMessage);
            assertTable(expectedRowsWhenRejected);
        } else {
            update(updateSql);
            assertTable(expectedRowsWhenAllowed);
        }
    }

    private void assertNamed(String table, String expectedRows) throws Exception {
        assertQuery("SELECT ts, v FROM " + table)
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns("ts\tv\n" + expectedRows);
    }

    /**
     * Sequences a WAL UPDATE whose SQL names a table other than its target, without going through
     * the client compiler - which now refuses to compile one. The only way such a statement can
     * still reach WAL apply is as a segment written by a build from before that rejection existed
     * and left unapplied across the upgrade, so that is what this simulates. It writes the very same
     * {@code CMD_UPDATE_TABLE} SQL event the old compiler's UpdateOperation produced
     * (WalWriter#applyNonStructural -> WalEventWriter#appendSql), carrying the same stored SQL text,
     * table id and structure version the client would have supplied.
     * <p>
     * The alternative route - compiling through an execution context that reports
     * {@code isWalApplication()} - was not taken: that flag also switches the code generator onto
     * the reader-metadata branch and suppresses the SQL text the WAL event needs
     * (SqlCompilerImpl:3173), so it would have to be undone piecemeal afterwards. Writing the event
     * is both smaller and closer to the thing being simulated. Idiom from
     * WalTableFailureTest#testRecompileUpdateWithOutOfDateStructure.
     */
    private void sequenceLegacySegmentUpdate(String updateSql, String updatedColumn) throws Exception {
        final TableToken target = engine.verifyTableName("t");
        try (
                TableRecordMetadata metadata = sqlExecutionContext.getMetadataForWrite(target);
                WalWriter writer = engine.getWalWriter(target)
        ) {
            final UpdateOperation operation = new UpdateOperation(
                    target,
                    metadata.getTableId(),
                    metadata.getMetadataVersion(),
                    0,
                    new ObjList<>(updatedColumn)
            );
            operation.withSqlStatement(updateSql);
            operation.withContext(sqlExecutionContext);
            writer.apply(operation);
        }
    }

    private void swapInFreshTableUnderOldName() throws Exception {
        execute("RENAME TABLE t TO t_old");
        execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO t VALUES ('2020-07-01T00:00:00.000000Z',7)");
    }

    private void assertTable(String expectedRows) throws Exception {
        assertQuery("SELECT ts, v FROM t")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns("ts\tv\n" + expectedRows);
    }

    private void createBoundsTable(int rows) throws Exception {
        execute("CREATE TABLE bounds (b TIMESTAMP)");
        final StringBuilder values = new StringBuilder();
        for (int i = 1; i <= rows; i++) {
            if (i > 1) {
                values.append(',');
            }
            values.append("('2020-01-0").append(i).append("T00:00:00.000000Z')");
        }
        execute("INSERT INTO bounds VALUES " + values);
    }

    private void createTargetTable(int rows) throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY"
                + (walEnabled ? " WAL" : ""));
        final StringBuilder values = new StringBuilder();
        for (int i = 1; i <= rows; i++) {
            if (i > 1) {
                values.append(',');
            }
            values.append("('2020-06-0").append(i).append("T00:00:00.000000Z',").append(i).append(')');
        }
        execute("INSERT INTO t VALUES " + values);
        // Not covered by prepareForQueryAssertion(): the tests that arm a FilesFacade on the WAL
        // event file, and the ones that tick the apply job a bounded number of times, need the seed
        // rows already applied before the statement under test is sequenced.
        if (walEnabled) {
            drainWalQueue();
        }
    }
}
