package io.questdb.test;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.jit.JitUtil;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.AbstractCharSequence;
import io.questdb.std.str.MutableUtf16Sink;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.tools.BindVarTuple;
import io.questdb.test.tools.MutationStep;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Standalone fluent query-assertion builder. It is parameterized at construction with the
 * {@link CairoEngine}, default {@link SqlExecutionContext} and a pre-assertion hook, and carries its
 * own copies of the cursor-assertion and factory-memory-leak primitives so it does not depend on the
 * test base class. A chain reads as query -&gt; options -&gt; expectation:
 * <pre>
 * new QueryAssertion(engine, context, hook, "select * from x")
 *         .timestamp("ts")
 *         .expectSize()
 *         .returns(expected);
 * </pre>
 * The {@link #withEngine}, {@link #withCompiler} and {@link #withContext} steps override the
 * construction-time collaborators for a single assertion.
 */
public class QueryAssertion {
    private static final double EPSILON = 0.000001;
    private static final boolean[] FACTORY_TAGS = new boolean[MemoryTag.SIZE];
    private static final Log LOG = LogFactory.getLog(QueryAssertion.class);
    private final long[] SNAPSHOT = new long[MemoryTag.SIZE];
    private final Runnable prepareHook;
    private final CharSequence query;
    private final LongList rows = new LongList();
    private final StringSink sink = new StringSink();
    private Class<?> baseFactoryClass;
    private SqlCompiler compiler;
    private SqlExecutionContext context;
    private CharSequence ddl;
    private CharSequence ddl2;
    private ObjList<CharSequence> ddl2More;
    private ObjList<CharSequence> ddlMore;
    private CairoEngine engine;
    private boolean expectSize;
    private IntList expectedColumnTypes;
    private CharSequence expectedPlan;
    private CharSequence expectedTimestamp;
    private TimestampOrder expectedTimestampOrder = TimestampOrder.ASC;
    private RecordCursorFactory factory;
    private boolean fullFatJoins;
    private boolean isRandomAccessInferred;
    private boolean isTimestampInferred;
    private boolean leakCheck = true;
    private long memoryUsage = -1;
    private boolean memoryUsageCheck = true;
    private boolean overridden;
    private ObjList<CharSequence> planFragments;
    private ObjList<CharSequence> planFragmentsAbsent;
    private boolean sizeCanBeVariable;
    private boolean supportsRandomAccess = true;

    public QueryAssertion(CairoEngine engine, SqlExecutionContext context, Runnable prepareHook, CharSequence query) {
        this.engine = engine;
        this.context = context;
        this.prepareHook = prepareHook;
        this.query = query;
    }

    /**
     * External-factory mode. The builder asserts {@code factory}, which the caller built and owns, instead
     * of compiling SQL. It does NOT run the prepare hook, DDL, leak check, factory free, or the shared
     * memory-snapshot accounting, and it asserts each cursor through caller-local scratch buffers, so it is
     * safe to call concurrently from multiple worker threads. Pass the per-thread context via
     * {@link #withContext(SqlExecutionContext)}.
     */
    public QueryAssertion(CairoEngine engine, RecordCursorFactory factory) {
        this.engine = engine;
        this.context = null;
        this.prepareHook = null;
        this.query = null;
        this.factory = factory;
        this.leakCheck = false;
    }

    /**
     * Terminal: assert the query prints exactly {@code expected} from a SINGLE cursor pass,
     * skipping the second read, calculate-size, variable-column and factory-property checks that
     * {@link #returns(CharSequence)} performs. Use for non-deterministic projections whose output is
     * not stable across a re-read. Supports only {@link #ddl}, {@link #noLeakCheck},
     * {@link #withContext} and {@link #withEngine}.
     */
    public void returnsOnce(CharSequence expected) throws Exception {
        requireSingleShotCompatible();
        runPrepareHook();
        if (leakCheck) {
            assertMemoryLeak(() -> {
                runDdl();
                assertSql(engine, context, query, sink, expected);
            });
        } else {
            runDdl();
            assertSql(engine, context, query, sink, expected);
        }
    }

    public static boolean doubleEquals(double a, double b, double epsilon) {
        return a == b || Math.abs(a - b) < epsilon;
    }

    public static boolean doubleEquals(double a, double b) {
        return doubleEquals(a, b, EPSILON);
    }

    public static long getMemUsedByFactories() {
        long memUsed = 0;
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            if (FACTORY_TAGS[i]) {
                memUsed += Unsafe.getMemUsedByTag(i);
            }
        }
        return memUsed;
    }

    /**
     * Terminal: compile the query once, then run every {@link BindVarTuple} in turn. Each case
     * clears the bind-variable service, applies its binds and re-opens the cursor; a successful case is
     * checked with the same battery {@link #returns(CharSequence)} runs (re-read, calculate-size,
     * random-access and variable-column checks), a failing case asserts the error message contains the
     * case's fragment at the optional position. Factory options ({@link #timestamp}, {@link #expectSize},
     * {@link #noRandomAccess}, {@link #sizeMayVary}) set the per-case defaults; individual cases can
     * override order/random-access/size where a particular bind value flips the factory's behavior.
     * Supports only {@link #ddl}, {@link #timestamp}, {@link #expectSize}, {@link #noRandomAccess},
     * {@link #sizeMayVary}, {@link #noLeakCheck}, {@link #withContext} and {@link #withEngine}.
     */
    public void assertBinds(ObjList<BindVarTuple> cases) throws Exception {
        requireBindsCompatible();
        runPrepareHook();
        if (leakCheck) {
            assertMemoryLeak(() -> runBinds(cases));
        } else {
            runBinds(cases);
        }
    }

    /**
     * Terminal: assert ONLY the query's execution plan (EXPLAIN output) matches {@code expectedPlan}
     * exactly, without running the query for a result. Use for EXPLAIN/plan-routing tests that have no
     * row result to assert. Supports only {@link #ddl}, {@link #noLeakCheck}, {@link #withContext} and
     * {@link #withEngine}.
     */
    public void assertsPlan(CharSequence expectedPlan) throws Exception {
        requirePlanOnlyCompatible();
        runPrepareHook();
        if (leakCheck) {
            assertMemoryLeak(() -> {
                runDdl();
                assertExactPlan(expectedPlan);
            });
        } else {
            runDdl();
            assertExactPlan(expectedPlan);
        }
    }

    /**
     * Terminal: assert ONLY the query's execution plan (EXPLAIN output) contains every one of
     * {@code fragments}, without running the query for a result. The plan-only counterpart of
     * {@link #withPlanContaining}. Supports only {@link #ddl}, {@link #noLeakCheck},
     * {@link #withContext} and {@link #withEngine}.
     */
    public void assertsPlanContaining(CharSequence... fragments) throws Exception {
        requirePlanOnlyCompatible();
        final ObjList<CharSequence> list = new ObjList<>(fragments.length);
        for (CharSequence fragment : fragments) {
            list.add(fragment);
        }
        runPrepareHook();
        if (leakCheck) {
            assertMemoryLeak(() -> {
                runDdl();
                assertPlanContains(list);
            });
        } else {
            runDdl();
            assertPlanContains(list);
        }
    }

    /**
     * Terminal: assert ONLY the query's execution plan (EXPLAIN output) contains NONE of
     * {@code fragments}, without running the query for a result. The negative counterpart of
     * {@link #assertsPlanContaining}. Supports only {@link #ddl}, {@link #noLeakCheck},
     * {@link #withContext} and {@link #withEngine}.
     */
    public void assertsPlanNotContaining(CharSequence... fragments) throws Exception {
        requirePlanOnlyCompatible();
        final ObjList<CharSequence> list = new ObjList<>(fragments.length);
        for (CharSequence fragment : fragments) {
            list.add(fragment);
        }
        runPrepareHook();
        if (leakCheck) {
            assertMemoryLeak(() -> {
                runDdl();
                assertPlanNotContains(list);
            });
        } else {
            runDdl();
            assertPlanNotContains(list);
        }
    }

    /**
     * Also assert that column {@code columnIndex} of the result has type {@code columnType} (a
     * {@link ColumnType} constant). Repeatable for multiple columns. Asserted on the
     * {@link #returns(CharSequence)} / {@link #returns(CharSequence, CharSequence)} path.
     */
    public QueryAssertion columnType(int columnIndex, int columnType) {
        if (expectedColumnTypes == null) {
            expectedColumnTypes = new IntList();
        }
        expectedColumnTypes.add(columnIndex);
        expectedColumnTypes.add(columnType);
        return this;
    }

    /**
     * SQL to execute before the query (typically a CREATE TABLE / INSERT). Drains the WAL queue
     * afterwards when WAL is enabled by default, exactly as the legacy helpers do.
     */
    public QueryAssertion ddl(CharSequence ddl) {
        this.ddl = ddl;
        this.ddlMore = null;
        return this;
    }

    /**
     * Multi-statement variant of {@link #ddl(CharSequence)}. The statements run in order, each as a
     * separate {@code engine.execute()} call, before the query is compiled. Use when setup needs more
     * than one statement (e.g. a CREATE TABLE followed by an INSERT), which {@code engine.execute()}
     * cannot run as a single {@code ;}-separated batch.
     */
    public QueryAssertion ddl(CharSequence ddl, CharSequence... more) {
        this.ddl = ddl;
        this.ddlMore = new ObjList<>(more.length);
        for (int i = 0, n = more.length; i < n; i++) {
            this.ddlMore.add(more[i]);
        }
        return this;
    }

    /**
     * Assert that {@code cursor.size()} returns a known (non-negative) value. Off by default.
     */
    public QueryAssertion expectSize() {
        return expectSize(true);
    }

    /**
     * Variable-driven variant of {@link #expectSize()} for helper methods that receive the flag as
     * a parameter. {@code expectSize(true)} asserts a known size; {@code expectSize(false)} asserts
     * an undetermined (-1) size, the default.
     */
    public QueryAssertion expectSize(boolean expectSize) {
        this.expectSize = expectSize;
        return this;
    }

    /**
     * Terminal: assert that compiling/running the query fails with an error whose message contains
     * {@code contains} at position {@code errorPos}. Pass {@code errorPos = -1} to skip the position
     * check (or use {@link #failsWith}).
     */
    public void fails(int errorPos, CharSequence contains) throws Exception {
        requireFailsCompatible();
        if (ddl != null && fullFatJoins) {
            throw new IllegalStateException("fullFatJoins() is not supported together with ddl() on the fails() path");
        }
        if (leakCheck) {
            assertMemoryLeak(() -> failsNoLeak(errorPos, contains));
        } else {
            failsNoLeak(errorPos, contains);
        }
    }

    /**
     * Terminal: assert the query fails with an error message containing {@code contains}, without
     * checking the error position.
     */
    public void failsWith(CharSequence contains) throws Exception {
        fails(-1, contains);
    }

    /**
     * Force full-fat (non-optimized) join execution. Supports {@link #timestamp},
     * {@link #expectSize}, {@link #noRandomAccess} and the {@link #fails}/{@link #failsWith}
     * terminals. Not supported together with {@link #ddl} on the {@link #fails} path.
     */
    public QueryAssertion fullFatJoins() {
        return fullFatJoins(true);
    }

    /**
     * Variable-driven variant of {@link #fullFatJoins()} for helper methods that receive the flag
     * as a parameter (e.g. a test parameterized over normal vs full-fat execution).
     */
    public QueryAssertion fullFatJoins(boolean fullFatJoins) {
        this.fullFatJoins = fullFatJoins;
        return this;
    }

    /**
     * Infer random-access support from the compiled factory instead of asserting it. The assertion
     * adopts {@link RecordCursorFactory#recordCursorSupportsRandomAccess()} and exercises the matching
     * battery: when the factory supports random access it runs the full record-positioning checks, and
     * when it does not it asserts the random-access methods throw.
     * <p>
     * Like {@link #inferTimestamp()} this is a deliberately weaker check than the explicit default or
     * {@link #noRandomAccess()}: it confirms the factory's cursor behaves consistently with its own
     * declared capability but does NOT pin that capability, so it cannot catch a query whose
     * random-access support silently changes. Use it only for bulk assertions over a heterogeneous list
     * of queries; prefer the explicit default or {@link #noRandomAccess()} whenever the expected behavior
     * is known. Mutually exclusive with {@link #noRandomAccess()} / {@link #supportsRandomAccess(boolean)}
     * set to {@code false}.
     */
    public QueryAssertion inferRandomAccess() {
        this.isRandomAccessInferred = true;
        return this;
    }

    /**
     * Infer the designated timestamp from the compiled factory's metadata instead of pinning it
     * explicitly. When the factory declares a designated timestamp, the assertion adopts that column and
     * the scan direction the factory reports: a forward scan asserts ascending order, a backward scan
     * descending, and an indeterminate ({@link RecordCursorFactory#SCAN_DIRECTION_OTHER}, e.g. a
     * bind-variable {@code generate_series} step) scan asserts only that the column exists and is of
     * TIMESTAMP type. When the factory declares no timestamp, the step asserts nothing.
     * <p>
     * This is a deliberately weaker check than {@link #timestamp(CharSequence)}: it confirms the
     * timestamp data is self-consistent with the factory's own metadata (a forward-scanning factory does
     * emit ascending rows) but does NOT pin the expected column, so it cannot catch a query that
     * silently gains, loses or relabels its designated timestamp. Use it only for bulk assertions over a
     * heterogeneous list of queries whose per-query timestamp is not known up front; prefer the explicit
     * {@code timestamp*()} steps whenever the expected timestamp is known. Mutually exclusive with
     * {@link #timestamp}/{@link #timestampAsc}/{@link #timestampDesc}.
     */
    public QueryAssertion inferTimestamp() {
        this.isTimestampInferred = true;
        return this;
    }

    /**
     * Terminal: compile the query once, then for each {@link MutationStep} run its statement against the
     * live engine (typically an INSERT that grows the input table) and re-assert the SAME held factory
     * against that step's expected result. The stepwise generalization of {@link #mutateWith}: it holds
     * one factory across many incremental mutations, asserting after each, to verify a shared factory
     * keeps producing correct (non-stale) results as its input grows. Supports {@link #ddl},
     * {@link #expectSize}, {@link #noRandomAccess}, {@link #sizeMayVary}, {@link #noLeakCheck},
     * {@link #withContext} and {@link #withEngine}.
     */
    public void mutateStepwise(ObjList<MutationStep> steps) throws Exception {
        requireMutateStepwiseCompatible();
        runPrepareHook();
        if (leakCheck) {
            assertMemoryLeak(() -> runMutationSteps(steps));
        } else {
            runMutationSteps(steps);
        }
    }

    /**
     * SQL to execute after the first assertion, re-checking the same factory against the second
     * expected value. Requires the two-argument {@link #returns(CharSequence, CharSequence)} (or
     * {@link #returnsRecords(Record[], Record[])}) terminal.
     */
    public QueryAssertion mutateWith(CharSequence ddl2) {
        this.ddl2 = ddl2;
        this.ddl2More = null;
        return this;
    }

    /**
     * Multi-statement variant of {@link #mutateWith(CharSequence)}. The statements run in order,
     * each as a separate {@code engine.execute()} call, before the second assertion re-checks the
     * same factory. Use when the mutation needs more than one statement (e.g. drop then recreate a
     * table), which {@code engine.execute()} cannot run as a single {@code ;}-separated batch.
     */
    public QueryAssertion mutateWith(CharSequence ddl2, CharSequence... more) {
        this.ddl2 = ddl2;
        this.ddl2More = new ObjList<>(more.length);
        for (int i = 0, n = more.length; i < n; i++) {
            this.ddl2More.add(more[i]);
        }
        return this;
    }

    /**
     * Skip the surrounding memory-leak check. Use inside a test body that is already wrapped in
     * {@code assertMemoryLeak(...)}.
     */
    public QueryAssertion noLeakCheck() {
        this.leakCheck = false;
        return this;
    }

    /**
     * Skip the post-close factory memory-usage check (the assertion that a cursor releases all but
     * up to 64 KiB of native memory after it is closed). Use for queries whose factory legitimately
     * retains more than that bound, where the check would be a false positive. The check runs by
     * default.
     */
    public QueryAssertion noMemoryUsageCheck() {
        this.memoryUsageCheck = false;
        return this;
    }

    /**
     * Declare that the factory does not support random access, skipping the random-access record
     * checks. Random access is exercised by default.
     */
    public QueryAssertion noRandomAccess() {
        this.supportsRandomAccess = false;
        return this;
    }

    public void printFactoryMemoryUsageDiff() {
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            if (!FACTORY_TAGS[i]) {
                continue;
            }

            long value = Unsafe.getMemUsedByTag(i) - SNAPSHOT[i];

            if (value != 0L) {
                System.out.println(MemoryTag.nameOf(i) + ":" + value);
            }
        }
    }

    /**
     * Terminal: assert the query produces exactly {@code expected}.
     */
    public void returns(CharSequence expected) throws Exception {
        if (ddl2 != null) {
            throw new IllegalStateException("mutateWith(...) requires returns(before, after)");
        }
        dispatch(expected, null);
    }

    /**
     * Terminal: assert the query produces {@code expectedBefore}, then run the {@link #mutateWith}
     * statement and assert the same factory now produces {@code expectedAfter}.
     */
    public void returns(CharSequence expectedBefore, CharSequence expectedAfter) throws Exception {
        if (ddl2 == null) {
            throw new IllegalStateException("returns(before, after) requires mutateWith(...)");
        }
        dispatch(expectedBefore, expectedAfter);
    }

    private static void assertSql(
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            CharSequence sql,
            MutableUtf16Sink sink,
            CharSequence expected
    ) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            TestUtils.printSql(compiler, sqlExecutionContext, sql, sink);
            TestUtils.assertEquals(expected, sink);
        }
    }

    /**
     * Terminal: assert the query produces the given raw records. Always leak-checked; supports only
     * {@link #ddl}, {@link #timestamp}, {@link #mutateWith} and {@link #expectSize}.
     */
    public void returnsRecords(Record[] expected) throws Exception {
        if (ddl2 != null) {
            throw new IllegalStateException("mutateWith(...) requires returnsRecords(before, after)");
        }
        requireRecordPathCompatible();
        assertReturnsRecords(expected, null);
    }

    /**
     * Terminal: the record-array counterpart of {@link #returns(CharSequence, CharSequence)}.
     */
    public void returnsRecords(Record[] expectedBefore, Record[] expectedAfter) throws Exception {
        if (ddl2 == null) {
            throw new IllegalStateException("returnsRecords(before, after) requires mutateWith(...)");
        }
        requireRecordPathCompatible();
        assertReturnsRecords(expectedBefore, expectedAfter);
    }

    /**
     * Allow {@code cursor.size()} to be reported as unknown (-1) in some passes. Off by default.
     */
    public QueryAssertion sizeMayVary() {
        return sizeMayVary(true);
    }

    /**
     * Variable-driven variant of {@link #sizeMayVary()} for helper methods that receive the flag as
     * a parameter.
     */
    public QueryAssertion sizeMayVary(boolean sizeCanBeVariable) {
        this.sizeCanBeVariable = sizeCanBeVariable;
        return this;
    }

    public void snapshotMemoryUsage() {
        if (!memoryUsageCheck) {
            return;
        }
        memoryUsage = getMemUsedByFactories();
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            SNAPSHOT[i] = Unsafe.getMemUsedByTag(i);
        }
    }

    /**
     * Variable-driven variant of {@link #noRandomAccess()} for helper methods that receive the
     * flag as a parameter.
     */
    public QueryAssertion supportsRandomAccess(boolean supportsRandomAccess) {
        this.supportsRandomAccess = supportsRandomAccess;
        return this;
    }

    /**
     * Assert the result has a designated timestamp on the given column. Without this step the
     * result is asserted to have no designated timestamp.
     */
    public QueryAssertion timestamp(CharSequence column) {
        this.expectedTimestamp = column;
        this.expectedTimestampOrder = TimestampOrder.ASC;
        return this;
    }

    /**
     * Like {@link #timestamp} but also assert ascending (forward) scan order.
     */
    public QueryAssertion timestampAsc(CharSequence column) {
        this.expectedTimestamp = column;
        this.expectedTimestampOrder = TimestampOrder.ASC;
        return this;
    }

    /**
     * Like {@link #timestamp} but also assert descending (backward) scan order.
     */
    public QueryAssertion timestampDesc(CharSequence column) {
        this.expectedTimestamp = column;
        this.expectedTimestampOrder = TimestampOrder.DESC;
        return this;
    }

    /**
     * Also assert that the compiled factory's base factory (see
     * {@link RecordCursorFactory#getBaseFactory()}) is exactly {@code baseFactoryClass}. Use to pin the
     * execution strategy a query must compile to, e.g. that a filtered scan routes through
     * {@code AsyncFilteredRecordCursorFactory}. Asserted on the {@link #returns(CharSequence)} /
     * {@link #returns(CharSequence, CharSequence)} path.
     */
    public QueryAssertion withBaseFactoryClass(Class<?> baseFactoryClass) {
        this.baseFactoryClass = baseFactoryClass;
        return this;
    }

    /**
     * Compile the query with a specific {@link SqlCompiler} instead of one borrowed from the engine.
     * Routes the assertion through the single-factory cursor path (no {@code calculateSize()} pass).
     * Supports {@link #ddl}, {@link #timestamp}, {@link #expectSize} and {@link #noRandomAccess}.
     */
    public QueryAssertion withCompiler(SqlCompiler compiler) {
        this.compiler = compiler;
        this.overridden = true;
        return this;
    }

    /**
     * Run with a specific {@link SqlExecutionContext} instead of the construction-time one.
     */
    public QueryAssertion withContext(SqlExecutionContext context) {
        this.context = context;
        this.overridden = true;
        return this;
    }

    /**
     * Run against a specific {@link CairoEngine} instead of the construction-time one.
     */
    public QueryAssertion withEngine(CairoEngine engine) {
        this.engine = engine;
        this.overridden = true;
        return this;
    }

    /**
     * Also assert the query's execution plan (EXPLAIN output) matches {@code expectedPlan} exactly.
     * Honors {@link #noLeakCheck()}; cannot be combined with {@link #fullFatJoins()} or
     * {@link #withCompiler}.
     */
    public QueryAssertion withPlan(CharSequence expectedPlan) {
        this.expectedPlan = expectedPlan;
        return this;
    }

    /**
     * Also assert the query's execution plan (EXPLAIN output) contains every one of {@code fragments}.
     * Honors {@link #noLeakCheck()}; cannot be combined with {@link #fullFatJoins()} or
     * {@link #withCompiler}.
     */
    public QueryAssertion withPlanContaining(CharSequence... fragments) {
        final ObjList<CharSequence> list = new ObjList<>(fragments.length);
        for (CharSequence fragment : fragments) {
            if (fragment != null) {
                list.add(fragment);
            }
        }
        this.planFragments = list;
        return this;
    }

    /**
     * Also assert the query's execution plan (EXPLAIN output) contains NONE of {@code fragments}.
     * The negative counterpart of {@link #withPlanContaining}. Honors {@link #noLeakCheck()}; cannot
     * be combined with {@link #fullFatJoins()} or {@link #withCompiler}.
     */
    public QueryAssertion withPlanNotContaining(CharSequence... fragments) {
        final ObjList<CharSequence> list = new ObjList<>(fragments.length);
        for (CharSequence fragment : fragments) {
            list.add(fragment);
        }
        this.planFragmentsAbsent = list;
        return this;
    }

    private static void assertCalculateSize(RecordCursorFactory factory, SqlExecutionContext sqlExecutionContext) throws SqlException {
        long size;
        SqlExecutionCircuitBreaker circuitBreaker = sqlExecutionContext.getCircuitBreaker();
        RecordCursor.Counter counter = new RecordCursor.Counter();

        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            cursor.calculateSize(circuitBreaker, counter);
            Assert.assertFalse("hasNext() returned true after calculateSize exhausted the cursor", cursor.hasNext());
            size = counter.get();
            long preComputeStateSize = cursor.preComputedStateSize();
            cursor.toTop();
            Assert.assertEquals(preComputeStateSize, cursor.preComputedStateSize());
            counter.clear();
            cursor.calculateSize(circuitBreaker, counter);
            Assert.assertFalse("hasNext() returned true after calculateSize exhausted the cursor", cursor.hasNext());
            long sizeAfterToTop = counter.get();

            Assert.assertEquals(size, sizeAfterToTop);
        }

        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            counter.clear();
            cursor.calculateSize(circuitBreaker, counter);
            long sizeAfterReopen = counter.get();

            Assert.assertEquals(size, sizeAfterReopen);
        }
    }

    private static void assertSymbolColumnThreadSafety(int numberOfIterations, int symbolColumnCount, ObjList<SymbolTable> symbolTables, int[] symbolTableKeySnapshot, String[][] symbolTableValueSnapshot) {
        final Rnd rnd = TestUtils.generateRandom(null);
        for (int i = 0; i < numberOfIterations; i++) {
            int symbolColIndex = rnd.nextInt(symbolColumnCount);
            SymbolTable symbolTable = symbolTables.getQuick(symbolColIndex);
            int max = symbolTableKeySnapshot[symbolColIndex] + 1;
            // max could be -1 meaning we have nulls; max can also be 0, meaning only one symbol value
            // basing boundary on 2 we convert -1 tp 1 and 0 to 2
            int key = rnd.nextInt(max + 1) - 1;
            String expected = symbolTableValueSnapshot[symbolColIndex][key + 1];
            TestUtils.assertEquals(expected, symbolTable.valueOf(key));
            // now test static symbol table
            if (expected != null && symbolTable instanceof StaticSymbolTable staticSymbolTable) {
                Assert.assertEquals(key, staticSymbolTable.keyOf(expected));
            }
        }
    }

    private static ApplyWal2TableJob createWalApplyJob(CairoEngine engine) {
        return new ApplyWal2TableJob(engine, 0);
    }

    private static void drainWalQueue(CairoEngine engine) {
        try (ApplyWal2TableJob walApplyJob = createWalApplyJob(engine)) {
            drainWalQueue(walApplyJob, engine);
        }
    }

    private static void drainWalQueue(ApplyWal2TableJob walApplyJob, CairoEngine engine) {
        CheckWalTransactionsJob checkWalTransactionsJob = new CheckWalTransactionsJob(engine);
        drainWalQueue0(walApplyJob);
        if (checkWalTransactionsJob.run(0)) {
            drainWalQueue0(walApplyJob);
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private static void drainWalQueue0(ApplyWal2TableJob walApplyJob) {
        while (walApplyJob.run(0)) ;
    }

    private static void releaseInactive(CairoEngine engine) {
        engine.releaseInactive();
        engine.releaseInactiveTableSequencers();
        engine.resetNameRegistryMemory();
        engine.getTxnScoreboardPool().clear();
        // Drain the per-workload memory-tracker pool: a tracker acquired by a
        // registered query is returned to the pool (not freed) and would
        // otherwise trip the leak checker as a retained native block.
        engine.getMemoryTrackerProvider().clear();
        Assert.assertEquals("busy writer count", 0, engine.getBusyWriterCount());
        Assert.assertEquals("busy reader count", 0, engine.getBusyReaderCount());
    }

    private static void testStringsLong256AndBinary(RecordMetadata metadata, RecordCursor cursor) {
        Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                switch (ColumnType.tagOf(metadata.getColumnType(i))) {
                    case ColumnType.STRING:
                        CharSequence s = record.getStrA(i);
                        if (s != null) {
                            Assert.assertEquals(s.length(), record.getStrLen(i));
                            CharSequence b = record.getStrB(i);
                            if (b instanceof AbstractCharSequence) {
                                // AbstractCharSequence are usually mutable. We cannot have a same mutable instance for A and B
                                Assert.assertNotSame("Expected string instances to be different for getStr and getStrB", s, b);
                            }
                        } else {
                            Assert.assertNull(record.getStrB(i));
                            Assert.assertEquals(TableUtils.NULL_LEN, record.getStrLen(i));
                        }
                        break;
                    case ColumnType.BINARY:
                        BinarySequence bs = record.getBin(i);
                        if (bs != null) {
                            Assert.assertEquals(bs.length(), record.getBinLen(i));
                        } else {
                            Assert.assertEquals(TableUtils.NULL_LEN, record.getBinLen(i));
                        }
                        break;
                    case ColumnType.LONG256:
                        Long256 l1 = record.getLong256A(i);
                        Long256 l2 = record.getLong256B(i);
                        if (l1 == Long256Impl.NULL_LONG256) {
                            Assert.assertSame(l1, l2);
                        } else {
                            Assert.assertNotSame(l1, l2);
                        }
                        Assert.assertEquals(l1.getLong0(), l2.getLong0());
                        Assert.assertEquals(l1.getLong1(), l2.getLong1());
                        Assert.assertEquals(l1.getLong2(), l2.getLong2());
                        Assert.assertEquals(l1.getLong3(), l2.getLong3());
                        break;
                    default:
                        break;
                }
            }
        }
    }

    private static void testSymbolAPI(RecordMetadata metadata, RecordCursor cursor, boolean fragmentedSymbolTables) {
        IntList symbolIndexes = null;
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (ColumnType.isSymbol(metadata.getColumnType(i))) {
                if (symbolIndexes == null) {
                    symbolIndexes = new IntList();
                }
                symbolIndexes.add(i);
            }
        }

        if (symbolIndexes != null) {

            // create new symbol tables and make sure they are not the same
            // as the default ones

            ObjList<SymbolTable> clonedSymbolTables = new ObjList<>();
            ObjList<SymbolTable> originalSymbolTables = new ObjList<>();
            int[] symbolTableKeySnapshot = new int[symbolIndexes.size()];
            String[][] symbolTableValueSnapshot = new String[symbolIndexes.size()][];
            try {
                cursor.toTop();
                if (!fragmentedSymbolTables && cursor.hasNext()) {
                    for (int i = 0, n = symbolIndexes.size(); i < n; i++) {
                        final int columnIndex = symbolIndexes.getQuick(i);
                        originalSymbolTables.add(cursor.getSymbolTable(columnIndex));
                    }

                    // take a snapshot of symbol tables
                    // multiple passes over the same cursor, if not very efficient, we
                    // can swap loops around
                    int sumOfMax = 0;
                    for (int i = 0, n = symbolIndexes.size(); i < n; i++) {
                        cursor.toTop();
                        final Record rec = cursor.getRecord();
                        final int column = symbolIndexes.getQuick(i);
                        int max = -1;
                        while (cursor.hasNext()) {
                            max = Math.max(max, rec.getInt(column));
                        }
                        String[] values = new String[max + 2];
                        final SymbolTable symbolTable = cursor.getSymbolTable(column);
                        for (int k = -1; k <= max; k++) {
                            values[k + 1] = Chars.toString(symbolTable.valueOf(k));
                        }
                        symbolTableKeySnapshot[i] = max;
                        symbolTableValueSnapshot[i] = values;
                        sumOfMax += max;
                    }

                    // We grab clones after iterating through the symbol values due to
                    // the cache warm up required by Cast*ToSymbolFunctionFactory functions.
                    for (int i = 0, n = symbolIndexes.size(); i < n; i++) {
                        final int columnIndex = symbolIndexes.getQuick(i);
                        SymbolTable tab = cursor.newSymbolTable(columnIndex);
                        Assert.assertNotNull(tab);
                        clonedSymbolTables.add(tab);
                    }

                    // Now start two threads, one will be using normal symbol table,
                    // another will be using a clone. Threads will randomly check that
                    // symbol table is able to convert keys to values without problems

                    int numberOfIterations = sumOfMax * 2;
                    int symbolColumnCount = symbolIndexes.size();
                    int workerCount = 2;
                    CyclicBarrier barrier = new CyclicBarrier(workerCount);
                    SOCountDownLatch doneLatch = new SOCountDownLatch(workerCount);
                    AtomicInteger errorCount = new AtomicInteger(0);

                    // thread that is hitting clones
                    new Thread(() -> {
                        try {
                            TestUtils.await(barrier);
                            assertSymbolColumnThreadSafety(numberOfIterations, symbolColumnCount, clonedSymbolTables, symbolTableKeySnapshot, symbolTableValueSnapshot);
                        } catch (Throwable e) {
                            errorCount.incrementAndGet();
                            //noinspection CallToPrintStackTrace
                            e.printStackTrace();
                        } finally {
                            doneLatch.countDown();
                        }
                    }).start();

                    // thread that is hitting the original symbol tables
                    new Thread(() -> {
                        try {
                            TestUtils.await(barrier);
                            assertSymbolColumnThreadSafety(numberOfIterations, symbolColumnCount, originalSymbolTables, symbolTableKeySnapshot, symbolTableValueSnapshot);
                        } catch (Throwable e) {
                            errorCount.incrementAndGet();
                            //noinspection CallToPrintStackTrace
                            e.printStackTrace();
                        } finally {
                            doneLatch.countDown();
                        }
                    }).start();

                    doneLatch.await();

                    Assert.assertEquals(0, errorCount.get());
                }

                cursor.toTop();
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    for (int i = 0, n = symbolIndexes.size(); i < n; i++) {
                        int column = symbolIndexes.getQuick(i);
                        SymbolTable symbolTable = cursor.getSymbolTable(column);
                        if (symbolTable instanceof StaticSymbolTable) {
                            CharSequence sym = Chars.toString(record.getSymA(column));
                            int value = record.getInt(column);
                            if (((StaticSymbolTable) symbolTable).containsNullValue() && value == ((StaticSymbolTable) symbolTable).getSymbolCount()) {
                                Assert.assertEquals(Integer.MIN_VALUE, ((StaticSymbolTable) symbolTable).keyOf(sym));
                            } else {
                                Assert.assertEquals(value, ((StaticSymbolTable) symbolTable).keyOf(sym));
                            }
                            TestUtils.assertEquals(sym, symbolTable.valueOf(value));
                        } else {
                            final int value = record.getInt(column);
                            TestUtils.assertEquals(record.getSymA(column), symbolTable.valueOf(value));
                        }
                    }
                }
            } finally {
                Misc.freeObjListIfCloseable(clonedSymbolTables);
            }
        }
    }

    private void assertBaseFactoryClass(RecordCursorFactory factory) {
        if (baseFactoryClass != null) {
            Assert.assertEquals(baseFactoryClass, factory.getBaseFactory().getClass());
        }
    }

    private void assertBindFailure(RecordCursorFactory factory, BindVarTuple testCase, SqlExecutionContext executionContext) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(executionContext)) {
            sink.clear();
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                TestUtils.println(record, factory.getMetadata(), sink);
            }
            Assert.fail(testCase.getDescription() + ": expected failure but the query succeeded");
        } catch (Throwable e) {
            if (!(e instanceof FlyweightMessageContainer container)) {
                throw e;
            }
            TestUtils.assertContains(testCase.getDescription(), container.getFlyweightMessage(), testCase.getExpected());
            if (testCase.getErrorPosition() != BindVarTuple.ANY_POSITION) {
                Assert.assertEquals(testCase.getDescription(), testCase.getErrorPosition(), container.getPosition());
            }
        }
    }

    private void assertBindSuccess(RecordCursorFactory factory, BindVarTuple testCase, SqlExecutionContext executionContext) throws SqlException {
        final boolean randomAccess = testCase.getRandomAccessOverride() != null
                ? testCase.getRandomAccessOverride()
                : supportsRandomAccess;
        final boolean sizeExpected = testCase.getExpectSizeOverride() != null
                ? testCase.getExpectSizeOverride()
                : expectSize;
        assertTimestamp(expectedTimestamp, resolveTimestampOrder(testCase), factory, executionContext);
        assertCursor(testCase.getExpected(), factory, randomAccess, sizeExpected, sizeCanBeVariable, executionContext);
        // re-open with the same binds and reproduce the same outcome
        assertCursor(testCase.getExpected(), factory, randomAccess, sizeExpected, sizeCanBeVariable, executionContext);
        assertVariableColumns(factory, executionContext);
        assertCalculateSize(factory, executionContext);
    }

    private void assertColumnTypes(RecordCursorFactory factory) {
        if (expectedColumnTypes != null) {
            final RecordMetadata metadata = factory.getMetadata();
            for (int i = 0, n = expectedColumnTypes.size(); i < n; i += 2) {
                final int columnIndex = expectedColumnTypes.getQuick(i);
                Assert.assertEquals(expectedColumnTypes.getQuick(i + 1), metadata.getColumnType(columnIndex));
            }
        }
    }

    private boolean assertCursor(
            CharSequence expected,
            boolean supportsRandomAccess,
            boolean sizeExpected,
            boolean sizeCanBeVariable,
            RecordCursor cursor,
            RecordMetadata metadata,
            boolean fragmentedSymbolTables,
            SqlExecutionContext sqlExecutionContext
    ) {
        return assertCursor(
                expected,
                supportsRandomAccess,
                sizeExpected,
                sizeCanBeVariable,
                cursor,
                metadata,
                sink,
                rows,
                fragmentedSymbolTables,
                sqlExecutionContext
        );
    }

    private boolean assertCursor(
            CharSequence expected,
            boolean supportsRandomAccess,
            boolean sizeExpected,
            boolean sizeCanBeVariable,
            RecordCursor cursor,
            RecordMetadata metadata,
            StringSink sink,
            LongList rows,
            boolean fragmentedSymbolTables,
            SqlExecutionContext sqlExecutionContext
    ) {
        long cursorSizeBeforeFetch = cursor.size();
        if (expected == null) {
            Assert.assertFalse(cursor.hasNext());
            cursor.toTop();
            Assert.assertFalse(cursor.hasNext());
            return true;
        }

        TestUtils.assertCursor(expected, cursor, metadata, true, sink);
        long preComputerStateSize = cursor.preComputedStateSize();

        testSymbolAPI(metadata, cursor, fragmentedSymbolTables);
        cursor.toTop();
        Assert.assertEquals(preComputerStateSize, cursor.preComputedStateSize());
        testStringsLong256AndBinary(metadata, cursor);

        // test API where the same record is being updated by cursor
        cursor.toTop();
        Assert.assertEquals(preComputerStateSize, cursor.preComputedStateSize());
        Record record = cursor.getRecord();
        Assert.assertNotNull(record);
        sink.clear();
        CursorPrinter.println(metadata, sink);
        long count = 0;
        long cursorSize = cursor.size();
        while (cursor.hasNext()) {
            TestUtils.println(record, metadata, sink);
            count++;
        }

        final Rnd rnd = TestUtils.generateRandom(LOG);
        int skip = count > 0 && rnd.nextBoolean() ? rnd.nextInt((int) count) : 0;
        int k = 0;
        while (k < skip && cursor.hasNext()) {
            k++;
        }
        sink.clear();
        cursor.toTop();
        TestUtils.assertCursor(expected, cursor, metadata, true, sink);

        if (!sizeCanBeVariable) {
            if (sizeExpected) {
                Assert.assertTrue("Concrete cursor size expected but was -1 (remove .expectSize())", cursorSize != -1);
            } else {
                Assert.assertTrue("Invalid/undetermined cursor size expected but was " + cursorSize + " (add .expectSize())", cursorSize <= 0);
            }
        }
        if (cursorSize != -1) {
            Assert.assertEquals("Expected: counted with hasNext(), actual: cursor.size()", count, cursorSize);
            if (cursorSizeBeforeFetch != -1) {
                Assert.assertEquals("Expected: cursor size before fetch, actual: cursor size after fetch",
                        cursorSizeBeforeFetch, cursorSize);
            }
        }
        if (count > 0) {
            int countReducedToInt = (int) Math.min(Integer.MAX_VALUE, count);
            RecordCursor.Counter counter = new RecordCursor.Counter();
            cursor.toTop();
            skip = rnd.nextBoolean() ? rnd.nextInt(countReducedToInt) : 0;
            while (counter.get() < skip && cursor.hasNext()) {
                counter.inc();
            }
            SqlExecutionCircuitBreaker breaker =
                    sqlExecutionContext != null ? sqlExecutionContext.getCircuitBreaker() : null;
            cursor.calculateSize(breaker, counter);
            Assert.assertEquals(
                    String.format("Skip %,d then calculateSize(). Expect: as counted with hasNext(), actual: cursor.calculateSize()", skip),
                    count, counter.get());

            cursor.toTop();
            counter.set(count + 1);
            cursor.skipRows(counter);
            Assert.assertEquals("skipRows(rowCountPlusOne) didn't leave the counter at 1", 1, counter.get());
            Assert.assertFalse("hasNext() returned true after skipRows exhausted the cursor", cursor.hasNext());

            if (count > 1) {
                skip = rnd.nextInt(countReducedToInt / 2);
                counter.set(skip);
                cursor.toTop();
                cursor.skipRows(counter);
                Assert.assertEquals("skipRows(lessThanRowCount) didn't bring the counter to 0", 0, counter.get());
                long remaining = 0;
                String countMethod;
                if (rnd.nextBoolean()) {
                    countMethod = "calculateSize()";
                    counter.clear();
                    cursor.calculateSize(breaker, counter);
                    remaining = counter.get();
                } else {
                    countMethod = "hasNext()";
                    while (cursor.hasNext()) {
                        remaining++;
                    }
                }
                Assert.assertEquals(
                        "skipRows(lessThanRowCount) didn't leave the correct number of remaining rows." +
                                " Remaining rows counted using " + countMethod,
                        count, skip + remaining);
            }
        } else {
            cursor.toTop();
            Assert.assertFalse(cursor.hasNext());
        }

        TestUtils.assertEquals(expected, sink);

        if (supportsRandomAccess) {
            cursor.toTop();
            Assert.assertEquals(preComputerStateSize, cursor.preComputedStateSize());
            sink.clear();
            rows.clear();
            while (cursor.hasNext()) {
                rows.add(record.getRowId());
            }

            final Record rec = cursor.getRecord();
            CursorPrinter.println(metadata, sink);
            for (int i = 0, n = rows.size(); i < n; i++) {
                cursor.recordAt(rec, rows.getQuick(i));
                TestUtils.println(rec, metadata, sink);
            }

            TestUtils.assertEquals(expected, sink);

            sink.clear();

            final Record factRec = cursor.getRecordB();
            CursorPrinter.println(metadata, sink);
            for (int i = 0, n = rows.size(); i < n; i++) {
                cursor.recordAt(factRec, rows.getQuick(i));
                TestUtils.println(factRec, metadata, sink);
            }

            TestUtils.assertEquals(expected, sink);

            // test that absolute positioning of record does not affect the state of record cursor
            if (rows.size() > 0) {
                sink.clear();

                cursor.toTop();
                Assert.assertEquals(preComputerStateSize, cursor.preComputedStateSize());
                int target = rows.size() / 2;
                CursorPrinter.println(metadata, sink);
                while (target-- > 0 && cursor.hasNext()) {
                    TestUtils.println(record, metadata, sink);
                }

                // no obliterated record with absolute positioning
                for (int i = 0, n = rows.size(); i < n; i++) {
                    cursor.recordAt(factRec, rows.getQuick(i));
                }

                // now continue normal fetch
                while (cursor.hasNext()) {
                    TestUtils.println(record, metadata, sink);
                }

                TestUtils.assertEquals(expected, sink);

                // now test that cursor.hasNext() won't affect the state of recordB
                sink.clear();
                CursorPrinter.println(metadata, sink);
                cursor.toTop();
                target = rows.size() / 2;
                boolean cursorExhausted = false;
                for (int i = 0, n = target; i < n; i++) {
                    cursor.recordAt(factRec, rows.getQuick(i));
                    // intentionally calling hasNext() twice: we want to advance the cursor position,
                    // but we do *NOT* want to call it in step-lock with recordAt()
                    if (!cursorExhausted) {
                        cursorExhausted = !cursor.hasNext();
                    }
                    if (!cursorExhausted) {
                        cursorExhausted = !cursor.hasNext();
                    }
                    TestUtils.println(factRec, metadata, sink);
                }
                cursor.toTop();
                for (int i = target, n = rows.size(); i < n; i++) {
                    // in the 2nd part, we are intentionally not calling hasNext() at all
                    cursor.recordAt(factRec, rows.getQuick(i));
                    TestUtils.println(factRec, metadata, sink);
                }
                TestUtils.assertEquals(expected, sink);
            }
        } else {
            try {
                cursor.getRecordB();
                Assert.fail();
            } catch (UnsupportedOperationException ignore) {
            }

            try {
                cursor.recordAt(record, 0);
                Assert.fail();
            } catch (UnsupportedOperationException ignore) {
            }
        }
        return false;
    }

    private void assertCursor(
            CharSequence expected,
            RecordCursorFactory factory,
            boolean supportsRandomAccess,
            boolean sizeExpected,
            boolean sizeCanBeVariable, // this means size() can either be -1 in some cases or known in others
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        supportsRandomAccess = resolveRandomAccess(supportsRandomAccess, factory);
        boolean cursorAsserted;
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertEquals("supports random access (" + (factory.recordCursorSupportsRandomAccess() ? "remove" : "add") + " .noRandomAccess())", supportsRandomAccess, factory.recordCursorSupportsRandomAccess());
            cursorAsserted = assertCursor(expected, supportsRandomAccess, sizeExpected, sizeCanBeVariable, cursor, factory.getMetadata(), factory.fragmentedSymbolTables(), sqlExecutionContext);
        }

        assertFactoryMemoryUsage();

        if (cursorAsserted) {
            return;
        }

        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            testSymbolAPI(factory.getMetadata(), cursor, factory.fragmentedSymbolTables());
        }

        assertFactoryMemoryUsage();
    }

    private void assertCursorRawRecords(Record[] expected, RecordCursorFactory factory, SqlExecutionContext sqlExecutionContext, boolean expectSize) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            if (expected == null) {
                Assert.assertFalse(cursor.hasNext());
                cursor.toTop();
                Assert.assertFalse(cursor.hasNext());
                return;
            }

            final long rowsCount = cursor.size();
            if (expectSize) {
                Assert.assertEquals(rowsCount, expected.length);
            }

            RecordMetadata metadata = factory.getMetadata();

            testSymbolAPI(metadata, cursor, factory.fragmentedSymbolTables());
            cursor.toTop();
            testStringsLong256AndBinary(metadata, cursor);

            cursor.toTop();
            final Record record = cursor.getRecord();
            Assert.assertNotNull(record);
            int expectedRow = 0;
            while (cursor.hasNext()) {
                for (int col = 0, n = metadata.getColumnCount(); col < n; col++) {
                    switch (ColumnType.tagOf(metadata.getColumnType(col))) {
                        case ColumnType.BOOLEAN:
                            Assert.assertEquals(expected[expectedRow].getBool(col), record.getBool(col));
                            break;
                        case ColumnType.BYTE:
                            Assert.assertEquals(expected[expectedRow].getByte(col), record.getByte(col));
                            break;
                        case ColumnType.SHORT:
                            Assert.assertEquals(expected[expectedRow].getShort(col), record.getShort(col));
                            break;
                        case ColumnType.CHAR:
                            Assert.assertEquals(expected[expectedRow].getChar(col), record.getChar(col));
                            break;
                        case ColumnType.INT:
                            Assert.assertEquals(expected[expectedRow].getInt(col), record.getInt(col));
                            break;
                        case ColumnType.LONG:
                            Assert.assertEquals(expected[expectedRow].getLong(col), record.getLong(col));
                            break;
                        case ColumnType.DATE:
                            Assert.assertEquals(expected[expectedRow].getDate(col), record.getDate(col));
                            break;
                        case ColumnType.TIMESTAMP:
                            Assert.assertEquals(expected[expectedRow].getTimestamp(col), record.getTimestamp(col));
                            break;
                        case ColumnType.FLOAT:
                            Assert.assertTrue(doubleEquals(expected[expectedRow].getFloat(col), record.getFloat(col)));
                            break;
                        case ColumnType.DOUBLE:
                            Assert.assertTrue(doubleEquals(expected[expectedRow].getDouble(col), record.getDouble(col)));
                            break;
                        case ColumnType.STRING:
                            TestUtils.assertEquals(expected[expectedRow].getStrA(col), record.getStrA(col));
                            break;
                        case ColumnType.SYMBOL:
                            TestUtils.assertEquals(expected[expectedRow].getSymA(col), record.getSymA(col));
                            break;
                        case ColumnType.IPv4:
                            Assert.assertEquals(expected[expectedRow].getIPv4(col), record.getIPv4(col));
                            break;
                        case ColumnType.LONG256:
                            Long256 l1 = expected[expectedRow].getLong256A(col);
                            Long256 l2 = record.getLong256A(col);
                            Assert.assertEquals(l1.getLong0(), l2.getLong0());
                            Assert.assertEquals(l1.getLong1(), l2.getLong1());
                            Assert.assertEquals(l1.getLong2(), l2.getLong2());
                            Assert.assertEquals(l1.getLong3(), l2.getLong3());
                            break;
                        case ColumnType.BINARY:
                            TestUtils.assertEquals(expected[expectedRow].getBin(col), record.getBin(col), record.getBin(col).length());
                        default:
                            Assert.fail("Unknown column type");
                            break;
                    }
                }
                expectedRow++;
            }
            Assert.assertTrue((expectSize && rowsCount != -1) || (!expectSize && rowsCount == -1));
            Assert.assertTrue(rowsCount == -1 || expectedRow == rowsCount);
        }
        assertFactoryMemoryUsage();
    }

    private void assertExactPlan(CharSequence expectedPlan) throws SqlException {
        final StringSink explainSql = new StringSink();
        explainSql.put("EXPLAIN ").put(query);
        try (
                RecordCursorFactory planFactory = selectPlanFactory(explainSql);
                RecordCursor cursor = planFactory.getCursor(context)
        ) {
            final CharSequence plan = JitUtil.isJitSupported() ? expectedPlan : Chars.toString(expectedPlan).replace("Async JIT", "Async");
            TestUtils.assertCursor(plan, cursor, planFactory.getMetadata(), false, sink);
        }
    }

    private void assertExternalFactory(CharSequence expected) throws SqlException {
        // External-factory mode: assert the caller-owned factory without compiling, freeing, leak-checking,
        // or touching the shared static memory snapshot. Use caller-local scratch buffers so concurrent
        // worker threads do not race on the static sink/rows fields.
        final StringSink localSink = new StringSink();
        final LongList localRows = new LongList();
        assertBaseFactoryClass(factory);
        assertColumnTypes(factory);
        if (expectedTimestamp != null || isTimestampInferred) {
            assertTimestamp(expectedTimestamp, expectedTimestampOrder, factory, context);
        }
        if (assertExternalFactoryCursor(expected, localSink, localRows)) {
            return;
        }
        // make sure we get the same outcome when the factory creates a new cursor
        assertExternalFactoryCursor(expected, localSink, localRows);
    }

    private boolean assertExternalFactoryCursor(CharSequence expected, StringSink localSink, LongList localRows) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(context)) {
            final boolean randomAccess = resolveRandomAccess(supportsRandomAccess, factory);
            Assert.assertEquals("supports random access (" + (factory.recordCursorSupportsRandomAccess() ? "remove" : "add") + " .noRandomAccess())", randomAccess, factory.recordCursorSupportsRandomAccess());
            return assertCursor(
                    expected,
                    randomAccess,
                    expectSize,
                    sizeCanBeVariable,
                    cursor,
                    factory.getMetadata(),
                    localSink,
                    localRows,
                    factory.fragmentedSymbolTables(),
                    context
            );
        }
    }

    private void assertFactoryCursor(
            CharSequence expected,
            RecordCursorFactory factory,
            boolean supportsRandomAccess,
            SqlExecutionContext executionContext,
            boolean expectSize
    ) throws SqlException {
        assertCursor(expected, factory, supportsRandomAccess, expectSize, false, executionContext);
        // v Please keep this check after ^ that one.
        // Factories that have a scan order dependent on the bind variable will not test correctly.
        // See generate_series
        assertTimestamp(expectedTimestamp, expectedTimestampOrder, factory, executionContext);
        // make sure we get the same outcome when we get factory to create new cursor
        assertCursor(expected, factory, supportsRandomAccess, expectSize, false, executionContext);
        // make sure strings, binary fields and symbols are compliant with expected record behaviour
        assertVariableColumns(factory, executionContext);
    }

    private void assertFactoryMemoryUsage() {
        if (memoryUsage > -1) {
            long memAfterCursorClose = getMemUsedByFactories();
            long limit = memoryUsage + 64 * 1024;
            if (memAfterCursorClose > limit) {
                dumpMemoryUsage();
                printFactoryMemoryUsageDiff();
                Assert.fail("cursor is allowed to keep up to 64 KiB of RSS after close. This cursor kept " + (memAfterCursorClose - memoryUsage) / 1024 + " KiB.");
            }
        }
    }

    private void assertMemoryLeak(TestUtils.LeakProneCode code) throws Exception {
        engine.clear();
        TestUtils.assertMemoryLeak(() -> {
            try {
                code.run();
            } finally {
                releaseInactive(engine);
            }
        });
    }

    private void assertPlanContains(ObjList<CharSequence> fragments) throws SqlException {
        final StringSink explainSql = new StringSink();
        explainSql.put("EXPLAIN ").put(query);
        final StringSink actualPlan = new StringSink();
        try (
                RecordCursorFactory planFactory = selectPlanFactory(explainSql);
                RecordCursor cursor = planFactory.getCursor(context)
        ) {
            CursorPrinter.println(cursor, planFactory.getMetadata(), actualPlan, false, false);
        }
        for (int i = 0, n = fragments.size(); i < n; i++) {
            CharSequence fragment = fragments.getQuick(i);
            if (!JitUtil.isJitSupported()) {
                fragment = Chars.toString(fragment).replace("Async JIT", "Async");
            }
            TestUtils.assertContains(actualPlan, fragment);
        }
    }

    private void assertPlanNotContains(ObjList<CharSequence> fragments) throws SqlException {
        final StringSink explainSql = new StringSink();
        explainSql.put("EXPLAIN ").put(query);
        final StringSink actualPlan = new StringSink();
        try (
                RecordCursorFactory planFactory = selectPlanFactory(explainSql);
                RecordCursor cursor = planFactory.getCursor(context)
        ) {
            CursorPrinter.println(cursor, planFactory.getMetadata(), actualPlan, false, false);
        }
        for (int i = 0, n = fragments.size(); i < n; i++) {
            CharSequence fragment = fragments.getQuick(i);
            if (!JitUtil.isJitSupported()) {
                fragment = Chars.toString(fragment).replace("Async JIT", "Async");
            }
            TestUtils.assertNotContains(actualPlan, fragment);
        }
    }

    private void assertReturns(CharSequence expected, CharSequence expected2) throws SqlException {
        snapshotMemoryUsage();
        RecordCursorFactory factory = compileSelect();
        try {
            assertBaseFactoryClass(factory);
            assertColumnTypes(factory);
            assertTimestamp(expectedTimestamp, expectedTimestampOrder, factory, context);
            assertCursor(expected, factory, supportsRandomAccess, expectSize, sizeCanBeVariable, context);
            // make sure we get the same outcome when we get factory to create new cursor
            assertCursor(expected, factory, supportsRandomAccess, expectSize, sizeCanBeVariable, context);
            // make sure strings, binary fields and symbols are compliant with expected record behaviour
            assertVariableColumns(factory, context);

            if (ddl2 != null) {
                runMutations();
                if (engine.getConfiguration().getWalEnabledDefault()) {
                    drainWalQueue(engine);
                }

                int count = 3;
                TableReferenceOutOfDateException lastError = null;
                while (count > 0) {
                    try {
                        assertCursor(expected2, factory, supportsRandomAccess, expectSize, sizeCanBeVariable, context);
                        // and again
                        assertCursor(expected2, factory, supportsRandomAccess, expectSize, sizeCanBeVariable, context);
                        return;
                    } catch (TableReferenceOutOfDateException e) {
                        lastError = e;
                        Misc.free(factory);
                        factory = compileSelect();
                        count--;
                    }
                }
                // Every attempt saw the table reference go stale, so expected2 was never asserted.
                // Fail loudly instead of silently falling through to assertCalculateSize().
                throw new AssertionError("table reference still out of date after 3 attempts; expected2 was not asserted", lastError);
            }

            // make sure calculateSize() produces consistent result
            assertCalculateSize(factory, context);
        } finally {
            Misc.free(factory);
        }
    }

    private void assertReturnsRecords(Record[] expected, Record[] expected2) throws Exception {
        runPrepareHook();
        assertMemoryLeak(() -> {
            runDdl();
            snapshotMemoryUsage();
            RecordCursorFactory factory = compileSelect();
            try {
                assertTimestamp(expectedTimestamp, expectedTimestampOrder, factory, context);
                assertCursorRawRecords(expected, factory, context, expectSize);
                // make sure we get the same outcome when we get factory to create new cursor
                assertCursorRawRecords(expected, factory, context, expectSize);
                assertVariableColumns(factory, context);

                if (ddl2 != null) {
                    runMutations();
                    if (engine.getConfiguration().getWalEnabledDefault()) {
                        drainWalQueue(engine);
                    }
                    int count = 3;
                    TableReferenceOutOfDateException lastError = null;
                    while (count > 0) {
                        try {
                            assertCursorRawRecords(expected2, factory, context, expectSize);
                            // and again
                            assertCursorRawRecords(expected2, factory, context, expectSize);
                            return;
                        } catch (TableReferenceOutOfDateException e) {
                            lastError = e;
                            Misc.free(factory);
                            factory = compileSelect();
                            count--;
                        }
                    }
                    // Every attempt saw the table reference go stale, so expected2 was never asserted.
                    // Fail loudly instead of silently returning success.
                    throw new AssertionError("table reference still out of date after 3 attempts; expected2 was not asserted", lastError);
                }
            } finally {
                Misc.free(factory);
            }
        });
    }

    private void assertThrows(int errorPos, CharSequence contains, boolean fullFat) throws Exception {
        Assert.assertNotNull(contains);
        try {
            if (compiler != null) {
                assertThrowsViaCompiler(fullFat);
            } else {
                TestUtils.assertException(engine, context, fullFat, query, sink);
            }
        } catch (Throwable e) {
            if (e instanceof FlyweightMessageContainer container) {
                if (contains.isEmpty()) {
                    Assert.fail("position: " + container.getPosition() + ", message: " + e.getMessage());
                }
                TestUtils.assertContains(container.getFlyweightMessage(), contains);
                if (errorPos > -1) {
                    Assert.assertEquals(errorPos, container.getPosition());
                }
            } else {
                throw e;
            }
        }
    }

    private void assertThrowsViaCompiler(boolean fullFat) throws SqlException {
        if (fullFat) {
            compiler.setFullFatJoins(true);
        }
        try (
                RecordCursorFactory factory = CairoEngine.select(compiler, query, context);
                RecordCursor cursor = factory.getCursor(context)
        ) {
            sink.clear();
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                // ignore the output, we're looking for an error
                TestUtils.println(record, factory.getMetadata(), sink);
                sink.clear();
            }
        }
        Assert.fail("SQL statement should have failed");
    }

    private void assertTimestamp(CharSequence column, TimestampOrder order, RecordCursorFactory factory, SqlExecutionContext sqlExecutionContext) throws SqlException {
        if (isTimestampInferred) {
            if (column != null) {
                throw new IllegalStateException("inferTimestamp() cannot be combined with timestamp()/timestampAsc()/timestampDesc()");
            }
            final int timestampIdx = factory.getMetadata().getTimestampIndex();
            if (timestampIdx == -1) {
                // The factory declares no designated timestamp. With an inferred expectation there is
                // nothing to pin, so assert nothing.
                return;
            }
            final int scanDirection = factory.getScanDirection();
            if (scanDirection == RecordCursorFactory.SCAN_DIRECTION_OTHER) {
                // The row order is runtime-dependent (e.g. a bind-variable generate_series step), so we
                // cannot assert monotonicity in a fixed direction. Confirm only that the inferred column
                // exists and is of TIMESTAMP type.
                Assert.assertEquals(ColumnType.TIMESTAMP, ColumnType.tagOf(factory.getMetadata().getColumnType(timestampIdx)));
                return;
            }
            column = factory.getMetadata().getColumnName(timestampIdx);
            order = scanDirection == RecordCursorFactory.SCAN_DIRECTION_BACKWARD ? TimestampOrder.DESC : TimestampOrder.ASC;
        }
        if (column == null || column.isEmpty()) {
            int timestampIdx = factory.getMetadata().getTimestampIndex();
            if (timestampIdx != -1) {
                Assert.fail("Expected no timestamp but found " + factory.getMetadata().getColumnName(timestampIdx) + ", idx=" + timestampIdx);
            }
        } else {
            boolean expectAscendingOrder = order != TimestampOrder.DESC;
            if (expectAscendingOrder) {
                try {
                    Assert.assertEquals(RecordCursorFactory.SCAN_DIRECTION_FORWARD, factory.getScanDirection());
                } catch (AssertionError e) {
                    throw new AssertionError("expected ASCENDING timestamp", e);
                }
            } else {
                try {
                    Assert.assertEquals(RecordCursorFactory.SCAN_DIRECTION_BACKWARD, factory.getScanDirection());
                } catch (AssertionError e) {
                    throw new AssertionError("expected DESCENDING timestamp", e);
                }
            }

            int index = factory.getMetadata().getColumnIndexQuiet(column);
            Assert.assertTrue("Column '" + column + "' can't be found in metadata", index > -1);
            Assert.assertNotEquals("Expected non-negative value as timestamp index", -1, index);
            Assert.assertEquals("Timestamp column index", index, factory.getMetadata().getTimestampIndex());
            assertTimestampColumnValues(factory, sqlExecutionContext, expectAscendingOrder);
        }
    }

    private void assertTimestampColumnValues(RecordCursorFactory factory, SqlExecutionContext sqlExecutionContext, boolean isAscending) throws SqlException {
        int index = factory.getMetadata().getTimestampIndex();
        Assert.assertEquals(ColumnType.TIMESTAMP, ColumnType.tagOf(factory.getMetadata().getColumnType(index)));
        long timestamp = isAscending ? Long.MIN_VALUE : Long.MAX_VALUE;
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final Record record = cursor.getRecord();
            long c = 0;
            while (cursor.hasNext()) {
                long ts = record.getTimestamp(index);
                if ((isAscending && timestamp > ts) || (!isAscending && timestamp < ts)) {
                    StringSink error = new StringSink();
                    error.put("record # ").put(c).put(" should have ").put(isAscending ? "bigger" : "smaller").put(" (or equal) timestamp than the row before. Values prior=");
                    MicrosFormatUtils.appendDateTimeUSec(error, timestamp);
                    error.put(" current=");
                    MicrosFormatUtils.appendDateTimeUSec(error, ts);

                    Assert.fail(error.toString());
                }
                timestamp = ts;
                c++;
            }
        }
        assertFactoryMemoryUsage();
    }

    private void assertVariableColumns(RecordCursorFactory factory, SqlExecutionContext executionContext) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(executionContext)) {
            RecordMetadata metadata = factory.getMetadata();
            final int columnCount = metadata.getColumnCount();
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                for (int i = 0; i < columnCount; i++) {
                    switch (ColumnType.tagOf(metadata.getColumnType(i))) {
                        case ColumnType.STRING: {
                            CharSequence a = record.getStrA(i);
                            CharSequence b = record.getStrB(i);
                            if (a == null) {
                                Assert.assertNull(b);
                                Assert.assertEquals(TableUtils.NULL_LEN, record.getStrLen(i));
                            } else {
                                if (a instanceof AbstractCharSequence) {
                                    // AbstractCharSequence are usually mutable. We cannot have a same mutable instance for A and B
                                    Assert.assertNotSame(a, b);
                                }
                                TestUtils.assertEquals(a, b);
                                Assert.assertEquals(a.length(), record.getStrLen(i));
                            }
                            break;
                        }
                        case ColumnType.VARCHAR: {
                            Utf8Sequence a = record.getVarcharA(i);
                            Utf8Sequence b = record.getVarcharB(i);
                            if (a == null) {
                                Assert.assertNull(b);
                                Assert.assertEquals(TableUtils.NULL_LEN, record.getVarcharSize(i));
                            } else {
                                TestUtils.assertEquals(a, b);
                                Assert.assertEquals(a.size(), record.getVarcharSize(i));
                            }
                            break;
                        }
                        case ColumnType.BINARY: {
                            BinarySequence s = record.getBin(i);
                            if (s == null) {
                                Assert.assertEquals(TableUtils.NULL_LEN, record.getBinLen(i));
                            } else {
                                Assert.assertEquals(s.length(), record.getBinLen(i));
                            }
                            break;
                        }
                        default:
                            break;
                    }
                }
            }
        }
        assertFactoryMemoryUsage();
    }

    private void assertViaFactoryCursor(CharSequence expected) throws SqlException {
        runDdl();
        snapshotMemoryUsage();
        if (compiler != null) {
            if (fullFatJoins) {
                compiler.setFullFatJoins(true);
            }
            try (RecordCursorFactory factory = CairoEngine.select(compiler, query, context)) {
                assertFactoryCursor(expected, factory, supportsRandomAccess, context, expectSize);
            }
        } else {
            try (SqlCompiler fullFatCompiler = engine.getSqlCompiler()) {
                fullFatCompiler.setFullFatJoins(true);
                try (RecordCursorFactory factory = CairoEngine.select(fullFatCompiler, query, context)) {
                    assertFactoryCursor(expected, factory, supportsRandomAccess, context, expectSize);
                }
            }
        }
    }

    private RecordCursorFactory compileSelect() throws SqlException {
        return compiler != null ? CairoEngine.select(compiler, query, context) : engine.select(query, context);
    }

    private void dispatch(CharSequence expected, CharSequence expected2) throws Exception {
        if (factory != null) {
            if (context == null) {
                throw new IllegalStateException("external-factory mode requires withContext(...)");
            }
            if (expected2 != null) {
                throw new IllegalStateException("mutateWith(...) is not supported in external-factory mode");
            }
            if (compiler != null || ddl != null || ddlMore != null || fullFatJoins
                    || expectedPlan != null || planFragments != null || planFragmentsAbsent != null) {
                throw new IllegalStateException("external-factory mode supports only withContext()/withBaseFactoryClass()/columnType()/timestamp()/inferTimestamp()/noRandomAccess()/inferRandomAccess()/expectSize()/sizeMayVary()");
            }
            assertExternalFactory(expected);
            return;
        }
        runPrepareHook();
        if (expectedPlan != null || planFragments != null || planFragmentsAbsent != null) {
            if (fullFatJoins || compiler != null) {
                throw new IllegalStateException("withPlan(...)/withPlanContaining(...)/withPlanNotContaining(...) cannot be combined with fullFatJoins()/withCompiler()");
            }
            final CharSequence exactPlan = expectedPlan;
            final ObjList<CharSequence> fragments = planFragments;
            final ObjList<CharSequence> absentFragments = planFragmentsAbsent;
            final TestUtils.LeakProneCode body = () -> {
                runDdl();
                assertReturns(expected, expected2);
                if (exactPlan != null) {
                    assertExactPlan(exactPlan);
                }
                if (fragments != null) {
                    assertPlanContains(fragments);
                }
                if (absentFragments != null) {
                    assertPlanNotContains(absentFragments);
                }
            };
            if (leakCheck) {
                assertMemoryLeak(body);
            } else {
                body.run();
            }
            return;
        }
        if (fullFatJoins || compiler != null) {
            if (ddl2 != null || sizeCanBeVariable) {
                throw new IllegalStateException("fullFatJoins()/withCompiler() supports only ddl()/timestamp()/expectSize()/noRandomAccess()");
            }
            if (leakCheck) {
                assertMemoryLeak(() -> assertViaFactoryCursor(expected));
            } else {
                assertViaFactoryCursor(expected);
            }
            return;
        }
        if (leakCheck) {
            assertMemoryLeak(() -> {
                runDdl();
                assertReturns(expected, expected2);
            });
        } else {
            runDdl();
            assertReturns(expected, expected2);
        }
    }

    private void failsNoLeak(int errorPos, CharSequence contains) throws Exception {
        runPrepareHook();
        if (ddl != null) {
            try {
                engine.execute(ddl, context);
                runDdlMore();
                assertThrows(errorPos, contains, false);
                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(0, engine.getBusyWriterCount());
            } finally {
                engine.clear();
            }
            return;
        }
        assertThrows(errorPos, contains, fullFatJoins);
    }

    private void requireBindsCompatible() {
        if (ddl2 != null || fullFatJoins || compiler != null
                || expectedPlan != null || planFragments != null || planFragmentsAbsent != null) {
            throw new IllegalStateException(
                    "assertBinds(...) supports only ddl()/timestamp*()/expectSize()/noRandomAccess()/sizeMayVary()/noLeakCheck()/withContext()/withEngine()");
        }
    }

    private void requireFailsCompatible() {
        if (expectSize || expectedTimestamp != null || isTimestampInferred || isRandomAccessInferred || ddl2 != null || !supportsRandomAccess
                || sizeCanBeVariable || expectedPlan != null || planFragments != null || planFragmentsAbsent != null) {
            throw new IllegalStateException("fails(...)/failsWith(...) supports only ddl()/fullFatJoins()/withCompiler()/noLeakCheck()/withContext()/withEngine()");
        }
    }

    private void requireMutateStepwiseCompatible() {
        if (ddl2 != null || fullFatJoins || compiler != null || isTimestampInferred
                || expectedPlan != null || planFragments != null || planFragmentsAbsent != null) {
            throw new IllegalStateException(
                    "mutateStepwise(...) supports only ddl()/expectSize()/noRandomAccess()/sizeMayVary()/noLeakCheck()/withContext()/withEngine()");
        }
    }

    private void requirePlanOnlyCompatible() {
        if (expectSize || expectedTimestamp != null || isTimestampInferred || isRandomAccessInferred || ddl2 != null || fullFatJoins
                || !supportsRandomAccess || sizeCanBeVariable || expectedPlan != null || planFragments != null || planFragmentsAbsent != null) {
            throw new IllegalStateException("assertsPlan(...)/assertsPlanContaining(...)/assertsPlanNotContaining(...) supports only ddl()/noLeakCheck()/withCompiler()/withContext()/withEngine()");
        }
    }

    private void requireRecordPathCompatible() {
        if (!leakCheck || fullFatJoins || compiler != null || expectedPlan != null || planFragments != null || planFragmentsAbsent != null || sizeCanBeVariable || !supportsRandomAccess || isRandomAccessInferred || overridden) {
            throw new IllegalStateException("returnsRecords(...) supports only ddl()/timestamp()/mutateWith()/expectSize()");
        }
    }

    private void requireSingleShotCompatible() {
        if (expectSize || expectedTimestamp != null || isTimestampInferred || isRandomAccessInferred || ddl2 != null || fullFatJoins || compiler != null || expectedPlan != null || planFragments != null || planFragmentsAbsent != null || !supportsRandomAccess || sizeCanBeVariable) {
            throw new IllegalStateException("returnsOnce(...) supports only ddl()/noLeakCheck()/withContext()/withEngine()");
        }
    }

    private boolean resolveRandomAccess(boolean supportsRandomAccess, RecordCursorFactory factory) {
        if (isRandomAccessInferred) {
            if (!supportsRandomAccess) {
                throw new IllegalStateException("inferRandomAccess() cannot be combined with noRandomAccess()/supportsRandomAccess(false)");
            }
            return factory.recordCursorSupportsRandomAccess();
        }
        return supportsRandomAccess;
    }

    private TimestampOrder resolveTimestampOrder(BindVarTuple testCase) {
        if (testCase.getOrder() == BindVarTuple.Order.INHERIT) {
            return expectedTimestampOrder;
        }
        return testCase.getOrder() == BindVarTuple.Order.ASC ? TimestampOrder.ASC : TimestampOrder.DESC;
    }

    private void runBinds(ObjList<BindVarTuple> cases) throws SqlException {
        runDdl();
        snapshotMemoryUsage();
        final BindVariableService bindVariableService = context.getBindVariableService();
        // Seed bind-variable types from the first success case before compiling: the compiler rejects an
        // undefined NAMED bind (:name), unlike an indexed one ($1). The per-case loop below re-clears and
        // re-applies binds for each value, so this initial assignment only establishes compile-time types.
        for (int i = 0, n = cases.size(); i < n; i++) {
            if (!cases.getQuick(i).isExpectedToFail()) {
                bindVariableService.clear();
                cases.getQuick(i).getBinds().assignBindVariables(bindVariableService);
                break;
            }
        }
        try (RecordCursorFactory factory = compileSelect()) {
            for (int i = 0, n = cases.size(); i < n; i++) {
                final BindVarTuple testCase = cases.getQuick(i);
                // A case may execute under its own context (with its own bind service) to verify the
                // factory reads binds from the execution context; otherwise it uses the chain context.
                final SqlExecutionContext caseContext = testCase.getExecutionContext() != null
                        ? testCase.getExecutionContext()
                        : context;
                caseContext.getBindVariableService().clear();
                testCase.getBinds().assignBindVariables(caseContext.getBindVariableService());
                if (testCase.isExpectedToFail()) {
                    assertBindFailure(factory, testCase, caseContext);
                } else {
                    try {
                        assertBindSuccess(factory, testCase, caseContext);
                    } catch (AssertionError e) {
                        // Tag generic per-cursor assertions (size, random access, row data) with the
                        // offending case, so a failure names the tuple instead of just the symptom.
                        throw new AssertionError(
                                "bind-variable case #" + i + " (" + testCase.getDescription() + "): " + e.getMessage(), e);
                    }
                }
            }
        }
    }

    private void runDdl() throws SqlException {
        if (ddl != null) {
            engine.execute(ddl, context);
            runDdlMore();
            // Drain unconditionally: a table created with an explicit WAL keyword is WAL even when
            // walEnabledDefault is false (the test-harness default), so its inserts stay in the WAL
            // until applied. drainWalQueue() is a no-op when nothing is pending.
            drainWalQueue(engine);
        }
    }

    private void runDdlMore() throws SqlException {
        if (ddlMore != null) {
            for (int i = 0, n = ddlMore.size(); i < n; i++) {
                engine.execute(ddlMore.getQuick(i), context);
            }
        }
    }

    private void runMutationSteps(ObjList<MutationStep> steps) throws SqlException {
        runDdl();
        snapshotMemoryUsage();
        try (RecordCursorFactory factory = compileSelect()) {
            for (int i = 0, n = steps.size(); i < n; i++) {
                final MutationStep step = steps.getQuick(i);
                engine.execute(step.getMutation(), context);
                drainWalQueue(engine);
                assertCursor(step.getExpected(), factory, supportsRandomAccess, expectSize, sizeCanBeVariable, context);
            }
        }
    }

    private void runMutations() throws SqlException {
        engine.execute(ddl2, context);
        if (ddl2More != null) {
            for (int i = 0, n = ddl2More.size(); i < n; i++) {
                engine.execute(ddl2More.getQuick(i), context);
            }
        }
    }

    private void runPrepareHook() {
        if (prepareHook != null) {
            prepareHook.run();
        }
    }

    private RecordCursorFactory selectPlanFactory(CharSequence explainSql) throws SqlException {
        return compiler != null ? CairoEngine.select(compiler, explainSql, context) : engine.select(explainSql, context);
    }

    protected static void dumpMemoryUsage() {
        for (int i = MemoryTag.MMAP_DEFAULT; i < MemoryTag.SIZE; i++) {
            LOG.info().$(MemoryTag.nameOf(i)).$(": ").$(Unsafe.getMemUsedByTag(i)).$();
        }
    }

    static {
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            FACTORY_TAGS[i] = !Chars.startsWith(MemoryTag.nameOf(i), "MMAP");
        }

        FACTORY_TAGS[MemoryTag.NATIVE_O3] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_JOIN_MAP] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_OFFLOAD] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_SAMPLE_BY_LONG_LIST] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_TABLE_READER] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_TABLE_WRITER] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_IMPORT] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_PARALLEL_IMPORT] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_REPL] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_INDEX_READER] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_TABLE_WAL_WRITER] = false;
    }

    // Expected designated-timestamp scan order, set by timestamp()/timestampAsc()/timestampDesc().
    private enum TimestampOrder {ASC, DESC}
}
