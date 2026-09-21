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

package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.TextPlanSink;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.griffin.fuzz.expr.BindContext;
import org.junit.Assert;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;

/** Shared query campaign over caller-owned tables and a quiescent writer pool. */
public final class QueryFuzzDriver {
    // Per-constant chance, in percent, of substituting a bindable literal
    // with a bind variable inside the bind variant.
    private static final int CONSTANT_BIND_PROBABILITY_PCT = 50;
    private static final Log LOG = LogFactory.getLog(QueryFuzzDriver.class);
    // Lowest share, in percent, of a shape's differential queries that must actually run rather
    // than be skipped on an expected error. Skips are normal - a fuzzed expression can overflow, a
    // cast can be inconvertible - and how often they happen swings with the randomly generated
    // tables, so this only catches a generator that has stopped producing compilable SQL at all:
    // without it, a shape whose SQL drifts out of step with the engine leaves the run green with
    // every one of its queries counted as skipped. Over a 1500-query run the shapes land between
    // 66% (GROUP BY, WINDOW) and 100% (posting index) accepted, so 25% leaves wide headroom; read
    // the per-shape line runFuzz logs to see where a shape actually sits.
    private static final int MIN_ACCEPTED_PCT_PER_SHAPE = 25;
    // Fault-injected queries a run needs before runFuzz asserts that at least one
    // fault actually fired. Below this count a zero-fire run is a small-sample
    // artifact (a fault arms at a random trigger point and a short query can run
    // fewer ops than the trigger, so it never bites); at or above it, zero fired
    // means the injector is disarmed. A default run injects ~15 (100 queries at a
    // 15% fault probability), so the floor holds for every unshrunk run.
    private static final int MIN_FAULT_QUERIES_FOR_FIRE_FLOOR = 5;
    // Fault queries of ONE type a run needs before runFuzz holds that type to its own fire floor.
    // Higher than the aggregate floor because a per-type zero-fire run is far likelier by chance:
    // measured per-arm fire rates run 49-88% by type, so at a pessimistic 30% it takes 20 arms for
    // an all-miss run to drop below 1e-3. See the guard in runFuzz.
    private static final int MIN_FAULT_QUERIES_PER_TYPE_FOR_FIRE_FLOOR = 20;
    // Differential queries a run needs before runFuzz asserts that a shape it could draw generated
    // at least one query. The rarest such shape takes ~4% of the differential queries, so at this
    // sample size an all-miss run sits below 1e-8; under it a zero is small-sample noise and
    // runFuzz only logs. FuzzConfig's default budget leaves ~850 differential queries after fault
    // injection takes its cut, so CI always guards. This floor is about the budget only: a shape
    // whose per-run precondition the run never met is exempt whatever the budget, which
    // describeUngeneratableShape decides.
    private static final int MIN_QUERIES_FOR_ZERO_GUARD = 500;
    // Differential queries a shape needs before runFuzz holds it to MIN_ACCEPTED_PCT_PER_SHAPE.
    // Below this count the accepted rate is too noisy to assert on. FuzzConfig's default budget is
    // sized so every shape the run can draw clears this - at the old 100-query default only
    // SAMPLE_BY did, leaving the guard dormant for the other eight shapes. runFuzz logs any shape
    // that still falls short, so a generator that goes rare cannot silently stop being guarded; it
    // is logged rather than asserted because the rarest shape draws ~3.4% of a run, which at the
    // default budget dips under the floor often enough by chance alone to make an assertion flaky.
    // A shape that generated nothing at all is a different case and MIN_QUERIES_FOR_ZERO_GUARD
    // guards it: no budget lifts POSTING off zero, because it needs the run's random schema to
    // carry a posting-indexed SYMBOL.
    private static final int MIN_SHAPE_QUERIES_FOR_ACCEPT_FLOOR = 25;
    // Per-query chance, in percent, of generating a bind-variable variant.
    private static final int QUERY_BIND_PROBABILITY_PCT = 20;
    private static final int SERIAL_PROBABILITY_PCT = 5;

    private QueryFuzzDriver() {
    }

    public static void run(CairoEngine engine, SqlExecutionContext sqlExecutionContext, FuzzConfig config, Rnd rnd,
                           ObjList<FuzzTable> tables, String queryWorkerNamePrefix, Phase phase) throws Exception {
        for (int i = 0; i < tables.size(); i++) {
            logSchema(tables.getQuick(i));
            if (tables.getQuick(i).getShadow() != null) {
                logSchema(tables.getQuick(i).getShadow());
            }
        }
        QueryRunner runner = new QueryRunner(engine, sqlExecutionContext, config.isDiffJitEnabled(), config.isDiffShadowEnabled(), config.isVerifyCursorEnabled(), tables, queryWorkerNamePrefix);
        // Snapshot the parallel-execution flags so the per-query serial
        // override can restore them. Snapshotting once outside the loop also
        // preserves any global override the user passed via system properties.
        // HORIZON JOIN and WINDOW JOIN must be included: without them the
        // serial control arm still runs those joins in parallel, defeating the
        // determinism the serial arm exists to provide.
        final boolean savedParallelFilter = sqlExecutionContext.isParallelFilterEnabled();
        final boolean savedParallelGroupBy = sqlExecutionContext.isParallelGroupByEnabled();
        final boolean savedParallelHorizonJoin = sqlExecutionContext.isParallelHorizonJoinEnabled();
        final boolean savedParallelReadParquet = sqlExecutionContext.isParallelReadParquetEnabled();
        final boolean savedParallelTopK = sqlExecutionContext.isParallelTopKEnabled();
        final boolean savedParallelWindowJoin = sqlExecutionContext.isParallelWindowJoinEnabled();
        int bindGen = 0;
        int faultGen = 0;
        int skipped = 0;
        int serial = 0;
        // Per-shape differential-query counts, so a generator that emits nothing the engine can
        // compile cannot hide behind the aggregate (see MIN_ACCEPTED_PCT_PER_SHAPE). Fault queries
        // run a different oracle and are left out of both counts.
        final int[] generatedByShape = new int[QueryShape.values().length];
        final int[] skippedByShape = new int[QueryShape.values().length];
        final int[] comparedByShape = new int[QueryShape.values().length];
        ObjList<QueryRunner.Result> failures = new ObjList<>();
        int budget = phase == Phase.FAULTS
                ? Math.max(1, config.getNumQueries() * config.getFaultProbabilityPct() / 100)
                : config.getNumQueries();
        try (BufferedWriter dump = openDump(config.getDumpPath())) {
            for (int q = 0; q < budget; q++) {
                // Decide fault injection before generation so a FUNCTION fault can
                // ask the generator to emit the fault function. Drawn from the
                // seeded rnd, so replay reproduces the choice.
                FaultType faultType = (phase == Phase.FAULTS || (phase == Phase.MIXED && config.isFaultInjectionEnabled() && rnd.nextInt(100) < config.getFaultProbabilityPct()))
                        ? runner.chooseFaultType(rnd)
                        : null;
                boolean injectFaultFn = faultType == FaultType.FUNCTION;
                long preGenS0 = rnd.getSeed0();
                long preGenS1 = rnd.getSeed1();
                GeneratedQuery query = QueryGenerator.generate(rnd, tables, null, injectFaultFn, config.isWindowEnabled(), config.isLatestOnEnabled(), config.isHorizonJoinEnabled(), config.isWindowJoinEnabled());
                QueryRunner.Result result;
                if (faultType != null) {
                    // Fault queries use a crash-and-recover oracle, not the
                    // differential one, so they skip the bind/serial variants.
                    faultGen++;
                    dumpQuery(dump, query);
                    // With the knob on, all fault types run under parallel SQL
                    // execution so the parallel filter / GROUP BY / top-K reduce
                    // error paths get exercised. The writer pool is halted for the
                    // whole query loop, so no background job competes: FUNCTION is
                    // data-scoped, FILE is scoped to the query execution, and
                    // MALLOC's process-global RSS ceiling can only be tripped by the
                    // query's own allocations.
                    boolean runFaultParallel = config.isParallelFaultEnabled();
                    LOG.info().$("fuzz fault (").$(faultType.name()).$(runFaultParallel ? ", parallel" : ", serial").$("): ").$safe(query.sql()).$();
                    if (!runFaultParallel) {
                        sqlExecutionContext.setParallelFilterEnabled(false);
                        sqlExecutionContext.setParallelGroupByEnabled(false);
                        sqlExecutionContext.setParallelHorizonJoinEnabled(false);
                        sqlExecutionContext.setParallelReadParquetEnabled(false);
                        sqlExecutionContext.setParallelTopKEnabled(false);
                        sqlExecutionContext.setParallelWindowJoinEnabled(false);
                    }
                    try {
                        result = runner.runFault(query, faultType, rnd, runFaultParallel);
                    } finally {
                        if (!runFaultParallel) {
                            sqlExecutionContext.setParallelFilterEnabled(savedParallelFilter);
                            sqlExecutionContext.setParallelGroupByEnabled(savedParallelGroupBy);
                            sqlExecutionContext.setParallelHorizonJoinEnabled(savedParallelHorizonJoin);
                            sqlExecutionContext.setParallelReadParquetEnabled(savedParallelReadParquet);
                            sqlExecutionContext.setParallelTopKEnabled(savedParallelTopK);
                            sqlExecutionContext.setParallelWindowJoinEnabled(savedParallelWindowJoin);
                        }
                    }
                } else {
                    generatedByShape[query.shape().ordinal()]++;
                    // With small probability, regenerate the same query with a
                    // BindContext threaded through so a fraction of bindable
                    // typed constants emit as ?::TYPE bind variables. The Rnd
                    // is rewound to the pre-literal state so the tree shape
                    // matches; the BindContext gets its own derived Rnd so the
                    // bind/no-bind decisions are deterministic per seed.
                    //
                    // Determinism invariant: the second QueryGenerator.generate()
                    // call must draw the same number and order of rnd ops as the
                    // first. The two calls only differ in whether they consult
                    // the BindContext (which uses its own seeded Rnd), so adding
                    // any rnd operation to the bind path - or skipping one on
                    // the literal path - desynchronises the two trees and the
                    // shapes will diverge. Take care when modifying any code
                    // reachable from QueryGenerator.generate().
                    if (rnd.nextInt(100) < QUERY_BIND_PROBABILITY_PCT) {
                        long bindS0 = rnd.nextLong();
                        long bindS1 = rnd.nextLong();
                        rnd.reset(preGenS0, preGenS1);
                        BindContext ctx = new BindContext(new Rnd(bindS0, bindS1), CONSTANT_BIND_PROBABILITY_PCT);
                        GeneratedQuery bindForm = QueryGenerator.generate(rnd, tables, ctx, injectFaultFn, config.isWindowEnabled(), config.isLatestOnEnabled(), config.isHorizonJoinEnabled(), config.isWindowJoinEnabled());
                        if (ctx.getBindValues().size() > 0) {
                            query = query.withBind(bindForm.sql(), ctx.getBindNames(), ctx.getBindValues());
                            bindGen++;
                            LOG.info().$("fuzz bind: ").$safe(query.bindSql()).$();
                        }
                    }
                    dumpQuery(dump, query);
                    boolean disableParallel = rnd.nextInt(100) < SERIAL_PROBABILITY_PCT;
                    if (disableParallel) {
                        serial++;
                        sqlExecutionContext.setParallelFilterEnabled(false);
                        sqlExecutionContext.setParallelGroupByEnabled(false);
                        sqlExecutionContext.setParallelHorizonJoinEnabled(false);
                        sqlExecutionContext.setParallelReadParquetEnabled(false);
                        sqlExecutionContext.setParallelTopKEnabled(false);
                        sqlExecutionContext.setParallelWindowJoinEnabled(false);
                        LOG.info().$("fuzz serial: ").$safe(query.sql()).$();
                    }
                    try {
                        result = runner.run(query);
                    } finally {
                        if (disableParallel) {
                            sqlExecutionContext.setParallelFilterEnabled(savedParallelFilter);
                            sqlExecutionContext.setParallelGroupByEnabled(savedParallelGroupBy);
                            sqlExecutionContext.setParallelHorizonJoinEnabled(savedParallelHorizonJoin);
                            sqlExecutionContext.setParallelReadParquetEnabled(savedParallelReadParquet);
                            sqlExecutionContext.setParallelTopKEnabled(savedParallelTopK);
                            sqlExecutionContext.setParallelWindowJoinEnabled(savedParallelWindowJoin);
                        }
                    }
                }
                if (result.isSkipped()) {
                    skipped++;
                    if (faultType == null) {
                        skippedByShape[query.shape().ordinal()]++;
                    }
                    LOG.info().$("fuzz skip (").$safe(result.getSkipReason()).$("): ").$safe(query.sql()).$();
                } else if (result.isFailed()) {
                    LOG.error().$("fuzz failure on query: ").$safe(query.sql())
                            .$(" -- ").$(result.getFailure().getClass().getName())
                            .$(": ").$safe(result.getFailure().getMessage())
                            .$();
                    failures.add(result);
                    dumpFailure(engine, sqlExecutionContext, tables, query);
                } else if (faultType == null && query.deterministic() && referencesTable(query, tables)) {
                    comparedByShape[query.shape().ordinal()]++;
                }
            }
        }
        LOG.info().$("fuzz done: ").$(budget).$(" queries, ")
                .$(serial).$(" serial, ")
                .$(faultGen).$(" with fault injection (").$(runner.getFaultsFired()).$(" fired), ")
                .$(bindGen).$(" with bind variant, ")
                .$(skipped).$(" skipped on expected errors, ")
                .$(failures.size()).$(" failures")
                .$();
        LOG.info().$("fuzz faults by type: ")
                .$("FILE ").$(runner.getFaultsFired(FaultType.FILE)).$('/').$(runner.getFaultsArmed(FaultType.FILE))
                .$(", MALLOC ").$(runner.getFaultsFired(FaultType.MALLOC)).$('/').$(runner.getFaultsArmed(FaultType.MALLOC))
                .$(", FUNCTION ").$(runner.getFaultsFired(FaultType.FUNCTION)).$('/').$(runner.getFaultsArmed(FaultType.FUNCTION))
                .$(" (fired/armed)")
                .$();
        LogRecord shapeLog = LOG.info().$("fuzz shapes (compared/accepted/generated): ");
        for (QueryShape shape : QueryShape.values()) {
            final int generated = generatedByShape[shape.ordinal()];
            shapeLog.$(shape.name()).$(' ').$(comparedByShape[shape.ordinal()]).$('/')
                    .$(generated - skippedByShape[shape.ordinal()]).$('/').$(generated).$(' ');
        }
        shapeLog.$();

        if (failures.size() > 0) {
            throw buildFailure(failures);
        }
        // Guard each generator against emitting SQL the engine cannot compile, and against emitting
        // nothing at all. A shape whose generator drifts out of step with the engine's rules still
        // leaves the run green: every query it emits raises an expected error, gets counted as
        // skipped, and asserts nothing. A shape whose dispatch in QueryGenerator has gone
        // unreachable is worse - it reports zero generated, and the accepted-rate floor below reads
        // a zero as "too small a sample" and skips it.
        int differentialGen = 0;
        for (QueryShape shape : QueryShape.values()) {
            differentialGen += generatedByShape[shape.ordinal()];
        }
        for (QueryShape shape : QueryShape.values()) {
            final int generated = generatedByShape[shape.ordinal()];
            if (generated == 0) {
                // Zero is legitimate for a shape this run could never draw: its -D toggle is off,
                // or - for POSTING - the run's random schema carries no posting-indexed SYMBOL.
                // Assert only when the run's tables and knobs allow the shape, so the guard cannot
                // fail a working generator on an unlucky seed.
                final String reason = describeUngeneratableShape(shape, config, tables);
                if (reason != null) {
                    LOG.info().$("fuzz shape cannot generate this run, NOT guarded: ")
                            .$(shape.name()).$(" (").$safe(reason).$(')')
                            .$();
                    continue;
                }
                if (differentialGen < MIN_QUERIES_FOR_ZERO_GUARD) {
                    LOG.info().$("fuzz shape generated nothing but the run is too small to guard it: ")
                            .$(shape.name()).$(", ").$(differentialGen).$('/').$(MIN_QUERIES_FOR_ZERO_GUARD)
                            .$(" differential queries; raise -D").$(FuzzConfig.QUERIES_PROP).$(" to guard it")
                            .$();
                    continue;
                }
                Assert.fail("the " + shape.name() + " generator emitted no query across "
                        + differentialGen + " differential queries, though this run's tables and knobs"
                        + " allow the shape; QueryGenerator no longer reaches that generator");
            }
            if (generated < MIN_SHAPE_QUERIES_FOR_ACCEPT_FLOOR) {
                // Not enough queries to hold this shape to the floor. Say so out loud: a shape that
                // quietly stops generating (or goes rare) is otherwise indistinguishable from one
                // that passed, and the guard for it is off for this run.
                LOG.info().$("fuzz shape below the accept-floor sample size, NOT guarded this run: ")
                        .$(shape.name()).$(' ').$(generated).$('/').$(MIN_SHAPE_QUERIES_FOR_ACCEPT_FLOOR)
                        .$(" queries; raise -D").$(FuzzConfig.QUERIES_PROP).$(" to guard it")
                        .$();
                continue;
            }
            final int accepted = generated - skippedByShape[shape.ordinal()];
            Assert.assertTrue("no completed content comparison for " + shape,
                    comparedByShape[shape.ordinal()] > 0);
            Assert.assertTrue(
                    "the " + shape.name() + " generator ran only " + accepted + " of its " + generated
                            + " queries; the rest were skipped on expected errors, so it looks out of step with the engine",
                    100L * accepted >= (long) MIN_ACCEPTED_PCT_PER_SHAPE * generated
            );
        }
        // Guard the fault injector against a silent disarm. It has several ways to stop biting while
        // the run stays green and tests nothing but the happy path: dev mode off (test_fault() folds
        // to the constant true), the FailureFileFacade not installed on the engine, or the MALLOC RSS
        // ceiling armed above what the query allocates.
        //
        // The facade case never even arms - QueryRunner drops FILE from its fault types when the
        // engine's FilesFacade is not a FailureFileFacade - so it reads as "armed 0, fired 0" and no
        // fire-count floor can see it. Assert the type is on offer instead.
        if (phase == Phase.DIFFERENTIAL || (phase == Phase.MIXED && !config.isFaultInjectionEnabled())) {
            return;
        }
        for (FaultType type : FaultType.values()) {
            Assert.assertTrue(
                    "fault type " + type.name() + " was never offered to the runner; that injector looks disarmed",
                    runner.isFaultTypeAvailable(type)
            );
        }
        if (faultGen >= MIN_FAULT_QUERIES_FOR_FIRE_FLOOR) {
            Assert.assertTrue(
                    "fault injection ran on " + faultGen + " queries but no fault fired; the injector looks disarmed",
                    runner.getFaultsFired() > 0
            );
        }
        // The aggregate floor above cannot tell a live FUNCTION injector from a dead FILE one: a single
        // FUNCTION fire satisfies it while FILE and MALLOC fire zero all run. Hold each type to its own
        // floor, but only once it has armed often enough that a zero-fire run means something. Arming
        // does not imply firing - each fault arms at a random trigger point (the Nth file op, the Nth
        // test_fault() call, an RSS ceiling a few KB up), and a query that does fewer ops than its
        // trigger runs clean - so a strict "armed once, must fire" would flake. Measured per-arm fire
        // rates run 49-88% by type; at a pessimistic 30% the odds of 20 arms all missing are below
        // 1e-3. A default run arms ~5 per type, so this floor engages on a soak
        // (-Dquestdb.fuzz.queries=500, or a raised -Dquestdb.fuzz.fault.pct).
        for (FaultType type : FaultType.values()) {
            final int armed = runner.getFaultsArmed(type);
            if (armed >= MIN_FAULT_QUERIES_PER_TYPE_FOR_FIRE_FLOOR) {
                Assert.assertTrue(
                        "fault type " + type.name() + " armed on " + armed
                                + " queries but never fired; that injector looks disarmed",
                        runner.getFaultsFired(type) > 0
                );
            }
        }
    }

    private static AssertionError buildFailure(ObjList<QueryRunner.Result> failures) {
        StringBuilder sb = new StringBuilder("query fuzz found ").append(failures.size())
                .append(" unexpected failure(s):\n");
        for (int i = 0, n = failures.size(); i < n; i++) {
            QueryRunner.Result r = failures.getQuick(i);
            sb.append("  [").append(i + 1).append("] ")
                    .append(r.getFailure().getClass().getSimpleName())
                    .append(": ").append(r.getFailure().getMessage()).append('\n')
                    .append("        sql: ").append(r.getSql()).append('\n');
        }
        // Chain the first cause so the stack trace still points at real source.
        return new AssertionError(sb.toString(), failures.getQuick(0).getFailure());
    }

    /**
     * QueryGenerator gates its whole join band on two or more tables, so with fewer than that every
     * join shape legitimately generates nothing. FuzzConfig draws at least two, so this normally
     * returns {@code null}; it exists so lowering that floor cannot turn the zero guard flaky.
     */
    private static String describeMissingJoinTables(ObjList<FuzzTable> tables) {
        return tables.size() >= 2
                ? null
                : "the run drew " + tables.size() + " table(s) and QueryGenerator gates the join band on two or more";
    }

    /**
     * Names why {@code shape} could not have generated a single query in this run, or returns
     * {@code null} when the run's tables and knobs allow it. runFuzz asserts a zero-query shape only
     * in the {@code null} case, so a deliberately disabled shape - or one whose per-run precondition
     * the random draw never met - cannot fail the run.
     * <p>
     * The switch covers {@link QueryShape} exhaustively on purpose: adding a shape stops the test
     * compiling until whoever adds it declares whether a zero-query run is legitimate for it.
     */
    private static String describeUngeneratableShape(QueryShape shape, FuzzConfig config, ObjList<FuzzTable> tables) {
        return switch (shape) {
            case GROUP_BY, SAMPLE_BY, SIMPLE -> null;
            case HORIZON_JOIN -> config.isHorizonJoinEnabled()
                    ? describeMissingJoinTables(tables)
                    : "-D" + FuzzConfig.HORIZON_JOIN_PROP + "=false";
            case LATEST_ON -> config.isLatestOnEnabled()
                    ? null
                    : "-D" + FuzzConfig.LATEST_ON_PROP + "=false";
            case POSTING -> hasPostingIndexedSymbol(tables)
                    ? null
                    : "the run's random schema drew no posting-indexed SYMBOL, so PostingClause.tryGenerate returns null on every draw";
            case TEMPORAL_JOIN -> describeMissingJoinTables(tables);
            case WINDOW -> config.isWindowEnabled()
                    ? null
                    : "-D" + FuzzConfig.WINDOW_PROP + "=false";
            case WINDOW_JOIN -> config.isWindowJoinEnabled()
                    ? describeMissingJoinTables(tables)
                    : "-D" + FuzzConfig.WINDOW_JOIN_PROP + "=false";
        };
    }

    private static void dumpFailure(CairoEngine engine, SqlExecutionContext context, ObjList<FuzzTable> tables, GeneratedQuery query) {
        if (query.hasBind()) {
            String bindings = query.bindNames() + "=" + query.bindValues();
            LOG.error().$("fuzz bindings: ").$safe(query.bindSql()).$(' ').$safe(bindings).$();
        }
        String shadowSql = query.sql();
        for (int i = 0; i < tables.size(); i++) {
            FuzzTable table = tables.getQuick(i);
            if (table.getShadow() != null) {
                shadowSql = shadowSql.replaceAll("\\b" + java.util.regex.Pattern.quote(table.getName()) + "\\b",
                        java.util.regex.Matcher.quoteReplacement(table.getShadow().getName()));
            }
        }
        logPlan(engine, context, query.sql());
        logPlan(engine, context, shadowSql);
    }

    private static void dumpQuery(BufferedWriter dump, GeneratedQuery query) throws IOException {
        if (dump == null) {
            return;
        }
        dump.write(query.sql());
        dump.newLine();
        if (query.hasBind()) {
            dump.write("-- bind: " + query.bindSql() + " " + query.bindNames() + "=" + query.bindValues());
            dump.newLine();
        }
        dump.flush();
    }

    /**
     * Says whether any of the run's tables carries a posting-indexed SYMBOL column, which is what
     * PostingClause.tryGenerate needs to emit anything at all. FuzzTableFactory.assignIndexes draws
     * the index kind per SYMBOL column per run - half the SYMBOL columns get an index and a quarter
     * of those draw BITMAP - so a run can draw a schema with none, and 7 of 40 measured runs did.
     * On those runs POSTING reports 0/0 with a perfectly working generator, whatever the budget.
     */
    private static boolean hasPostingIndexedSymbol(ObjList<FuzzTable> tables) {
        for (int i = 0, n = tables.size(); i < n; i++) {
            final FuzzTable t = tables.getQuick(i);
            for (int j = 0, m = t.getColumnCount(); j < m; j++) {
                final FuzzIndex index = t.getColumn(j).getIndex();
                if (index != null && index.isPosting()) {
                    return true;
                }
            }
        }
        return false;
    }

    private static void logPlan(CairoEngine engine, SqlExecutionContext context, String sql) {
        try (RecordCursorFactory factory = engine.select(sql, context)) {
            TextPlanSink plan = new TextPlanSink();
            plan.of(factory, context);
            LOG.error().$("fuzz plan: ").$safe(sql).$("\n").$safe(plan.getSink()).$();
        } catch (Exception e) {
            LOG.error().$("fuzz plan failed: ").$safe(sql).$(" ").$(e).$();
        }
    }

    private static void logSchema(FuzzTable t) {
        StringBuilder sb = new StringBuilder("fuzz schema ").append(t.getName())
                .append(" (parquet=").append(t.getParquetMode());
        if (t.getParquetPartitions() != null) {
            sb.append(" partitions=[").append(t.getParquetPartitions()).append(']');
        }
        sb.append("):");
        for (int j = 0, n = t.getColumnCount(); j < n; j++) {
            FuzzColumn c = t.getColumn(j);
            sb.append(' ').append(c.getName()).append('=').append(c.getType().getDdl());
            if (c.isIndexed()) {
                sb.append(c.getIndex().describe());
            }
        }
        String message = sb.toString();
        LOG.info().$safe(message).$();
    }

    private static BufferedWriter openDump(String path) throws IOException {
        if (path == null || path.isEmpty()) {
            return null;
        }
        return new BufferedWriter(new FileWriter(Paths.get(path).toFile(), true));
    }

    private static boolean referencesTable(GeneratedQuery query, ObjList<FuzzTable> tables) {
        String sql = query.sql().toLowerCase(java.util.Locale.ROOT);
        for (int i = 0; i < tables.size(); i++) {
            if (sql.contains(tables.getQuick(i).getName().toLowerCase(java.util.Locale.ROOT))) {
                return true;
            }
        }
        return false;
    }

    public enum Phase {DIFFERENTIAL, FAULTS, MIXED}
}
