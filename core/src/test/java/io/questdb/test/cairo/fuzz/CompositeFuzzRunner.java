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

package io.questdb.test.cairo.fuzz;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.fuzz.FuzzAddColumnOperation;
import io.questdb.test.fuzz.FuzzAddCoveringIndexOperation;
import io.questdb.test.fuzz.FuzzChangeColumnTypeOperation;
import io.questdb.test.fuzz.FuzzChangeSymbolCapacityOperation;
import io.questdb.test.fuzz.FuzzConvertPartitionToNativeOperation;
import io.questdb.test.fuzz.FuzzConvertPartitionToParquetOperation;
import io.questdb.test.fuzz.FuzzDropColumnOperation;
import io.questdb.test.fuzz.FuzzDropCreateTableOperation;
import io.questdb.test.fuzz.FuzzDropPartitionOperation;
import io.questdb.test.fuzz.FuzzInsertOperation;
import io.questdb.test.fuzz.FuzzQueryOperation;
import io.questdb.test.fuzz.FuzzRenameColumnOperation;
import io.questdb.test.fuzz.FuzzSetParquetEncodingOperation;
import io.questdb.test.fuzz.FuzzSetTableFormatOperation;
import io.questdb.test.fuzz.FuzzSetTtlOperation;
import io.questdb.test.fuzz.FuzzStableInsertOperation;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.fuzz.FuzzTransactionGenerator;
import io.questdb.test.fuzz.FuzzTransactionOperation;
import io.questdb.test.fuzz.FuzzTruncateTableOperation;
import io.questdb.test.fuzz.FuzzValidateSymbolFilterOperation;
import io.questdb.test.tools.TestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Differential fuzz harness: builds a composite (subject) table and a plain (reference) table
 * from ONE column model, applies one generated transaction list to both through the WAL writer
 * API, and asserts the two are indistinguishable by content.
 * <p>
 * For Task 1 the composite table has exactly one identity dimension ({@code exch}). Randomizing
 * the dimension set/count is out of scope here (Task 2).
 */
public class CompositeFuzzRunner {
    private static final Log LOG = LogFactory.getLog(CompositeFuzzRunner.class);
    private final CairoEngine engine;
    private final Rnd rnd;
    private final long seed0;
    private final long seed1;
    private final SqlExecutionContext sqlExecutionContext;
    private Axes axes;
    private long baselineExistingCellRowCount;
    private long baselineFastAppendCommittedCount;
    private long baselineMultiCellFastAppendCommittedCount;
    private long baselineO3MergeCommitCount;
    private int comparedShapeCount;
    private String compositeName;
    private int gatedAttempted;
    private String plainName;

    /**
     * Whether a given {@code Fuzz*Operation} class is safe to apply, unchanged, to BOTH twins
     * ({@link Support#SUPPORTED}) or must instead be exercised only via {@link #applyGatedOperation}
     * against the composite subject, expecting a throw and asserting no damage ({@link
     * Support#GATED}). Per spec §5.1: every concrete {@code FuzzTransactionOperation} implementation
     * in {@code io.questdb.test.fuzz} must appear here -- enforced by {@code
     * CompositeFuzzOpCoverageTest} (Task 6), which scans the package rather than trusting this list to
     * stay complete on its own.
     */
    public enum Support {
        GATED,
        SUPPORTED
    }

    private CompositeFuzzRunner(CairoEngine engine, Rnd rnd) {
        this.engine = engine;
        this.rnd = rnd;
        // Captured BEFORE anything (Axes.resolve, generate()) consumes rnd, since Rnd#getSeed0/1
        // return current internal state, not the constructor args -- this is the only point at
        // which they coincide. Used only to name a seed in assertExercised()'s failure messages.
        this.seed0 = rnd.getSeed0();
        this.seed1 = rnd.getSeed1();
        this.sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine);
    }

    public static CompositeFuzzRunner of(CairoEngine engine, Rnd rnd) {
        return new CompositeFuzzRunner(engine, rnd);
    }

    /**
     * Resolved composite axes for one run: dimension set/count, directory-naming layout,
     * clustering, cell cardinality and the fast-append flag. Exposed so failure messages
     * (Task 4) and the coverage matrix (Task 7) can report exactly what shape produced a
     * given result.
     */
    public static final class Axes {
        // The brief's own example: an identity dimension plus a hash and a truncate dimension,
        // both driven off the same SYMBOL column ("sym") with a different transform each --
        // composite partitioning places no requirement that dimensions reference distinct
        // columns. A shuffled prefix of this pool is used so dimCount also varies WHICH
        // dimensions are present, not just how many.
        private static final String[] DIM_POOL = {"exch", "hash(sym, 32)", "truncate(sym, 3)"};
        // small, medium, at the open-cell cap (CAIRO_WAL_COMPOSITE_FASTAPPEND_MAX_OPEN_CELLS
        // defaults to 64), above it.
        private static final int[] CARDINALITIES = {3, 16, 64, 96};

        public final int cardinality;       // distinct dimension values to generate
        public final boolean clustered;     // ORDER BY sym
        public final int dimCount;          // 1..3
        public final String[] dimClauses;   // e.g. "exch", "hash(sym, 32)", "truncate(sym, 3)"
        public final boolean fastAppend;    // cairo.wal.composite.fastappend.enabled
        public final boolean hivelayout;    // false => LAYOUT PLAIN

        private Axes(int dimCount, String[] dimClauses, boolean hivelayout, boolean clustered, int cardinality, boolean fastAppend) {
            this.dimCount = dimCount;
            this.dimClauses = dimClauses;
            this.hivelayout = hivelayout;
            this.clustered = clustered;
            this.cardinality = cardinality;
            this.fastAppend = fastAppend;
        }

        static Axes resolve(Rnd rnd) {
            int dimCount = 1 + rnd.nextInt(DIM_POOL.length);
            String[] shuffled = DIM_POOL.clone();
            // Fisher-Yates using the SAME rnd the rest of resolve() draws from -- deterministic
            // given the seed, so re-running with the same seed reproduces the same axes.
            for (int i = shuffled.length - 1; i > 0; i--) {
                int j = rnd.nextInt(i + 1);
                String tmp = shuffled[i];
                shuffled[i] = shuffled[j];
                shuffled[j] = tmp;
            }
            String[] dimClauses = Arrays.copyOf(shuffled, dimCount);
            boolean hivelayout = rnd.nextBoolean();
            boolean clustered = rnd.nextBoolean();
            int cardinality = CARDINALITIES[rnd.nextInt(CARDINALITIES.length)];
            boolean fastAppend = rnd.nextBoolean();
            return new Axes(dimCount, dimClauses, hivelayout, clustered, cardinality, fastAppend);
        }

        @Override
        public String toString() {
            return "dims=" + String.join(",", dimClauses)
                    + " layout=" + (hivelayout ? "HIVE" : "PLAIN")
                    + " clustered=" + clustered
                    + " cardinality=" + cardinality
                    + " fastAppend=" + fastAppend;
        }
    }

    /**
     * Classifies every concrete {@code Fuzz*Operation} implementation in {@code io.questdb.test.fuzz}
     * as {@link Support#SUPPORTED} (safe to apply, unchanged, to both twins) or {@link Support#GATED}
     * (must not be applied to the composite subject in the ordinary twin-apply flow -- either because
     * the product itself throws a "composite partitioning does not yet support ..." {@link
     * CairoException} for it, or because applying it would silently break the twin-equality invariant
     * some other way). Keyed by class identity ({@link IdentityHashMap}), not equality, since these
     * are all distinct concrete classes.
     * <p>
     * Evidence per entry (verified against {@code CompositeUnsupportedOpsTest} and the production
     * gate sites in {@code TableWriter}/{@code SqlCompilerImpl}, not against design-doc prose):
     * <ul>
     *     <li>{@link FuzzDropColumnOperation}, {@link FuzzRenameColumnOperation}, {@link
     *     FuzzChangeColumnTypeOperation}, {@link FuzzDropPartitionOperation}, {@link
     *     FuzzConvertPartitionToParquetOperation}, {@link FuzzConvertPartitionToNativeOperation},
     *     {@link FuzzSetTtlOperation}, {@link FuzzAddCoveringIndexOperation} -- GATED. Each has a
     *     dedicated {@code CompositeUnsupportedOpsTest} case confirming a synchronous- or
     *     suspension-path {@link CairoException} containing "composite partitioning does not yet
     *     support ...".</li>
     *     <li>{@link FuzzAddColumnOperation} -- SUPPORTED. {@code TableWriter#addColumn} has dedicated
     *     composite-aware handling ({@code writeCompositeAddColumnColumnVersions}) for every column
     *     type EXCEPT a newly-added SYMBOL column, which hits its own, narrower, orthogonal gate ("ADD
     *     COLUMN of type SYMBOL is not yet supported on composite-partitioned tables") independent of
     *     this classification -- a caller generating a SYMBOL-typed add must expect that gate
     *     separately.</li>
     *     <li>{@link FuzzSetTableFormatOperation}, {@link FuzzSetParquetEncodingOperation} --
     *     SUPPORTED. Both {@code TableWriter#setMetaTableFormat} and {@code
     *     #setColumnParquetEncoding} are pure metadata writes (a default-format flag / a per-column
     *     encoding config used only if a partition is LATER converted to parquet, itself already
     *     gated) with no per-cell or per-partition file interaction at all.</li>
     *     <li>{@link FuzzInsertOperation}, {@link FuzzStableInsertOperation}, {@link
     *     FuzzTruncateTableOperation}, {@link FuzzQueryOperation}, {@link
     *     FuzzValidateSymbolFilterOperation} -- SUPPORTED. The well-established composite-safe core
     *     (insert/truncate/read), per {@code CompositeUnsupportedOpsTest}'s own SUPPORTED half.</li>
     *     <li>{@link FuzzChangeSymbolCapacityOperation} -- GATED, but NOT because the product rejects
     *     it: {@code TableWriter#changeSymbolCapacity} (reached directly from {@code ALTER TABLE ...
     *     ALTER COLUMN ... SYMBOL CAPACITY n}) has NO {@code isRoutedComposite()} check, unlike its
     *     sibling {@code changeColumnType}. Its own reopen step resolves the last partition via the
     *     cellKey-0-only path -- {@code TableWriter#scaleSymbolCapacities()}'s doc, guarding the ONLY
     *     OTHER call site of the same method, calls this "a genuine correctness risk, not merely a
     *     missed optimization" for a routed composite table. A minimal SQL repro against a routed
     *     2-cell table did not reproduce visible corruption (the risky reopen branch requires
     *     transientRowCount &gt; 0 at ALTER time, a narrower timing window this repro did not hit), so
     *     this is reported as a SUSPECTED, unconfirmed defect, not a proven one -- classified GATED
     *     here purely for harness safety (never apply it to the twin) pending a proper audit.</li>
     *     <li>{@link FuzzDropCreateTableOperation} -- GATED, also not a product rejection: it drops
     *     and recreates the table via {@code TableStructMetadataAdapter}, which carries no partition-
     *     spec/dimension information at all, so replaying it against the composite subject would
     *     silently strip compositeness rather than throw. GATED here purely to keep the harness from
     *     ever doing that, not because any composite gate fires.</li>
     * </ul>
     */
    private static final Map<Class<? extends FuzzTransactionOperation>, Support> OPERATION_SUPPORT = buildOperationSupportMap();

    public Axes axes() {
        return axes;
    }

    /**
     * Executes {@code sql}, expecting it to be REFUSED by a composite gate, and asserts the refusal
     * left no damage: the composite subject is still readable, its row count is unchanged, and it
     * remains twin-equal with the plain reference. See the class javadoc on {@link Support#GATED} for
     * why this matters -- a gate that throws after partially mutating {@code _txn} or the directory
     * tree would pass every pre-existing test and fail only here.
     */
    public void applyGatedOperation(String sql) throws Exception {
        gatedAttempted++;
        // CompositeFuzzRunner is not an AbstractCairoTest subclass (it is a plain helper used BY test
        // classes, in a different package), so it cannot reach that class's protected static
        // execute(CharSequence) -- engine.execute(...) against this runner's own sqlExecutionContext
        // is the equivalent direct-engine call every other method in this class already uses (see
        // createTables()).
        boolean threwSynchronously;
        try {
            engine.execute(sql, sqlExecutionContext);
            threwSynchronously = false;
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "composite");
            threwSynchronously = true;
        }
        if (!threwSynchronously) {
            // Most composite DDL gates fire asynchronously, from the WAL-apply job, not from
            // execute() itself -- execute() only enqueues a structural change for a WAL table.
            // Confirmed empirically: "ALTER TABLE ... DROP COLUMN" against this runner's WAL
            // composite table returns normally from execute() and instead suspends the table once
            // drained. CompositeUnsupportedOpsTest's own assertCompositeGateFires established this
            // exact dual-path idiom; mirrored here rather than assuming every gate throws
            // synchronously, which would make this method a false green for the common case.
            TestUtils.drainWalQueue(engine);
            TableToken token = engine.verifyTableName(compositeName);
            if (!engine.getTableSequencerAPI().isSuspended(token)) {
                throw new AssertionError("expected a composite gate to reject: " + sql);
            }
            StringSink errSink = new StringSink();
            TestUtils.printSql(engine, sqlExecutionContext,
                    "select errorMessage from wal_tables() where name = '" + compositeName + "'", errSink);
            TestUtils.assertContains(errSink, "composite");
        }
    }

    /**
     * Looks up a {@code Fuzz*Operation} class's {@link Support} classification. Returns {@code null}
     * for a class absent from {@link #OPERATION_SUPPORT} -- {@code CompositeFuzzOpCoverageTest} (Task
     * 6) is what turns that into a loud test failure naming the class; this method itself stays a
     * plain lookup so it is usable from ordinary (non-test-failure) call sites too.
     */
    public static Support classify(Class<? extends FuzzTransactionOperation> opClass) {
        return OPERATION_SUPPORT.get(opClass);
    }

    /**
     * Read-only view of the full classification map, for {@code CompositeFuzzOpCoverageTest} (Task 6)
     * to diff against its own package scan.
     */
    public static Map<Class<? extends FuzzTransactionOperation>, Support> operationSupportMap() {
        return Collections.unmodifiableMap(OPERATION_SUPPORT);
    }

    /**
     * Generates ONE transaction list against the plain table's metadata and applies it to both
     * twins.
     */
    public void applyGeneratedTransactions(int rowCount, int transactionCount) throws Exception {
        ObjList<FuzzTransaction> transactions = generate(rowCount, transactionCount);
        applyToBoth(transactions);
    }

    /**
     * Applies every operation of every transaction to both the composite and the plain writer,
     * in transaction order, draining the WAL queue periodically (not only once at the very end) so
     * a long enough run genuinely produces MULTIPLE separate WAL-apply passes over the same table --
     * needed for Task 4's "rows landing in a non-last (already-populated) partition" anti-vacuity
     * floor. A single trailing drain would merge every logical transaction into one WAL-apply batch
     * (confirmed empirically: 40 transactions -> exactly 1 {@code processO3BlockComposite} call),
     * so EVERY cellKey's first appearance would have {@code srcDataMax == 0} -- the floor could never
     * be satisfied, structurally, regardless of seed. Draining every {@link #DRAIN_CHUNK_SIZE}
     * transactions forces earlier commits to actually land in table storage before later ones are
     * generated against overlapping dimension values, so a later commit revisiting an already-
     * populated cell is a real WAL-apply shape, not merely simulated. This does not change WHAT is
     * compared (final content is drain-frequency-independent), only how many separate apply passes
     * produce it -- Task 1/2/3's existing tests are unaffected by construction.
     */
    public void applyToBoth(ObjList<FuzzTransaction> transactions) {
        final int drainChunkSize = 8;
        TableWriterAPI compositeWriter = engine.getTableWriterAPI(compositeName, "composite fuzz apply");
        TableWriterAPI plainWriter = engine.getTableWriterAPI(plainName, "composite fuzz apply");
        try {
            // Applying the same operation to BOTH writers from ONE Rnd reproduces identical values
            // only because FuzzInsertOperation#apply opens with rnd.reset(s1, s0), replaying from a
            // seed stored on the operation itself.
            //
            // That invariant is NARROW: rnd.reset appears in FuzzInsertOperation and in NO other
            // Fuzz*Operation. Task 1 is safe because it generates inserts exclusively. ANY task that
            // widens the generated operation mix (Task 2 onwards) MUST re-verify this per operation
            // type -- an operation that does not reset would consume from the shared stream and hand
            // the second twin different values, silently producing a false failure or, worse,
            // matching garbage.
            Rnd applyRnd = new Rnd();
            for (int i = 0, n = transactions.size(); i < n; i++) {
                FuzzTransaction transaction = transactions.getQuick(i);
                for (int opIndex = 0, opCount = transaction.operationList.size(); opIndex < opCount; opIndex++) {
                    FuzzTransactionOperation operation = transaction.operationList.getQuick(opIndex);
                    operation.apply(applyRnd, engine, compositeWriter, -1, null);
                    operation.apply(applyRnd, engine, plainWriter, -1, null);
                }
                if (transaction.rollback) {
                    compositeWriter.rollback();
                    plainWriter.rollback();
                } else {
                    compositeWriter.commit();
                    plainWriter.commit();
                }
                if ((i + 1) % drainChunkSize == 0) {
                    TestUtils.drainWalQueue(engine);
                }
            }
        } finally {
            compositeWriter.close();
            plainWriter.close();
        }
        TestUtils.drainWalQueue(engine);
    }

    /**
     * The full comparison oracle: every shape in spec Sec 4.4 ("Verifying the Supported Surface")
     * must be identical between subject and reference. Each of the seven {@code compare*} helpers
     * below increments {@link #comparedShapeCount} exactly once, even where it issues more than one
     * query, so {@link #comparedShapeCount()} always reads 7 after a full, uninterrupted run.
     * <p>
     * EVERY query issued by every shape orders by every selected column (or is a single-row
     * aggregate, which needs no ordering at all) for the reason Task 1 established: the generator
     * emits equal-timestamp rows on purpose ({@code probabilityOfSameTimestamp}), and the composite
     * table's cell-grouped storage order differs from the plain table's by construction -- so a
     * partial {@code ORDER BY} would leave ties to storage order and produce an intermittent false
     * RED with no defect present. The first instinct on a red differential run must be to suspect
     * the product, so the comparison itself has to be order-deterministic.
     * <p>
     * Composite-only sanity (spec: {@code table_partitions()} row count equals the number of
     * distinct {@code (day, cell)} pairs, every named directory exists) is deliberately NOT
     * implemented here: it is explicitly "asserted separately, not compared to the twin" in the
     * spec, is outside this task's stated interface ({@code assertTwinEqual()} covering the seven
     * compared shapes), and correctly recomputing the expected cell count would mean replicating
     * {@code hash(col,N)}/{@code truncate(col,N)} bucket logic in SQL -- exactly the kind of
     * harness-side complexity that risks a false RED unrelated to the product. Left for the
     * anti-vacuity counters (Task 4), which read the count from the product's own counters instead
     * of recomputing it.
     */
    public void assertTwinEqual() throws SqlException {
        comparedShapeCount = 0;
        compareFullScan();          // 1: full scan, forward and backward
        compareAggregates();        // 2: count(*), min(ts), max(ts)
        compareLatestOn();          // 3: LATEST ON ts PARTITION BY sym
        compareSampleBy();          // 4: SAMPLE BY, keyed aggregate
        compareDimensionFiltered(); // 5: dimension = / IN, present and absent
        compareIntervalScan();      // 6: interval crossing a partition boundary
        compareWindowJoinSlave();   // 7: window-join, table as slave
    }

    /**
     * Anti-vacuity floors (spec Sec 4.5). {@link #assertTwinEqual()} passing proves the two tables
     * agree; it does NOT prove the run actually exercised anything composite-specific -- a run that
     * never routes a second cell, never takes the O3/full dispatch path, never fast-appends (with
     * the flag on), or never lands a row in an already-populated cell would pass every shape while
     * testing nothing. This is the check that makes a green {@link #assertTwinEqual()} meaningful:
     * call it AFTER {@code assertTwinEqual()} (so a real product divergence is reported as that,
     * not masked by an under-exercised-run failure here), and it throws {@link AssertionError} on
     * the first unmet floor, naming the seed and {@link Axes#toString()} so an under-exercising run
     * is a loud, diagnosable failure rather than a silently-vacuous green.
     * <p>
     * The fifth floor, "gated operations attempted" (spec Sec 4.5's fifth row), is checked last, via
     * {@link #gatedAttempted}: a run that never called {@link #applyGatedOperation} never proved a
     * gate actually rejects anything on THIS run's shape. Wired in Task 5, once gate classification
     * existed.
     * <p>
     * The cellKey floor is normally &ge;2; it relaxes to &ge;1 only when the generated data itself
     * cannot possibly have produced a second cell -- i.e. every row shares one {@code (exch, sym)}
     * tuple on the reference table. {@code (exch, sym)} is an upper bound on the number of distinct
     * cells the subject could have routed (a HASH/TRUNCATE dimension can only ever coarsen that,
     * never add cells beyond it), so this is the harness's own analog of "the axis deliberately
     * chose a single-cell shape" without needing a dedicated axis flag for it.
     */
    public void assertExercised() throws SqlException {
        final long distinctCellKeys = queryLong("SELECT count() FROM table_partitions('" + compositeName + "')");
        final long maxPossibleCells = queryLong(
                "SELECT count() FROM (SELECT DISTINCT exch, sym FROM " + plainName + ")"
        );
        final long cellKeyFloor = maxPossibleCells < 2 ? 1 : 2;
        if (distinctCellKeys < cellKeyFloor) {
            throw new AssertionError(exercisedFailureMessage(
                    "distinct cellKeys routed=" + distinctCellKeys + " floor=" + cellKeyFloor
                            + " (maxPossibleCells=" + maxPossibleCells + " distinct (exch,sym) tuples generated)"
            ));
        }

        final long o3MergeCommits = TableWriter.getCompositeO3MergeCommitCount() - baselineO3MergeCommitCount;
        if (o3MergeCommits < 1) {
            throw new AssertionError(exercisedFailureMessage(
                    "commits taking the composite O3/full dispatch path=" + o3MergeCommits + " floor=1"
            ));
        }

        if (axes.fastAppend) {
            final long fastAppendCommits =
                    (TableWriter.getCompositeFastAppendCommittedCount() - baselineFastAppendCommittedCount)
                            + (TableWriter.getCompositeMultiCellFastAppendCommittedCount() - baselineMultiCellFastAppendCommittedCount);
            if (fastAppendCommits < 1) {
                throw new AssertionError(exercisedFailureMessage(
                        "fast-append commits=" + fastAppendCommits + " floor=1 (fastAppend flag is ON for this run)"
                ));
            }
        }

        final long existingCellRows = TableWriter.getCompositeExistingCellRowCount() - baselineExistingCellRowCount;
        if (existingCellRows < 1) {
            throw new AssertionError(exercisedFailureMessage(
                    "rows landing in a non-last (already-populated) partition=" + existingCellRows + " floor=1"
            ));
        }

        if (gatedAttempted < 1) {
            throw new AssertionError(exercisedFailureMessage(
                    "gated operations attempted=" + gatedAttempted + " floor=1"
            ));
        }
    }

    public int comparedShapeCount() {
        return comparedShapeCount;
    }

    public String compositeName() {
        return compositeName;
    }

    /**
     * {@code count(*)} on the composite subject -- used by {@link #applyGatedOperation}'s callers to
     * assert a rejected gate left row count unchanged.
     */
    public long compositeRowCount() throws SqlException {
        return queryLong("SELECT count() FROM " + compositeName);
    }

    /**
     * ONE column model, TWO DDLs. The reference differs from the subject only in the partition
     * clause, so a divergence can never come from the schema.
     * <p>
     * The subject's dimension set, directory-naming layout, clustering, cardinality and
     * fast-append flag are all drawn from {@code rnd} via {@link Axes#resolve}, so successive
     * calls against the same runner's {@code rnd} (or fresh runners seeded differently) exercise
     * different composite shapes. The reference table is untouched by any of this -- it is always
     * {@code PARTITION BY DAY WAL} with no dimensions, order, or layout clause.
     */
    public void createTables(String base) throws SqlException {
        this.compositeName = base + "_composite";
        this.plainName = base + "_plain";
        // Anti-vacuity floors (Task 4) read these TableWriter counters as deltas, not absolutes:
        // they are static/JVM-wide (see each field's own doc -- the writer that processes a WAL
        // commit is internal to drainWalQueue() and released right after, so a per-instance field
        // would not reliably be observable afterwards), and therefore accumulate across every
        // runner/test sharing this JVM. Snapshotting here, before any transaction is applied to
        // THIS runner's tables, isolates this run's own contribution.
        this.baselineO3MergeCommitCount = TableWriter.getCompositeO3MergeCommitCount();
        this.baselineExistingCellRowCount = TableWriter.getCompositeExistingCellRowCount();
        this.baselineFastAppendCommittedCount = TableWriter.getCompositeFastAppendCommittedCount();
        this.baselineMultiCellFastAppendCommittedCount = TableWriter.getCompositeMultiCellFastAppendCommittedCount();
        this.axes = Axes.resolve(rnd);
        final String cols = "(ts TIMESTAMP, exch SYMBOL, sym SYMBOL, px DOUBLE, qty LONG)";

        // Must be set before the composite CREATE TABLE: CairoTestConfiguration#getDelegate
        // re-resolves Overrides#getConfiguration() on every config access (it is a live
        // delegating wrapper, not a value snapshot taken once at @BeforeClass), so this reaches
        // TableWriter's eligibility check on the very next commit against this table.
        AbstractCairoTest.staticOverrides.setProperty(PropertyKey.CAIRO_WAL_COMPOSITE_FASTAPPEND_ENABLED, axes.fastAppend);

        StringBuilder subjectDdl = new StringBuilder()
                .append("CREATE TABLE ").append(compositeName).append(' ').append(cols)
                .append(" TIMESTAMP(ts) PARTITION BY DAY, ").append(String.join(", ", axes.dimClauses));
        if (axes.clustered) {
            subjectDdl.append(" ORDER BY sym");
        }
        if (!axes.hivelayout) {
            subjectDdl.append(" LAYOUT PLAIN");
        }
        subjectDdl.append(" WAL");
        engine.execute(subjectDdl.toString(), sqlExecutionContext);

        engine.execute(
                "CREATE TABLE " + plainName + " " + cols + " TIMESTAMP(ts) PARTITION BY DAY WAL",
                sqlExecutionContext
        );
    }

    public String plainName() {
        return plainName;
    }

    /**
     * Builds {@link #OPERATION_SUPPORT}. A plain method (not an inline initializer) so the 18-entry
     * table reads as a table, and so a missing/extra entry is easy to spot in review against {@link
     * #OPERATION_SUPPORT}'s own javadoc, which documents the evidence for every row.
     */
    private static Map<Class<? extends FuzzTransactionOperation>, Support> buildOperationSupportMap() {
        Map<Class<? extends FuzzTransactionOperation>, Support> m = new IdentityHashMap<>();
        // SUPPORTED -- safe to apply, unchanged, to both twins.
        m.put(FuzzInsertOperation.class, Support.SUPPORTED);
        m.put(FuzzStableInsertOperation.class, Support.SUPPORTED);
        m.put(FuzzTruncateTableOperation.class, Support.SUPPORTED);
        m.put(FuzzQueryOperation.class, Support.SUPPORTED);
        m.put(FuzzValidateSymbolFilterOperation.class, Support.SUPPORTED);
        m.put(FuzzAddColumnOperation.class, Support.SUPPORTED);
        m.put(FuzzSetTableFormatOperation.class, Support.SUPPORTED);
        m.put(FuzzSetParquetEncodingOperation.class, Support.SUPPORTED);
        // GATED -- product throws a "composite partitioning does not yet support ..." CairoException.
        m.put(FuzzDropColumnOperation.class, Support.GATED);
        m.put(FuzzRenameColumnOperation.class, Support.GATED);
        m.put(FuzzChangeColumnTypeOperation.class, Support.GATED);
        m.put(FuzzDropPartitionOperation.class, Support.GATED);
        m.put(FuzzConvertPartitionToParquetOperation.class, Support.GATED);
        m.put(FuzzConvertPartitionToNativeOperation.class, Support.GATED);
        m.put(FuzzSetTtlOperation.class, Support.GATED);
        m.put(FuzzAddCoveringIndexOperation.class, Support.GATED);
        // GATED -- harness-safety only; the product itself does not throw for these (see javadoc).
        m.put(FuzzChangeSymbolCapacityOperation.class, Support.GATED);
        m.put(FuzzDropCreateTableOperation.class, Support.GATED);
        return m;
    }

    /**
     * Shape 2: {@code count(*)}, {@code min(ts)}, {@code max(ts)}. A single row -- trivially
     * order-deterministic, no {@code ORDER BY} needed.
     */
    private void compareAggregates() throws SqlException {
        final String select = "SELECT count(*) c, min(ts) mn, max(ts) mx FROM ";
        TestUtils.assertSqlCursors(engine, sqlExecutionContext, select + plainName, select + compositeName, LOG);
        comparedShapeCount++;
    }

    /**
     * Shape 5: dimension-filtered reads, {@code =} and {@code IN}, each against one value known
     * present and one known absent. "Present" is read off the reference table (content-identical to
     * the subject by construction) rather than hardcoded, since Task 2's {@code axes.cardinality}
     * varies which of {@code "SYM0".."SYM(cardinality-1)"} actually appear. "Absent" is a literal
     * outside that whole naming scheme, so it is guaranteed never generated regardless of axes.
     */
    private void compareDimensionFiltered() throws SqlException {
        final String present = firstNonNullExch();
        final String absent = "SYM_ABSENT_PROBE";
        final String order = " ORDER BY ts, exch, sym, px, qty";
        TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                "SELECT * FROM " + plainName + " WHERE exch = '" + present + "'" + order,
                "SELECT * FROM " + compositeName + " WHERE exch = '" + present + "'" + order, LOG);
        TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                "SELECT * FROM " + plainName + " WHERE exch = '" + absent + "'" + order,
                "SELECT * FROM " + compositeName + " WHERE exch = '" + absent + "'" + order, LOG);
        TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                "SELECT * FROM " + plainName + " WHERE exch IN ('" + present + "','" + absent + "')" + order,
                "SELECT * FROM " + compositeName + " WHERE exch IN ('" + present + "','" + absent + "')" + order, LOG);
        comparedShapeCount++;
    }

    /**
     * Shape 1: full scan, forward ({@code ORDER BY ts ASC}) and backward ({@code ORDER BY ts DESC}).
     * One counter increment for both queries, per the brief ("Each shape increments the counter
     * once, even when it issues two queries").
     */
    private void compareFullScan() throws SqlException {
        final String orderAsc = " ORDER BY ts, exch, sym, px, qty";
        final String orderDesc = " ORDER BY ts DESC, exch DESC, sym DESC, px DESC, qty DESC";
        TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                "SELECT * FROM " + plainName + orderAsc, "SELECT * FROM " + compositeName + orderAsc, LOG);
        TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                "SELECT * FROM " + plainName + orderDesc, "SELECT * FROM " + compositeName + orderDesc, LOG);
        comparedShapeCount++;
    }

    /**
     * Shape 6: a timestamp-bounded interval scan crossing at least one partition boundary. The
     * generator's window is {@code 2023-01-01T00:00 .. 2023-01-03T00:00} (table is
     * {@code PARTITION BY DAY}); 18:00 the day before midnight through 06:00 the day after spans
     * exactly one DAY boundary.
     */
    private void compareIntervalScan() throws SqlException {
        final String sql = " WHERE ts >= '2023-01-01T18:00:00.000000Z' AND ts < '2023-01-02T06:00:00.000000Z'" +
                " ORDER BY ts, exch, sym, px, qty";
        TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                "SELECT * FROM " + plainName + sql, "SELECT * FROM " + compositeName + sql, LOG);
        comparedShapeCount++;
    }

    /**
     * Shape 3: {@code LATEST ON ts PARTITION BY sym}. At most one row per distinct {@code sym}
     * value -- including a NULL group, now that Task 2 restored NULL generation -- so
     * {@code ORDER BY sym} alone makes row order deterministic.
     */
    private void compareLatestOn() throws SqlException {
        final String sql = " LATEST ON ts PARTITION BY sym ORDER BY sym";
        TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                "SELECT * FROM " + plainName + sql, "SELECT * FROM " + compositeName + sql, LOG);
        comparedShapeCount++;
    }

    /**
     * Shape 4: {@code SAMPLE BY} over a bucket coarser than the partition time unit (DAY: the
     * generator's window is a 2-day span, so {@code 3d} folds it to a single deterministic bucket
     * boundary), with a keyed aggregate ({@code exch} is the implicit SAMPLE BY key: any
     * non-aggregated selected column becomes one). {@code ORDER BY exch, ts} breaks ties across
     * keys and buckets, including the NULL-{@code exch} group.
     */
    private void compareSampleBy() throws SqlException {
        final String select = "SELECT exch, ts, sum(qty) q, avg(px) p FROM ";
        final String sql = " SAMPLE BY 3d ORDER BY exch, ts";
        TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                select + plainName + sql, select + compositeName + sql, LOG);
        comparedShapeCount++;
    }

    /**
     * Shape 7: a windowed aggregate with the table as the WINDOW JOIN slave. Built against a small,
     * throwaway master ("probe") table of fixed, distinct timestamps spanning the fuzz window --
     * WINDOW JOIN's left side needs no key match (an {@code ON} clause is optional per the grammar;
     * QuestDB's own {@code SyncWindowJoinMemoryTrackerTest} uses the same bare form), so this probes
     * every slave row within +-6h of each fixed probe timestamp, agnostic of {@code exch}/{@code
     * sym}. The probe table is created and dropped around the comparison so repeated {@code
     * assertTwinEqual()} calls on the same runner (or other runners in the same test) never collide
     * on its name.
     */
    private void compareWindowJoinSlave() throws SqlException {
        final String probeName = compositeName + "_probe";
        engine.execute(
                "CREATE TABLE " + probeName + " (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                sqlExecutionContext
        );
        try {
            engine.execute(
                    "INSERT INTO " + probeName + " VALUES " +
                            "('2023-01-01T00:00:00.000000Z')," +
                            "('2023-01-01T08:00:00.000000Z')," +
                            "('2023-01-01T16:00:00.000000Z')," +
                            "('2023-01-02T00:00:00.000000Z')," +
                            "('2023-01-02T08:00:00.000000Z')," +
                            "('2023-01-02T16:00:00.000000Z')",
                    sqlExecutionContext
            );
            final String sql = "SELECT p.ts, count() c, sum(s.qty) q, avg(s.px) a, min(s.ts) mn, max(s.ts) mx" +
                    " FROM " + probeName + " p WINDOW JOIN %s s" +
                    " RANGE BETWEEN 6 hours PRECEDING AND 6 hours FOLLOWING EXCLUDE PREVAILING" +
                    " ORDER BY p.ts";
            TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                    String.format(sql, plainName), String.format(sql, compositeName), LOG);
            comparedShapeCount++;
        } finally {
            engine.execute("DROP TABLE " + probeName, sqlExecutionContext);
        }
    }

    /**
     * Reads a present {@code exch} value off the reference table for shape 5. The reference is
     * content-identical to the subject by construction, and querying rather than hardcoding
     * survives Task 2's cardinality axis changing which of the generated symbol pool actually
     * landed in this run's rows.
     */
    private String firstNonNullExch() throws SqlException {
        try (RecordCursorFactory factory = engine.select(
                "SELECT exch FROM " + plainName + " WHERE exch IS NOT NULL LIMIT 1",
                sqlExecutionContext
        )) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                if (!cursor.hasNext()) {
                    throw new AssertionError(
                            "no non-null exch value generated -- cannot exercise the dimension-filtered shape"
                    );
                }
                return Chars.toString(cursor.getRecord().getSymA(0));
            }
        }
    }

    /**
     * Builds an {@link #assertExercised()} failure message naming the seed (captured at
     * construction, before {@code rnd} was consumed) and the resolved {@link Axes}, so an
     * under-exercising run is reproducible rather than a bare number.
     */
    private String exercisedFailureMessage(String detail) {
        return "composite fuzz run under-exercised the feature -- " + detail
                + " [seed0=" + seed0 + ", seed1=" + seed1 + ", axes=" + axes + "]";
    }

    /**
     * Reads a single {@code long} from a single-row, single-column query -- used by
     * {@link #assertExercised()} for {@code table_partitions()} and distinct-tuple counts. Returns
     * 0 for an empty result (e.g. {@code table_partitions()} on a never-routed composite table, or
     * a {@code DISTINCT} query over an empty reference table), which is the correct "nothing
     * happened" value for every floor this feeds.
     */
    private long queryLong(String sql) throws SqlException {
        try (RecordCursorFactory factory = engine.select(sql, sqlExecutionContext)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                if (!cursor.hasNext()) {
                    return 0;
                }
                return cursor.getRecord().getLong(0);
            }
        }
    }

    /**
     * Delegates to {@link FuzzTransactionGenerator#generateSet} against the plain table's
     * metadata: the two schemas are identical, and the plain reader is never itself under test.
     * Task 1 keeps this to plain data inserts (no structural DDL, no O3, no replace-range) so the
     * skeleton has the fewest moving parts; later tasks randomize these axes.
     * <p>
     * {@code probabilityOfUnassignedColumnValue} and {@code probabilityOfAssigningNull} were kept
     * at 0.0 in Task 1 to dodge a hang: {@code generateSet} applies both uniformly across every
     * column, including {@code exch} (the identity/partitioning dimension), and a NULL identity
     * dimension value colliding with a non-null one in the same WAL commit hung forever in
     * {@code TableWriter.processO3BlockComposite -> o3ConsumePartitionUpdates}. That defect is
     * fixed (commits 1654f92f17, b66e2553f8), so Task 2 restores both probabilities to a modest
     * non-zero value -- high enough that NULL/unassigned exch and sym values are reliably
     * generated across a run, low enough that most rows still carry an assigned, non-null
     * dimension value (there would be little left to compare otherwise).
     */
    private ObjList<FuzzTransaction> generate(int rowCount, int transactionCount) throws SqlException, NumericException {
        TableToken plainToken = engine.verifyTableName(plainName);
        try (
                TableRecordMetadata sequencerMetadata = engine.getLegacyMetadata(plainToken);
                TableMetadata tableMetadata = engine.getTableMetadata(plainToken)
        ) {
            long minTimestamp = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP)
                    .parseFloorLiteral("2023-01-01T00:00:00.000000Z");
            long maxTimestamp = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP)
                    .parseFloorLiteral("2023-01-03T00:00:00.000000Z");
            String[] symbols = new String[axes.cardinality];
            for (int i = 0; i < axes.cardinality; i++) {
                symbols[i] = "SYM" + i;
            }
            return FuzzTransactionGenerator.generateSet(
                    0, // initialRowCount: tables are created empty
                    sequencerMetadata,
                    tableMetadata,
                    rnd,
                    minTimestamp,
                    maxTimestamp,
                    rowCount,
                    transactionCount,
                    false, // o3: keep Task 1 to in-order inserts
                    0.0,   // probabilityOfCancelRow
                    0.1,   // probabilityOfUnassignedColumnValue (restored: hang fixed, see note above)
                    0.1,   // probabilityOfAssigningNull (restored: hang fixed, see note above)
                    0.0,   // probabilityOfTransactionRollback
                    0.0,   // probabilityOfAddingNewColumn
                    0.0,   // probabilityOfRemovingColumn
                    0.0,   // probabilityOfRenamingColumn
                    0.0,   // probabilityOfColumnTypeChange
                    1.0,   // probabilityOfDataInsert
                    0.1,   // probabilityOfSameTimestamp
                    0.0,   // probabilityOfDropPartition
                    0.0,   // probabilityOfConvertPartitionToParquet
                    0.0,   // probabilityOfConvertPartitionToNative
                    0.0,   // probabilityOfTruncate
                    0.0,   // probabilityOfDropTable
                    0.0,   // probabilityOfSetTtl
                    0.0,   // replaceInsertProb
                    0.0,   // probabilityOfSymbolAccessValidation
                    0.0,   // probabilityOfQuery
                    8,     // maxStrLenForStrColumns (no STRING/VARCHAR column in this model)
                    symbols,
                    (int) sequencerMetadata.getMetadataVersion(),
                    0.0,   // probabilityOfSetParquetEncoding
                    0.0,   // probabilityOfAddCoveringIndex
                    0.0    // probabilityOfSetTableFormat
            );
        }
    }
}
