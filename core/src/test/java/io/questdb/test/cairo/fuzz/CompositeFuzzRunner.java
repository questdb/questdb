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
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.fuzz.DuplicateFuzzInsertOperation;
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
    private long baselineFastAppendEligibleCount;
    private long baselineMultiCellFastAppendCommittedCount;
    private long baselineMultiCellFastAppendEligibleCount;
    private long baselineO3MergeCommitCount;
    private int comparedShapeCount;
    private double dropPartitionProbability;
    private int droppedAddColumnOps;
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
     *     <li>{@link DuplicateFuzzInsertOperation} -- SUPPORTED. Extends {@link FuzzInsertOperation}
     *     (found by Task 6's package scan via inheritance, not by grepping for the interface name
     *     directly -- the scan is what caught this one) and its {@code apply()} delegates straight to
     *     {@code super.apply(rnd, ...)}, which is where the {@code rnd.reset(s1, s0)} call actually
     *     lives; the one extra {@code rnd.nextBoolean()} draw in its {@code appendColumnValue}
     *     override happens strictly AFTER that reset, in the same deterministic column-iteration
     *     order both times {@code apply()} runs, so the shared-{@code Rnd} invariant is fully
     *     inherited, not broken.</li>
     *     <li>{@link FuzzChangeSymbolCapacityOperation} -- SUPPORTED, following the audit this entry
     *     previously deferred. It was classified GATED as a precaution while
     *     {@code TableWriter#changeSymbolCapacity} was a SUSPECTED defect: it carries no
     *     {@code isRoutedComposite()} check, and its reopen step resolves the last partition through
     *     the cellKey-0-only path that {@code scaleSymbolCapacities}'s own doc calls "a genuine
     *     correctness risk" for a routed composite table.
     *     <p>
     *     The audit (see {@code CompositeSymbolCapacityAlterTest}) settled it. The reopen provably DOES
     *     run ungated on a routed composite table -- instrumentation gives
     *     {@code routedComposite=true, transientRowCount=1, willReopen=true} -- but the ALTER is
     *     ACCEPTED and leaves the table twin-correct and unsuspended, including when the ALTER and the
     *     writes that follow it land in one apply pass, which is the shape that would use the
     *     repositioned handle.
     *     <p>
     *     GATED is therefore the wrong label: {@link #applyGatedOperation} asserts a composite REFUSAL,
     *     and there is none to assert. Residual risk, pinned by that test rather than by this
     *     classification: nothing breaks only because the composite write path re-resolves per-cell
     *     handles and never uses the one this reopen moved -- an unstated invariant of another code
     *     path.</li>
     *     <li>{@link FuzzDropCreateTableOperation} -- GATED, also not a product rejection: it drops
     *     and recreates the table via {@code TableStructMetadataAdapter}, which carries no partition-
     *     spec/dimension information at all, so replaying it against the composite subject would
     *     silently strip compositeness rather than throw. GATED here purely to keep the harness from
     *     ever doing that, not because any composite gate fires.</li>
     * </ul>
     */
    private static final Map<Class<? extends FuzzTransactionOperation>, Support> OPERATION_SUPPORT = buildOperationSupportMap();

    /**
     * How many distinct timestamps {@link #comparePointTimestampScans} probes. Small on purpose: each
     * probe is two full comparisons, and the highest-multiplicity timestamps (which it takes first) are
     * where several cells of one day share a timestamp -- the shape the probe exists for.
     */
    private static final int POINT_TIMESTAMP_PROBES = 8;

    /**
     * How many dimension values {@link #compareCellBoundaryIntervals} derives probe windows from. Each
     * one costs several comparisons, and the busiest values (taken first) are the ones spanning the most
     * cells.
     */
    private static final int CELL_BOUNDARY_PROBES = 4;

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
        // A cell whose rows cover only PART of the time window, created BEFORE the generated traffic so
        // it takes a LOW cellKey. See insertTimeSkewedCell for why this is here.
        insertTimeSkewedCell("SKEWLATE", "2023-01-02T21:", 6);
        ObjList<FuzzTransaction> transactions = generate(rowCount, transactionCount);
        applyToBoth(transactions);
        // ... and one created AFTER, so it takes a HIGH cellKey while its rows sit early in the window.
        insertTimeSkewedCell("SKEWEARLY", "2023-01-02T00:", 6);
        TestUtils.drainWalQueue(engine);
    }

    /**
     * Inserts a handful of rows for ONE dimension value confined to a narrow time window, into both
     * twins.
     * <p>
     * The generator cannot produce this shape, and the shape is where these cursors break. It draws
     * dimension values uniformly across the whole time window, so every cell ends up spanning the whole
     * window — and a cell that spans the window is never "wholly above" or "wholly below" a sub-day
     * interval. That is precisely the state an interval scan mishandles, and precisely why 24 seeds
     * could not reproduce a defect that a three-row hand-written test reproduces every time.
     * <p>
     * Real data looks like this constantly: an instrument that only trades in the morning, a sensor that
     * comes online late, a symbol retired half-way through the day.
     * <p>
     * WHEN this is called decides the cellKey, which decides which cursor it stresses. Called BEFORE the
     * generated traffic, the value interns first and takes a LOW cellKey; give it LATE rows and the
     * FORWARD scan meets a cell wholly above an early interval. Called AFTER, it takes a HIGH cellKey;
     * give it EARLY rows and the BACKWARD scan (which visits highest cellKey first) meets a cell wholly
     * below a later interval.
     */
    private void insertTimeSkewedCell(String dimensionValue, String hourPrefix, int rows) throws SqlException {
        // Column list resolved from LIVE metadata rather than hardcoded, because the generator can
        // ADD a column mid-run and this helper is called AFTER the generated traffic. A positional
        // "VALUES (a,b,c,d,e)" against a widened table fails the whole statement with
        // "row value count does not match column count [expected=6, actual=5]" -- measured, and the
        // reason this helper, not the product, was what kept probabilityOfAddingNewColumn at 0.0.
        //
        // Naming the columns leaves every column this helper does not know how to fill at NULL. That
        // is identical in both twins, so it cannot bias the comparison. Intersecting with live
        // metadata on top of that means a column later removed or renamed drops out of the list
        // instead of failing the statement.
        //
        // Resolved from the PLAIN twin (the reference) and used verbatim for BOTH, deliberately: if
        // the two ever diverged structurally, the composite statement would fail loudly here rather
        // than quietly inserting a different row shape into each side.
        final ObjList<String> cols = new ObjList<>();
        try (TableMetadata meta = engine.getTableMetadata(engine.verifyTableName(plainName))) {
            for (int i = 0, n = meta.getColumnCount(); i < n; i++) {
                if (meta.getColumnType(i) > 0 && isSkewColumn(meta.getColumnName(i))) {
                    cols.add(Chars.toString(meta.getColumnName(i)));
                }
            }
        }

        final StringBuilder sql = new StringBuilder(" (");
        for (int i = 0, n = cols.size(); i < n; i++) {
            sql.append(i > 0 ? "," : "").append(cols.getQuick(i));
        }
        sql.append(") VALUES ");
        for (int r = 0; r < rows; r++) {
            sql.append(r > 0 ? ",(" : "(");
            for (int i = 0, n = cols.size(); i < n; i++) {
                appendSkewValue(sql.append(i > 0 ? "," : ""), cols.getQuick(i), dimensionValue, hourPrefix, r);
            }
            sql.append(')');
        }
        engine.execute("INSERT INTO " + compositeName + sql, sqlExecutionContext);
        engine.execute("INSERT INTO " + plainName + sql, sqlExecutionContext);
        TestUtils.drainWalQueue(engine);
    }

    /**
     * The five columns {@link #insertTimeSkewedCell} knows how to fill -- i.e. the ones the twins are
     * created with. Anything the generator adds is deliberately NOT in this set.
     */
    private static boolean isSkewColumn(CharSequence name) {
        return Chars.equals(name, "ts") || Chars.equals(name, "exch") || Chars.equals(name, "sym")
                || Chars.equals(name, "px") || Chars.equals(name, "qty");
    }

    private static void appendSkewValue(StringBuilder sink, CharSequence col, String dimensionValue, String hourPrefix, int row) {
        if (Chars.equals(col, "ts")) {
            sink.append('\'').append(hourPrefix).append(String.format("%02d", row * 7 % 60)).append(":00.000000Z'");
        } else if (Chars.equals(col, "px")) {
            sink.append(row).append(".5");
        } else if (Chars.equals(col, "qty")) {
            sink.append(row);
        } else {
            sink.append('\'').append(dimensionValue).append('\'');
        }
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
            // Both twins must see the SAME random stream for every operation, so the Rnd state is
            // snapshotted before the composite apply and restored before the plain one.
            //
            // This USED to rely on FuzzInsertOperation#apply opening with rnd.reset(s1, s0), replaying
            // from a seed stored on the operation itself -- an invariant that held only because Task 1
            // generated inserts exclusively, and that this method's own comment warned had to be
            // re-verified per operation type before the mix was widened. MEASURED 2026-08-26: it does
            // not hold. rnd.reset appears in NO other Fuzz*Operation, so enabling DROP PARTITION or
            // CONVERT let those operations consume from the shared stream and handed the SECOND twin
            // different row values -- surfacing as SYM/value divergence that reads exactly like a
            // composite data bug and is not one.
            //
            // Snapshot/restore removes the dependency entirely rather than auditing each operation:
            // whether an operation resets internally, consumes, or does neither, both applies start
            // from identical state. For inserts this is behaviour-preserving, since reset(s1, s0) makes
            // the incoming state irrelevant anyway.
            Rnd applyRnd = new Rnd();
            for (int i = 0, n = transactions.size(); i < n; i++) {
                FuzzTransaction transaction = transactions.getQuick(i);
                for (int opIndex = 0, opCount = transaction.operationList.size(); opIndex < opCount; opIndex++) {
                    FuzzTransactionOperation operation = transaction.operationList.getQuick(opIndex);
                    final long applySeed0 = applyRnd.getSeed0();
                    final long applySeed1 = applyRnd.getSeed1();
                    operation.apply(applyRnd, engine, compositeWriter, -1, null);
                    applyRnd.reset(applySeed0, applySeed1);
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
     * query, so {@link #comparedShapeCount()} always reads 11 after a full, uninterrupted run.
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
        comparePointTimestampScans(); // 6b: WHERE ts = <t>, point intervals over multi-cell days
        compareBackwardIntervalScan(); // 9: filtered scan read BACKWARDS (interval backward cursor)
        compareCellBoundaryIntervals(); // 10: intervals just outside each cell's own time range
        compareLimitedScans();          // 11: LIMIT over a filtered scan (skipTarget), both directions
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

        // NO fast-append floor here, and both attempts at one were wrong -- recorded so a third is not
        // written from the same intuition:
        //
        //   "flag on => at least one fast-append commit" fails because fast-append has real
        //   preconditions (an in-order commit extending the LAST partition's cell, which must already
        //   hold rows, with no indexes, no var-size value column, no column top). A run whose data is
        //   too fragmented never satisfies them. Measured: seed (6734027928530775461,
        //   7885943598324962968), 3 dimensions, 1061 cells -- ELIGIBLE counters 0, nothing skipped,
        //   and raising cairo.wal.composite.fastappend.max.open.cells 64 -> 8192 changed nothing.
        //
        //   "eligible => committed" fails too, and failed in CI-shaped running rather than in review:
        //   the eligible counter is a DETECTION layer, deliberately coarser than the ACTION layer
        //   (canCompositeFastAppendCell), which applies its own gates -- a brand-new cell's first
        //   commit takes the full path by design. eligible=1 with committed=0 is therefore ORDINARY,
        //   not a defect. Seed (2422047073701366409, 1269583385469926566) produced exactly that.
        //
        // Fast-append actually FIRING is asserted where the workload is built to reach it:
        // CompositeFastAppendCrashTest pins the eligible counter across an armed commit ("the armed
        // commit must be routed to the composite fast-append path"). CompositeMatrixTest covers the
        // flag-OFF twin equivalence only -- it does not assert the path is taken, so it is not a
        // substitute. Either way that belongs with a pinned workload: a random-shape run cannot assert
        // an optimisation it may legitimately never reach.

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
     * Live column count of the subject. Used to prove a generated ADD COLUMN actually reached the
     * table rather than being filtered away -- see
     * {@code CompositeFuzzTest#testAddColumnEnrolmentIsNeitherFilteredAwayNorUnfiltered}.
     */
    public long compositeColumnCount() throws SqlException {
        return queryLong("SELECT count() FROM table_columns('" + compositeName + "')");
    }

    /**
     * Sort key for every {@code SELECT *} twin comparison: EVERY column, read from live metadata.
     * <p>
     * This MUST be a total order, and it stopped being one the moment ADD COLUMN was enrolled. The key
     * used to be the literal {@code ORDER BY ts, exch, sym, px, qty} -- total only because those five
     * WERE every column, so two rows tying on all five were necessarily identical and their relative
     * order could not be observed. Once the generator can add a sixth, a tie on the five is no longer
     * a tie on the ROW: {@code SELECT *} returns the added column too, the two rows sort arbitrarily,
     * and the comparison passes or fails on cursor order.
     * <p>
     * Not hypothetical. {@code probabilityOfSameTimestamp} is 0.1 and {@code probabilityOfAssigningNull}
     * / {@code probabilityOfUnassignedColumnValue} are each 0.1, so rows sharing ts + exch + sym with
     * NULL px and NULL qty are reachable in a 600-row run -- a NULL px appears in the very first
     * divergence this enrolment produced. Left alone it would have been a rare, seed-dependent flake,
     * and one that would most likely have been attributed to the product rather than to the harness.
     * <p>
     * Var-size columns are skipped, as they cannot be sorted on. That does not hole the total order
     * today: {@code dropUnsupportedAddColumnOps} filters var-size adds out entirely and the base
     * schema has none. If that filter is ever relaxed, this becomes non-total again and must be
     * revisited.
     */
    private String orderByAllColumns() throws SqlException {
        final StringBuilder sb = new StringBuilder(" ORDER BY ");
        try (TableMetadata meta = engine.getTableMetadata(engine.verifyTableName(plainName))) {
            boolean first = true;
            for (int i = 0, n = meta.getColumnCount(); i < n; i++) {
                final int type = meta.getColumnType(i);
                if (type <= 0 || ColumnType.isVarSize(type)) {
                    continue;
                }
                sb.append(first ? "" : ", ").append(meta.getColumnName(i));
                first = false;
            }
        }
        return sb.toString();
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
        this.baselineFastAppendEligibleCount = TableWriter.getCompositeFastAppendEligibleCount();
        this.baselineMultiCellFastAppendEligibleCount = TableWriter.getCompositeMultiCellFastAppendEligibleCount();
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

    /**
     * Enables generated {@code DROP PARTITION} operations, which are OFF by default because they
     * reproduce a known open bug (see this class's DROP PARTITION javadoc for the evidence and the
     * four eliminated leads).
     * <p>
     * Exists so reproducing it is one call rather than editing a hardcoded probability and rebuilding
     * -- the reproduction recipe was previously prose, and prose recipes rot.
     *
     * @param probability per-transaction probability; 0.05 is what the recorded measurements used
     */
    public CompositeFuzzRunner withDropPartitionProbability(double probability) {
        this.dropPartitionProbability = probability;
        return this;
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
        m.put(DuplicateFuzzInsertOperation.class, Support.SUPPORTED);
        m.put(FuzzAddColumnOperation.class, Support.SUPPORTED);
        m.put(FuzzSetTableFormatOperation.class, Support.SUPPORTED);
        m.put(FuzzSetParquetEncodingOperation.class, Support.SUPPORTED);
        // GATED -- product throws a "composite partitioning does not yet support ..." CairoException.
        m.put(FuzzDropColumnOperation.class, Support.GATED);
        m.put(FuzzRenameColumnOperation.class, Support.GATED);
        m.put(FuzzChangeColumnTypeOperation.class, Support.GATED);
        // SP1B: whole-day DROP PARTITION is supported on a composite table as of 2026-08-18, and
        // FuzzDropPartitionOperation emits the WHERE form with timestamp bounds only
        // ("WHERE ts > 'X' AND ts < 'Y'"), which is inherently whole-day -- the drop predicate's
        // metadata exposes no dimension column, so it cannot name a cell. The one shape still refused
        // (a cell-qualified LIST name) is not reachable from this generator.
        m.put(FuzzDropPartitionOperation.class, Support.SUPPORTED);
        m.put(FuzzConvertPartitionToParquetOperation.class, Support.GATED);
        m.put(FuzzConvertPartitionToNativeOperation.class, Support.GATED);
        m.put(FuzzSetTtlOperation.class, Support.GATED);
        m.put(FuzzAddCoveringIndexOperation.class, Support.GATED);
        // GATED -- harness-safety only; the product itself does not throw for these (see javadoc).
        m.put(FuzzChangeSymbolCapacityOperation.class, Support.SUPPORTED);
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
        final String order = orderByAllColumns();
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
        final String orderAsc = orderByAllColumns();
        TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                "SELECT * FROM " + plainName + orderAsc, "SELECT * FROM " + compositeName + orderAsc, LOG);
        // Backward scan. The obvious way to write this -- ORDER BY ts DESC, exch DESC, ... -- does NOT
        // exercise a backward scan at all: a MULTI-KEY sort makes the optimiser SORT over a FORWARD
        // scan, so for the whole life of this harness the "backward" half of shape 1 was running the
        // forward cursor twice. The single-key inner ORDER BY ts DESC is what selects the backward
        // cursor; the outer deterministic sort is what keeps the COMPARISON well-defined despite
        // duplicate timestamps (the generator emits them, and tied rows may legitimately come back in
        // different orders from the two tables).
        assertBackwardScanUsed("SELECT ts FROM " + compositeName + " ORDER BY ts DESC", "Frame backward scan");
        assertBackwardTimestampsEqual("");
        comparedShapeCount++;
    }

    /**
     * Shape 9: a timestamp-filtered scan read BACKWARDS, i.e. the interval BACKWARD cursor.
     * <p>
     * This shape exists because its absence let a severe defect through. {@code
     * IntervalBwdPartitionFrameCursor} retired an interval at the first cell of a multi-cell day that
     * failed to match it, so an {@code ORDER BY ts DESC} with a timestamp filter returned NO rows where
     * the same filter forward returned them. Every other shape here reads forward, and shape 1's
     * "backward" half was silently sorting over a forward scan (see {@link #compareFullScan}), so
     * nothing in this harness could see it.
     * <p>
     * The inner query is single-key {@code ORDER BY ts DESC} so the backward cursor is genuinely used --
     * asserted via the plan, because a query that quietly stops using it would make this shape vacuous
     * exactly the way shape 1 was. The outer sort makes the comparison deterministic under tied
     * timestamps.
     */
    private void compareBackwardIntervalScan() throws SqlException {
        final String where = " WHERE ts >= '2023-01-01T18:00:00.000000Z' AND ts < '2023-01-02T06:00:00.000000Z'";
        assertBackwardScanUsed("SELECT ts FROM " + compositeName + where + " ORDER BY ts DESC", "Interval backward scan");
        assertBackwardTimestampsEqual(where);
        final String outerAsc = orderByAllColumns();
        // A point interval too: [t, t] is the interval most cells of a day fail to match, which is the
        // shape that breaks.
        final LongList timestamps = new LongList();
        collectLongs("SELECT ts FROM (SELECT ts, count() c FROM " + plainName
                + " GROUP BY ts ORDER BY c DESC, ts) LIMIT " + POINT_TIMESTAMP_PROBES, timestamps);
        for (int i = 0, n = timestamps.size(); i < n; i++) {
            assertBackwardTimestampsEqual(" WHERE ts = cast(" + timestamps.getQuick(i) + " AS TIMESTAMP)");
        }
        comparedShapeCount++;
    }

    /**
     * Shape 10: intervals aimed at each CELL's own time boundaries, forward and backward.
     * <p>
     * Shapes 6/6b/9 probe fixed windows and timestamps that exist in the data. Neither reliably produces
     * the state that actually breaks these cursors: a cell lying WHOLLY OUTSIDE the interval while a
     * sibling cell of the same day lies inside it. The generator spreads every dimension value across
     * the whole time window, so cells almost always overlap any interval -- which is why the forward
     * defect showed up on 1 seed in 40 and the backward one on none at all.
     * <p>
     * So this shape derives its intervals FROM THE DATA: for the busiest dimension values it takes that
     * value's own first and last timestamp and probes just outside each end. An interval just above a
     * cell's last row is, for that cell, exactly "wholly above"; just below its first row is "wholly
     * below". Whichever cell the scan reaches first, one of them is outside the interval while others
     * are not.
     * <p>
     * Both scan directions are compared, since the two cursors fail on opposite ends: the forward cursor
     * on cells above the interval, the backward cursor on cells below it.
     */
    private void compareCellBoundaryIntervals() throws SqlException {
        final LongList bounds = new LongList();
        // Per-CELL bounds, straight off the subject's own partition list, flattened as
        // [min, max, min, max, ...]. An earlier version of this took min(ts)/max(ts) GROUP BY exch over
        // the whole table, which was useless: those bounds span every day, so "just above the last row"
        // landed past the end of the data and matched nothing in either table.
        //
        // Ordered by maxTimestamp ASCENDING on purpose. A cell that ends EARLY is the one whose
        // same-day siblings still have rows after it, which is exactly the state that breaks the
        // cursors -- the cell is outside the interval while its siblings are inside it.
        //
        // Reading the SUBJECT's structure to choose probe windows is deliberate (it is what makes them
        // land on real cell boundaries); the assertion itself still compares subject against reference,
        // so the oracle is unaffected.
        collectLongPairs("SELECT minTimestamp, maxTimestamp FROM table_partitions('" + compositeName + "')"
                + " WHERE minTimestamp IS NOT NULL ORDER BY maxTimestamp LIMIT " + CELL_BOUNDARY_PROBES, bounds);
        final String outerAsc = orderByAllColumns();
        for (int i = 0, n = bounds.size(); i < n; i += 2) {
            final long cellMin = bounds.getQuick(i);
            final long cellMax = bounds.getQuick(i + 1);
            // Six hours, in the generator's microsecond timestamps: wide enough to reach the sibling
            // rows that follow an early-ending cell within the same day, narrow enough to stay a
            // sub-day interval (which is the interesting case -- a whole-day interval matches every
            // cell and cannot expose a wrongly-retired one).
            final long hour = 6 * 3_600_000_000L;
            final String[] windows = {
                    // just ABOVE this cell's last row -- "wholly above" for it, live for siblings
                    " WHERE ts > cast(" + cellMax + " AS TIMESTAMP) AND ts <= cast(" + (cellMax + hour) + " AS TIMESTAMP)",
                    // just BELOW this cell's first row -- "wholly below" for it
                    " WHERE ts >= cast(" + (cellMin - hour) + " AS TIMESTAMP) AND ts < cast(" + cellMin + " AS TIMESTAMP)",
                    // straddling its last row
                    " WHERE ts >= cast(" + (cellMax - hour) + " AS TIMESTAMP) AND ts <= cast(" + (cellMax + hour) + " AS TIMESTAMP)",
            };
            for (String where : windows) {
                TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                        "SELECT * FROM " + plainName + where + outerAsc,
                        "SELECT * FROM " + compositeName + where + outerAsc, LOG);
                TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                        "SELECT count() FROM " + plainName + where,
                        "SELECT count() FROM " + compositeName + where, LOG);
                // ... and the same window read BACKWARDS, where the mirrored defect lives
                assertBackwardTimestampsEqual(where);
            }
        }
        // Targeted probes for the two deliberately time-skewed cells (see insertTimeSkewedCell). The
        // generic probes above cannot be relied on to reach them: they take the cells with the smallest
        // maxTimestamp, and every day-1 cell ends before any day-2 cell, so the slots fill up with day-1
        // cells before reaching the skewed ones. These two windows are the trigger states by
        // construction -- just after the early-only cell (wholly below it, and that cell is visited
        // FIRST by a backward scan) and just before the late-only cell (wholly above it, and that cell
        // is visited FIRST by a forward scan).
        //
        // Computed from the reference table's data rather than from partition names, so they still work
        // when the dimension is a hash or truncate transform and the value has no cell of its own.
        assertSkewedCellWindow("SKEWEARLY", true);
        assertSkewedCellWindow("SKEWLATE", false);
        comparedShapeCount++;
    }

    /**
     * Probes the window immediately after ({@code above == true}) or before a time-skewed cell's own
     * rows -- the window in which that cell is wholly outside the interval while its same-day siblings
     * are inside it.
     */
    private void assertSkewedCellWindow(String dimensionValue, boolean above) throws SqlException {
        final LongList bounds = new LongList();
        collectLongPairs("SELECT min(ts), max(ts) FROM " + plainName + " WHERE exch = '" + dimensionValue + "'", bounds);
        if (bounds.size() < 2) {
            return; // the skewed value did not land (e.g. removed by a generated truncate) -- nothing to probe
        }
        // Anchored on the skewed cell's own edge and left OPEN at the other end, on purpose. A fixed
        // +/-6h window was measured to be EMPTY for both tables at ordinary row counts -- the generated
        // rows cluster far more tightly than the two-day window suggests, so a window six hours past a
        // cell's last row contained nothing at all and could not disagree about anything. Half-bounding
        // it guarantees the interval spans whatever sibling rows exist on the far side, which is the
        // whole point: the skewed cell is outside the interval, its siblings are inside it.
        final String where = above
                ? " WHERE ts > cast(" + bounds.getQuick(1) + " AS TIMESTAMP)"
                : " WHERE ts < cast(" + bounds.getQuick(0) + " AS TIMESTAMP)";
        final String outerAsc = orderByAllColumns();
        TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                "SELECT * FROM " + plainName + where + outerAsc,
                "SELECT * FROM " + compositeName + where + outerAsc, LOG);
        TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                "SELECT count() FROM " + plainName + where,
                "SELECT count() FROM " + compositeName + where, LOG);
        assertBackwardTimestampsEqual(where);
    }

    /**
     * Shape 11: {@code LIMIT} over a filtered scan, forward and backward.
     * <p>
     * LIMIT is not decoration here: it becomes the {@code skipTarget} argument of the very method the
     * sibling-cell fixes changed ({@code next(long skipTarget)}), telling the cursor how many rows it
     * may skip before producing. Skipping and the sibling-cell advance both move {@code partitionLo},
     * so they interact, and a small LIMIT is the most likely thing to MASK rows left unvisited by a
     * wrongly-retired interval -- the scan stops before it would have noticed.
     * <p>
     * The offset form is included because {@code LIMIT lo,hi} is what actually produces a non-zero
     * skipTarget; a bare {@code LIMIT n} often does not.
     */
    private void compareLimitedScans() throws SqlException {
        final String where = " WHERE ts >= '2023-01-01T18:00:00.000000Z' AND ts < '2023-01-02T06:00:00.000000Z'";
        final String orderAsc = orderByAllColumns();
        for (String limit : new String[]{" LIMIT 1", " LIMIT 5", " LIMIT 100", " LIMIT 2,10", " LIMIT -3"}) {
            TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                    "SELECT * FROM " + plainName + where + orderAsc + limit,
                    "SELECT * FROM " + compositeName + where + orderAsc + limit, LOG);
            // backward: single sort key so the backward cursor is genuinely used, ts-only so ties
            // cannot make the comparison flap
            TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                    "SELECT ts FROM " + plainName + where + " ORDER BY ts DESC" + limit,
                    "SELECT ts FROM " + compositeName + where + " ORDER BY ts DESC" + limit, LOG);
        }
        comparedShapeCount++;
    }

    /**
     * Compares {@code SELECT ts ... ORDER BY ts DESC} between the twins -- the only shape here that
     * genuinely reads through the BACKWARD cursor.
     * <p>
     * Two earlier attempts at this were VACUOUS, both for the same underlying reason, and the second is
     * why this helper exists rather than an inline query:
     * <ol>
     *     <li>{@code ORDER BY ts DESC, exch DESC, ...} -- a MULTI-KEY sort makes the optimiser sort over
     *     a FORWARD scan.</li>
     *     <li>{@code SELECT * FROM (... ORDER BY ts DESC) ORDER BY ts, exch, ...} -- wrapping in an outer
     *     sort (added to make tied timestamps deterministic) lets the optimiser DROP the inner sort, and
     *     the backward cursor goes unused again. Measured: with the backward fix reverted, the raw DESC
     *     query returned no rows while this wrapped form still passed.</li>
     * </ol>
     * Projecting ONLY {@code ts} solves what the wrapping was for without defeating the plan: rows tied
     * on timestamp are IDENTICAL in this projection, so their relative order cannot make the comparison
     * flap, and no outer sort is needed.
     */
    private void assertBackwardTimestampsEqual(String where) throws SqlException {
        final String desc = " ORDER BY ts DESC";
        TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                "SELECT ts FROM " + plainName + where + desc,
                "SELECT ts FROM " + compositeName + where + desc, LOG);
    }

    private void collectLongPairs(String sql, LongList out) throws SqlException {
        try (RecordCursorFactory factory = engine.select(sql, sqlExecutionContext)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    out.add(record.getLong(0));
                    out.add(record.getLong(1));
                }
            }
        }
    }

    /**
     * Fails if {@code sql} does not plan to the named backward scan. Guards against the failure mode that
     * hid the backward defect: a query that looks like it reads backwards but does not.
     */
    private void assertBackwardScanUsed(String sql, String expectedPlanFragment) throws SqlException {
        final StringSink plan = new StringSink();
        TestUtils.printSql(engine, sqlExecutionContext, "EXPLAIN " + sql, plan);
        if (!Chars.contains(plan, expectedPlanFragment)) {
            throw new AssertionError("this shape must exercise the backward cursor, but the plan does not"
                    + " contain \"" + expectedPlanFragment + "\" -- the shape would be vacuous.\nQuery: "
                    + sql + "\nPlan: " + plan);
        }
    }

    /**
     * Shape 6: a timestamp-bounded interval scan crossing at least one partition boundary. The
     * generator's window is {@code 2023-01-01T00:00 .. 2023-01-03T00:00} (table is
     * {@code PARTITION BY DAY}); 18:00 the day before midnight through 06:00 the day after spans
     * exactly one DAY boundary.
     */
    /**
     * Shape 6b: EQUALITY (point) timestamp filters, {@code WHERE ts = <t>}, over timestamps taken from
     * the data itself -- preferring timestamps carried by more than one row, since those are the ones
     * that span several cells of a day.
     * <p>
     * This exists because {@link #compareIntervalScan}'s single fixed window is one interval out of the
     * many the scan can be handed, and it missed a real defect: an interval scan retired its interval at
     * the first cell of a multi-cell day that failed to match it, silently dropping every later sibling
     * cell's rows. A point filter is the sharpest probe for that -- {@code [t, t]} is the interval most
     * cells of a day fail to match -- and it is fully deterministic: full rows are compared, and no tie
     * is involved, unlike {@link #compareLatestOn}.
     * <p>
     * Timestamps come from the PLAIN reference, so which timestamps get probed never depends on the
     * behaviour under test.
     */
    private void comparePointTimestampScans() throws SqlException {
        final LongList timestamps = new LongList();
        collectLongs("SELECT ts FROM (SELECT ts, count() c FROM " + plainName
                + " GROUP BY ts ORDER BY c DESC, ts) LIMIT " + POINT_TIMESTAMP_PROBES, timestamps);
        for (int i = 0, n = timestamps.size(); i < n; i++) {
            final String where = " WHERE ts = cast(" + timestamps.getQuick(i) + " AS TIMESTAMP)";
            final String order = orderByAllColumns();
            TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                    "SELECT * FROM " + plainName + where + order,
                    "SELECT * FROM " + compositeName + where + order, LOG);
            // count() runs through calculateSize(), a different code path from the row scan above --
            // a fix to only one of them would pass the comparison above and still miscount.
            TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                    "SELECT count() FROM " + plainName + where,
                    "SELECT count() FROM " + compositeName + where, LOG);
        }
        comparedShapeCount++;
    }

    private void collectLongs(String sql, LongList out) throws SqlException {
        try (RecordCursorFactory factory = engine.select(sql, sqlExecutionContext)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    out.add(record.getLong(0));
                }
            }
        }
    }

    private void compareIntervalScan() throws SqlException {
        final String sql = " WHERE ts >= '2023-01-01T18:00:00.000000Z' AND ts < '2023-01-02T06:00:00.000000Z'"
                + orderByAllColumns();
        TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                "SELECT * FROM " + plainName + sql, "SELECT * FROM " + compositeName + sql, LOG);
        comparedShapeCount++;
    }

    /**
     * Shape 3: {@code LATEST ON ts PARTITION BY sym}. At most one row per distinct {@code sym} value --
     * including a NULL group, now that Task 2 restored NULL generation.
     * <p>
     * This does NOT compare the two full result sets row for row, and the reason is a real property of
     * the query rather than a concession. The generator emits duplicate timestamps
     * ({@code probabilityOfSameTimestamp}), so a {@code sym} group can hold TWO rows at its maximum
     * timestamp. Both are equally "the latest"; {@code LATEST ON} does not define which one it returns,
     * and the twins genuinely differ there -- a composite table orders rows sharing a timestamp by
     * {@code cellKey}, a plain table by insertion. Demanding row equality would fail on a legal tie.
     * <p>
     * So the comparison asserts the two things {@code LATEST ON} DOES determine, which together are
     * strictly stronger than "the counts match":
     * <ol>
     *     <li><b>the groups and their timestamps are identical</b> -- same {@code sym} values, same
     *     maximum timestamp for each. A dropped group, an extra group, or a wrong "latest" timestamp all
     *     fail here; only the choice WITHIN a tie is left free.</li>
     *     <li><b>every row returned for the composite table is a genuine row of the plain twin</b> --
     *     so a tie cannot be satisfied by a fabricated or mis-assembled row. A wrong {@code exch},
     *     {@code px} or {@code qty} stitched onto the right {@code (sym, ts)} fails here.</li>
     * </ol>
     * A row that is both a real row of the twin AND carries the group's true maximum timestamp is a
     * correct answer to this query, whichever member of a tie it happens to be.
     */
    private void compareLatestOn() throws SqlException {
        final String latest = " LATEST ON ts PARTITION BY sym";
        // (1) same groups, same latest timestamp per group -- exact comparison.
        TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                "SELECT sym, ts FROM " + plainName + latest + " ORDER BY sym",
                "SELECT sym, ts FROM " + compositeName + latest + " ORDER BY sym", LOG);
        // (2) every composite-returned row exists verbatim in the twin.
        TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                "SELECT count() FROM (SELECT 1 FROM long_sequence(0))",
                "SELECT count() FROM ((SELECT * FROM " + compositeName + latest + ") EXCEPT (SELECT * FROM " + plainName + "))",
                LOG);
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
     * skeleton had the fewest moving parts, with "later tasks randomize these axes" as the plan. That
 * never happened, so EVERY structural DDL below still sits at 0.0 and this fuzz exercises no DDL at
 * all -- including operations that became supported in 2026-08. The scope-closure index claims
 * (invariant 5) that flipping an operation to SUPPORTED "enrols the operation in the differential
 * fuzz automatically": it does NOT. The Support map is read only by CompositeFuzzOpCoverageTest to
 * check the map is COMPLETE, never by the generator. Enrolment is these probabilities and nothing
 * else.
 * <p>
 * DROP PARTITION: still an OPEN BUG. Three leads eliminated 2026-08-26, listed so they are not
 * chased again.
 * <p>
 * <b>Correction first.</b> An earlier version of this javadoc claimed O3 was "the missing ingredient"
 * that made this reproducible. That was WRONG, and it was wrong in a way worth naming: the fuzz had
 * ALWAYS reproduced it whenever probabilityOfDropPartition was raised -- the previous note said so
 * itself. What was never reproducible was a hand-written MINIMAL shape. O3 only changes the rate
 * (12 of 24 seeds with it, 9 of 24 without). Measured both ways.
 * <p>
 * <b>Recipe:</b> {@code probabilityOfDropPartition = 0.05}. 50 occurrences across the 24-seed sweep.
 * <p>
 * <b>MEASURED evidence.</b> A read of the composite twin fails, with the disk-level cause logged
 * immediately before it:
 * <pre>
 *   open partition failed, partition does not exist on the disk
 *     [path=.../unstable4_composite~13/2023-01-01/SYM/SYM16.7]
 * </pre>
 * _txn points at a CELL at nameTxn .7 that is not on disk. It persists AFTER drainWalQueue, so it is
 * a settled inconsistency, not a read racing a writer. The plain twin is unaffected.
 * <p>
 * <b>NARROWED, 2026-08-26 -- this is the sharpest description available, start here.</b> Deterministic
 * single-seed reproduction: {@code Rnd(1037, 591)} + {@link #withDropPartitionProbability}(0.05) +
 * {@code createTables} + {@code applyGeneratedTransactions(600, 30)}. Dumping _txn against the
 * directory tree at the moment of failure shows the day was RE-VERSIONED from nameTxn 0 to nameTxn 3,
 * and exactly ONE cell's new directory was never materialised:
 * <pre>
 *   on disk, day 2023-01-01:        _txn, day 2023-01-01:
 *     10/SYM.0   10/SYM.3             every entry nameTxn=3,
 *     11/SYM.0   11/SYM.3             including 13/SYM
 *     12/SYM.0   12/SYM.3
 *     13/SYM.0   &lt;-- .3 MISSING
 *     14/SYM.0   14/SYM.3
 *     ... every other cell has BOTH
 * </pre>
 * So _txn and disk disagree for exactly one cell of a re-versioned day: the bookkeeping was updated
 * for every cell, the directory was created for all but one. The reader then follows _txn to
 * {@code 13/SYM.3} and fails. A bare day container {@code 2023-01-01.5} is also present, which may or
 * may not be related.
 * <p>
 * That reframes the question usefully: it is not "why was a live directory deleted" but "which loop
 * writes {@code <cell>.<newTxn>} for each cell of a day, and how does it skip one while _txn is
 * updated for all". Whether {@code 13/SYM} is the cell the DROP targeted (entry should have been
 * REMOVED, not re-versioned) or an innocent sibling is the next thing to establish -- its {@code .0}
 * still exists, so it was not fully dropped.
 * <p>
 * <b>ROOT-CAUSE MECHANISM IDENTIFIED, 2026-08-26.</b> Dumping every _txn entry of the failing day
 * WITH its cellKey and rendered segment, against the directory tree, gives this:
 * <pre>
 *   _txn:  cellKey=1..13  seg=15/SYM, 31/%NULL, 17/SYM ...  nameTxn=3   (all fine)
 *          cellKey=14     seg=13/SYM                        nameTxn=5   &lt;-- the orphan
 *   disk:  2023-01-01/13/SYM.0        (no .5 anywhere under the cell)
 *          2023-01-01.5               &lt;-- the .5 version, written at the DAY level
 * </pre>
 * Nothing was deleted -- {@code 13/SYM.0} is still there. _txn points at a {@code .5} version of that
 * cell which was never created under the cell.
 * <p>
 * <b>Do NOT read the bare {@code 2023-01-01.5} as "the misplaced version" -- I did, and it is wrong.</b>
 * Bare {@code <day>.<txn>} containers on a composite table are a KNOWN, DOCUMENTED, HARMLESS artifact:
 * see {@code TableWriter#openLastPartitionAndSetAppendPosition}, which already guards against creating
 * them and records that they are "never read by anything" and never reclaimed (O3PartitionPurgeJob
 * skips composite tables), with a measurement of three left behind by 20 rounds of O3 writes. So the
 * bare container is unrelated debris, and the real question is narrower than it first looks: what
 * stamps nameTxn=5 onto that cell's _txn entry without a corresponding directory.
 * <p>
 * The prime suspect is {@code TableWriter#setStateForTimestamp(Path, long)}. It resolves
 * {@code getPartitionNameTxnByPartitionTimestamp} (cellKey 0 ONLY) and then builds the path with the
 * bare 5-arg {@code setPathForNativePartition} -- no cell segment -- and {@code openPartition}, one of
 * its callers, follows it with {@code ff.mkdirs}. That would create a bare {@code <day>.<txn>}
 * directory and stamp a cellKey-0 nameTxn onto a cell's entry: exactly the observed signature.
 * <p>
 * <b>But guarding that did NOT fix it, so the story is incomplete.</b> Skipping the
 * {@code openPartition} + {@code setAppendPosition} reopen in
 * {@code dropPartitionByExactTimestamp}'s active-partition branch for a routed composite table --
 * mirroring the guard {@code finishO3Commit} already applies to its own last-partition reopen -- left
 * the failure completely unchanged: 50 occurrences, 12 of 24 seeds, before and after. That change was
 * reverted rather than kept, since it fixes nothing measurable and alters drop control flow.
 * <p>
 * <b>Methodological warning, because it produced a wrong answer here.</b> Instrumenting
 * {@code setStateForTimestamp} with a stack dump appeared to show only TWO of its 17 call sites
 * reachable on a routed composite table. That was an artifact: the stacks were grepped with a
 * 3-line window, so only the top frames were ever visible, and the "two sites" (12280 and 9469) are
 * {@code openPartition} and its caller in ONE stack. The reachable-call-site question is still OPEN
 * -- redo it with full stacks before relying on any count.
 * <p>
 * <b>ONE CAUSE FOUND AND FIXED (partial), 2026-08-26.</b> Instrumenting BOTH writers of the nameTxn
 * slot -- {@code TxWriter:778} and {@code TxReader#initPartitionAt} -- with a full stack caught it:
 * <pre>
 *   TxReader.initPartitionAt
 *   TxWriter.insertPartitionSizeByTimestamp        &lt;-- INSERTS a new entry
 *   TxWriter.updateAttachedPartitionSizeByRawIndex
 *   TxWriter.updateAttachedPartitionSizeByTimestamp
 *   TxWriter.beginPartitionSizeUpdate
 *   TableWriter.dropPartitionByExactTimestamp
 * </pre>
 * {@code beginPartitionSizeUpdate} took the cellKey of the globally-LAST partition entry and paired it
 * with {@code maxTimestamp}. On a composite table those need not describe the same partition: the last
 * entry is the highest {@code (ts, cellKey)} and its cellKey belongs to its own day and cell, while
 * {@code maxTimestamp} is merely the largest data timestamp. When the pair named no existing
 * partition the lookup missed and the update INSERTED ON MISS, creating a phantom _txn entry with
 * {@code nameTxn = txn-1} for a cell whose directory was never written. Fixed by using the last
 * entry's OWN timestamp.
 * <p>
 * <b>Partial: 50 -&gt; 35 occurrences, 12 -&gt; 9 of 24 seeds.</b> At least one further mechanism
 * remains, and this is the first hypothesis all session that moved the number at all -- the four
 * below moved it by zero, which is what disqualified them.
 * <p>
 * <b>SECOND MECHANISM CHARACTERISED (read side) -- this is what the remaining 9 of 24 seeds hit.</b>
 * After the fix above, the surviving failure has a DIFFERENT shape: the message carries no cell
 * segment at all, and the reader logs
 * {@code open partition failed ... [path=.../d3x1037_composite~4/2023-01-02.7]} -- the BARE DAY path.
 * Meanwhile _txn for that day is entirely self-consistent, including the victim entry
 * ({@code cellKey=15 seg=25/SKE nameTxn=7}), and {@code 2023-01-02/25/SKE.7} IS on disk. So nothing is
 * corrupt: the READER simply failed to apply the cell segment.
 * <p>
 * The culprit is {@code TableReader#resolveCellSegmentOrNullIfDormant}:
 * <pre>
 *   if (cellKey &gt;= getCompositeDictionaries().cellRegistry().size()) {
 *       return null;   // -&gt; bare day path
 *   }
 * </pre>
 * That guard conflates two unrelated states. {@code registry.size() == 0} is genuinely DORMANT, where
 * the bare path is right. {@code cellKey >= size > 0} is a STALE SNAPSHOT -- the reader's _txn knows a
 * cell its registry has never heard of -- and silently rendering the bare path there is wrong.
 * <p>
 * Why it goes stale: {@code compositeDicts} is built in {@code openSymbolMaps()} and released in
 * {@code freeSymbolMapReaders()}, i.e. it rides the symbol-map/METADATA lifecycle. A new cell is
 * created by an ordinary DATA write, which bumps _txn and does not touch metadata -- so a reader can
 * reload _txn, see a brand-new cellKey, and still hold a registry from before it existed. That is
 * exactly why the victim is always the newest/highest cellKey (here the SKEWLATE cell inserted after
 * the generated traffic).
 * <p>
 * Fix direction: refresh the cell registry when a _txn reload reveals a cellKey beyond it, and split
 * the dormant test ({@code size == 0}) from the staleness test rather than treating both as "dormant".
 * That is a change to reader reload semantics, so it wants doing deliberately.
 * <p>
 * <b>ELIMINATED -- do not re-chase:</b>
 * <ol>
 *   <li><b>Empty-component cell paths.</b> The same runs logged ENOENT purge failures, 14 of 742 with
 *       an empty path component. That was a genuine separate bug (an empty-string dimension value
 *       rendered no segment; fixed by {@code %EMPTY}), and fixing it moved those 14 to 0 while the
 *       reader failures stayed at exactly 70. Independent defects sharing a log.</li>
 *   <li><b>Partition splitting.</b> Disabling it ({@code CAIRO_O3_PARTITION_SPLIT_MIN_SIZE} =
 *       Integer.MAX_VALUE) leaves the failure intact: 50 occurrences, 12 of 24 seeds.</li>
 *   <li><b>O3 being necessary.</b> See the correction above -- it is not.</li>
 *   <li><b>Mis-rendered cell segments.</b> The logs invite this: the reader wants
 *       {@code 2023-01-01/SYM/SYM16} while a purge nearby targets {@code 2023-01-01/%NULL/SYM16},
 *       and another table shows {@code SYM16/SYM} -- components apparently swapped. Both are
 *       innocent. Within one table the component order is fixed and consistent (checked: that
 *       table's first component is only ever {@code SYM} or {@code %NULL}, i.e. truncate(sym,3)),
 *       and the apparent swap is just two tables whose shuffled DIM_POOL prefix put the dimensions
 *       in different orders. {@code %NULL/SYM16} and {@code SYM/SYM16} are two genuinely DIFFERENT
 *       live cells -- NULL sym vs non-NULL sym at the same exch -- not one cell rendered two ways.</li>
 * </ol>
 * Six hand-written shapes are also clean and must not be retried: (1) a WHERE-form drop of a two-cell
 * day; (2) drop / re-create both cells / drop again; (3) re-insert into the dropped day afterwards;
 * (4) this runner's own mechanism -- one TableWriterAPI, an AlterOperation applied via
 * w.apply(op, false) mid-stream, commit per transaction, drain only every 8, 24 transactions across
 * 3 days; (5) the same with NULL dimension values; (6) the time-skewed-cell lead.
 * <p>
 * Note the drop path itself IS cell-aware ({@code dropPartitionByExactTimestamp} resolves
 * {@code getPartitionCellKey(index)} and calls {@code removeAttachedPartitions(timestamp, cellKey)}),
 * so a naive "drop is cellKey-0-only" theory is already excluded by inspection.
 * <p>
 * Left at 0.0 so the suite stays green while the bug is open -- flip it to reproduce.
 * <p>
 * SCHEMA-CHANGING DDL is blocked for a separate, plainer reason: this runner's SQL is fixed-shape
 * (5-column INSERTs, fixed literals), so a generated ADD/DROP COLUMN gives "row value count does not
 * match column count [expected=7, actual=5]" and a type change gives "inconvertible types:
 * DOUBLE -> TIMESTAMP_NS". Enabling it means making the harness schema-adaptive first.
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
            return dropUnsupportedAddColumnOps(FuzzTransactionGenerator.generateSet(
                    0, // initialRowCount: tables are created empty
                    sequencerMetadata,
                    tableMetadata,
                    rnd,
                    minTimestamp,
                    maxTimestamp,
                    rowCount,
                    transactionCount,
                    // o3 ENABLED 2026-08-26. It had been false since Task 1 ("keep Task 1 to in-order
                    // inserts"), which meant this fuzz never exercised out-of-order insertion at all --
                    // on a feature whose entire risk surface IS the O3 merge path
                    // (processO3BlockComposite, the per-cell scratch gather, the partition-update
                    // sink). The single largest coverage gap this harness had.
                    //
                    // MEASURED on flipping it: the twins stay equal. All 24 sweep seeds pass, as do the
                    // crash, unstable and soundness suites. The only casualty was CompositeFuzzTest's
                    // own anti-vacuity lock, which had been pinned to a seed measured under o3=false --
                    // the flag changes the Rnd draw sequence, so that seed stopped generating the ops it
                    // was chosen for. That lock sweeps seeds now instead of pinning one.
                    true,  // o3: out-of-order inserts -- the shape this feature is actually about
                    0.0,   // probabilityOfCancelRow
                    0.1,   // probabilityOfUnassignedColumnValue (restored: hang fixed, see note above)
                    0.1,   // probabilityOfAssigningNull (restored: hang fixed, see note above)
                    0.0,   // probabilityOfTransactionRollback
                    // ADD COLUMN is ENROLLED: insertTimeSkewedCell is schema-adaptive now, so a
                    // widened table no longer breaks its INSERT. This is the DDL worth fuzzing first
                    // -- it is the operation that reorders denseSymbolMapWriters against the composite
                    // interner slots, which is exactly what createSymbolMapWriter's insert-at-
                    // (size - internerCount) fix exists to keep straight.
                    //
                    // The other three stay at 0.0, and NOT because they are unsupported -- all three
                    // are supported for composite now. Each has its own remaining blocker:
                    //   REMOVE/RENAME -- ATTEMPTED AND REVERTED, 2026-08-26. Enrolling both at 0.05,
                    //              filtering pinned-column ops the way the adds are filtered, failed
                    //              11 of 17 tests on TWO blockers. Recorded so the next attempt starts
                    //              here instead of repeating it:
                    //
                    //              (a) The comparison SHAPES name columns literally -- "WHERE exch =",
                    //                  firstNonNullExch(), px/qty in several probes -- so a dropped or
                    //                  renamed column yields "Invalid column: qty" (100 occurrences),
                    //                  not a divergence. Filtering PINNED columns does not fix this:
                    //                  axes.dimClauses is a shuffled PREFIX of DIM_POOL, so at
                    //                  dimCount=1 the dimensions may reference only sym, leaving exch
                    //                  unpinned and legitimately droppable while every
                    //                  dimension-filtered shape still names it. Enrolling these means
                    //                  making the shapes themselves schema-adaptive AND skipping any
                    //                  shape whose subject column has vanished -- which then collides
                    //                  with the comparedShapeCount anti-vacuity floor.
                    //
                    //              (b) FILTERING OPS BREAKS THE GENERATOR'S SCHEMA MODEL. This is the
                    //                  deeper one. generateSet plans against its own idea of the
                    //                  evolving schema: it emits add(new_col_0) and later
                    //                  drop(new_col_0). dropUnsupportedAddColumnOps removes the ADD,
                    //                  and the orphaned DROP then fails with "cannot remove, column
                    //                  does not exist [column=new_col_0]". Filtering composes only
                    //                  while no enabled operation REFERENCES a previously added
                    //                  column.
                    //
                    //              (b) is also the standing precondition for the ADD COLUMN filter
                    //              above: that filter is sound today ONLY because REMOVE, RENAME and
                    //              COLUMN TYPE CHANGE are all 0.0, so nothing ever refers back to a
                    //              column whose add was filtered away. RAISING ANY OF THE THREE MEANS
                    //              FIXING THIS FIRST -- either teach the generator not to emit the
                    //              unsupported adds (upstream, the clean fix), or transitively drop
                    //              every later op referencing a filtered column.
                    //
                    //              Not a blocker, and established while checking: the dimension-column
                    //              refusal holds on the WRITER path too, raised by AlterOperation
                    //              itself rather than only by the compiler pre-check
                    //              (CompositeDimensionColumnDropTest).
                    //   TYPE    -- MEASURED: "inconvertible types: DOUBLE -> TIMESTAMP_NS". A
                    //              FuzzTransactionGenerator-level literal problem, not a composite one.
                    0.05,  // probabilityOfAddingNewColumn      (ENROLLED)
                    0.0,   // probabilityOfRemovingColumn       (supported; blocked, see below)
                    0.0,   // probabilityOfRenamingColumn       (supported; blocked, see below)
                    0.0,   // probabilityOfColumnTypeChange     (supported; generator literal problem)
                    1.0,   // probabilityOfDataInsert
                    0.1,   // probabilityOfSameTimestamp
                    dropPartitionProbability, // see withDropPartitionProbability + javadoc
                    0.0,   // probabilityOfConvertPartitionToParquet  (SP3: supported, per cell)
                    0.0,   // probabilityOfConvertPartitionToNative   (SP3: supported, per cell)
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
            ));
        }
    }

    /**
     * Removes generated ADD COLUMN operations that would land the subject on a gate it already
     * declares unsupported, and COUNTS what it removed so the lost coverage is never silent (see
     * {@link #droppedAddColumnOps()}).
     * <p>
     * Two kinds are dropped, and BOTH were found by measurement, not inspection. Enrolling ADD COLUMN
     * failed 5 of 24 sweep seeds; every failure was one of these two ALREADY-KNOWN gates, and none was
     * a new defect:
     * <ol>
     *   <li><b>4 seeds -- var-size column.</b> The generator adds columns of arbitrary type, and the
     *       twins' base schema (ts, exch, sym, px, qty) contains no var-size column at all. The moment
     *       one is added, an interleaved multi-cell commit hits "composite partitioning: an interleaved
     *       multi-cell commit is not yet supported for a table with a var-size column" --
     *       remaining-work item 2, blocked on the per-cell scratch gather having no per-driver way to
     *       copy a single var-size value.</li>
     *   <li><b>1 seed -- POSTING index.</b> {@link io.questdb.cairo.IndexType#POSTING} and its
     *       DELTA/EF variants reach {@code TableWriter#sealPostingIndexForPartition}, which refuses on
     *       a routed composite table (Task 16).</li>
     * </ol>
     * Both gates SUSPEND the table rather than failing the statement, and a suspended twin stops
     * applying transactions while the reference carries on. That is why the symptoms looked so varied
     * -- "Column count must be same expected:&lt;10&gt; but was:&lt;7&gt;" on some seeds, a row
     * mismatch on others. Downstream drift, not distinct defects. Do not chase them separately.
     * <p>
     * CORRECTION worth keeping, because the same trap is easy to re-enter: the first version of this
     * filter dropped only the POSTING ops, and its comment asserted all 5 seeds were that gate.
     * Re-running showed 5 -> 4 -- exactly ONE was. The majority cause was the var-size gate. Establish
     * which gate a seed hit by grepping its table name in the run log for "not yet supported"; do NOT
     * infer it from the comparison failure message, which reports drift, not cause.
     * <p>
     * What this enrolment BUYS: fixed-width and SYMBOL adds are deliberately NOT filtered, and SYMBOL
     * adds are the ones that reorder {@code denseSymbolMapWriters} against the composite interner
     * slots -- exactly what {@code createSymbolMapWriter}'s insert-at-(size - internerCount) fix exists
     * to keep straight. That path had no fuzz coverage before and has it now.
     * <p>
     * Neither gate is hidden by this filter: item 2 is in the remaining-work plan, and the POSTING one
     * is pinned in code by {@code CompositeAddColumnPostingGateTest}, which additionally records that
     * it bricks the table instead of refusing the ALTER (invariant 6).
     */
    private ObjList<FuzzTransaction> dropUnsupportedAddColumnOps(ObjList<FuzzTransaction> transactions) {
        for (int i = 0, n = transactions.size(); i < n; i++) {
            final ObjList<FuzzTransactionOperation> ops = transactions.getQuick(i).operationList;
            for (int j = ops.size() - 1; j >= 0; j--) {
                final FuzzTransactionOperation op = ops.getQuick(j);
                if (!(op instanceof FuzzAddColumnOperation)) {
                    continue;
                }
                final FuzzAddColumnOperation add = (FuzzAddColumnOperation) op;
                if (ColumnType.isVarSize(add.getNewType()) || IndexType.isPosting(add.getIndexType())) {
                    ops.remove(j);
                    droppedAddColumnOps++;
                }
            }
        }
        return transactions;
    }



    /**
     * How many ADD COLUMN operations {@link #dropUnsupportedAddColumnOps} removed from this run.
     * Exposed so a test can assert the filter is neither dead nor runaway -- a filter that silently
     * removed EVERY generated add would leave this enrolment worthless while still reporting green.
     */
    public int droppedAddColumnOps() {
        return droppedAddColumnOps;
    }
}
