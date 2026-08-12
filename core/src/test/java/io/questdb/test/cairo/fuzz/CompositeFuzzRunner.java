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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.fuzz.FuzzTransactionGenerator;
import io.questdb.test.fuzz.FuzzTransactionOperation;
import io.questdb.test.tools.TestUtils;

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
    private final SqlExecutionContext sqlExecutionContext;
    private String compositeName;
    private String plainName;

    private CompositeFuzzRunner(CairoEngine engine, Rnd rnd) {
        this.engine = engine;
        this.rnd = rnd;
        this.sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine);
    }

    public static CompositeFuzzRunner of(CairoEngine engine, Rnd rnd) {
        return new CompositeFuzzRunner(engine, rnd);
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
     * in transaction order, then drains the WAL queue so both twins are fully applied before
     * comparison.
     */
    public void applyToBoth(ObjList<FuzzTransaction> transactions) {
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
            }
        } finally {
            compositeWriter.close();
            plainWriter.close();
        }
        TestUtils.drainWalQueue(engine);
    }

    /**
     * Orders by EVERY column, not just {@code ts}. The generator emits equal-timestamp rows on
     * purpose ({@code probabilityOfSameTimestamp}), and {@code assertSqlCursors} compares cursors
     * row by row -- so ordering by {@code ts} alone would leave tied rows in storage order, which
     * differs between the twins by construction (the composite table groups rows by cell, the plain
     * one does not). That would produce intermittent RED runs with no defect present, which is
     * strictly worse than no harness: the first instinct on a red differential run must be to
     * suspect the product, so the comparison itself has to be order-deterministic.
     */
    public void assertTwinEqual() throws SqlException {
        final String order = " ORDER BY ts, exch, sym, px, qty";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "SELECT * FROM " + plainName + order,
                "SELECT * FROM " + compositeName + order,
                LOG
        );
    }

    public String compositeName() {
        return compositeName;
    }

    /**
     * ONE column model, TWO DDLs. The reference differs from the subject only in the partition
     * clause, so a divergence can never come from the schema.
     */
    public void createTables(String base) throws SqlException {
        this.compositeName = base + "_composite";
        this.plainName = base + "_plain";
        final String cols = "(ts TIMESTAMP, exch SYMBOL, sym SYMBOL, px DOUBLE, qty LONG)";
        engine.execute(
                "CREATE TABLE " + compositeName + " " + cols + " TIMESTAMP(ts) PARTITION BY DAY, exch WAL",
                sqlExecutionContext
        );
        engine.execute(
                "CREATE TABLE " + plainName + " " + cols + " TIMESTAMP(ts) PARTITION BY DAY WAL",
                sqlExecutionContext
        );
    }

    public String plainName() {
        return plainName;
    }

    /**
     * Delegates to {@link FuzzTransactionGenerator#generateSet} against the plain table's
     * metadata: the two schemas are identical, and the plain reader is never itself under test.
     * Task 1 keeps this to plain data inserts (no structural DDL, no O3, no replace-range) so the
     * skeleton has the fewest moving parts; later tasks randomize these axes.
     * <p>
     * {@code probabilityOfUnassignedColumnValue} and {@code probabilityOfAssigningNull} are kept
     * at 0.0 deliberately: {@code generateSet} applies both uniformly across every column,
     * including {@code exch} (the identity/partitioning dimension), and there is no per-column
     * override. A minimal repro (single WAL commit, composite table, one row with NULL exch mixed
     * with a non-null-exch row) showed that a NULL identity-dimension value colliding with a
     * non-null one in the same WAL commit hangs forever in
     * {@code TableWriter.processO3BlockComposite -> o3ConsumePartitionUpdates} -- a genuine
     * production defect, out of scope to fix here (see Task 1 report). Leaving both probabilities
     * at 0.0 keeps every column, including exch, always explicitly assigned and avoids the hang;
     * NULL-dimension coverage belongs in a follow-up task once the underlying defect is fixed.
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
            String[] symbols = new String[]{"NYSE", "NASDAQ", "LSE", "XETRA", "ARCA", "BATS"};
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
                    0.0,   // probabilityOfUnassignedColumnValue (see note above: avoids a confirmed hang)
                    0.0,   // probabilityOfAssigningNull (see note above: avoids a confirmed hang)
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
