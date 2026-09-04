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

package io.questdb.test.cairo.fuzz;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.fuzz.FuzzTransactionGenerator;
import io.questdb.test.fuzz.FuzzTransactionOperation;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Control for {@link CompositeFuzzRunner#applyToBoth}'s load-bearing assumption: that replaying ONE
 * {@code Rnd} through the same operation twice — once per twin — hands both writers identical data.
 * <p>
 * The assumption is narrow. It holds only because {@code FuzzInsertOperation#apply} opens with
 * {@code rnd.reset(s1, s0)}, replaying from a seed stored on the operation itself. Nothing enforces it,
 * and a single operation that drew from the shared stream without resetting would hand the second twin
 * different values — turning every downstream comparison into either a false failure or, worse, a green
 * on matching garbage.
 * <p>
 * <b>Why this test and not an inspection.</b> A divergence observed between the composite subject and
 * its plain reference has two possible causes, and the final table contents cannot tell them apart: the
 * harness fed the two writers different data, or it fed them the same data and the PRODUCT returned
 * different answers. Attributing such a divergence to the harness without this control risks dismissing
 * a real product defect as harness noise.
 * <p>
 * So this test removes the product from the equation. It replays the identical generated workload —
 * including the unassigned-column and NULL-assignment draws that make the replay non-trivial — into two
 * tables that are BOTH plain and byte-for-byte identically declared. Any difference between them can
 * only be the replay, because nothing else differs. A green here means a composite-vs-plain divergence
 * at the same seed is the product's, and must be investigated as such.
 */
public class CompositeFuzzHarnessSoundnessTest extends AbstractCairoTest {

    private static final String COLS = "(ts TIMESTAMP, exch SYMBOL, sym SYMBOL, px DOUBLE, qty LONG)";

    /**
     * The seed recorded as producing a composite-vs-plain divergence on a clean run (no faults, no
     * gated operations).
     */
    @Test
    public void testSharedRndReplayIsIdenticalAcrossTwins4242() throws Exception {
        assertReplayIsIdentical(4242, 2424);
    }

    /**
     * A second seed, recorded as clean. Included so this test cannot pass merely by being run against a
     * seed whose draws happen to avoid the interesting branches.
     */
    @Test
    public void testSharedRndReplayIsIdenticalAcrossTwins1234() throws Exception {
        assertReplayIsIdentical(1234, 5678);
    }

    /**
     * A third, previously unexercised seed. The replay property must hold for ALL seeds, not the two
     * that happen to be written down.
     */
    @Test
    public void testSharedRndReplayIsIdenticalAcrossTwins97531() throws Exception {
        assertReplayIsIdentical(97531, 13579);
    }

    private void assertReplayIsIdentical(long seed0, long seed1) throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd(seed0, seed1);
            execute("CREATE TABLE a " + COLS + " TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b " + COLS + " TIMESTAMP(ts) PARTITION BY DAY WAL");

            final ObjList<FuzzTransaction> transactions = generate(rnd, 500, 40);

            // Exactly CompositeFuzzRunner#applyToBoth's loop: one Rnd, two writers, same operation
            // applied twice in succession, periodic drain.
            final Rnd applyRnd = new Rnd();
            try (
                    TableWriterAPI wa = engine.getTableWriterAPI("a", "soundness control");
                    TableWriterAPI wb = engine.getTableWriterAPI("b", "soundness control")
            ) {
                for (int i = 0, n = transactions.size(); i < n; i++) {
                    final FuzzTransaction transaction = transactions.getQuick(i);
                    for (int op = 0, opCount = transaction.operationList.size(); op < opCount; op++) {
                        final FuzzTransactionOperation operation = transaction.operationList.getQuick(op);
                        operation.apply(applyRnd, engine, wa, -1, null);
                        operation.apply(applyRnd, engine, wb, -1, null);
                    }
                    if (transaction.rollback) {
                        wa.rollback();
                        wb.rollback();
                    } else {
                        wa.commit();
                        wb.commit();
                    }
                    if ((i + 1) % 8 == 0) {
                        drainWalQueue();
                    }
                }
            }
            drainWalQueue();

            // Both tables are plain and identically declared, so ANY difference is the replay.
            final String order = " ORDER BY ts, exch, sym, px, qty";
            assertSqlCursors("SELECT * FROM a" + order, "SELECT * FROM b" + order);
            assertSqlCursors("SELECT count() FROM a", "SELECT count() FROM b");
        });
    }

    private ObjList<FuzzTransaction> generate(Rnd rnd, int rowCount, int transactionCount) throws Exception {
        final TableToken token = engine.verifyTableName("a");
        try (
                TableRecordMetadata sequencerMetadata = engine.getLegacyMetadata(token);
                TableMetadata tableMetadata = engine.getTableMetadata(token)
        ) {
            final long minTimestamp = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP)
                    .parseFloorLiteral("2023-01-01T00:00:00.000000Z");
            final long maxTimestamp = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP)
                    .parseFloorLiteral("2023-01-03T00:00:00.000000Z");
            final String[] symbols = new String[8];
            for (int i = 0; i < symbols.length; i++) {
                symbols[i] = "SYM" + i;
            }
            // Same probability vector as CompositeFuzzRunner#generate -- in particular the 0.1
            // unassigned-column and 0.1 NULL-assignment draws, which are what make the replay
            // non-trivial: an unassigned column consumes a different number of draws than an
            // assigned one.
            return FuzzTransactionGenerator.generateSet(
                    0,
                    sequencerMetadata,
                    tableMetadata,
                    rnd,
                    minTimestamp,
                    maxTimestamp,
                    rowCount,
                    transactionCount,
                    false, // o3
                    0.0,   // probabilityOfCancelRow
                    0.1,   // probabilityOfUnassignedColumnValue
                    0.1,   // probabilityOfAssigningNull
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
                    8,     // maxStrLenForStrColumns
                    symbols,
                    (int) sequencerMetadata.getMetadataVersion(),
                    0.0,   // probabilityOfSetParquetEncoding
                    0.0,   // probabilityOfAddCoveringIndex
                    0.0    // probabilityOfSetTableFormat
            );
        }
    }
}
