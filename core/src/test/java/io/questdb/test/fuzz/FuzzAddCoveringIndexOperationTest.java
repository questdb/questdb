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

package io.questdb.test.fuzz;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.std.IntList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Regression test for the {@link FuzzAddCoveringIndexOperation} flake observed in
 * ReplicationFuzzTest.testPrimaryMigration (Azure build 241911):
 * <pre>
 *   io.questdb.griffin.SqlException: [48] indexes are only supported for symbol type
 *       [column=new_col_12, type=BYTE]
 *       at io.questdb.test.fuzz.FuzzAddCoveringIndexOperation.executePostDrain(...)
 * </pre>
 *
 * <p>Root cause: {@code executePostDrain} validates that its target column is a SYMBOL by
 * reading {@link io.questdb.cairo.CairoEngine#getTableMetadata}, which for WAL tables is the
 * <b>lagging</b> applied metadata. It then issues an {@code ALTER TABLE ... ADD INDEX} which is
 * validated against {@code getMetadataForWrite} → {@code getSequencerMetadata}, the <b>fresh</b>
 * write-side metadata. When the table metadata lags behind a drop/re-add (column-name reuse) that
 * changed the column from SYMBOL to a non-symbol type, the operation emits an ALTER on a name that
 * now resolves to a non-symbol column, and the resulting {@code SqlException} is not in its
 * swallowed-error set — so it propagates and fails the fuzz test.
 *
 * <p>This test reproduces the metadata skew deterministically (no fuzz seed, no concurrency) by
 * issuing the structural change through the sequencer but not draining it to the table. It is RED
 * against the current operation and should go GREEN once {@code executePostDrain} tolerates the
 * column no longer being a symbol when resolved at execution time.
 */
public class FuzzAddCoveringIndexOperationTest extends AbstractCairoTest {

    @Test
    public void testExecutePostDrainToleratesStaleSymbolColumnTypeChange() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "covering_idx_skew";

            // WAL table with a SYMBOL column 's' at index 1 (index 0 is the designated timestamp).
            execute("create table " + tableName + " (ts timestamp, s symbol) timestamp(ts) partition by day wal");
            drainWalQueue();

            final TableToken tableToken = engine.verifyTableName(tableName);
            final int symbolColumnIndex;
            try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
                symbolColumnIndex = metadata.getColumnIndex("s");
            }

            // Drop 's' (SYMBOL) and re-add 's' as a non-symbol (BYTE) via the sequencer, reusing the name.
            // Deliberately DO NOT drain the WAL queue: the applied table metadata read by
            // executePostDrain still reports 's' as SYMBOL at symbolColumnIndex, while the
            // sequencer metadata that ALTER ... ADD INDEX validates against already sees 's' as BYTE.
            execute("alter table " + tableName + " drop column s");
            execute("alter table " + tableName + " add column s byte");

            // INCLUDE the designated timestamp column (index 0) so the covering index SQL is emitted.
            final IntList includeColumnIndices = new IntList();
            includeColumnIndices.add(0);

            final FuzzAddCoveringIndexOperation op =
                    new FuzzAddCoveringIndexOperation(symbolColumnIndex, includeColumnIndices);

            // Must not surface "indexes are only supported for symbol type": the operation has to
            // be resilient to the target column no longer being a symbol when the ALTER executes.
            op.executePostDrain(engine, tableName);
        });
    }
}
