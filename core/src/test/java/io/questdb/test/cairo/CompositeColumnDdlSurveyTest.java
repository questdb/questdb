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

package io.questdb.test.cairo;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Sub-project 2 (column DDL) SURVEY — which of these already work on a composite table once their
 * gate is lifted, and which genuinely need cell-aware work?
 * <p>
 * This is the same measurement-first step that paid for itself three times today. In 1A it showed the
 * O3 leak was a WRITER defect and the purge job was innocent, sending the fix to a different file. In
 * 1B a probe showed an ungated cell-qualified DROP destroyed a whole day, turning "lift the gate" into
 * "narrow the gate". In 1D it FALSIFIED the plan's own hypothesis: TTL and FORCE DROP had NOT
 * inherited the shared fix, and lifting their gates would have made them silent no-ops.
 * <p>
 * The point is to learn which of the eight column-DDL gates are cheap and which are not, BEFORE
 * writing a plan that assumes either. Each test is a twin comparison: whatever the operation does to a
 * composite table it must do to its plain twin.
 * <p>
 * All {@code @Ignore}d — they are run by temporarily lifting the writer-side gates, exactly as 1D
 * Task 1 was run, and the findings recorded. They are NOT a claim that any of this works.
 */
public class CompositeColumnDdlSurveyTest extends AbstractCompositeTwinTest {

    /**
     * ADD INDEX on a non-dimension symbol column. Indexes are per-partition, therefore already
     * per-cell, so this may need only the gate removed — but an index is built by scanning each
     * partition, and a composite partition is a cell.
     */
    @Ignore("SP2 SURVEY -- run by temporarily lifting the writer-side gate, then restoring it. Results"
            + " recorded 2026-08-18 in the ledger: ADD INDEX, DROP COLUMN and DROP INDEX PASS with only"
            + " the gate removed; RENAME COLUMN and ALTER COLUMN TYPE FAIL.")
    @Test(timeout = 60_000)
    public void surveyAddIndex() throws Exception {
        assertMemoryLeak(() -> {
            createTwins("ts TIMESTAMP, exch SYMBOL, sym SYMBOL, px DOUBLE",
                    "PARTITION BY DAY, exch LAYOUT PLAIN");
            seedTwoDays();
            execute("ALTER TABLE c ALTER COLUMN sym ADD INDEX");
            execute("ALTER TABLE p ALTER COLUMN sym ADD INDEX");
            drainWalQueue();
            assertTwinEqual("");
            assertTwinEqual(" WHERE sym = 'S1'");
        });
    }

    /**
     * DROP COLUMN. Column files live per partition, so on a composite table they live per CELL. The
     * question is whether the removal walks cells or days.
     */
    @Ignore("SP2 SURVEY -- run by temporarily lifting the writer-side gate, then restoring it. Results"
            + " recorded 2026-08-18 in the ledger: ADD INDEX, DROP COLUMN and DROP INDEX PASS with only"
            + " the gate removed; RENAME COLUMN and ALTER COLUMN TYPE FAIL.")
    @Test(timeout = 60_000)
    public void surveyDropColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTwins("ts TIMESTAMP, exch SYMBOL, sym SYMBOL, px DOUBLE",
                    "PARTITION BY DAY, exch LAYOUT PLAIN");
            seedTwoDays();
            execute("ALTER TABLE c DROP COLUMN px");
            execute("ALTER TABLE p DROP COLUMN px");
            drainWalQueue();
            assertTwinEqual("", " ORDER BY ts, exch");
        });
    }

    /**
     * DROP INDEX, the counterpart of {@link #surveyAddIndex()}.
     */
    @Ignore("SP2 SURVEY -- run by temporarily lifting the writer-side gate, then restoring it. Results"
            + " recorded 2026-08-18 in the ledger: ADD INDEX, DROP COLUMN and DROP INDEX PASS with only"
            + " the gate removed; RENAME COLUMN and ALTER COLUMN TYPE FAIL.")
    @Test(timeout = 60_000)
    public void surveyDropIndex() throws Exception {
        assertMemoryLeak(() -> {
            createTwins("ts TIMESTAMP, exch SYMBOL, sym SYMBOL INDEX, px DOUBLE",
                    "PARTITION BY DAY, exch LAYOUT PLAIN");
            seedTwoDays();
            execute("ALTER TABLE c ALTER COLUMN sym DROP INDEX");
            execute("ALTER TABLE p ALTER COLUMN sym DROP INDEX");
            drainWalQueue();
            assertTwinEqual("");
        });
    }

    /**
     * RENAME COLUMN is the cheapest candidate in the whole sub-project: it is metadata-only and touches
     * no partition data at all. If ANY column DDL works unchanged on a composite table, it is this one
     * — which is exactly why it is worth measuring rather than assuming.
     */
    @Ignore("SP2 SURVEY -- run by temporarily lifting the writer-side gate, then restoring it. Results"
            + " recorded 2026-08-18 in the ledger: ADD INDEX, DROP COLUMN and DROP INDEX PASS with only"
            + " the gate removed; RENAME COLUMN and ALTER COLUMN TYPE FAIL.")
    @Test(timeout = 60_000)
    public void surveyRenameColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTwins("ts TIMESTAMP, exch SYMBOL, sym SYMBOL, px DOUBLE",
                    "PARTITION BY DAY, exch LAYOUT PLAIN");
            seedTwoDays();
            execute("ALTER TABLE c RENAME COLUMN px TO price");
            execute("ALTER TABLE p RENAME COLUMN px TO price");
            drainWalQueue();
            assertTwinEqual("", " ORDER BY ts, exch, price");
        });
    }

    /**
     * ALTER COLUMN TYPE rewrites every partition's column file, so on a composite table it must rewrite
     * every CELL's. Expected to be the most expensive of the four.
     */
    @Ignore("SP2 SURVEY -- run by temporarily lifting the writer-side gate, then restoring it. Results"
            + " recorded 2026-08-18 in the ledger: ADD INDEX, DROP COLUMN and DROP INDEX PASS with only"
            + " the gate removed; RENAME COLUMN and ALTER COLUMN TYPE FAIL.")
    @Test(timeout = 60_000)
    public void surveyAlterColumnType() throws Exception {
        assertMemoryLeak(() -> {
            createTwins("ts TIMESTAMP, exch SYMBOL, sym SYMBOL, px DOUBLE",
                    "PARTITION BY DAY, exch LAYOUT PLAIN");
            seedTwoDays();
            execute("ALTER TABLE c ALTER COLUMN px TYPE FLOAT");
            execute("ALTER TABLE p ALTER COLUMN px TYPE FLOAT");
            drainWalQueue();
            assertTwinEqual("");
        });
    }

    /**
     * Two days, three cells each — enough that a day-blind operation is distinguishable from a
     * cell-aware one.
     */
    private void seedTwoDays() throws Exception {
        final StringBuilder sb = new StringBuilder();
        for (int day = 1; day <= 2; day++) {
            for (int cell = 0; cell <= 2; cell++) {
                if (sb.length() > 0) {
                    sb.append(',');
                }
                sb.append("('2023-01-0").append(day).append('T')
                        .append(String.format("%02d", 1 + cell * 4)).append(":00:00.000000Z','E")
                        .append(cell).append("','S").append(cell).append("',")
                        .append(day * 10 + cell).append(".0)");
            }
        }
        insertIntoBoth(sb.toString());
        drainWalQueue();
    }
}
