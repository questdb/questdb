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

import io.questdb.PropertyKey;

import io.questdb.cairo.TableReader;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@code ALTER TABLE ... ALTER COLUMN ... SYMBOL CAPACITY n} on a ROUTED composite table.
 * <p>
 * <b>Why this exists.</b> {@code TableWriter#changeSymbolCapacity} ends with a reopen step —
 * {@code setStateForTimestamp(path, lastPartitionTimestamp)} + {@code getColumnNameTxn(ts, columnIndex)}
 * + {@code openColumnFiles} + {@code setColumnAppendPosition} — that is entirely CELL-BLIND: it builds a
 * DAY path, where a composite table's data lives under {@code <day>/<cell>}, and uses the cellKey-0
 * variant of the column-name-txn lookup. Its sibling {@code scaleSymbolCapacities} refuses the identical
 * step on a routed composite table, its own comment calling the reposition "a genuine correctness risk,
 * not merely a missed optimization". {@code changeSymbolCapacity} — reached from this very SQL — carries
 * no such gate.
 * <p>
 * <b>What was measured.</b> Instrumenting the branch condition proves the reopen DOES run, ungated, on a
 * routed composite table ({@code routedComposite=true, transientRowCount=1, willReopen=true}); it is not
 * skipped by some earlier condition. Yet no corruption is observable: with the ALTER and the following
 * inserts drained in ONE apply pass — so the repositioned handle is the one subsequent writes would use —
 * the table stays twin-correct, unsuspended, and grows no files it would not have grown anyway (the
 * day-level {@code ts.d}/{@code exch.d}/{@code px.d} that sit beside the cell directories are present
 * WITHOUT the ALTER too; that was checked before drawing any conclusion from them).
 * <p>
 * <b>So this test locks behaviour rather than proving a bug.</b> The likely reason nothing breaks is that
 * the composite write path re-resolves per-cell column handles for every write and never uses the handle
 * this reopen repositioned — which makes the safety of {@code changeSymbolCapacity} depend on an
 * UNSTATED invariant of a different code path. If that ever stops holding, this test is what says so.
 * Whether to additionally gate {@code changeSymbolCapacity} like its sibling is a product call: it would
 * refuse or silently skip a user's explicit ALTER, which is a functional limitation this evidence does
 * not justify on its own.
 */
public class CompositeSymbolCapacityAlterTest extends AbstractCairoTest {

    /**
     * The shape the reopen needs: a routed multi-cell last day and a non-zero transient row count, with
     * the ALTER and the writes that follow it landing in the SAME apply pass.
     */
    @Test
    public void testAlterSymbolCapacityThenWriteInSameApplyPass() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-02T01:00:00.000000Z','A',1.0),"
                    + "('2023-01-02T02:00:00.000000Z','B',2.0),"
                    + "('2023-01-02T03:00:00.000000Z','C',3.0),"
                    + "('2023-01-02T04:00:00.000000Z','A',4.0)");
            drainWalQueue();
            assertTwinEqual(4);

            // Queue the ALTER and the following inserts together: draining once means the writer that
            // performed the cell-blind reopen is the same writer that then applies these rows.
            execute("ALTER TABLE c ALTER COLUMN exch SYMBOL CAPACITY 1024");
            execute("ALTER TABLE p ALTER COLUMN exch SYMBOL CAPACITY 1024");
            insertIntoBoth("('2023-01-02T05:00:00.000000Z','A',5.0),"
                    + "('2023-01-02T06:00:00.000000Z','B',6.0),"
                    + "('2023-01-02T07:00:00.000000Z','D',7.0)");
            drainWalQueue();
            assertNotSuspended();
            assertTwinEqual(7);
            assertSymbolCapacity("c", 1024);
            assertSymbolCapacity("p", 1024);
            assertNoStrayDayLevelData();

            // A further pass, in case a bad handle only surfaces on the commit after the one that set it.
            insertIntoBoth("('2023-01-02T08:00:00.000000Z','A',8.0),"
                    + "('2023-01-02T09:00:00.000000Z','C',9.0)");
            drainWalQueue();
            assertNotSuspended();
            assertTwinEqual(9);
            assertSymbolCapacity("c", 1024);
            assertSymbolCapacity("p", 1024);
            assertNoStrayDayLevelData();
        });
    }

    /**
     * The same ALTER against a table whose last day holds MANY cells, so the one cell the cell-blind
     * reopen could reposition onto is a small minority of the day.
     */
    @Test
    public void testAlterSymbolCapacityManyCells() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            final StringBuilder rows = new StringBuilder();
            for (int i = 0; i < 12; i++) {
                if (i > 0) {
                    rows.append(',');
                }
                rows.append("('2023-01-02T0").append(i / 2).append(':').append(i % 2 == 0 ? "00" : "30")
                        .append(":00.000000Z','E").append(i).append("',").append(i).append(".0)");
            }
            insertIntoBoth(rows.toString());
            drainWalQueue();
            assertTwinEqual(12);

            execute("ALTER TABLE c ALTER COLUMN exch SYMBOL CAPACITY 2048");
            execute("ALTER TABLE p ALTER COLUMN exch SYMBOL CAPACITY 2048");
            insertIntoBoth("('2023-01-02T09:00:00.000000Z','E0',90.0),"
                    + "('2023-01-02T09:30:00.000000Z','E11',91.0),"
                    + "('2023-01-02T10:00:00.000000Z','NEW',92.0)");
            drainWalQueue();
            assertNotSuspended();
            assertTwinEqual(15);
            assertSymbolCapacity("c", 2048);
            assertSymbolCapacity("p", 2048);
            assertNoStrayDayLevelData();
        });
    }

    /**
     * The ALTER must actually TAKE EFFECT. Without this, the fix that skips the cell-blind reopen could
     * be "achieved" by turning the statement into a no-op.
     * <p>
     * Read from the symbol map reader rather than {@code SHOW CREATE TABLE}: that renderer omits the
     * capacity clause entirely in this build, for a PLAIN table just as much as a composite one, so it
     * cannot witness this at all.
     */
    private void assertSymbolCapacity(String table, int expected) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(table))) {
            Assert.assertEquals("ALTER must have changed the symbol capacity of " + table,
                    expected, reader.getSymbolMapReader(1).getSymbolCapacity());
        }
    }

    /**
     * The day-level column files that sit beside a composite day's cell directories must stay EMPTY.
     * They are a normal artefact of partition creation, but nothing should ever write to them: a
     * composite table's data lives under {@code <day>/<cell>}. A non-zero size here means cell-blind
     * code opened one and set an append position on it -- which is exactly what changeSymbolCapacity's
     * reopen did before it was gated (0 bytes -> 2 MiB per ALTER).
     */
    private void assertNoStrayDayLevelData() throws Exception {
        final java.nio.file.Path root = java.nio.file.Paths.get(configuration.getDbRoot());
        try (java.util.stream.Stream<java.nio.file.Path> walk = java.nio.file.Files.walk(root, 4)) {
            walk.filter(p -> !java.nio.file.Files.isDirectory(p))
                    .filter(p -> p.toString().contains("/c~"))
                    .filter(p -> {
                        // a DAY-level column file: <table>/<day>/<name>.d, i.e. its parent is the day
                        // directory itself rather than a cell directory inside it
                        final java.nio.file.Path parent = p.getParent();
                        return p.getFileName().toString().endsWith(".d")
                                && parent != null && parent.getFileName().toString().startsWith("2023-");
                    })
                    .forEach(p -> {
                        try {
                            Assert.assertEquals("day-level column file must stay empty on a composite table: "
                                    + root.relativize(p), 0L, java.nio.file.Files.size(p));
                        } catch (java.io.IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }

    private void assertNotSuspended() {
        Assert.assertFalse("composite table must not be suspended by ALTER SYMBOL CAPACITY",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c")));
    }

    private void assertTwinEqual(long expectedRows) throws Exception {
        assertQuery("SELECT count() FROM c").noLeakCheck().noRandomAccess().expectSize()
                .returns("count\n" + expectedRows + "\n");
        assertSqlCursors("SELECT * FROM p ORDER BY ts, exch, px", "SELECT * FROM c ORDER BY ts, exch, px");
        assertSqlCursors("SELECT exch, count() FROM p ORDER BY exch", "SELECT exch, count() FROM c ORDER BY exch");
    }

    /**
     * AUTOSCALE, the automatic sibling of the explicit ALTER above. {@code scaleSymbolCapacities} runs
     * from {@code housekeep()} on ordinary commits and simply calls {@code changeSymbolCapacity} -- the
     * same statement this suite already proves safe for composite, whose cell-blind reopen is skipped
     * for a routed table.
     *
     * <p>Asserts the capacity ACTUALLY GREW. Without that, enabling autoscale could be "achieved" by
     * leaving the skip in place, which is precisely the silent no-op this suite exists to prevent.
     */
    @Test
    public void testSymbolCapacityAutoScalesOnACompositeTable() throws Exception {
        node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_AUTO_SCALE_SYMBOL_CAPACITY, "true");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, sym SYMBOL CAPACITY 4, px DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY, exch LAYOUT PLAIN WAL");
            try (TableReader reader = engine.getReader(engine.verifyTableName("c"))) {
                Assert.assertEquals("precondition: sym starts small", 4,
                        reader.getSymbolMapReader(2).getSymbolCapacity());
            }

            // Enough distinct values in the NON-dimension symbol column to cross the autoscale
            // threshold. The dimension column stays low-cardinality so the day keeps a sane cell count.
            final StringBuilder rows = new StringBuilder();
            for (int i = 0; i < 40; i++) {
                if (i > 0) {
                    rows.append(',');
                }
                rows.append("('2023-01-01T00:").append(String.format("%02d", i)).append(":00.000000Z','E")
                        .append(i % 3).append("','S").append(i).append("',").append(i).append(".0)");
            }
            execute("INSERT INTO c VALUES " + rows);
            drainWalQueue();
            engine.releaseInactive();

            Assert.assertFalse("autoscale must not suspend the table",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c")));
            try (TableReader reader = engine.getReader(engine.verifyTableName("c"))) {
                final int capacity = reader.getSymbolMapReader(2).getSymbolCapacity();
                Assert.assertTrue("symbol capacity must have auto-scaled past its initial 4, was " + capacity,
                        capacity > 4);
            }
            // every row landed, i.e. autoscale did not disturb the write path
            printSql("select count() from c");
            TestUtils.assertContains(sink, "40");
        });
    }

    private void createTwins() throws SqlException {
        execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY, exch LAYOUT PLAIN WAL");
        execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private void insertIntoBoth(String values) throws SqlException {
        execute("INSERT INTO c VALUES " + values);
        execute("INSERT INTO p VALUES " + values);
    }
}
