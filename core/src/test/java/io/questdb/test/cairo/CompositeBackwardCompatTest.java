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

package io.questdb.test.cairo;

import io.questdb.cairo.sql.TableMetadata;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Backward-compatibility guard for composite partitioning (Plan 1, Task 8).
 * <p>
 * Plan 1 delivered only grammar + metadata: parsing, validating and persisting a
 * {@link io.questdb.cairo.PartitionSpec}, plus SHOW CREATE re-emission. The writer's physical
 * partition ROUTING is deliberately unchanged -- a composite table must, for now, land every row in
 * the same single time-partition directory a plain table would use (no per-symbol cell
 * subdirectories yet; that is Plans 3-4). These tests prove:
 * <ol>
 *     <li>a plain (non-composite) table's SHOW CREATE output is byte-identical to pre-feature
 *     output;</li>
 *     <li>a composite table accepts INSERT and WAL apply, landing its data in exactly one ordinary
 *     {@code YYYY-MM-DD} partition;</li>
 *     <li>the persisted {@code PartitionSpec} survives a metadata reload ({@code
 *     engine.releaseInactive()} forcing a fresh {@code _meta} read) without disrupting ingestion into
 *     either kind of table.</li>
 * </ol>
 */
public class CompositeBackwardCompatTest extends AbstractCairoTest {

    @Test
    public void testCompositeSpecStaysNonCompositeForPlainTableAfterReopen() throws Exception {
        // Negative case, the mirror of testReopenAfterRestartKeepsSpec in CompositeMetaFormatTest:
        // a PLAIN table's spec must remain non-composite after a forced metadata reload, proving the
        // version gate never mistakes a plain _meta for a composite one.
        assertMemoryLeak(() -> {
            execute("create table p (ts timestamp, s symbol) timestamp(ts) partition by day wal");
            engine.releaseInactive(); // force a fresh _meta read from disk

            try (TableMetadata m = engine.getTableMetadata(engine.verifyTableName("p"))) {
                Assert.assertNotNull(m.getPartitionSpec());
                Assert.assertFalse(
                        "plain table must never read back a composite block",
                        m.getPartitionSpec().isComposite()
                );
            }
        });
    }

    @Test
    public void testCompositeStoresLikePlainForNow() throws Exception {
        assertMemoryLeak(() -> {
            // Until Plans 3-4, a composite table writes data into ONE time partition dir (no cell subdirs yet).
            execute("create table t (ts timestamp, exchange symbol, price double) " +
                    "timestamp(ts) partition by day, exchange wal");
            execute("insert into t values ('2023-01-01T00:00:00.000000Z','NYSE',1.0)");
            drainWalQueue();
            assertQuery("select count() from t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n1\n");
            // exactly one partition, named as today
            assertQuery("select name from table_partitions('t')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("name\n2023-01-01\n");
        });
    }

    @Test
    public void testCompositeSurvivesReopenAndStillIngests() throws Exception {
        // Proves the persisted spec doesn't disrupt the writer after a metadata reload: create a
        // composite table, insert, force a fresh _meta read (engine.releaseInactive()), insert again,
        // and confirm both rows are visible and still land in the single day partition.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, exchange symbol, price double) " +
                    "timestamp(ts) partition by day, exchange wal");
            execute("insert into t values ('2023-01-01T00:00:00.000000Z','NYSE',1.0)");
            drainWalQueue();

            engine.releaseInactive(); // force a fresh _meta read from disk

            execute("insert into t values ('2023-01-01T01:00:00.000000Z','LSE',2.0)");
            drainWalQueue();

            assertQuery("select count() from t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n2\n");
            // still exactly one partition after the reload -- the persisted spec did not disrupt routing
            assertQuery("select name from table_partitions('t')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("name\n2023-01-01\n");
        });
    }

    @Test
    public void testExistingPlainTableUnaffected() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table legacy (ts timestamp, s symbol) timestamp(ts) partition by day wal");
            // Full SHOW CREATE identical to pre-feature output (no composite/ORDER BY/LAYOUT tokens).
            // NOTE: WAL-enabled is the silent default for a partitioned table, so putWal() omits the
            // keyword entirely for plain tables (see testPartitioning/testPartitioningButBypassingWAL
            // in ShowCreateTableTest, both pre-existing and untouched by this feature) -- this is NOT
            // a composite-partitioning behavior, just the existing convention this test must match.
            assertQuery("show create table legacy").noLeakCheck().noRandomAccess().returns("""
                    ddl
                    CREATE TABLE 'legacy' (\s
                    \tts TIMESTAMP,
                    \ts SYMBOL
                    ) timestamp(ts) PARTITION BY DAY;
                    """);
        });
    }
}
