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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Plan 3b, Task 2: locks in that engine sites which open their OWN raw {@code TxReader} directly against
 * a table's {@code _txn} file -- never through {@link io.questdb.cairo.TableReader}, and never having
 * called the package-private {@code setComposite(boolean)} at all -- now read a composite table's true
 * partition count, purely from Task 1's self-describing {@code _txn} base-header stride marker.
 * <p>
 * {@code table_storage()} ({@link io.questdb.griffin.engine.table.TableStorageRecordCursorFactory}) is
 * the concrete example named in the task brief: its {@code TableStorageRecordCursor} constructs a single
 * {@link io.questdb.cairo.TxReader} field once, in the factory's constructor, and reuses it across every
 * table row via {@code TableUtils#setTxReaderPath} + {@code unsafeLoadRowCount()} -- with no
 * table-metadata-derived compositeness signal threaded in anywhere. Before Task 1's marker existed, this
 * always folded a composite table's attached-partitions region at the plain (4-long) stride regardless of
 * its true (8-long) on-disk stride, which -- since the raw region's byte length is unaffected by which
 * stride you fold it at -- silently DOUBLED the reported partition count (each true 8-long record misread
 * as two spurious 4-long ones).
 * <p>
 * See the task report for the RED evidence captured by temporarily disabling the marker read in {@code
 * TxReader#unsafeLoadBaseOffset} while this test's assertions were in place (the doubled 6-vs-3 count this
 * test prevents).
 * <p>
 * <b>Deliberately TWO separate test methods, each with only ONE user table.</b> Do not "simplify" this
 * into one test with both a composite and a plain table present together: {@code
 * TableStorageRecordCursorFactory} reuses that SAME {@code TxReader} field across every table row of a
 * SINGLE query, and {@code TxReader#clearData()}/{@code #ofRO} never reset {@code
 * longsPerAttachedPartition} back to the plain default (Task 1's marker read is deliberately
 * upgrade-only). A query that scans a composite table and a plain table in the same {@code
 * table_storage()} cursor walk -- even with a {@code WHERE tableName = '...'} filter, which only
 * discards non-matching ROWS after the shared reader has already been mutated for them -- can carry a
 * stride-8 upgrade over into the plain table's fold and silently UNDER-count it (confirmed empirically:
 * 1 instead of the true 3, i.e. floor(12 longs / 8) instead of 12 longs / 4). That was a real, separate
 * latent bug in this reused-single-instance call site, out of Task 2's scope (see that task's report) --
 * not a reason to doubt the marker itself, which is exactly why the two tests above each only ever have
 * one user table in the engine at a time.
 * <p>
 * Plan 3b, Task 3 closes that separate gap: {@code TableUtils#createTxn} now writes the table's real
 * marker at CREATE time (instead of always writing the plain-default {@code 0}, even for a composite
 * table), and {@code TxReader#unsafeLoadBaseOffset()}'s marker read is now SYMMETRIC -- every load
 * re-derives the stride from whichever marker value it just read, in EITHER direction -- rather than
 * upgrade-only. {@link #testReusedTxReaderDoesNotLeakCompositeStrideIntoPlainTable()} below is the direct
 * repro/regression lock for that fix: it deliberately drives a composite table and then a plain table
 * through the SAME reused {@code TxReader}, in that order -- precisely the scenario the two tests above
 * avoid.
 */
public class CompositeTxnConsumerSitesTest extends AbstractCairoTest {

    /**
     * The actual Task 2 self-heal proof: {@code SELECT partitionCount FROM table_storage() WHERE
     * tableName = 'c'} for a composite table with rows across 3 day partitions (all landing at cellKey 0
     * -- real (ts, cellKey) write-routing is Plan 4) must report exactly 3 -- not 6, the pre-marker
     * misread (each real stride-8 record folded as two spurious stride-4 ones).
     */
    @Test
    public void testTableStoragePartitionCountNotDoubledForCompositeTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exchange symbol, x double) " +
                    "timestamp(ts) partition by day, exchange wal");
            execute("insert into c values " +
                    "('2020-01-01T00:00:00.000000Z','A',1.0), " +
                    "('2020-01-02T00:00:00.000000Z','A',2.0), " +
                    "('2020-01-03T00:00:00.000000Z','A',3.0)");
            drainWalQueue();
            engine.releaseAllWriters();

            assertQuery("select partitionCount from table_storage() where tableName = 'c'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("partitionCount\n3\n");
        });
    }

    /**
     * Independent confirmation of the "true" partition count asserted above: an equivalent PLAIN table
     * (same rows, same day partitioning, no composite dimension) built and queried the same way, in ITS
     * OWN engine (no composite table ever exists alongside it here -- see the class Javadoc), reports the
     * same 3. Together with the test above, this establishes the composite table's count equals what an
     * equivalent plain table's is, without needing both in the same query.
     */
    @Test
    public void testTableStoragePartitionCountForEquivalentPlainTableIsAlsoThree() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p (ts timestamp, exchange symbol, x double) " +
                    "timestamp(ts) partition by day");
            execute("insert into p values " +
                    "('2020-01-01T00:00:00.000000Z','A',1.0), " +
                    "('2020-01-02T00:00:00.000000Z','A',2.0), " +
                    "('2020-01-03T00:00:00.000000Z','A',3.0)");
            engine.releaseAllWriters();

            assertQuery("select partitionCount from table_storage() where tableName = 'p'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("partitionCount\n3\n");
        });
    }

    /**
     * Plan 3b, Task 3: the actual reused-reader repro. Builds a composite table {@code c} (3 day
     * partitions) and a plain table {@code p} (3 day partitions), then drives ONE {@link TxReader}
     * instance across both -- {@code c} first, {@code p} second -- via the exact same {@code
     * TableUtils#setTxReaderPath} + {@code unsafeLoadRowCount()} call sequence {@code
     * TableStorageRecordCursor#getTableStats} uses on its single reused reader field (see {@link
     * #loadLikeTableStorage}). Pre-fix, reading {@code c} first upgrades the shared reader to stride 8
     * (Task 1's marker read was upgrade-only), and reading {@code p} right after leaves that stride-8 in
     * place (plain's marker {@code 0} "leaves the stride as-is" under the old rule), silently folding
     * {@code p}'s stride-4 region at stride 8 and under-counting it to 1. Post-fix, the read is
     * symmetric, so the read of {@code p} always re-derives stride 4 from {@code p}'s own on-disk marker,
     * regardless of what the reader just read for {@code c}.
     */
    @Test
    public void testReusedTxReaderDoesNotLeakCompositeStrideIntoPlainTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exchange symbol, x double) " +
                    "timestamp(ts) partition by day, exchange wal");
            execute("insert into c values " +
                    "('2020-01-01T00:00:00.000000Z','A',1.0), " +
                    "('2020-01-02T00:00:00.000000Z','A',2.0), " +
                    "('2020-01-03T00:00:00.000000Z','A',3.0)");
            drainWalQueue();

            execute("create table p (ts timestamp, x double) timestamp(ts) partition by day");
            execute("insert into p values " +
                    "('2020-01-01T00:00:00.000000Z',1.0), " +
                    "('2020-01-02T00:00:00.000000Z',2.0), " +
                    "('2020-01-03T00:00:00.000000Z',3.0)");
            engine.releaseAllWriters();

            try (TxReader txReader = new TxReader(engine.getConfiguration().getFilesFacade())) {
                TableToken cToken = engine.verifyTableName("c");
                loadLikeTableStorage(txReader, cToken);
                Assert.assertEquals(
                        "sanity: composite table must report its true partition count first",
                        3, txReader.getPartitionCount());

                TableToken pToken = engine.verifyTableName("p");
                loadLikeTableStorage(txReader, pToken);
                Assert.assertEquals(
                        "reused TxReader must not leak the composite table's stride into the very next " +
                                "plain-table read -- pre-fix this under-counted to 1",
                        3, txReader.getPartitionCount());
            }
        });
    }

    /**
     * Mirrors {@code TableStorageRecordCursor.TableStorageRecord#getTableStats}'s exact reuse idiom: the
     * SAME {@link TxReader} instance, repointed at a different table's {@code _txn} file via {@code
     * TableUtils#setTxReaderPath}, then reloaded via {@code unsafeLoadRowCount()} -- with no
     * table-metadata-derived compositeness signal threaded in anywhere, exactly like the real call site.
     */
    private void loadLikeTableStorage(TxReader txReader, TableToken token) {
        CairoConfiguration configuration = engine.getConfiguration();
        int partitionBy;
        int timestampType;
        try (TableMetadata tm = engine.getTableMetadata(token)) {
            partitionBy = tm.getPartitionBy();
            timestampType = tm.getTimestampType();
        }
        final Path path = Path.getThreadLocal(configuration.getDbRoot()).concat(token.getDirName());
        TableUtils.setTxReaderPath(txReader, path, timestampType, partitionBy);
        txReader.unsafeLoadRowCount();
    }
}
