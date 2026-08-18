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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The WAL apply stamps each native partition's last-modifying seqTxn into the _txn parquet-file-size
 * word (bit 63 REMOTE masked off): a monotonic-safe per-partition version (>= the seqTxn that last
 * wrote it, strictly increasing on a write), not a cross-instance identity. Converting a partition
 * to parquet overwrites the slot with the file size.
 */
public class NativePartitionSeqTxnTest extends AbstractCairoTest {

    @Test
    public void testChangeColumnTypeOnRemoteNativePartitionClearsRemote() throws Exception {
        // A native partition with REMOTE set has its bytes rewritten under a new writer index by
        // ALTER COLUMN TYPE. The changeColumnType pre-pass must clear REMOTE, stamping the ALTER's
        // seqTxn as the partition's fresh version (a real version is available on WAL, so the slot
        // advances rather than dropping to the -1 sentinel), so the partition's remote copy is invalidated instead of
        // a read of the stale remote parquet mapping the new column index to a missing field id and decoding it as NULL.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10), ('2024-01-01T01:00:00', 20)");
            drainWalQueue();
            // day2 becomes the active partition, leaving day1 (index 0) non-active and stamped.
            execute("INSERT INTO t VALUES ('2024-01-02T00:00:00', 30)");
            drainWalQueue();

            // Stage day1 with a remote copy (REMOTE set) on the physical writer.
            final long preAlterSeqTxn;
            try (TableWriter writer = getWriter("t")) {
                TxWriter tx = writer.getTxWriter();
                Assert.assertFalse(tx.isPartitionParquet(0));
                preAlterSeqTxn = tx.getNativePartitionSeqTxn(0);
                Assert.assertTrue("day1 must be stamped before staging REMOTE", preAlterSeqTxn > 0);
                tx.setPartitionRemote(0, true);
                Assert.assertTrue(tx.isPartitionRemote(0));
                writer.bumpPartitionTableVersion();
                writer.commit();
            }

            execute("ALTER TABLE t ALTER COLUMN x TYPE LONG");
            drainWalQueue();

            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                Assert.assertFalse("ALTER COLUMN TYPE must clear REMOTE on the rewritten native partition",
                        tx.isPartitionRemote(0));
                Assert.assertTrue("the slot keeps a real version -- the ALTER's seqTxn -- not the -1 sentinel",
                        tx.getNativePartitionSeqTxn(0) > preAlterSeqTxn);
                Assert.assertFalse(tx.isPartitionParquet(0));
            }

            // The rewritten column reads back its values (cast to LONG), not NULL.
            assertQuery("SELECT * FROM t ORDER BY ts")
                    .noLeakCheck().timestamp("ts").expectSize()
                    .returns("ts\tx\n" +
                            "2024-01-01T00:00:00.000000Z\t10\n" +
                            "2024-01-01T01:00:00.000000Z\t20\n" +
                            "2024-01-02T00:00:00.000000Z\t30\n");
        });
    }

    @Test
    public void testChangeColumnTypeOnRemoteParquetPartitionClearsRemote() throws Exception {
        // A local parquet-format partition with REMOTE set remains parquet and lazy under the
        // replacingIndex model. ALTER only invalidates the stale remote copy: REMOTE clears while
        // parquet_generated, the format bit, and the local parquet file-size word are preserved.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10), ('2024-01-01T01:00:00', 20)");
            drainWalQueue();
            // day2 becomes the active partition, leaving day1 (index 0) non-active and convertible.
            execute("INSERT INTO t VALUES ('2024-01-02T00:00:00', 30)");
            drainWalQueue();

            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            // Stage day1 with a remote copy (REMOTE set) on the physical writer.
            final long preAlterParquetSize;
            try (TableWriter writer = getWriter("t")) {
                TxWriter tx = writer.getTxWriter();
                Assert.assertTrue("day1 must be parquet before the ALTER", tx.isPartitionParquet(0));
                Assert.assertTrue("day1 must have a local parquet source before the ALTER",
                        tx.isPartitionParquetGenerated(0));
                preAlterParquetSize = tx.getPartitionParquetFileSize(0);
                Assert.assertTrue("the slot holds the parquet file size before the ALTER", preAlterParquetSize > 0);
                tx.setPartitionRemote(0, true);
                Assert.assertTrue(tx.isPartitionRemote(0));
                writer.bumpPartitionTableVersion();
                writer.commit();
            }

            execute("ALTER TABLE t ALTER COLUMN x TYPE LONG");
            drainWalQueue();

            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                Assert.assertTrue("ALTER COLUMN TYPE must keep the local partition parquet-format",
                        tx.isPartitionParquet(0));
                Assert.assertFalse("ALTER COLUMN TYPE must clear REMOTE for the stale remote parquet",
                        tx.isPartitionRemote(0));
                Assert.assertTrue("metadata-only remote invalidation preserves the local parquet source",
                        tx.isPartitionParquetGenerated(0));
                Assert.assertEquals("metadata-only remote invalidation preserves offset-3 parquet file size",
                        preAlterParquetSize, tx.getPartitionParquetFileSize(0));
            }

            // The lazy parquet read path converts values to LONG, not NULL.
            assertQuery("SELECT * FROM t ORDER BY ts")
                    .noLeakCheck().timestamp("ts").expectSize()
                    .returns("ts\tx\n" +
                            "2024-01-01T00:00:00.000000Z\t10\n" +
                            "2024-01-01T01:00:00.000000Z\t20\n" +
                            "2024-01-02T00:00:00.000000Z\t30\n");
        });
    }

    @Test
    public void testChangeColumnTypeOnRemoteParquetPartitionFullTopClearsRemote() throws Exception {
        // A schema-present all-NULL STRING has columnTop == partitionSize. STRING -> SYMBOL
        // takes the eager parquet-to-native pre-pass, but there are then zero native values for
        // the normal conversion loop to visit. The pre-pass caller must invalidate REMOTE itself;
        // otherwise the stale object generation survives forever and suppresses re-upload.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00'), ('2024-01-01T01:00:00')");
            drainWalQueue();
            // ADD while day1 is active records a real full top (partitionSize), rather than
            // writing physical NULL values or leaving the older-partition -1 sentinel.
            execute("ALTER TABLE t ADD COLUMN x STRING");
            drainWalQueue();
            execute("INSERT INTO t (ts, x) VALUES ('2024-01-02T00:00:00', 'active')");
            drainWalQueue();

            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            try (TableWriter writer = getWriter("t")) {
                TxWriter tx = writer.getTxWriter();
                Assert.assertTrue("day1 must be parquet before the ALTER", tx.isPartitionParquet(0));
                Assert.assertTrue("day1 must retain a local parquet source", tx.isPartitionParquetGenerated(0));
                tx.setPartitionRemote(0, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
            }

            try (TableReader reader = getReader("t")) {
                final TxReader tx = reader.getTxFile();
                final int partitionIndex = tx.getPartitionIndex(reader.getMinTimestamp());
                final long partitionTimestamp = tx.getPartitionTimestampByIndex(partitionIndex);
                final int xIndex = reader.getMetadata().getColumnIndex("x");
                final int xWriterIndex = reader.getMetadata().getWriterIndex(xIndex);
                Assert.assertEquals(
                        "the reproducer must reach the schema-present full-top branch",
                        tx.getPartitionSize(partitionIndex),
                        reader.getColumnVersionReader().getColumnTop(partitionTimestamp, xWriterIndex)
                );
                reader.openPartition(partitionIndex);
                final int parquetXIndex = reader.getAndInitParquetPartitionDecoder(partitionIndex)
                        .metadata()
                        .getColumnIndex("x");
                Assert.assertTrue("native-to-parquet must keep x in the physical schema", parquetXIndex >= 0);
                Assert.assertEquals(
                        "the parquet source must still carry the pre-ALTER STRING type",
                        ColumnType.STRING,
                        reader.getAndInitParquetPartitionDecoder(partitionIndex).metadata().getColumnType(parquetXIndex)
                );
            }

            execute("ALTER TABLE t ALTER COLUMN x TYPE SYMBOL");
            drainWalQueue();

            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                Assert.assertFalse("the eager pre-pass converts day1 back to native", tx.isPartitionParquet(0));
                Assert.assertFalse("ALTER must invalidate the stale remote object for a full-top column",
                        tx.isPartitionRemote(0));
                Assert.assertFalse("the native result no longer has a staged local parquet",
                        tx.isPartitionParquetGenerated(0));
                Assert.assertTrue("the ALTER must leave a real partition version",
                        tx.getNativePartitionSeqTxn(0) > 0);
            }

            assertQuery("SELECT * FROM t ORDER BY ts")
                    .timestamp("ts").expectSize()
                    .returns("ts\tx\n" +
                            "2024-01-01T00:00:00.000000Z\t\n" +
                            "2024-01-01T01:00:00.000000Z\t\n" +
                            "2024-01-02T00:00:00.000000Z\tactive\n");
        });
    }

    @Test
    public void testChangeColumnTypeRejectedOnReadOnlyPartition() throws Exception {
        // ALTER COLUMN TYPE reopens every partition's column files, so a read-only partition (e.g.
        // a remotely-served one) is rejected up front -- before any partition is converted -- with a clear
        // message, not a cryptic fail-stop deeper on the missing local parquet.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1), ('2024-01-02T00:00:00', 2)");

            try (TableWriter writer = getWriter("t")) {
                TxWriter tx = writer.getTxWriter();
                tx.setPartitionReadOnlyByTimestamp(tx.getPartitionTimestampByIndex(0), true);
                writer.bumpPartitionTableVersion();
                writer.commit();
            }

            CairoException ex = Assert.assertThrows(
                    CairoException.class,
                    () -> execute("ALTER TABLE t ALTER COLUMN x TYPE LONG"));
            TestUtils.assertContains(ex.getFlyweightMessage(), "cannot change column type, partition is read-only");

            // the ALTER was rejected before any rewrite; the data still reads back unchanged
            assertQuery("SELECT * FROM t ORDER BY ts")
                    .timestamp("ts").expectSize()
                    .returns("ts\tx\n" +
                            "2024-01-01T00:00:00.000000Z\t1\n" +
                            "2024-01-02T00:00:00.000000Z\t2\n");
        });
    }

    @Test
    public void testConvertParquetToNativePreservesRemoteClearsGenerated() throws Exception {
        // CONVERT PARTITION TO NATIVE is a pure format transition: the rows do not change, so the
        // remote copy still vouches for the partition and REMOTE must survive. The conversion does
        // delete the local data.parquet, so parquet_generated must not outlive it, and the
        // structural WAL apply stamps a real seqTxn.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // separate commits so day1 is a stamped, non-active partition
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1)");
            drainWalQueue();
            execute("INSERT INTO t VALUES ('2024-01-02T00:00:00', 2)");
            drainWalQueue();

            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            // Stage day1 with a remote copy (REMOTE set) on the physical writer.
            try (TableWriter writer = getWriter("t")) {
                TxWriter tx = writer.getTxWriter();
                Assert.assertTrue("day1 must be parquet before the convert", tx.isPartitionParquet(0));
                tx.setPartitionRemote(0, true);
                Assert.assertTrue(tx.isPartitionRemote(0));
                writer.bumpPartitionTableVersion();
                writer.commit();
            }

            execute("ALTER TABLE t CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueue();

            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                Assert.assertFalse("day1 converted back to native", tx.isPartitionParquet(0));
                Assert.assertTrue("a format transition does not change the rows, REMOTE must survive",
                        tx.isPartitionRemote(0));
                Assert.assertFalse("conversion deletes the local parquet, generated must not outlive it",
                        tx.isPartitionParquetGenerated(0));
                Assert.assertTrue("the structural WAL apply stamped a real seqTxn, not the -1 sentinel",
                        tx.getNativePartitionSeqTxn(0) > 0);
            }

            assertQuery("SELECT * FROM t ORDER BY ts")
                    .timestamp("ts").expectSize()
                    .returns("ts\tx\n" +
                            "2024-01-01T00:00:00.000000Z\t1\n" +
                            "2024-01-02T00:00:00.000000Z\t2\n");
        });
    }

    @Test
    public void testConvertPartitionToNativeStampsStructuralSeqTxn() throws Exception {
        // CONVERT PARTITION TO NATIVE is a structural WAL apply: walApplySeqTxn is unset, so the
        // isWal() branch in convertPartitionParquetToNative stamps the op's getSeqTxn() rather than
        // dropping offset 3 to the -1 sentinel. The round-tripped native partition keeps a real
        // version instead of looking version-less.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // separate commits so day1 is a stamped, non-active native partition
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1)");
            drainWalQueue();
            execute("INSERT INTO t VALUES ('2024-01-02T00:00:00', 2)");
            drainWalQueue();

            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                Assert.assertFalse("day1 is native before convert", tx.isPartitionParquet(0));
                Assert.assertTrue("day1 native partition is stamped", tx.getNativePartitionSeqTxn(0) > 0);
            }

            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            try (TableReader reader = getReader("t")) {
                Assert.assertTrue("day1 converted to parquet", reader.getTxFile().isPartitionParquet(0));
            }

            execute("ALTER TABLE t CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueue();

            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                Assert.assertFalse("day1 converted back to native", tx.isPartitionParquet(0));
                Assert.assertTrue("the structural WAL apply stamped a real seqTxn, not the -1 sentinel",
                        tx.getNativePartitionSeqTxn(0) > 0);
                Assert.assertFalse("conversion deletes the local parquet, generated must not outlive it",
                        tx.isPartitionParquetGenerated(0));
                Assert.assertFalse("REMOTE was never set in this test, the round trip must not invent it",
                        tx.isPartitionRemote(0));
            }

            assertQuery("SELECT * FROM t ORDER BY ts")
                    .timestamp("ts").expectSize()
                    .returns("ts\tx\n" +
                            "2024-01-01T00:00:00.000000Z\t1\n" +
                            "2024-01-02T00:00:00.000000Z\t2\n");
        });
    }

    @Test
    public void testConvertReadOnlyPartitionToNativeRejected() throws Exception {
        // A read-only parquet partition (e.g. a remotely-served one) has no local data.parquet to decode back
        // to native, so CONVERT PARTITION TO NATIVE rejects it on the read-only bit with a clear
        // message. Marks a locally-present parquet partition read-only to isolate the read-only guard
        // from the missing-file path.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1), ('2024-01-02T00:00:00', 2)");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");

            try (TableWriter writer = getWriter("t")) {
                TxWriter tx = writer.getTxWriter();
                final long day1Ts = tx.getPartitionTimestampByIndex(0);
                Assert.assertTrue("day1 must be parquet", tx.isPartitionParquet(0));
                tx.setPartitionReadOnlyByTimestamp(day1Ts, true);
                writer.bumpPartitionTableVersion();
                writer.commit();

                CairoException ex = Assert.assertThrows(
                        CairoException.class,
                        () -> writer.convertPartitionParquetToNative(day1Ts));
                TestUtils.assertContains(ex.getFlyweightMessage(), "cannot convert read-only partition to native");

                // the partition is untouched by the rejected convert: still parquet, still read-only
                Assert.assertTrue("partition stays parquet", tx.isPartitionParquet(0));
                Assert.assertTrue("read-only preserved", tx.isPartitionReadOnly(0));
            }
        });
    }

    @Test
    public void testConvertToParquetOverwritesSeqTxnWithFileSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1), ('2024-01-01T01:00:00', 2)");
            drainWalQueue();
            // day2 active so day1 is a convertible, non-active partition.
            execute("INSERT INTO t VALUES ('2024-01-02T00:00:00', 3)");
            drainWalQueue();

            try (TableReader reader = getReader("t")) {
                Assert.assertTrue("day1 native partition is stamped",
                        reader.getTxFile().getNativePartitionSeqTxn(0) > 0);
            }

            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                Assert.assertTrue("day1 converted to parquet", tx.isPartitionParquet(0));
                // The slot now holds the parquet file size (KBs), not the small seqTxn.
                Assert.assertTrue("slot now holds the parquet file size", tx.getPartitionParquetFileSize(0) > 0);
            }
        });
    }

    @Test
    public void testConvertToParquetStampsPartitionSeqTxnInPm() throws Exception {
        // The _pm seqTxn the cold gate reads for a parquet partition is the partition's OWN offset-3
        // at convert time, not the table high-water -- so two instances converting the same partition
        // at different high-waters agree, and a re-upload is not provoked by an unrelated high-water.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1)");
            drainWalQueue();

            final long day1SeqTxn;
            try (TableReader reader = getReader("t")) {
                day1SeqTxn = reader.getTxFile().getNativePartitionSeqTxn(0);
                Assert.assertTrue("day1 native partition is stamped", day1SeqTxn > 0);
            }

            // Climb the table high-water above day1's stamp via writes to a later partition.
            execute("INSERT INTO t VALUES ('2024-01-02T00:00:00', 2)");
            drainWalQueue();
            execute("INSERT INTO t VALUES ('2024-01-02T01:00:00', 3)");
            drainWalQueue();
            final long highWater;
            try (TableReader reader = getReader("t")) {
                highWater = reader.getTxFile().getSeqTxn();
                Assert.assertTrue("high-water is now above day1's stamp", highWater > day1SeqTxn);
            }

            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            final long pmSeqTxn = readPmSeqTxn("t", 0);
            Assert.assertEquals("the _pm carries the partition's own seqTxn, not the convert-time high-water",
                    day1SeqTxn, pmSeqTxn);
        });
    }

    @Test
    public void testFailedToParquetSwitchKeepsNativeSeqTxn() throws Exception {
        // switchNativePartitionWithParquet aborts with SWITCH_NO_PARQUET when data.parquet is
        // missing. The partition stays native and its data is unchanged, so its stamped seqTxn must
        // survive: the abort clears only parquet_generated, it must not blank offset 3 to the -1
        // sentinel (that would throw away a still-valid version and make the partition look changed when it is not).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10)");
            drainWalQueue();
            // day2 becomes the active partition, leaving day1 (index 0) non-active and stamped.
            execute("INSERT INTO t VALUES ('2024-01-02T00:00:00', 20)");
            drainWalQueue();

            try (TableWriter writer = getWriter("t")) {
                TxWriter tx = writer.getTxWriter();
                long partitionTs = tx.getPartitionTimestampByIndex(0);
                Assert.assertFalse(tx.isPartitionParquet(0));
                final long preSwitchSeqTxn = tx.getNativePartitionSeqTxn(0);
                Assert.assertTrue("day1 must be stamped", preSwitchSeqTxn > 0);

                // Flag the partition parquet-generated (keeping its seqTxn) without creating a
                // data.parquet, so the switch reaches the "no parquet file to switch to" abort.
                tx.setPartitionParquetGenerated(0, true);
                writer.bumpPartitionTableVersion();
                writer.commit();

                Assert.assertEquals(TableWriter.SWITCH_NO_PARQUET,
                        writer.switchNativePartitionWithParquet(partitionTs, 0L));

                Assert.assertFalse("the aborted switch clears parquet_generated", tx.isPartitionParquetGenerated(0));
                Assert.assertFalse("partition stays native", tx.isPartitionParquet(0));
                Assert.assertEquals("the stamped seqTxn must survive the aborted switch",
                        preSwitchSeqTxn, tx.getNativePartitionSeqTxn(0));
            }
        });
    }

    @Test
    public void testMarkPartitionParquetReadyLegacyStampsSeqTxn() throws Exception {
        // A legacy/unstamped WAL partition reads offset 3 as -1 (an old binary's word or a cleared
        // slot). markPartitionParquetReady's "< 0 && isWal()" gate fires, stamping the current table
        // seqTxn so the staged-parquet native partition gains a real version instead of staying -1.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1)");
            drainWalQueue();
            execute("INSERT INTO t VALUES ('2024-01-02T00:00:00', 2)");
            drainWalQueue();

            final long day1Ts;
            final long day1NameTxn;
            final int tsType;
            try (TableWriter writer = getWriter("t")) {
                TxWriter tx = writer.getTxWriter();
                Assert.assertFalse("day1 must be native", tx.isPartitionParquet(0));
                // Roll day1 back to the cleared/unstamped sentinel to mimic a legacy partition.
                tx.setPartitionSeqTxnByRawIndex(0, 0L);
                Assert.assertEquals("legacy day1 reads as unstamped", -1L, tx.getNativePartitionSeqTxn(0));
                day1Ts = tx.getPartitionTimestampByIndex(0);
                day1NameTxn = tx.getPartitionNameTxn(0);
                tsType = tx.getTimestampType();

                plantEmptyDataParquet(day1Ts, day1NameTxn, tsType);
                Assert.assertTrue(writer.markPartitionParquetReady(day1Ts));

                Assert.assertTrue("the legacy gate stamps the table seqTxn", tx.getNativePartitionSeqTxn(0) > 0);
                Assert.assertTrue("partition is flagged parquet-generated", tx.isPartitionParquetGenerated(0));
                Assert.assertFalse("partition stays native", tx.isPartitionParquet(0));
            }
        });
    }

    @Test
    public void testMarkPartitionParquetReadyNonLegacyDoesNotBumpVersion() throws Exception {
        // Non-legacy WAL partition already carries a stamped seqTxn (separate commits). Flagging it
        // parquet-generated must NOT bump that version -- the "< 0" gate keeps the existing seqTxn,
        // and the partition stays native, not remote.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // separate commits so day1 is a stamped, non-active native partition
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1)");
            drainWalQueue();
            execute("INSERT INTO t VALUES ('2024-01-02T00:00:00', 2)");
            drainWalQueue();

            final long day1Ts;
            final long day1NameTxn;
            final int tsType;
            final long preSeqTxn;
            try (TableWriter writer = getWriter("t")) {
                TxWriter tx = writer.getTxWriter();
                Assert.assertFalse("day1 must be native", tx.isPartitionParquet(0));
                preSeqTxn = tx.getNativePartitionSeqTxn(0);
                Assert.assertTrue("day1 is stamped", preSeqTxn > 0);
                day1Ts = tx.getPartitionTimestampByIndex(0);
                day1NameTxn = tx.getPartitionNameTxn(0);
                tsType = tx.getTimestampType();

                plantEmptyDataParquet(day1Ts, day1NameTxn, tsType);
                Assert.assertTrue(writer.markPartitionParquetReady(day1Ts));

                Assert.assertTrue("partition is flagged parquet-generated", tx.isPartitionParquetGenerated(0));
                Assert.assertEquals("a present version must not be bumped", preSeqTxn, tx.getNativePartitionSeqTxn(0));
                Assert.assertFalse("partition stays native", tx.isPartitionParquet(0));
                Assert.assertFalse("staging parquet does not mark the partition remote", tx.isPartitionRemote(0));
            }
        });
    }

    @Test
    public void testMarkPartitionParquetReadyNonWalLeavesUnknownVersion() throws Exception {
        // Non-WAL partition never carries a seqTxn (offset 3 stays -1). markPartitionParquetReady's
        // "< 0 && isWal()" gate skips the stamp because there is no real version to write, yet still
        // flags the partition parquet-generated and reads its native data unchanged.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1), ('2024-01-02T00:00:00', 2)");

            final long day1Ts;
            final long day1NameTxn;
            final int tsType;
            try (TableWriter writer = getWriter("t")) {
                TxWriter tx = writer.getTxWriter();
                Assert.assertFalse("day1 must be native", tx.isPartitionParquet(0));
                Assert.assertEquals("non-WAL day1 has no version", -1L, tx.getNativePartitionSeqTxn(0));
                day1Ts = tx.getPartitionTimestampByIndex(0);
                day1NameTxn = tx.getPartitionNameTxn(0);
                tsType = tx.getTimestampType();

                plantEmptyDataParquet(day1Ts, day1NameTxn, tsType);
                Assert.assertTrue(writer.markPartitionParquetReady(day1Ts));

                Assert.assertEquals("the isWal() gate skips the stamp on a non-WAL table",
                        -1L, tx.getNativePartitionSeqTxn(0));
                Assert.assertTrue("partition is flagged parquet-generated", tx.isPartitionParquetGenerated(0));
                Assert.assertFalse("partition stays native", tx.isPartitionParquet(0));
            }

            // the native data still reads correctly; the empty staged parquet is ignored
            assertQuery("SELECT * FROM t ORDER BY ts")
                    .timestamp("ts").expectSize()
                    .returns("ts\tx\n" +
                            "2024-01-01T00:00:00.000000Z\t1\n" +
                            "2024-01-02T00:00:00.000000Z\t2\n");
        });
    }

    @Test
    public void testMarkRemoteParquetStaleRejectsNonGeneratedPartition() throws Exception {
        // [F G R U] = [1 0 0 1] is not produced by the storage-policy state machine: marking a
        // parquet partition uploaded also sets G. Keep the defensive writer guard covered so a
        // malformed/future state cannot silently lose U and become a local parquet partition with
        // no generated data.parquet to read.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 10), ('2024-01-02T00:00:00', 20)");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");

            try (TableWriter writer = getWriter("t")) {
                TxWriter tx = writer.getTxWriter();
                Assert.assertTrue("precondition: old partition is parquet-format", tx.isPartitionParquet(0));
                Assert.assertTrue("precondition: local parquet was generated", tx.isPartitionParquetGenerated(0));
                Assert.assertFalse("precondition: partition is writable", tx.isPartitionReadOnly(0));
                Assert.assertFalse("precondition: partition is not uploaded", tx.isPartitionRemote(0));

                tx.setPartitionParquetGenerated(0, false);
                tx.setPartitionRemote(0, true);
                Assert.assertTrue("the malformed tuple is classified as remotely served", tx.isPartitionRemotelyServed(0));
                final long partitionTableVersion = tx.getPartitionTableVersion();

                CairoException ex = Assert.assertThrows(
                        CairoException.class,
                        () -> writer.markParquetPartitionRemoteStale(0)
                );
                TestUtils.assertContains(ex.getFlyweightMessage(), "cannot invalidate remotely-served parquet partition");

                Assert.assertTrue("rejection preserves parquet format", tx.isPartitionParquet(0));
                Assert.assertFalse("rejection preserves the absent local parquet marker", tx.isPartitionParquetGenerated(0));
                Assert.assertFalse("rejection preserves writable state", tx.isPartitionReadOnly(0));
                Assert.assertTrue("rejection must not clear REMOTE", tx.isPartitionRemote(0));
                Assert.assertEquals("rejection must not bump partition metadata", partitionTableVersion, tx.getPartitionTableVersion());
            }
        });
    }

    @Test
    public void testNativePartitionWithStrayParquetMetadataReadsNativeOnDrop() throws Exception {
        // A native-format partition can transiently carry a data.parquet/_pm next to its native columns
        // (a staged/stray parquet sidecar), while its offset-3 word is a seqTxn, not a file size. A DROP of an
        // adjacent partition recomputes this partition's min/max; the recompute must key on the format
        // bit and read the NATIVE columns, not route through the parquet branch and feed the seqTxn to
        // the footer reader (which asserts/throws). Plants a stray _pm to exercise the dispatch directly.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1), ('2024-01-01T06:00:00', 2)");
            execute("INSERT INTO t VALUES ('2024-01-02T00:00:00', 3)");
            execute("INSERT INTO t VALUES ('2024-01-03T00:00:00', 4)");
            drainWalQueue();

            final long day1Ts;
            final long day1NameTxn;
            final int tsType;
            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                Assert.assertFalse("day1 must be native", tx.isPartitionParquet(0));
                Assert.assertTrue("day1 must carry a stamped seqTxn", tx.getNativePartitionSeqTxn(0) > 0);
                day1Ts = tx.getPartitionTimestampByIndex(0);
                day1NameTxn = tx.getPartitionNameTxnByPartitionTimestamp(day1Ts, -1L);
                tsType = tx.getTimestampType();
            }

            // Plant a stray _pm next to day1's native columns (mimics a staged/orphan parquet sidecar).
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path p = new Path()) {
                TableToken tt = engine.verifyTableName("t");
                p.of(configuration.getDbRoot()).concat(tt);
                TableUtils.setPathForNativePartition(p, tsType, PartitionBy.DAY, day1Ts, day1NameTxn);
                p.concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
                Assert.assertTrue("plant stray _pm", ff.touch(p.$()));
            }

            // DROP the middle partition (day2) recomputes day1's min/max via the prev-partition path.
            execute("ALTER TABLE t DROP PARTITION LIST '2024-01-02'");
            drainWalQueue();

            // day1 (read from its native columns) and day3 survive with correct data; no throw.
            assertQuery("SELECT * FROM t ORDER BY ts")
                    .timestamp("ts").expectSize()
                    .returns("ts\tx\n" +
                            "2024-01-01T00:00:00.000000Z\t1\n" +
                            "2024-01-01T06:00:00.000000Z\t2\n" +
                            "2024-01-03T00:00:00.000000Z\t4\n");
        });
    }

    @Test
    public void testNonWalO3MutateClearsStaleVersionAndRemote() throws Exception {
        // A non-WAL O3 write into a non-active partition rewrites its data, so a stale stamp and
        // REMOTE must not survive: a non-WAL mutate has no real seqTxn to write, so the slot
        // drops to the -1 no-version word and REMOTE + parquet_generated clear with it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1), ('2024-01-02T00:00:00', 2)");

            try (TableWriter writer = getWriter("t")) {
                TxWriter tx = writer.getTxWriter();
                Assert.assertFalse(tx.isPartitionParquet(0));
                // a real stamp first; setPartitionSeqTxn itself clears REMOTE, so REMOTE goes on after
                tx.setPartitionSeqTxn(0, 5L);
                Assert.assertEquals(5L, tx.getNativePartitionSeqTxn(0));
                tx.setPartitionRemote(0, true);
                tx.setPartitionParquetGenerated(0, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
            }

            // O3 write into day1 (behind the active day2) rewrites the non-active partition directly.
            execute("INSERT INTO t VALUES ('2024-01-01T05:00:00', 99)");

            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                Assert.assertFalse("partition stays native", tx.isPartitionParquet(0));
                Assert.assertEquals("non-WAL O3 clears the stale stamp to the no-version word",
                        -1L, tx.getNativePartitionSeqTxn(0));
                Assert.assertFalse("a write clears stale REMOTE", tx.isPartitionRemote(0));
                Assert.assertFalse("a write invalidates stale staged parquet", tx.isPartitionParquetGenerated(0));
            }

            assertQuery("SELECT * FROM t ORDER BY ts")
                    .timestamp("ts").expectSize()
                    .returns("ts\tx\n" +
                            "2024-01-01T00:00:00.000000Z\t1\n" +
                            "2024-01-01T05:00:00.000000Z\t99\n" +
                            "2024-01-02T00:00:00.000000Z\t2\n");
        });
    }

    @Test
    public void testO3RewriteOfParquetPartitionAdvancesPmSeqTxn() throws Exception {
        // An O3 write into a parquet-format partition rewrites it in place. The new _pm must carry a
        // higher seqTxn than before, so the cold gate sees a newer version and re-uploads the changed
        // bytes instead of skipping them as already-durable.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1), ('2024-01-01T02:00:00', 2)");
            drainWalQueue();
            // A later partition leaves 2024-01-01 non-active and convertible.
            execute("INSERT INTO t VALUES ('2024-01-02T00:00:00', 3)");
            drainWalQueue();
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            final long pmSeqTxnBefore = readPmSeqTxn("t", 0);
            Assert.assertTrue("converted parquet partition carries a _pm seqTxn", pmSeqTxnBefore > 0);

            // O3 write into the parquet partition -> in-place rewrite.
            execute("INSERT INTO t VALUES ('2024-01-01T01:00:00', 99)");
            drainWalQueue();
            try (TableReader reader = getReader("t")) {
                Assert.assertTrue("partition stays parquet after the in-place O3 rewrite",
                        reader.getTxFile().isPartitionParquet(0));
            }

            final long pmSeqTxnAfter = readPmSeqTxn("t", 0);
            Assert.assertTrue("the in-place O3 rewrite advances the _pm seqTxn (" + pmSeqTxnBefore
                    + " -> " + pmSeqTxnAfter + ")", pmSeqTxnAfter > pmSeqTxnBefore);
        });
    }

    @Test
    public void testReResolveRebindsSeqTxnToSelectedFooter() throws Exception {
        // One reader instance re-resolved across a real two-footer MVCC chain must serve each
        // selected footer's seqTxn: the cached native reader is bound to the resolved snapshot,
        // and a re-resolve that picks a different footer rebinds it instead of serving the old
        // parse. Small row groups + the ratio/max-bytes overrides make the O3 write an
        // incremental _pm append (one row group replaced, footer appended), so the old snapshot
        // stays reachable through the chain walk.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00', 1), ('2024-01-01T01:00:00', 2),
                    ('2024-01-01T02:00:00', 3), ('2024-01-01T03:00:00', 4),
                    ('2024-01-01T04:00:00', 5), ('2024-01-01T05:00:00', 6),
                    ('2024-01-01T06:00:00', 7), ('2024-01-01T07:00:00', 8),
                    ('2024-01-01T08:00:00', 9), ('2024-01-01T09:00:00', 10),
                    ('2024-01-01T10:00:00', 11), ('2024-01-01T11:00:00', 12)
                    """);
            drainWalQueue();
            execute("INSERT INTO t VALUES ('2024-01-02T00:00:00', 99)");
            drainWalQueue();
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            final long oldParquetSize;
            try (TableReader reader = getReader("t")) {
                oldParquetSize = reader.getTxFile().getPartitionParquetFileSize(0);
            }
            final long oldSeqTxn = readPmSeqTxn("t", 0);
            Assert.assertTrue("converted partition carries a _pm seqTxn", oldSeqTxn > 0);

            // O3 write into one row group's interior -> incremental update appends a footer.
            execute("INSERT INTO t VALUES ('2024-01-01T04:30:00', 100)");
            drainWalQueue();

            final long newParquetSize;
            final long partitionTs;
            final long nameTxn;
            final int timestampType;
            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                Assert.assertTrue(tx.isPartitionParquet(0));
                newParquetSize = tx.getPartitionParquetFileSize(0);
                partitionTs = tx.getPartitionTimestampByIndex(0);
                nameTxn = tx.getPartitionNameTxn(0);
                timestampType = tx.getTimestampType();
            }
            final long newSeqTxn = readPmSeqTxn("t", 0);
            Assert.assertTrue("the rewrite advances the seqTxn", newSeqTxn > oldSeqTxn);
            Assert.assertNotEquals("the rewrite changes the parquet file size", oldParquetSize, newParquetSize);

            final FilesFacade ff = configuration.getFilesFacade();
            final ParquetMetaFileReader pmReader = new ParquetMetaFileReader();
            try (Path p = new Path()) {
                p.of(configuration.getDbRoot()).concat(engine.verifyTableName("t"));
                TableUtils.setPathForParquetPartitionMetadata(p, timestampType, PartitionBy.DAY, partitionTs, nameTxn);
                final long addr = ParquetMetaFileReader.openAndMapRO(ff, p.$(), pmReader);
                Assert.assertTrue("post-rewrite _pm must open", addr != 0);
                final long size = pmReader.getFileSize();
                try {
                    Assert.assertTrue(pmReader.resolveFooter(oldParquetSize));
                    Assert.assertEquals("older version's footer seqTxn", oldSeqTxn, pmReader.getResolvedSeqTxn());
                    Assert.assertTrue(pmReader.resolveFooter(newParquetSize));
                    Assert.assertEquals("re-resolve to the latest footer rebinds", newSeqTxn, pmReader.getResolvedSeqTxn());
                    Assert.assertTrue(pmReader.resolveFooter(oldParquetSize));
                    Assert.assertEquals("and back to the older footer", oldSeqTxn, pmReader.getResolvedSeqTxn());
                } finally {
                    pmReader.clear();
                    ff.munmap(addr, size, MemoryTag.MMAP_PARQUET_METADATA_READER);
                }
            }
        });
    }

    @Test
    public void testShowPartitionsDoesNotLeakSeqTxnAsFileSize() throws Exception {
        // Reader regression: every native partition now carries a non-(-1) seqTxn in offset 3, but
        // table_partitions gates the parquet-file-size read on the format bit, so it must still show
        // -1 for native partitions, never the seqTxn.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1)");
            drainWalQueue();
            execute("INSERT INTO t VALUES ('2024-01-02T00:00:00', 2)");
            drainWalQueue();

            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                Assert.assertTrue(tx.getNativePartitionSeqTxn(0) > 0);
                Assert.assertTrue(tx.getNativePartitionSeqTxn(1) > 0);
            }

            assertQuery("SELECT index, isParquet, parquetFileSize FROM table_partitions('t')")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("index\tisParquet\tparquetFileSize\n" +
                            "0\tfalse\t-1\n" +
                            "1\tfalse\t-1\n");

            assertQuery("SELECT * FROM t ORDER BY ts")
                    .noLeakCheck().timestamp("ts").expectSize()
                    .returns("ts\tx\n" +
                            "2024-01-01T00:00:00.000000Z\t1\n" +
                            "2024-01-02T00:00:00.000000Z\t2\n");
        });
    }

    @Test
    public void testSquashOverUntrustedSeqTxnNeverPromotesIt() throws Exception {
        // The laundering hazard: squashSplitPartitions reads each source's seqTxn and restamps the
        // merge with the max. A released-base word (a parquet file size, no VALID bit) on one split
        // must not be promoted into a trusted stamp by that read -- the gated read quarantines it to
        // -1 (floored to 0 by the merge), so the result is max(trusted sources), never the file size.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 << 10);
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 2);

            execute("CREATE TABLE xq (ts TIMESTAMP, y LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO xq SELECT timestamp_sequence('2020-02-04T00', 60*1_000_000L), x FROM long_sequence(1320)");
            drainWalQueue();
            // O3 write into the middle of 2020-02-04 splits it.
            execute("INSERT INTO xq SELECT timestamp_sequence('2020-02-04T20:01', 1_000_000L), x FROM long_sequence(200)");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("xq");
            final long trustedSeqTxn;
            try (TableReader reader = getReader("xq")) {
                TxReader tx = reader.getTxFile();
                Assert.assertEquals("2020-02-04 must be split into two sub-partitions", 2, tx.getPartitionCount());
                // The lo split carries the split-commit stamp; the hi split is the active
                // partition, unstamped until a later commit closes it.
                trustedSeqTxn = tx.getNativePartitionSeqTxn(0);
                Assert.assertTrue(trustedSeqTxn > 0);
            }

            // Poison the hi split with a released-base word: a big file size, no flag bits.
            final long poison = 50_000_000L;
            pokeRawOffset3OnDisk(tt, 1, poison);
            try (TableReader reader = getReader("xq")) {
                Assert.assertEquals("the poisoned split must read quarantined",
                        -1L, reader.getTxFile().getNativePartitionSeqTxn(1));
            }

            // A later partition makes 2020-02-04 non-active, then squash forces the merge.
            execute("INSERT INTO xq VALUES ('2020-02-05T00:00:00', 1)");
            drainWalQueue();
            execute("ALTER TABLE xq SQUASH PARTITIONS");
            drainWalQueue();

            try (TableReader reader = getReader("xq")) {
                TxReader tx = reader.getTxFile();
                Assert.assertEquals("2020-02-04 merged back to one partition, plus 2020-02-05", 2, tx.getPartitionCount());
                final long mergedSeqTxn = tx.getNativePartitionSeqTxn(0);
                Assert.assertNotEquals("the file size must never be promoted into a stamp", poison, mergedSeqTxn);
                Assert.assertEquals("the merge stamps max(trusted sources)", trustedSeqTxn, mergedSeqTxn);
            }
        });
    }

    @Test
    public void testSquashStampsMaxSourceSeqTxnNotHighWater() throws Exception {
        // squashSplitPartitions stamps the merged partition with max(merged sources' seqTxn), not the
        // table high-water. Build a split partition last written at seqTxn S, let the high-water climb
        // past S (a later partition + the squash command), then squash: the merged partition keeps S,
        // not the high-water -- otherwise a manager switch would spuriously re-upload unchanged bytes.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 << 10);
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 2);

            execute("CREATE TABLE x (ts TIMESTAMP, y LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x SELECT timestamp_sequence('2020-02-04T00', 60*1_000_000L), x FROM long_sequence(1320)");
            drainWalQueue();
            // O3 write into the middle of 2020-02-04 splits it.
            execute("INSERT INTO x SELECT timestamp_sequence('2020-02-04T20:01', 1_000_000L), x FROM long_sequence(200)");
            drainWalQueue();

            final long seqTxnAfterSplit;
            try (TableReader reader = getReader("x")) {
                TxReader tx = reader.getTxFile();
                seqTxnAfterSplit = tx.getSeqTxn();
                Assert.assertEquals("2020-02-04 must be split into two sub-partitions", 2, tx.getPartitionCount());
                Assert.assertEquals("both sub-partitions share the floor",
                        tx.getPartitionFloor(tx.getPartitionTimestampByIndex(0)),
                        tx.getPartitionFloor(tx.getPartitionTimestampByIndex(1)));
            }

            // A later partition makes 2020-02-04 non-active, then squash forces the merge.
            execute("INSERT INTO x VALUES ('2020-02-05T00:00:00', 1)");
            drainWalQueue();
            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();

            try (TableReader reader = getReader("x")) {
                TxReader tx = reader.getTxFile();
                final long seqTxnAfterSquash = tx.getSeqTxn();
                Assert.assertEquals("2020-02-04 merged back to one partition, plus 2020-02-05", 2, tx.getPartitionCount());
                final long mergedSeqTxn = tx.getNativePartitionSeqTxn(0);
                Assert.assertTrue("the squash advanced the high-water past the merged sources",
                        seqTxnAfterSquash > seqTxnAfterSplit);
                Assert.assertEquals("merged partition keeps max(sources) -- the last seqTxn that wrote it",
                        seqTxnAfterSplit, mergedSeqTxn);
                Assert.assertTrue("...not the table high-water at squash time", mergedSeqTxn < seqTxnAfterSquash);
            }
        });
    }

    @Test
    public void testUpdateRejectsParquetFormatPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1), ('2024-01-02T00:00:00', 2)");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");

            try (TableReader reader = getReader("t")) {
                Assert.assertTrue(reader.getTxFile().isPartitionParquet(0));
            }

            CairoException ex = Assert.assertThrows(
                    CairoException.class,
                    () -> execute("UPDATE t SET x = 10 WHERE ts = '2024-01-01T00:00:00'")
            );
            TestUtils.assertContains(ex.getFlyweightMessage(), "cannot update parquet-format partition");
        });
    }

    @Test
    public void testUpdateOnNonWalNativePartitionClearsRemoteState() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1), ('2024-01-02T00:00:00', 2)");

            try (TableWriter writer = getWriter("t")) {
                TxWriter tx = writer.getTxWriter();
                Assert.assertFalse(tx.isPartitionParquet(0));
                // a real stamp first; setPartitionSeqTxn itself clears REMOTE, so REMOTE goes on after
                tx.setPartitionSeqTxn(0, 5L);
                Assert.assertEquals(5L, tx.getNativePartitionSeqTxn(0));
                tx.setPartitionRemote(0, true);
                tx.setPartitionParquetGenerated(0, true);
                writer.bumpPartitionTableVersion();
            }

            execute("UPDATE t SET x = 11 WHERE ts = '2024-01-01T00:00:00'");

            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                Assert.assertFalse(tx.isPartitionParquet(0));
                Assert.assertFalse("UPDATE must clear stale native REMOTE", tx.isPartitionRemote(0));
                Assert.assertFalse("UPDATE must invalidate stale staged parquet", tx.isPartitionParquetGenerated(0));
                Assert.assertEquals("non-WAL UPDATE clears the stale stamp to the no-version word",
                        -1L, tx.getNativePartitionSeqTxn(0));
            }

            assertQuery("SELECT * FROM t ORDER BY ts")
                    .timestamp("ts").expectSize()
                    .returns("ts\tx\n" +
                            "2024-01-01T00:00:00.000000Z\t11\n" +
                            "2024-01-02T00:00:00.000000Z\t2\n");
        });
    }

    @Test
    public void testWalAppendStampsActivePartitionSeqTxn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1)");
            drainWalQueue();
            execute("INSERT INTO t VALUES ('2024-01-01T01:00:00', 2)");
            drainWalQueue();

            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                Assert.assertEquals(1, tx.getPartitionCount());
                Assert.assertFalse(tx.isPartitionParquet(0));
                Assert.assertFalse(tx.isPartitionRemote(0));
                // every commit appends to the active partition and stamps it with the committed seqTxn
                Assert.assertTrue(tx.getSeqTxn() > 0);
                Assert.assertEquals(tx.getSeqTxn(), tx.getNativePartitionSeqTxn(0));
            }
        });
    }

    @Test
    public void testWalAppendStampIsVisibleToOpenReader() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1)");
            drainWalQueue();

            try (TableWriter writer = getWriter("t")) {
                writer.getTxWriter().setPartitionParquetGenerated(0, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
            }

            try (TableReader reader = getReader("t")) {
                final TxReader tx = reader.getTxFile();
                Assert.assertEquals(1L, tx.getSeqTxn());
                Assert.assertEquals(1L, tx.getNativePartitionSeqTxn(0));
                Assert.assertTrue("precondition: open reader sees the generated bit",
                        tx.isPartitionParquetGenerated(0));
                final long partitionTableVersion = tx.getPartitionTableVersion();

                execute("INSERT INTO t VALUES ('2024-01-01T01:00:00', 2)");
                drainWalQueue();

                Assert.assertTrue("open reader must observe the second WAL apply", reader.reload());
                Assert.assertEquals("a plain WAL append must not bump partitionTableVersion; the reload must take the fast path this test exercises",
                        partitionTableVersion, tx.getPartitionTableVersion());
                Assert.assertEquals("the serial drain fully commits its only WAL transaction",
                        0L, tx.getLagRowCount());
                Assert.assertEquals(2L, tx.getSeqTxn());
                Assert.assertEquals("active-partition stamp must reload with the committed seqTxn",
                        tx.getSeqTxn(), tx.getNativePartitionSeqTxn(0));
                Assert.assertFalse("active-partition stamp must reload the cleared generated bit",
                        tx.isPartitionParquetGenerated(0));
            }
        });
    }

    @Test
    public void testWalBlockStampsFirstPartitionWithBlockFinalSeqTxn() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 2);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");

            // Two separately committed INSERTs are visible to the same apply pass. With an exact
            // two-transaction lookahead they form one block spanning day1 and day2; day1 was last
            // written by the first transaction but must be conservatively stamped with the block's
            // final seqTxn.
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1)");
            execute("INSERT INTO t VALUES ('2024-01-02T00:00:00', 2)");
            drainWalQueue();

            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                Assert.assertEquals("the two commits must create two partitions", 2, tx.getPartitionCount());
                Assert.assertEquals("a fresh table with two INSERT commits ends at seqTxn 2", 2L, tx.getSeqTxn());
                Assert.assertEquals(
                        "the first partition touched by a WAL block must use its final seqTxn",
                        tx.getSeqTxn(),
                        tx.getNativePartitionSeqTxn(0)
                );
            }
        });
    }

    @Test
    public void testWalUpdateNoRowsLeavesPartitionVersionBitsUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1), ('2024-01-02T00:00:00', 2)");
            drainWalQueue();

            final long seqTxnBefore;
            try (TableWriter writer = getWriter("t")) {
                TxWriter tx = writer.getTxWriter();
                seqTxnBefore = tx.getNativePartitionSeqTxn(0);
                Assert.assertTrue(seqTxnBefore > 0);
                tx.setPartitionRemote(0, true);
                tx.setPartitionParquetGenerated(0, true);
                writer.bumpPartitionTableVersion();
            }

            execute("UPDATE t SET x = x + 100 WHERE ts < '2020-01-01T00:00:00'");
            drainWalQueue();

            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                Assert.assertTrue("the WAL seqTxn still advances for the applied no-op SQL", tx.getSeqTxn() > seqTxnBefore);
                Assert.assertEquals("no touched rows means no partition restamp", seqTxnBefore, tx.getNativePartitionSeqTxn(0));
                Assert.assertTrue("no touched rows means REMOTE stays as it was", tx.isPartitionRemote(0));
                Assert.assertTrue("no touched rows means staged parquet stays as it was", tx.isPartitionParquetGenerated(0));
            }
        });
    }

    @Test
    public void testWalO3MutateAdvancesPartitionSeqTxn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // separate commits so each day is stamped as the active partition at its own commit
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 1)");
            drainWalQueue();
            execute("INSERT INTO t VALUES ('2024-01-02T00:00:00', 2)");
            drainWalQueue();
            execute("INSERT INTO t VALUES ('2024-01-03T00:00:00', 3)");
            drainWalQueue();

            long day1SeqTxn;
            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                Assert.assertEquals(3, tx.getPartitionCount());
                day1SeqTxn = tx.getNativePartitionSeqTxn(0);
                Assert.assertTrue(day1SeqTxn > 0);
                Assert.assertFalse(tx.isPartitionRemote(0));
            }

            // O3 write into day1 (now far behind the active day3) rewrites it and advances its seqTxn
            execute("INSERT INTO t VALUES ('2024-01-01T05:00:00', 99)");
            drainWalQueue();

            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                long newSeqTxn = tx.getSeqTxn();
                Assert.assertTrue("O3 commit advanced the global seqTxn", newSeqTxn > day1SeqTxn);
                Assert.assertEquals("day1 restamped to the O3 commit seqTxn", newSeqTxn, tx.getNativePartitionSeqTxn(0));
                Assert.assertFalse("a write clears REMOTE", tx.isPartitionRemote(0));
                Assert.assertFalse(tx.isPartitionParquet(0));
            }
        });
    }

    @Test
    public void testWalUpdateStampsTouchedNativePartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 1), " +
                    "('2024-01-02T00:00:00', 2), " +
                    "('2024-01-03T00:00:00', 3)");
            drainWalQueue();

            try (TableWriter writer = getWriter("t")) {
                TxWriter tx = writer.getTxWriter();
                Assert.assertEquals(3, tx.getPartitionCount());
                for (int i = 0; i < 2; i++) {
                    Assert.assertFalse(tx.isPartitionParquet(i));
                    tx.setPartitionRemote(i, true);
                    tx.setPartitionParquetGenerated(i, true);
                }
                writer.bumpPartitionTableVersion();
            }

            execute("UPDATE t SET x = x + 100 WHERE ts < '2024-01-03T00:00:00'");
            drainWalQueue();

            try (TableReader reader = getReader("t")) {
                TxReader tx = reader.getTxFile();
                final long updateSeqTxn = tx.getSeqTxn();
                for (int i = 0; i < 2; i++) {
                    Assert.assertFalse(tx.isPartitionParquet(i));
                    Assert.assertFalse("UPDATE must clear REMOTE on touched partition " + i, tx.isPartitionRemote(i));
                    Assert.assertFalse("UPDATE must clear staged parquet on touched partition " + i, tx.isPartitionParquetGenerated(i));
                    Assert.assertEquals("UPDATE must stamp touched partition " + i, updateSeqTxn, tx.getNativePartitionSeqTxn(i));
                }
                Assert.assertEquals("untouched day3 remains stamped to its own insert seqTxn", updateSeqTxn - 1, tx.getNativePartitionSeqTxn(2));
            }

            assertQuery("SELECT * FROM t ORDER BY ts")
                    .timestamp("ts").expectSize()
                    .returns("ts\tx\n" +
                            "2024-01-01T00:00:00.000000Z\t101\n" +
                            "2024-01-02T00:00:00.000000Z\t102\n" +
                            "2024-01-03T00:00:00.000000Z\t3\n");
        });
    }

    // Reads the seqTxn stored in a parquet partition's _pm, keyed by its offset-3 file size --
    // the same value the cold-upload gate compares against the manifest.
    private long readPmSeqTxn(String table, int partitionIndex) {
        try (TableReader reader = getReader(table)) {
            TxReader tx = reader.getTxFile();
            final long fileSize = tx.getPartitionParquetFileSize(partitionIndex);
            final long partitionTs = tx.getPartitionTimestampByIndex(partitionIndex);
            final long nameTxn = tx.getPartitionNameTxn(partitionIndex);
            try (Path p = new Path()) {
                p.of(configuration.getDbRoot()).concat(engine.verifyTableName(table));
                TableUtils.setPathForParquetPartitionMetadata(p, tx.getTimestampType(), PartitionBy.DAY, partitionTs, nameTxn);
                return TestUtils.readSeqTxnForVersion(configuration.getFilesFacade(), p.$(), fileSize);
            }
        }
    }

    // Plants an empty data.parquet next to a native partition's columns; markPartitionParquetReady
    // only checks the file exists, it never reads it.
    private void plantEmptyDataParquet(long partitionTimestamp, long partitionNameTxn, int tsType) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path p = new Path()) {
            TableToken tt = engine.verifyTableName("t");
            p.of(configuration.getDbRoot()).concat(tt);
            TableUtils.setPathForNativePartition(p, tsType, PartitionBy.DAY, partitionTimestamp, partitionNameTxn);
            p.concat(TableUtils.PARQUET_PARTITION_NAME).$();
            Assert.assertTrue("plant empty data.parquet", ff.touch(p.$()));
        }
    }

    // Rewrites a partition's raw offset-3 _txn word with an in-place 8-byte file write, no version
    // bump -- exactly the bytes the released base left behind (it stored the generated data.parquet
    // file size there with no flag bits). Pooled writers/readers are purged first so every later
    // open reloads the poked word.
    private void pokeRawOffset3OnDisk(TableToken tt, int partitionIndex, long word) {
        final int tsType;
        try (TableReader reader = engine.getReader(tt)) {
            tsType = reader.getTxFile().getTimestampType();
        }
        engine.releaseInactive();
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(tt).concat(TableUtils.TXN_FILE_NAME);
            final long fileOffset;
            try (TxReader txReader = new TxReader(ff)) {
                txReader.ofRO(path.$(), tsType, PartitionBy.DAY);
                txReader.unsafeLoadAll();
                // raw long index of the partition's offset-3 (version) slot in the partition table
                final int rawIndex = partitionIndex * TableUtils.LONGS_PER_TX_ATTACHED_PARTITION + 3;
                fileOffset = txReader.getBaseOffset() + TableUtils.getPartitionTableIndexOffset(
                        TableUtils.getPartitionTableSizeOffset(txReader.getSymbolColumnCount()), rawIndex);
            }
            try (MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(ff, path.$(), MemoryTag.MMAP_DEFAULT);
                mem.putLong(fileOffset, word);
                mem.close(false);
            }
        }
    }
}
