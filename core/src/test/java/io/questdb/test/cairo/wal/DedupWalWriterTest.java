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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class DedupWalWriterTest extends AbstractCairoTest {
    @Test
    public void testANoDedupCommitKeepsTheRowsTheTableWouldCollapse() throws Exception {
        // WAL_DEDUP_MODE_NO_DEDUP is a producer saying "these rows are not to be
        // deduplicated against this table's keys", and the apply is what has to honour it.
        // Left unread it applies as the default, and the default on a dedup-keyed table
        // collapses every repeated key into the last row carrying it - which is row loss
        // the producer explicitly asked against, and which nothing downstream detects. The
        // control arm below is the same commit at the default mode.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (ts TIMESTAMP, x INT, v LONG) TIMESTAMP(ts)"
                    + " PARTITION BY DAY WAL DEDUP UPSERT KEYS (ts, x)");
            execute("INSERT INTO test VALUES ('2022-02-24T00:00:00.000000Z', 1, 10)");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("test");
            try (WalWriter walWriter = engine.getWalWriter(token)) {
                appendRow(walWriter, "2022-02-24T00:00:00.000000Z", 1, 20);
                appendRow(walWriter, "2022-02-24T00:00:00.000000Z", 1, 30);
                walWriter.commitWithParams(0, 0, WalUtils.WAL_DEDUP_MODE_NO_DEDUP);
            }
            drainWalQueue();

            assertQuery("test")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("ts\tx\tv\n"
                            + "2022-02-24T00:00:00.000000Z\t1\t10\n"
                            + "2022-02-24T00:00:00.000000Z\t1\t20\n"
                            + "2022-02-24T00:00:00.000000Z\t1\t30\n");

            // The control: the same two rows at the default mode are one row by the time
            // they land - the pair collapses to the last value written - and that row then
            // overwrites every stored row carrying the pair. Three distinct values in,
            // one value out.
            try (WalWriter walWriter = engine.getWalWriter(token)) {
                appendRow(walWriter, "2022-02-24T00:00:00.000000Z", 1, 40);
                appendRow(walWriter, "2022-02-24T00:00:00.000000Z", 1, 50);
                walWriter.commitWithParams(0, 0, WalUtils.WAL_DEDUP_MODE_DEFAULT);
            }
            drainWalQueue();

            assertQuery("test")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("ts\tx\tv\n"
                            + "2022-02-24T00:00:00.000000Z\t1\t50\n"
                            + "2022-02-24T00:00:00.000000Z\t1\t50\n"
                            + "2022-02-24T00:00:00.000000Z\t1\t50\n");
        });
    }

    @Test
    public void testANoDedupCommitKeepsTheRowsTheSkipTableWouldDrop() throws Exception {
        // testANoDedupCommitKeepsTheRowsTheTableWouldCollapse over DEDUP SKIP. Left at the
        // default, the commit would apply under the table's SKIP mode and drop every row whose
        // key the table already holds. The control arm is the same commit at the default mode,
        // which keeps the stored rows and drops the new ones.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (ts TIMESTAMP, x INT, v LONG) TIMESTAMP(ts)"
                    + " PARTITION BY DAY WAL DEDUP SKIP KEYS (ts, x)");
            execute("INSERT INTO test VALUES ('2022-02-24T00:00:00.000000Z', 1, 10)");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("test");
            try (WalWriter walWriter = engine.getWalWriter(token)) {
                appendRow(walWriter, "2022-02-24T00:00:00.000000Z", 1, 20);
                appendRow(walWriter, "2022-02-24T00:00:00.000000Z", 1, 30);
                walWriter.commitWithParams(0, 0, WalUtils.WAL_DEDUP_MODE_NO_DEDUP);
            }
            drainWalQueue();

            final String allRows = """
                    ts\tx\tv
                    2022-02-24T00:00:00.000000Z\t1\t10
                    2022-02-24T00:00:00.000000Z\t1\t20
                    2022-02-24T00:00:00.000000Z\t1\t30
                    """;
            assertQuery("test").noLeakCheck().expectSize().timestamp("ts").returns(allRows);

            try (WalWriter walWriter = engine.getWalWriter(token)) {
                appendRow(walWriter, "2022-02-24T00:00:00.000000Z", 1, 40);
                appendRow(walWriter, "2022-02-24T00:00:00.000000Z", 1, 50);
                walWriter.commitWithParams(0, 0, WalUtils.WAL_DEDUP_MODE_DEFAULT);
            }
            drainWalQueue();

            assertQuery("test").noLeakCheck().expectSize().timestamp("ts").returns(allRows);
        });
    }

    @Test
    public void testDedupKeyColumnTopDecimal256() throws Exception {
        assertMemoryLeak(() -> testDedupKeyColumnTop("decimal(60, 0)", "1::decimal(60, 0)", false));
    }

    @Test
    public void testDedupKeyColumnTopDecimal256MultipleKeys() throws Exception {
        assertMemoryLeak(() -> testDedupKeyColumnTop("decimal(60, 0)", "1::decimal(60, 0)", true));
    }

    @Test
    public void testDedupKeyColumnTopLong256() throws Exception {
        assertMemoryLeak(() -> testDedupKeyColumnTop("long256", "0x01", false));
    }

    @Test
    public void testDedupKeyColumnTopLong256MultipleKeys() throws Exception {
        assertMemoryLeak(() -> testDedupKeyColumnTop("long256", "0x01", true));
    }

    @Test
    public void testDedupKeyColumnTopUuid() throws Exception {
        assertMemoryLeak(() -> testDedupKeyColumnTop("uuid", "'00000000-0000-0001-0000-000000000001'", false));
    }

    @Test
    public void testDedupKeyColumnTopUuidMultipleKeys() throws Exception {
        assertMemoryLeak(() -> testDedupKeyColumnTop("uuid", "'00000000-0000-0001-0000-000000000001'", true));
    }

    @Test
    public void testDedupNoPartitionRewriteArray() throws Exception {
        assertMemoryLeak(() -> testSameAndShuffledInserts("double[]", "null", "ARRAY[1.0,1.0]", "ARRAY[2.0,2.0]", "ARRAY[3.0,3.0]"));
    }

    @Test
    public void testDedupNoPartitionRewriteByte() throws Exception {
        assertMemoryLeak(() -> testSameAndShuffledInserts("byte", "0", "123", "127", "22"));
    }

    @Test
    public void testDedupNoPartitionRewriteInt() throws Exception {
        assertMemoryLeak(() -> testSameAndShuffledInserts("int", "null", "123", "2345567", "22"));
    }

    @Test
    public void testDedupNoPartitionRewriteLong() throws Exception {
        assertMemoryLeak(() -> testSameAndShuffledInserts("long", "null", "123", "2345567", "22"));
    }

    @Test
    public void testDedupNoPartitionRewriteLong256() throws Exception {
        assertMemoryLeak(() -> testSameAndShuffledInserts(
                        "Long256",
                        "",
                        "0x01",
                        "0x02",
                        "0x03"
                )
        );
    }

    @Test
    public void testDedupNoPartitionRewriteShort() throws Exception {
        assertMemoryLeak(() -> testSameAndShuffledInserts("short", "0", "123", "2342", "22"));
    }

    @Test
    public void testDedupNoPartitionRewriteString() throws Exception {
        assertMemoryLeak(() -> testSameAndShuffledInserts("string", "", "'123'", "'2345567'", "'22'"));
    }

    @Test
    public void testDedupNoPartitionRewriteUUID() throws Exception {
        assertMemoryLeak(() -> testSameAndShuffledInserts(
                        "UUID",
                        "",
                        "'00000000-0000-0006-0000-000000000006'",
                        "'00000000-0000-0005-0000-000000000005'",
                        "'00000000-0000-0007-0000-000000000007'"
                )
        );
    }

    @Test
    public void testDedupNoPartitionRewriteVarchar() throws Exception {
        assertMemoryLeak(() -> testSameAndShuffledInserts("varchar", "", "'123'", "'2345567'", "'22'"));
    }

    @Test
    public void testDedupSkipNoPartitionRewriteArray() throws Exception {
        assertMemoryLeak(() -> testSkippedInsertsKeepPartition("double[]", "null", "ARRAY[1.0,1.0]", "ARRAY[2.0,2.0]", "ARRAY[3.0,3.0]"));
    }

    @Test
    public void testDedupSkipNoPartitionRewriteInt() throws Exception {
        assertMemoryLeak(() -> testSkippedInsertsKeepPartition("int", "null", "123", "2345567", "22"));
    }

    @Test
    public void testDedupSkipNoPartitionRewriteString() throws Exception {
        assertMemoryLeak(() -> testSkippedInsertsKeepPartition("string", "", "'123'", "'2345567'", "'22'"));
    }

    @Test
    public void testDedupSkipNoPartitionRewriteSymbol() throws Exception {
        assertMemoryLeak(() -> testSkippedInsertsKeepPartition("symbol", "", "'123'", "'2345567'", "'22'"));
    }

    @Test
    public void testDedupSkipNoPartitionRewriteVarchar() throws Exception {
        assertMemoryLeak(() -> testSkippedInsertsKeepPartition("varchar", "", "'123'", "'2345567'", "'a varchar value that is not inlined'"));
    }

    private static void appendRow(WalWriter walWriter, String ts, int x, long v) {
        final TableWriter.Row row = walWriter.newRow(MicrosTimestampDriver.floor(ts));
        row.putInt(1, x);
        row.putLong(2, v);
        row.append();
    }

    // The dedup key is added after the partition data, the existing rows read the key's null value
    // from the dedup comparer, which must not assume any alignment of that value.
    private void testDedupKeyColumnTop(String keyType, String keyValue, boolean isMultipleKeys) throws Exception {
        execute("create table test (ts timestamp, v long, i int) timestamp(ts) partition by DAY WAL");
        execute("insert into test values ('2022-02-24T00:00:00', 1, 1), ('2022-02-24T00:00:01', 2, 1), ('2022-02-24T00:00:02', 3, 1)");
        drainWalQueue();

        execute("alter table test add column k " + keyType);
        execute("alter table test dedup enable upsert keys(ts, k" + (isMultipleKeys ? ", i)" : ")"));
        execute("insert into test(ts, v, i, k) values " +
                "('2022-02-24T00:00:01', 10, 1, null), " +
                "('2022-02-24T00:00:01', 11, 1, " + keyValue + "), " +
                "('2022-02-24T00:00:00.5', 12, 1, null)"
        );
        drainWalQueue();
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("test")));

        assertQuery("select ts, v from test")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns("""
                        ts\tv
                        2022-02-24T00:00:00.000000Z\t1
                        2022-02-24T00:00:00.500000Z\t12
                        2022-02-24T00:00:01.000000Z\t10
                        2022-02-24T00:00:01.000000Z\t11
                        2022-02-24T00:00:02.000000Z\t3
                        """);
    }

    private void testSameAndShuffledInserts(String columnType, String nullValue, String value1, String value2, String nullValueUpdated) throws Exception {
        execute("create table test (ts timestamp, x int, v " + columnType + ") timestamp(ts) partition by DAY WAL dedup upsert keys (ts, x) ");
        execute("insert into test(ts,x,v) values ('2022-02-24', 1, " + value1 + "), ('2022-02-24', 2, null), ('2022-02-24', 3, " + value2 + ")");
        drainWalQueue();

        String value1Unquoted = unquote(value1);
        String value2Unquoted = unquote(value2);
        assertQuery("test")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns("ts\tx\tv\n" +
                        "2022-02-24T00:00:00.000000Z\t1\t" + value1Unquoted + "\n" +
                        "2022-02-24T00:00:00.000000Z\t2\t" + nullValue + "\n" +
                        "2022-02-24T00:00:00.000000Z\t3\t" + value2Unquoted + "\n");

        TableToken tt = engine.verifyTableName("test");
        String partitionsTxnFile = readTxnToString(tt, false, true, true, true);

        // Insert same values
        execute("insert into test(ts,x,v) values ('2022-02-24', 1, " + value1 + "), ('2022-02-24', 2, null), ('2022-02-24', 3, " + value2 + ")");
        drainWalQueue();

        assertQuery("test")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns("ts\tx\tv\n" +
                        "2022-02-24T00:00:00.000000Z\t1\t" + value1Unquoted + "\n" +
                        "2022-02-24T00:00:00.000000Z\t2\t" + nullValue + "\n" +
                        "2022-02-24T00:00:00.000000Z\t3\t" + value2Unquoted + "\n");
        Assert.assertEquals(partitionsTxnFile, readTxnToString(tt, false, true, true, true));

        // Insert same values reordered
        execute("insert into test(ts,x,v) values ('2022-02-24', 3, " + value2 + "), ('2022-02-24', 2, null)");
        drainWalQueue();

        assertQuery("test")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns("ts\tx\tv\n" +
                        "2022-02-24T00:00:00.000000Z\t1\t" + value1Unquoted + "\n" +
                        "2022-02-24T00:00:00.000000Z\t2\t" + nullValue + "\n" +
                        "2022-02-24T00:00:00.000000Z\t3\t" + value2Unquoted + "\n");
        Assert.assertEquals(partitionsTxnFile, readTxnToString(tt, false, true, true, true));

        // Change one varchar
        execute("insert into test(ts,x,v) values ('2022-02-24', 3, " + value2 + "), ('2022-02-24', 2, " + nullValueUpdated + ")");
        drainWalQueue();

        assertQuery("test")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns("ts\tx\tv\n" +
                        "2022-02-24T00:00:00.000000Z\t1\t" + value1Unquoted + "\n" +
                        "2022-02-24T00:00:00.000000Z\t2\t" + unquote(nullValueUpdated) + "\n" +
                        "2022-02-24T00:00:00.000000Z\t3\t" + value2Unquoted + "\n");
    }

    // In SKIP mode a commit made of duplicates only leaves the partition as is, even when the
    // non-key values of the duplicates differ from the existing rows.
    private void testSkippedInsertsKeepPartition(String columnType, String nullValue, String value1, String value2, String value3) throws Exception {
        execute("create table test (ts timestamp, x int, v " + columnType + ") timestamp(ts) partition by DAY WAL dedup skip keys (ts, x) ");
        execute("insert into test(ts,x,v) values ('2022-02-24', 1, " + value1 + "), ('2022-02-24', 2, null), ('2022-02-24', 3, " + value2 + ")");
        drainWalQueue();

        final String expected = "ts\tx\tv\n" +
                "2022-02-24T00:00:00.000000Z\t1\t" + unquote(value1) + "\n" +
                "2022-02-24T00:00:00.000000Z\t2\t" + nullValue + "\n" +
                "2022-02-24T00:00:00.000000Z\t3\t" + unquote(value2) + "\n";
        assertQuery("test")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns(expected);

        TableToken tt = engine.verifyTableName("test");
        String partitionsTxnFile = readTxnToString(tt, false, true, true, true);

        // Insert the same keys with the same and with different values, reordered
        execute("insert into test(ts,x,v) values ('2022-02-24', 3, " + value3 + "), ('2022-02-24', 2, " + value3 + "), ('2022-02-24', 1, " + value1 + ")");
        drainWalQueue();

        assertQuery("test")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns(expected);
        Assert.assertEquals(partitionsTxnFile, readTxnToString(tt, false, true, true, true));

        // A new key with a duplicate in the same commit, the first of them is kept
        execute("insert into test(ts,x,v) values ('2022-02-24', 4, " + value3 + "), ('2022-02-24', 2, " + value1 + "), ('2022-02-24', 4, " + value1 + ")");
        drainWalQueue();

        assertQuery("test")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns(expected + "2022-02-24T00:00:00.000000Z\t4\t" + unquote(value3) + "\n");
    }

    private String unquote(String v) {
        if (v.startsWith("'") && v.endsWith("'")) {
            return v.substring(1, v.length() - 1);
        }
        if (v.startsWith("ARRAY")) {
            return v.substring(5); // remove ARRAY
        }
        return v;
    }
}
