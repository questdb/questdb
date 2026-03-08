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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.TableToken;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class DedupWalWriterTest extends AbstractCairoTest {
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

    private void testSameAndShuffledInserts(String columnType, String nullValue, String value1, String value2, String nullValueUpdated) throws SqlException {
        execute("create table test (ts timestamp, x int, v " + columnType + ") timestamp(ts) partition by DAY WAL dedup upsert keys (ts, x) ");
        execute("insert into test(ts,x,v) values ('2022-02-24', 1, " + value1 + "), ('2022-02-24', 2, null), ('2022-02-24', 3, " + value2 + ")");
        drainWalQueue();

        String value1Unquoted = unquote(value1);
        String value2Unquoted = unquote(value2);
        assertSql(
                "ts\tx\tv\n" +
                        "2022-02-24T00:00:00.000000Z\t1\t" + value1Unquoted + "\n" +
                        "2022-02-24T00:00:00.000000Z\t2\t" + nullValue + "\n" +
                        "2022-02-24T00:00:00.000000Z\t3\t" + value2Unquoted + "\n",
                "test"
        );

        TableToken tt = engine.verifyTableName("test");
        String partitionsTxnFile = readTxnToString(tt, false, true, true, true);

        // Insert same values
        execute("insert into test(ts,x,v) values ('2022-02-24', 1, " + value1 + "), ('2022-02-24', 2, null), ('2022-02-24', 3, " + value2 + ")");
        drainWalQueue();

        assertSql(
                "ts\tx\tv\n" +
                        "2022-02-24T00:00:00.000000Z\t1\t" + value1Unquoted + "\n" +
                        "2022-02-24T00:00:00.000000Z\t2\t" + nullValue + "\n" +
                        "2022-02-24T00:00:00.000000Z\t3\t" + value2Unquoted + "\n",
                "test"
        );
        Assert.assertEquals(partitionsTxnFile, readTxnToString(tt, false, true, true, true));

        // Insert same values reordered
        execute("insert into test(ts,x,v) values ('2022-02-24', 3, " + value2 + "), ('2022-02-24', 2, null)");
        drainWalQueue();

        assertSql(
                "ts\tx\tv\n" +
                        "2022-02-24T00:00:00.000000Z\t1\t" + value1Unquoted + "\n" +
                        "2022-02-24T00:00:00.000000Z\t2\t" + nullValue + "\n" +
                        "2022-02-24T00:00:00.000000Z\t3\t" + value2Unquoted + "\n",
                "test"
        );
        Assert.assertEquals(partitionsTxnFile, readTxnToString(tt, false, true, true, true));

        // Change one varchar
        execute("insert into test(ts,x,v) values ('2022-02-24', 3, " + value2 + "), ('2022-02-24', 2, " + nullValueUpdated + ")");
        drainWalQueue();

        assertSql(
                "ts\tx\tv\n" +
                        "2022-02-24T00:00:00.000000Z\t1\t" + value1Unquoted + "\n" +
                        "2022-02-24T00:00:00.000000Z\t2\t" + unquote(nullValueUpdated) + "\n" +
                        "2022-02-24T00:00:00.000000Z\t3\t" + value2Unquoted + "\n",
                "test"
        );
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
