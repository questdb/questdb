/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.PropertyKey;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class WalTransactionsFunctionTest extends AbstractCairoTest {

    @Test
    public void testNonWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, x int, y int) timestamp(ts) partition by DAY BYPASS WAL");
            execute("insert into x values ('2020-01-01T00:00:00.000000Z', 1, 2)");
            execute("insert into x values ('2020-01-01T00:00:00.000000Z', 2, 3)");
            execute("alter table x add column z int");

            try (RecordCursorFactory ignore = select("select * from wal_transactions('x')")) {
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "table is not a WAL table: x");
                Assert.assertEquals("select * from wal_transactions(".length(), e.getPosition());
            }
        });
    }

    @Test
    public void testTableDoesNotExist() throws Exception {
        assertMemoryLeak(() -> {
            try (RecordCursorFactory ignore = select("select * from wal_transactions('x')")) {
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "table does not exist: x");
                Assert.assertEquals("select * from wal_transactions(".length(), e.getPosition());
            }
        });
    }

    @Test
    public void testWalTransactionIdempotency() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE 'trades' ( \n" +
                    "\tsymbol SYMBOL CAPACITY 256 CACHE,\n" +
                    "\tside SYMBOL CAPACITY 256 CACHE,\n" +
                    "\tprice DOUBLE,\n" +
                    "\tamount DOUBLE,\n" +
                    "\ttimestamp TIMESTAMP\n" +
                    ") timestamp(timestamp) PARTITION BY DAY WAL\n");

            assertQueryNoLeakCheck(
                    "column\n" +
                            "null\n",
                    "with segments as (\n" +
                            "\tselect walid, segmentId from wal_transactions('trades')\n" +
                            "\twhere sequencerTxn = 10\n" +
                            ")\n" +
                            "select max(wt.sequencerTxn) + 1 from wal_transactions('trades') wt\n" +
                            "join segments s on s.segmentId = wt.segmentId and s.walId = wt.walId\n" +
                            "where sequencerTxn > 10;\n",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testWalTransactions() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosTimestampDriver.floor("2023-11-22T19:00:53.950468Z"));
            execute("create table x (ts timestamp, x int, y int) timestamp(ts) partition by DAY WAL");
            execute("insert into x values ('2020-01-01T00:00:00.000000Z', 1, 2)");
            execute("insert into x values ('2020-01-01T00:00:00.000000Z', 2, 3)");
            execute("alter table x add column z int");

            drainWalQueue();

            assertSql(
                    "sequencerTxn\ttimestamp\twalId\tsegmentId\tsegmentTxn\tstructureVersion\tminTimestamp\tmaxTimestamp\trowCount\talterCommandType\n" +
                            "1\t2023-11-22T19:00:53.950468Z\t1\t0\t0\t0\t\t\tnull\t0\n" +
                            "2\t2023-11-22T19:00:53.950468Z\t1\t0\t1\t0\t\t\tnull\t0\n" +
                            "3\t2023-11-22T19:00:53.950468Z\t-1\t-1\t-1\t1\t\t\tnull\t0\n",
                    "select * from wal_transactions('x')"
            );
        });
    }

    @Test
    public void testWalTransactionsLastLine() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosTimestampDriver.floor("2023-11-22T19:00:53.950468Z"));
            execute("create table x (ts timestamp, x int, y int) timestamp(ts) partition by DAY WAL");
            execute("insert into x values ('2020-01-01T00:00:00.000000Z', 1, 2)");
            execute("insert into x values ('2020-01-01T00:00:00.000000Z', 2, 3)");
            execute("alter table x add column z int");

            drainWalQueue();

            assertSql(
                    "sequencerTxn\ttimestamp\twalId\tsegmentId\tsegmentTxn\tstructureVersion\tminTimestamp\tmaxTimestamp\trowCount\talterCommandType\n" +
                            "3\t2023-11-22T19:00:53.950468Z\t-1\t-1\t-1\t1\t\t\tnull\t0\n",
                    "select * from wal_transactions('x') limit -1"
            );
        });
    }

    @Test
    public void testWalTransactionsV2() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosTimestampDriver.floor("2023-11-22T19:00:53.950468Z"));
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
            execute("create table x (ts timestamp, x int, y int) timestamp(ts) partition by DAY WAL");
            execute("insert into x values ('2020-01-01T00:00:00.000000Z', 1, 2)");
            execute("insert into x values ('2020-02-01T00:00:00.000000Z', 2, 3)");
            execute("alter table x add column z int");
            execute("alter table x drop column z");

            drainWalQueue();

            assertSql(
                    "sequencerTxn\ttimestamp\twalId\tsegmentId\tsegmentTxn\tstructureVersion\tminTimestamp\tmaxTimestamp\trowCount\talterCommandType\n" +
                            "1\t2023-11-22T19:00:53.950468Z\t1\t0\t0\t0\t2020-01-01T00:00:00.000000Z\t2020-01-01T00:00:00.000000Z\t1\t0\n" +
                            "2\t2023-11-22T19:00:53.950468Z\t1\t0\t1\t0\t2020-02-01T00:00:00.000000Z\t2020-02-01T00:00:00.000000Z\t1\t0\n" +
                            "3\t2023-11-22T19:00:53.950468Z\t-1\t-1\t-1\t1\t\t\tnull\t1\n" +
                            "4\t2023-11-22T19:00:53.950468Z\t-1\t-1\t-1\t2\t\t\tnull\t8\n",
                    "select * from wal_transactions('x')"
            );
        });
    }
}
