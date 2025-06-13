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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.TableToken;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class DedupWalWriterTest extends AbstractCairoTest {
    @Test
    public void testDedupNoPartitionRewrite() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ts timestamp, x int, v varchar) timestamp(ts) partition by DAY WAL dedup upsert keys (ts, x) ");
            execute("insert into test(ts,x,v) values ('2022-02-24', 1, 'abcd123456'), ('2022-02-24', 2, 'b'), ('2022-02-24', 3, 'bcde2345567')");
            drainWalQueue();

            assertSql(
                    "ts\tx\tv\n" +
                            "2022-02-24T00:00:00.000000Z\t1\tabcd123456\n" +
                            "2022-02-24T00:00:00.000000Z\t2\tb\n" +
                            "2022-02-24T00:00:00.000000Z\t3\tbcde2345567\n",
                    "test"
            );

            TableToken tt = engine.verifyTableName("test");
            String partitionsTxnFile = readTxnToString(tt, false, true);

            // Insert same values
            execute("insert into test(ts,x,v) values ('2022-02-24', 1, 'abcd123456'), ('2022-02-24', 2, 'b'), ('2022-02-24', 3, 'bcde2345567')");
            drainWalQueue();

            assertSql(
                    "ts\tx\tv\n" +
                            "2022-02-24T00:00:00.000000Z\t1\tabcd123456\n" +
                            "2022-02-24T00:00:00.000000Z\t2\tb\n" +
                            "2022-02-24T00:00:00.000000Z\t3\tbcde2345567\n",
                    "test"
            );
            Assert.assertEquals(partitionsTxnFile, readTxnToString(tt, false, true));

            // Insert same values reodered
            execute("insert into test(ts,x,v) values ('2022-02-24', 3, 'bcde2345567'), ('2022-02-24', 2, 'b')");
            drainWalQueue();

            assertSql(
                    "ts\tx\tv\n" +
                            "2022-02-24T00:00:00.000000Z\t1\tabcd123456\n" +
                            "2022-02-24T00:00:00.000000Z\t2\tb\n" +
                            "2022-02-24T00:00:00.000000Z\t3\tbcde2345567\n",
                    "test"
            );
            Assert.assertEquals(partitionsTxnFile, readTxnToString(tt, false, true));

            // Change one varchar
            execute("insert into test(ts,x,v) values ('2022-02-24', 3, 'bcde2345567'), ('2022-02-24', 2, 'bbbbbbbb')");
            drainWalQueue();

            assertSql(
                    "ts\tx\tv\n" +
                            "2022-02-24T00:00:00.000000Z\t1\tabcd123456\n" +
                            "2022-02-24T00:00:00.000000Z\t2\tbbbbbbbb\n" +
                            "2022-02-24T00:00:00.000000Z\t3\tbcde2345567\n",
                    "test"
            );
            Assert.assertEquals(partitionsTxnFile, readTxnToString(tt, false, true));
        });
    }
}
