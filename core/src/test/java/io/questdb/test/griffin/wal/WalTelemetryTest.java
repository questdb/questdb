/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.griffin.wal;

import io.questdb.TelemetryJob;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.tasks.TelemetryTask;
import io.questdb.tasks.TelemetryWalTask;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("SameParameterValue")
public class WalTelemetryTest extends AbstractGriffinTest {
    @BeforeClass
    public static void setUpStatic() throws Exception {
        walTxnNotificationQueueCapacity = 8;
        AbstractGriffinTest.setUpStatic();
    }

    @Test
    public void testTelemetryWal() throws Exception {
        assertMemoryLeak(() -> {
            currentMicros = 1000L;
            TelemetryJob telemetryJob = new TelemetryJob(engine);

            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            currentMicros = 2000L;
            executeInsert("insert into " + tableName +
                    " values (101, 'dfd', '2022-02-24T01', 'asd')");

            currentMicros = 3000L;
            drainWalQueue();

            currentMicros = 4000L;
            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "2\tBC\t2022-02-24T00:00:01.000000Z\tFG\n" +
                    "3\tCD\t2022-02-24T00:00:02.000000Z\tFG\n" +
                    "4\tCD\t2022-02-24T00:00:03.000000Z\tFG\n" +
                    "5\tAB\t2022-02-24T00:00:04.000000Z\tDE\n" +
                    "101\tdfd\t2022-02-24T01:00:00.000000Z\tasd\n");

            telemetryJob.runSerially();
            telemetryJob.close();
            CharSequence sysPrefix = configuration.getSystemTableNamePrefix();
            assertSql(sysPrefix + TelemetryWalTask.TABLE_NAME, "created\tevent\ttableId\twalId\tseqTxn\trowCount\tphysicalRowCount\tlatency\n" +
                    "1970-01-01T00:00:00.004000Z\t103\t4\t1\t1\t-1\t-1\t2.0000\n" +
                    "1970-01-01T00:00:00.004000Z\t105\t4\t1\t1\t5\t0\t0.0000\n" +
                    "1970-01-01T00:00:00.004000Z\t103\t4\t1\t2\t-1\t-1\t1.0000\n" +
                    "1970-01-01T00:00:00.004000Z\t105\t4\t1\t2\t1\t1\t0.0000\n");

            assertSql(TelemetryTask.TABLE_NAME, "created\tevent\torigin\n" +
                    "1970-01-01T00:00:00.001000Z\t100\t1\n" +
                    "1970-01-01T00:00:00.004000Z\t101\t1\n");
        });
    }
}
