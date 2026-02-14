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

import io.questdb.TelemetryJob;
import io.questdb.tasks.TelemetryTask;
import io.questdb.tasks.TelemetryWalTask;
import io.questdb.test.AbstractCairoTest;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.questdb.PropertyKey.CAIRO_WAL_TXN_NOTIFICATION_QUEUE_CAPACITY;

@SuppressWarnings("SameParameterValue")
public class WalTelemetryTest extends AbstractCairoTest {
    @BeforeClass
    public static void setUpStatic() throws Exception {
        setProperty(CAIRO_WAL_TXN_NOTIFICATION_QUEUE_CAPACITY, 8);
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testTelemetryWal() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(1000);
            try (TelemetryJob telemetryJob = new TelemetryJob(engine)) {
                String tableName = testName.getMethodName();
                execute("create table " + tableName + " as (" +
                        "select x, " +
                        " rnd_symbol('AB', 'BC', 'CD') sym, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                        " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                        " from long_sequence(5)" +
                        ") timestamp(ts) partition by DAY WAL");

                setCurrentMicros(2000);
                execute("insert into " + tableName +
                        " values (101, 'dfd', '2022-02-24T01', 'asd')");

                var tt = engine.verifyTableName(tableName);
                var control = engine.getTableSequencerAPI().getTxnTracker(tt).getMemPressureControl();
                control.setMaxBlockRowCount(1);

                setCurrentMicros(3000);
                drainWalQueue();

                setCurrentMicros(4000);
                assertSql("""
                        x\tsym\tts\tsym2
                        1\tAB\t2022-02-24T00:00:00.000000Z\tEF
                        2\tBC\t2022-02-24T00:00:01.000000Z\tFG
                        3\tCD\t2022-02-24T00:00:02.000000Z\tFG
                        4\tCD\t2022-02-24T00:00:03.000000Z\tFG
                        5\tAB\t2022-02-24T00:00:04.000000Z\tDE
                        101\tdfd\t2022-02-24T01:00:00.000000Z\tasd
                        """, tableName);

                telemetryJob.runSerially();
            }

            CharSequence sysPrefix = configuration.getSystemTableNamePrefix();
            assertSql("""
                    created\tevent\ttableId\twalId\tseqTxn\trowCount\tphysicalRowCount\tlatency\tminTimestamp\tmaxTimestamp
                    1970-01-01T00:00:00.004000Z\t103\t5\t1\t1\t-1\t-1\t2.0\t2022-02-24T00:00:00.000000Z\t2022-02-24T00:00:04.000000Z
                    1970-01-01T00:00:00.004000Z\t105\t5\t1\t1\t5\t5\t0.0\t2022-02-24T00:00:00.000000Z\t2022-02-24T00:00:04.000000Z
                    1970-01-01T00:00:00.004000Z\t103\t5\t1\t2\t-1\t-1\t1.0\t2022-02-24T01:00:00.000000Z\t2022-02-24T01:00:00.000000Z
                    1970-01-01T00:00:00.004000Z\t105\t5\t1\t2\t1\t1\t0.0\t2022-02-24T01:00:00.000000Z\t2022-02-24T01:00:00.000000Z
                    """, sysPrefix + TelemetryWalTask.TABLE_NAME);

            assertSql("""
                    created\tevent\torigin
                    1970-01-01T00:00:00.001000Z\t100\t1
                    1970-01-01T00:00:00.004000Z\t101\t1
                    """, TelemetryTask.TABLE_NAME + " where event >= 0");
        });
    }
}
