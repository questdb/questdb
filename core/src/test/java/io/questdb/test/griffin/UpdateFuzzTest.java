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

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.ColumnType;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class UpdateFuzzTest extends AbstractCairoTest {
    @Test
    public void testConvertColumnAndUpdate() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            boolean partitioned = rnd.nextInt(10) > 2;
            if (partitioned) {
                execute(
                        "create table up as" +
                                " (select timestamp_sequence('2022-02-24T04', 1000000) ts " +
                                " from long_sequence(0))" +
                                " timestamp(ts) partition by DAY" + (rnd.nextBoolean() ? " WAL" : "")
                );
            } else {
                execute(
                        "create table up as" +
                                " (select timestamp_sequence('2022-02-24T04', 1000000) ts " +
                                " from long_sequence(0))"
                );
            }

            if (rnd.nextBoolean()) {
                execute(" insert into up " +
                        " select timestamp_sequence('2022-02-24T10', 1000000) ts " +
                        " from long_sequence(2)");
            }

            if (rnd.nextBoolean() && partitioned) {
                execute(" insert into up " +
                        " select timestamp_sequence('2022-02-24', 1000000) ts " +
                        " from long_sequence(2)");
            }

            if (rnd.nextBoolean()) {
                execute(" insert into up " +
                        " select timestamp_sequence('2022-02-25T09', 1000000) ts " +
                        " from long_sequence(1)");
            }

            if (rnd.nextBoolean() && partitioned) {
                execute(" insert into up " +
                        " select timestamp_sequence('2022-02-25T04', 1000000) ts " +
                        " from long_sequence(1)");
            }

            execute("alter table up add column x string");

            if (rnd.nextBoolean() && partitioned) {
                execute(" insert into up " +
                        " select timestamp_sequence('2022-02-25T03', 1000000) ts, '1' as x" +
                        " from long_sequence(2)");

            }

            if (rnd.nextBoolean()) {
                execute(" insert into up " +
                        " select timestamp_sequence('2022-02-25T10', 1000000) ts, '1' as x" +
                        " from long_sequence(2)");

            }

            if (rnd.nextBoolean()) {
                execute(" insert into up " +
                        " select timestamp_sequence('2022-02-26T10', 1000000) ts, '1' as x" +
                        " from long_sequence(5)");
            }

            short lastType = ColumnType.STRING;

            if (rnd.nextBoolean() && partitioned) {
                execute(" insert into up " +
                        " select timestamp_sequence('2022-02-24', 1000000) ts, '12' " +
                        " from long_sequence(2)");
            }

            if (rnd.nextBoolean()) {
                execute("alter table up alter column x type symbol");
                lastType = ColumnType.SYMBOL;
                drainWalQueue();
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    purgeJob.run(0);
                }
            }

            if (rnd.nextBoolean()) {
                update("UPDATE up SET x = '2' WHERE ts > '2022-02-25'");
                drainWalQueue();
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    purgeJob.run(0);
                }
            }

            if (rnd.nextBoolean()) {
                execute("alter table up alter column x type varchar");
                drainWalQueue();
                lastType = ColumnType.VARCHAR;
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    purgeJob.run(0);
                }
            }

            if (rnd.nextBoolean() && partitioned) {
                execute(" insert into up " +
                        " select timestamp_sequence('2022-02-24T10', 1000000) ts, '1' as x" +
                        " from long_sequence(1)");

            }

            if (rnd.nextBoolean() && lastType != ColumnType.STRING) {
                execute("alter table up alter column x type string");
                drainWalQueue();
                lastType = ColumnType.STRING;
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    purgeJob.run(0);
                }
            }

            if (rnd.nextBoolean() && lastType != ColumnType.SYMBOL) {
                execute("alter table up alter column x type symbol");
                drainWalQueue();
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    purgeJob.run(0);
                }
            }

            // This will read whole table, verifying that the TableReader can handle the column top / versions / types
            printSql("up");
        });
    }

}
