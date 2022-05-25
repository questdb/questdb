/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.Log2TableWriter;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static io.questdb.griffin.AbstractO3Test.sink2;

public class WriteApplyLogTest extends AbstractGriffinTest {
    static {
        LogFactory.configureSync();
    }

    private static final Log LOG = LogFactory.getLog(WriteApplyLogTest.class);

    @Test
    public void testApplyInOrder() throws Exception {
        assertMemoryLeak(() -> {
            long tsIncrement = 5*60*1_000_000L;
            int count1 = 100;
            int count2 = 140;
            String timestart1 = "1970-01-06T18:56";
            compile(
                    "create table wal_all as (" +
                            "select" +
                            " indexed_timestamp_sequence(0L, '" + timestart1 + "', " + tsIncrement + "L) ts," +
                            " timestamp_sequence('" + timestart1 + "', " + tsIncrement + "L) ts1," +
                            " cast(x as int) i," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
//                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " rnd_byte(2,50) l," +
                            " rnd_long256() l256" +
                            " from long_sequence(" + count1 + ")" +
                            ")",
                    sqlExecutionContext
            );

            String timestart2 = "1970-01-07T00:32";
            compile(
                    "insert into wal_all\n" +
                            "select" +
                            " indexed_timestamp_sequence("+ count1 +"L, '" + timestart2 + "', " + tsIncrement + "L) ts," +
                            " timestamp_sequence('" + timestart2 + "', " + tsIncrement + "L) ts1," +
                            " cast(x as int) i," +
                            " to_timestamp('2018-02', 'yyyy-MM') + x * 720000000 timestamp," +
                            " rnd_boolean() b," +
                            " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
//                            " rnd_symbol(4,4,4,2) ik," +
                            " rnd_long() j," +
                            " rnd_byte(2,50) l," +
                            " rnd_long256() l256" +
                            " from long_sequence(" + count2 + ")",
                    sqlExecutionContext
            );

            compile("create table wal_clean as (select * from wal_all)");
            compile("alter table wal_clean drop column ts");
            compile("alter table wal_clean rename column ts1 to ts");
            compile("create table x as (select * from wal_clean where 1 != 1) timestamp(ts) partition by DAY");

            try (
                    TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "test");
                    Path walPath = new Path()
            ) {
                walPath.of(configuration.getRoot()).concat("wal_all").concat("default");
                long timestampLo = IntervalUtils.parseFloorPartialDate(timestart1);
                long timestampHi = timestampLo + count1 * tsIncrement;

                Log2TableWriter log2TableWriter = new Log2TableWriter();

                LOG.info().$("=== Applying WAL transaction ===").$();
                log2TableWriter.applyWriteAheadLogData(writer, walPath, 0, count1, true, timestampLo, timestampHi);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, "wal_clean limit " + count1, "x", LOG);

                // Apply second WAL segment
                LOG.info().$("=== Applying WAL transaction 2 ===").$();
                long timestampLo2 = IntervalUtils.parseFloorPartialDate(timestart2);
                long timestampHi2 = timestampLo2 + count2 * tsIncrement;

                log2TableWriter.applyWriteAheadLogData(writer, walPath, count1, count1 + count2, true, timestampLo2, timestampHi2);

                sink.clear();
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from wal_clean order by ts", sink);

                sink2.clear();
                TestUtils.printSql(compiler, sqlExecutionContext, "x", sink2);

                TestUtils.assertEquals(sink, sink2);
            }
        });
    }
}
