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

package io.questdb.test.griffin;

import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class ColumnVersionTest extends AbstractCairoTest {
    @Test
    public void testMultipleColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setRandom(new Rnd(9005735847243117419L, 3979535605596560453L));

            execute(
                    "create table t_col_top_ooo_day as (" +
                            "select " +
                            " x" +
                            ", rnd_symbol('a', 'b', 'c', null) m" +
                            ", timestamp_sequence('1970-01-01T01', " + Micros.HOUR_MICROS + "L) ts" +
                            " from long_sequence(96)," +
                            "), index(m) timestamp(ts) partition by DAY"
            );
            execute("alter table t_col_top_ooo_day add column день symbol");// .execute(null).await();
            execute("alter table t_col_top_ooo_day add column str string");//.execute(null).await();
            execute(
                    "insert into t_col_top_ooo_day " +
                            "select " +
                            " x" +
                            ", rnd_symbol('a', 'b', 'c', null) m" +
                            ", timestamp_sequence('1970-01-05T02:30', " + Micros.HOUR_MICROS + "L) ts" +
                            ", rnd_symbol('a', 'b', 'c', null)" +
                            ", rnd_str()" +
                            " from long_sequence(10),"
            );
            execute(
                    "insert into t_col_top_ooo_day " +
                            "select " +
                            " x" +
                            ", rnd_symbol('a', 'b', 'c', null) m" +
                            ", timestamp_sequence('1970-01-01T01:30', " + Micros.HOUR_MICROS + "L) ts" +
                            ", rnd_symbol('a', 'b', 'c', null)" +
                            ", rnd_str()" +
                            " from long_sequence(36)"
            );

            sqlExecutionContext.setRandom(new Rnd(3784807164251091079L, 1558467903141138059L));
            execute(
                    "insert into t_col_top_ooo_day " +
                            "select " +
                            " x" +
                            ", rnd_symbol('a', 'b', 'c', null) m" +
                            ", timestamp_sequence('1970-01-05T04:25', " + Micros.HOUR_MICROS + "L) ts" +
                            ", rnd_symbol('a', 'b', 'c', null)" +
                            ", rnd_str()" +
                            " from long_sequence(10),"
            );
            assertSql("x\tm\tts\tдень\tstr\n" +
                    "6\tc\t1970-01-01T06:30:00.000000Z\ta\tSFCI\n" +
                    "12\ta\t1970-01-01T12:30:00.000000Z\ta\tJNOXB\n" +
                    "14\t\t1970-01-01T14:30:00.000000Z\ta\tLJYFXSBNVN\n" +
                    "16\tb\t1970-01-01T16:30:00.000000Z\ta\tTPUL\n" +
                    "21\tb\t1970-01-01T21:30:00.000000Z\ta\tGQWSZMUMXM\n" +
                    "24\t\t1970-01-02T00:30:00.000000Z\ta\tNTPYXUB\n" +
                    "33\tb\t1970-01-02T09:30:00.000000Z\ta\tFLNGCEFBTD\n" +
                    "34\tb\t1970-01-02T10:30:00.000000Z\ta\tTIGUTKI\n" +
                    "1\t\t1970-01-05T02:30:00.000000Z\ta\tHQJHN\n" +
                    "4\tc\t1970-01-05T05:30:00.000000Z\ta\tXRGUOXFH\n" +
                    "5\t\t1970-01-05T06:30:00.000000Z\ta\tFVFFOB\n" +
                    "7\tb\t1970-01-05T10:25:00.000000Z\ta\tHFLPBNH\n" +
                    "9\tc\t1970-01-05T10:30:00.000000Z\ta\tLEQD\n" +
                    "8\t\t1970-01-05T11:25:00.000000Z\ta\tCCNGTNLE\n" +
                    "10\t\t1970-01-05T11:30:00.000000Z\ta\tKNHV\n" +
                    "9\ta\t1970-01-05T12:25:00.000000Z\ta\tHIUG\n", "t_col_top_ooo_day where день = 'a'"
            );

            execute(
                    "insert into t_col_top_ooo_day " +
                            "select " +
                            " x" +
                            ", rnd_symbol('a', 'b', 'c', null) m" +
                            ", timestamp_sequence('1970-01-01T01:27', " + Micros.HOUR_MICROS + "L) ts" +
                            ", rnd_symbol('a', 'b', 'c', null)" +
                            ", rnd_str()" +
                            " from long_sequence(36)"
            );

            assertSql("x\tm\tts\tдень\tstr\n" +
                    "1\t\t1970-01-01T01:27:00.000000Z\ta\tTLQZSLQ\n" +
                    "6\tc\t1970-01-01T06:30:00.000000Z\ta\tSFCI\n" +
                    "12\ta\t1970-01-01T12:30:00.000000Z\ta\tJNOXB\n" +
                    "14\t\t1970-01-01T14:30:00.000000Z\ta\tLJYFXSBNVN\n" +
                    "16\tb\t1970-01-01T16:30:00.000000Z\ta\tTPUL\n" +
                    "19\tb\t1970-01-01T19:27:00.000000Z\ta\tTZODWKOCPF\n" +
                    "21\tb\t1970-01-01T21:30:00.000000Z\ta\tGQWSZMUMXM\n" +
                    "24\t\t1970-01-02T00:30:00.000000Z\ta\tNTPYXUB\n" +
                    "31\ta\t1970-01-02T07:27:00.000000Z\ta\tGFI\n" +
                    "32\ta\t1970-01-02T08:27:00.000000Z\ta\tVZWEV\n" +
                    "33\tb\t1970-01-02T09:30:00.000000Z\ta\tFLNGCEFBTD\n" +
                    "34\tb\t1970-01-02T10:30:00.000000Z\ta\tTIGUTKI\n" +
                    "35\t\t1970-01-02T11:27:00.000000Z\ta\tPTYXYGYFUX\n" +
                    "1\t\t1970-01-05T02:30:00.000000Z\ta\tHQJHN\n" +
                    "4\tc\t1970-01-05T05:30:00.000000Z\ta\tXRGUOXFH\n" +
                    "5\t\t1970-01-05T06:30:00.000000Z\ta\tFVFFOB\n" +
                    "7\tb\t1970-01-05T10:25:00.000000Z\ta\tHFLPBNH\n" +
                    "9\tc\t1970-01-05T10:30:00.000000Z\ta\tLEQD\n" +
                    "8\t\t1970-01-05T11:25:00.000000Z\ta\tCCNGTNLE\n" +
                    "10\t\t1970-01-05T11:30:00.000000Z\ta\tKNHV\n" +
                    "9\ta\t1970-01-05T12:25:00.000000Z\ta\tHIUG\n", "t_col_top_ooo_day where день = 'a'"
            );
        });
    }

}
