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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import org.junit.Test;

public class CharGroupByFunctionTest extends AbstractCairoTest {

    @Test
    public void testNonNull() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setRandom(new Rnd());
            execute("create table tab as ( select rnd_char() ch from long_sequence(100) )");

            assertSql("""
                    min
                    B
                    """, "select min(ch) from tab"
            );

            assertSql("""
                    max
                    Z
                    """, "select max(ch) from tab"
            );

            assertSql("""
                    first
                    V
                    """, "select first(ch) from tab"
            );

            assertSql("""
                    last
                    J
                    """, "select last(ch) from tab"
            );
        });
    }

    @Test
    public void testSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            TableModel tm = new TableModel(configuration, "tab", PartitionBy.DAY);
            tm.timestamp("ts").col("ch", ColumnType.CHAR);
            createPopulateTable(tm, 100, "2020-01-01", 2);

            String expected = """
                    ts\tmin\tmax\tfirst\tlast\tcount
                    2020-01-01T00:28:47.990000Z\t\u0001\t3\t\u0001\t3\t51
                    2020-01-02T00:28:47.990000Z\t4\td\t4\td\t49
                    """;
            assertSql(expected, "select ts, min(ch), max(ch), first(ch), last(ch), count() from tab sample by d align to first observation");
            assertSql("""
                    ts\tmin\tmax\tfirst\tlast\tcount
                    2020-01-01T00:00:00.000000Z\t\u0001\t2\t\u0001\t2\t50
                    2020-01-02T00:00:00.000000Z\t3\td\t3\td\t50
                    """, "select ts, min(ch), max(ch), first(ch), last(ch), count() from tab sample by d");
            assertSql("""
                    ts\tmin\tmax\tfirst\tlast\tcount
                    2020-01-01T00:00:00.000000Z\t\u0001\t2\t\u0001\t2\t50
                    2020-01-02T00:00:00.000000Z\t3\td\t3\td\t50
                    """, "select ts, min(ch), max(ch), first(ch), last(ch), count() from tab sample by d align to calendar"
            );
        });
    }
}
