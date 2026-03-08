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

public class FloatGroupByFunctionsTest extends AbstractCairoTest {

    @Test
    public void testRndFloatsWithAggregates() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setRandom(new Rnd());
            execute("create table tab as ( select rnd_float() ch from long_sequence(100) )");

            assertSql(
                    """
                            min\tmax\tfirst\tlast\tcount
                            0.0011075139\t0.9856291\t0.66077775\t0.7997733\t100
                            """,
                    "select min(ch), max(ch), first(ch), last(ch), count() from tab"
            );
        });
    }

    @Test
    public void testSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setRandom(new Rnd());
            TableModel tm = new TableModel(configuration, "tab", PartitionBy.DAY);
            tm.timestamp("ts").col("ch", ColumnType.FLOAT);
            createPopulateTable(tm, 100, "2020-01-01", 2);

            assertSql(
                    """
                            ts\tmin\tmax\tfirst\tlast\tcount
                            2020-01-01T00:28:47.990000Z\t0.001\t0.051\t0.001\t0.051\t51
                            2020-01-02T00:28:47.990000Z\t0.052\t0.1\t0.052\t0.1\t49
                            """,
                    "select ts, min(ch), max(ch), first(ch), last(ch), count() from tab sample by d align to first observation"
            );

            assertSql(
                    """
                            ts\tmin\tmax\tfirst\tlast\tcount
                            2020-01-01T00:00:00.000000Z\t0.001\t0.05\t0.001\t0.05\t50
                            2020-01-02T00:00:00.000000Z\t0.051\t0.1\t0.051\t0.1\t50
                            """,
                    "select ts, min(ch), max(ch), first(ch), last(ch), count() from tab sample by d align to calendar"
            );
        });
    }
}
