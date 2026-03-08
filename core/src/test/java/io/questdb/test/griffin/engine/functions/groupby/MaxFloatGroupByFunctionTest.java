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
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import org.junit.Test;

public class MaxFloatGroupByFunctionTest extends AbstractCairoTest {

    @Test
    public void testSampleByWithFill() throws Exception {
        assertMemoryLeak(() -> {
            TableModel tm = new TableModel(configuration, "tab", PartitionBy.DAY);
            tm.timestamp("ts").col("ch", ColumnType.FLOAT);
            createPopulateTable(tm, 100, "2020-01-01", 2);

            assertSql(
                    """
                            ts\tmin\tmax\tfirst\tlast\tcount
                            2020-01-01T00:28:47.990000Z\t0.001\t0.001\t0.001\t0.001\t1
                            2020-01-01T00:29:47.990000Z\t0.0010357143\t0.0010357143\t0.0010357143\t0.0010357143\t1
                            """,
                    "select ts, min(ch), max(ch), first(ch), last(ch), count() from tab sample by 1m FILL(LINEAR) align to first observation LIMIT 2"
            );

            assertSql(
                    """
                            ts\tmin\tmax\tfirst\tlast\tcount
                            2020-01-01T00:28:00.000000Z\t0.001\t0.001\t0.001\t0.001\t1
                            2020-01-01T00:29:00.000000Z\t0.0010344828\t0.0010344828\t0.0010344828\t0.0010344828\t1
                            """,
                    "select ts, min(ch), max(ch), first(ch), last(ch), count() from tab sample by 1m FILL(LINEAR) align to calendar LIMIT 2"
            );
        });
    }
}
