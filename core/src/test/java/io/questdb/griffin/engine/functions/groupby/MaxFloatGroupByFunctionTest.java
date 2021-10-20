/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableModel;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.std.NumericException;
import org.junit.Test;

public class MaxFloatGroupByFunctionTest extends AbstractGriffinTest {
    @Test
    public void testSampleByWithFill() throws SqlException, NumericException {
        try (TableModel tm = new TableModel(configuration, "tab", PartitionBy.DAY)) {
            tm.timestamp("ts").col("ch", ColumnType.FLOAT);
            createPopulateTable(tm, 100, "2020-01-01", 2);
        }

        assertSql(
                "select ts, min(ch), max(ch), first(ch), last(ch), count() from tab sample by 1m FILL(LINEAR) LIMIT 2",
                "ts\tmin\tmax\tfirst\tlast\tcount\n" +
                        "2020-01-01T00:28:47.990000Z\t0.0010\t0.0010\t0.0010\t0.0010\t1\n" +
                        "2020-01-01T00:29:47.990000Z\t0.0010\t0.0010\t0.0010\t0.0010\t1\n");
    }
}
