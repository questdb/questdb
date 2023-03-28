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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import org.junit.Test;

public class FloatGroupByFunctionsTest extends AbstractGriffinTest {
    @Test
    public void testSampleBy() throws SqlException, NumericException {
        sqlExecutionContext.setRandom(new Rnd());
        try (TableModel tm = new TableModel(configuration, "tab", PartitionBy.DAY)) {
            tm.timestamp("ts").col("ch", ColumnType.FLOAT);
            createPopulateTable(tm, 100, "2020-01-01", 2);
        }

        assertSql("select ts, min(ch), max(ch), first(ch), last(ch), count() from tab sample by d",
                "ts\tmin\tmax\tfirst\tlast\tcount\n" +
                        "2020-01-01T00:28:47.990000Z\t0.0010\t0.0510\t0.0010\t0.0510\t51\n" +
                        "2020-01-02T00:28:47.990000Z\t0.0520\t0.1000\t0.0520\t0.1000\t49\n");
    }

    @Test
    public void testSampleByWithNulls() throws SqlException {
        sqlExecutionContext.setRandom(new Rnd());
        compiler.compile("create table tab as ( select rnd_float() ch from long_sequence(100) )", sqlExecutionContext);

        assertSql("select min(ch), max(ch), first(ch), last(ch), count() from tab",
                "min\tmax\tfirst\tlast\tcount\n" +
                        "0.0011\t0.9856\t0.6608\t0.7998\t100\n");
    }
}
