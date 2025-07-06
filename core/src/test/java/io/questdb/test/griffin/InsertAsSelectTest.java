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

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class InsertAsSelectTest extends AbstractCairoTest {
    @Test
    public void testInsertAsSelectStringToVarChar() throws SqlException {
        try {
            ColumnType.makeUtf16DefaultString();

            execute("create table append as (" +
                    "select" +
                    "  timestamp_sequence(518300000010L,100000L) ts," +
                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                    " rnd_varchar('ABC', 'CDE', null, 'XYZ') d," +
                    " rnd_char() t" +
                    " from long_sequence(1000)" +
                    ")"
            );

            execute("create table target (" +
                    "ts timestamp," +
                    "c varchar," +
                    "d string," +
                    "t varchar" +
                    ") timestamp (ts) partition by DAY BYPASS WAL"
            );

            drainWalQueue();

            // insert as select
            execute("insert into target select * from append");
            drainWalQueue();


            // check
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertEquals(
                        compiler,
                        sqlExecutionContext,
                        "append order by ts",
                        "target"
                );
            }
        } finally {
            ColumnType.resetStringToDefault();
        }
    }
}
