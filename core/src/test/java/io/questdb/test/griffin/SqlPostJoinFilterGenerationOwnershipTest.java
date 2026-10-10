/*+*****************************************************************************
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

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class SqlPostJoinFilterGenerationOwnershipTest extends AbstractCairoTest {
    private static final String SUBQUERY = "(SELECT max(v) FROM owned_table(2))";
    private static final String[] QUERIES = {
            "SELECT * FROM owned_table(0) a JOIN owned_table(1) b ON a.id = b.id WHERE a.v - b.v > " + SUBQUERY,
            "SELECT * FROM owned_table(0) a JOIN owned_table(1) b ON a.id = b.id AND a.v > b.v WHERE a.v - b.v < " + SUBQUERY,
            "SELECT * FROM owned_table(0) a ASOF JOIN owned_table(1) b ON a.id = b.id WHERE a.v - b.v > " + SUBQUERY
    };

    @Test
    public void testPostJoinFilterFailureClosesEveryOwnerOnce() throws Exception {
        assertMemoryLeak(() -> {
            try (OwnershipFixture fixture = new OwnershipFixture(engine);
                 SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                fixture.table("ts", ColumnType.TIMESTAMP_MICRO, "id", ColumnType.INT, "v", ColumnType.INT)
                        .timestamp(0).closeFailure = new RuntimeException("master close");
                fixture.table("ts", ColumnType.TIMESTAMP_MICRO, "id", ColumnType.INT, "v", ColumnType.INT).timestamp(0);
                fixture.table("v", ColumnType.INT);
                for (String query : QUERIES) {
                    fixture.assertMetadataFaultSweep(compiler, sqlExecutionContext, query);
                }
            }
        });
    }
}
