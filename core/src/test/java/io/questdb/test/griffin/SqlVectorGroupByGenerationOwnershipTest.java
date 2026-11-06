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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class SqlVectorGroupByGenerationOwnershipTest extends AbstractCairoTest {
    private static final String QUERY = "SELECT k, count() FROM vg_owner ORDER BY k";

    @Test
    public void testRetainedFactoryOwnsFunctionCopyAfterGeneratorCloses() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final RecordCursorFactory retained;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                retained = compiler.compile(QUERY, sqlExecutionContext).getRecordCursorFactory();
            }
            try (retained) {
                final TextPlanSink plan = new TextPlanSink();
                plan.of(retained, sqlExecutionContext);
                TestUtils.assertContains(plan.getSink(), "vectorized: true");
                assertFactory(retained).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary()
                        .returns("k\tcount\n1\t2\n2\t1\n");
            }
        });
    }

    private static void createTable() throws Exception {
        execute("CREATE TABLE vg_owner(k INT, v LONG)");
        execute("INSERT INTO vg_owner VALUES (1, 10), (2, 20), (1, 30)");
    }
}
