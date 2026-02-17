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

package io.questdb.test.griffin.engine.functions.finance;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class WeightedMidPriceFunctionFactoryTest extends AbstractCairoTest {
    @Test
    public void testBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            snapshotMemoryUsage();

            try (
                    final RecordCursorFactory factory = select(
                            "select wmid($1, $2, $3, $4)")
            ) {
                final String expected = "wmid\n2.0\n";

                sqlExecutionContext.getBindVariableService().setDouble(0, 100.0);
                sqlExecutionContext.getBindVariableService().setDouble(1, 3);
                sqlExecutionContext.getBindVariableService().setDouble(2, 1);
                sqlExecutionContext.getBindVariableService().setDouble(3, 100);

                try (final RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursor(expected, cursor, factory.getMetadata(), true);
                }

                assertFactoryMemoryUsage();
            }
        });
    }

    @Test
    public void testNonFiniteNumber() throws Exception {
        assertMemoryLeak(() -> {
            assertSql("wmid\nnull\n", "select wmid(NULL, 1.0, 1.0, 1.0)");
            assertSql("wmid\nnull\n", "select wmid(100.0, NULL, 1.0, 0.4)");
            assertSql("wmid\nnull\n", "select wmid(NULL, NULL, 1.0, 100.1)");
            assertSql("wmid\nnull\n", "select wmid(NULL, 1.0, NULL, 100.0)");
            assertSql("wmid\nnull\n", "select wmid(NULL, 1.0, 1.0, NULL)");
        });
    }

    @Test
    public void testNullBehavior() throws Exception {
        assertMemoryLeak(() -> {
            assertSql("wmid\nnull\n", "select wmid(100.1, NULL, 1.0, 100.34)");
            assertSql("wmid\nnull\n", "select wmid(NULL, 1.0, 1.0, 1.2)");
            assertSql("wmid\nnull\n", "select wmid(100.3, 1.0, NULL, 100.0)");
            assertSql("wmid\nnull\n", "select wmid(30.0, 1.0, 1.0, NULL)");
            assertSql("wmid\nnull\n", "select wmid(NULL, NULL, NULL, NULL)");
        });
    }

    @Test
    public void testThatOrderDoesNotMatter() throws Exception {
        assertMemoryLeak(() -> {
            assertSql("wmid\n2.0\n", "select wmid(100.0, 1.0, 3.0, 100.0)");
            assertSql("wmid\n2.0\n", "select wmid(100.0, 3.0, 1.0, 100.0)");
        });
    }

    @Test
    public void testWeightedMidPrice() throws Exception {
        assertMemoryLeak(() -> {
            assertSql("wmid\n2.0\n", "select wmid(100.0, 2.0, 2.0, 100.0)");
            assertSql("wmid\n2.1999999999999997\n", "select wmid(300.0, 1.0, 3.0, 200.0)");
            assertSql("wmid\n2.2857142857142856\n", "select wmid(400.0, 0.0, 4.0, 300.0)");
            assertSql("wmid\n1.5\n", "select wmid(100.0, 1.0, 2.0, 100.0)");
            assertSql("wmid\n1.5227272727272727\n", "select wmid(100.0, 1.5, 1.75, 1000.0)");
            assertSql("wmid\n1.554862842892768\n", "select wmid(100.0, 1.5, 1.61, 100.5)");
            assertSql("wmid\n0.0\n", "select wmid(200.0, 0.0, 0.0, 0.01)");
            assertSql("wmid\n0.33333333333333326\n", "select wmid(200.0, -1.0, 1.0, 100.0)");
            assertSql("wmid\n-0.6661118508655126\n", "select wmid(100.3, -1.0, 0.0, 200.1)");
            assertSql("wmid\n-1.4997511199601792\n", "select wmid(100.5, -2.0, -1.0, 100.4)");
            assertSql("wmid\n-1.8518503333333334\n", "select wmid(100.2, -2.22222, -1.111111, 200.4)");
        });
    }
}