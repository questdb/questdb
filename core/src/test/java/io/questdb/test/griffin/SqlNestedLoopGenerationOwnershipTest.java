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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlNestedLoopGenerationOwnershipTest extends AbstractCairoTest {
    private static final String[] JOINS = {
            "CROSS JOIN owned_table(1) b",
            "JOIN owned_table(1) b ON a.id > b.id",
            "LEFT JOIN owned_table(1) b ON a.id > b.id",
            "RIGHT JOIN owned_table(1) b ON a.id > b.id",
            "FULL JOIN owned_table(1) b ON a.id > b.id"
    };
    private static final String[] PLANS = {"Cross Join", "Cross Join", "Nested Loop Left Join", "Nested Loop Right Join", "Nested Loop Full Join"};

    @Test
    public void testFactoriesRetainInputsAfterCompilerCloses() throws Exception {
        assertMemoryLeak(() -> {
            try (OwnershipFixture fixture = new OwnershipFixture(engine)) {
                fixture.table("id", ColumnType.INT);
                fixture.table("id", ColumnType.INT);
                for (int i = 0; i < JOINS.length; i++) {
                    fixture.reset();
                    final RecordCursorFactory retained;
                    try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                        retained = compiler.compile("SELECT * FROM owned_table(0) a " + JOINS[i], sqlExecutionContext).getRecordCursorFactory();
                    }
                    try (RecordCursorFactory factory = retained) {
                        final TextPlanSink plan = new TextPlanSink();
                        plan.of(factory, sqlExecutionContext);
                        TestUtils.assertContains(plan.getSink(), PLANS[i]);
                        Assert.assertFalse(factory.recordCursorSupportsRandomAccess());
                        Assert.assertEquals(2, fixture.tableCount());
                        fixture.assertNoneClosed();
                    }
                    fixture.assertAllClosedOnce();
                }
            }
        });
    }

    @Test
    public void testFullConstructorFailureClosesOwnersOnceAndPreservesFailure() throws Exception {
        assertMetadataFaults(4, 4);
    }

    @Test
    public void testNullRecordFailureBeforeAdoptionClosesEveryOwner() throws Exception {
        assertMetadataFaults(2, 3);
    }

    private static void assertMetadataFaults(int firstJoin, int lastJoin) throws Exception {
        assertMemoryLeak(() -> {
            try (OwnershipFixture fixture = new OwnershipFixture(engine);
                 SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                fixture.table("id", ColumnType.INT).closeFailure = new RuntimeException("master close");
                fixture.table("id", ColumnType.INT);
                for (int i = firstJoin; i <= lastJoin; i++) {
                    fixture.assertMetadataFaultSweep(compiler, sqlExecutionContext, "SELECT * FROM owned_table(0) a " + JOINS[i]);
                }
            }
        });
    }
}
