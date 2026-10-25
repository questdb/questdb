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

package io.questdb.test.cairo.sql.async;

import io.questdb.DefaultFactoryProvider;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.async.AdaptiveWorkStealingStrategy;
import io.questdb.cairo.sql.async.WorkStealingStrategy;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class PageFrameSequenceTest extends AbstractCairoTest {

    @Test
    public void testOwnerConsultsStrategyBeforeStealingOwnFrame() throws Exception {
        // No worker pool consumes the reduce queue, so the owner publishes each frame and then
        // steals it back in the gang-steal loop of dispatch. A strategy that gates the owner on a
        // worker, such as SlotGatedWorkStealingStrategy, can hold it back only if the loop calls
        // onBeforeDirectSteal() before each steal.
        final AtomicInteger directStealCount = new AtomicInteger();
        factoryProvider = new DefaultFactoryProvider() {
            @Override
            public @NotNull WorkStealingStrategy getWorkStealingStrategy(
                    @NotNull CairoConfiguration configuration,
                    int workerCount,
                    @NotNull StatefulAtom atom
            ) {
                return new AdaptiveWorkStealingStrategy(
                        configuration.getSqlParallelWorkStealingThreshold(),
                        configuration.getSqlParallelWorkStealingSpinTimeout()
                ) {
                    @Override
                    public void onBeforeDirectSteal() {
                        directStealCount.incrementAndGet();
                    }
                };
            }
        };
        assertMemoryLeak(() -> {
            // two daily partitions, so two page frames
            execute("""
                    CREATE TABLE t AS (
                        SELECT x, timestamp_sequence(0, 43_200_000_000) ts
                        FROM long_sequence(4)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            try (RecordCursorFactory factory = select("SELECT x FROM t WHERE x % 2 = 0")) {
                TestUtils.assertFactoryInTree(factory, AsyncFilteredRecordCursorFactory.class);
                for (int i = 0; i < 2; i++) {
                    directStealCount.set(0);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        int rowCount = 0;
                        while (cursor.hasNext()) {
                            Assert.assertEquals(0, cursor.getRecord().getLong(0) % 2);
                            rowCount++;
                        }
                        Assert.assertEquals(2, rowCount);
                    }
                    Assert.assertEquals("execution " + i, 2, directStealCount.get());
                }
            }
        });
    }
}
