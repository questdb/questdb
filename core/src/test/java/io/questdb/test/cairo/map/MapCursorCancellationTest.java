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

package io.questdb.test.cairo.map;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.ShardedMapCursor;
import io.questdb.cairo.map.Unordered4Map;
import io.questdb.cairo.map.Unordered8Map;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.Hash;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.CountingSqlExecutionCircuitBreaker;
import org.junit.Assert;
import org.junit.Test;

public class MapCursorCancellationTest extends AbstractCairoTest {
    @Test
    public void testSparseInitializationAndTraversalCancelAndRebind() throws Exception {
        assertMemoryLeak(() -> {
            for (boolean wide : new boolean[]{false, true}) {
                for (boolean prefix : new boolean[]{false, true}) {
                    try (Map map = wide
                            ? new Unordered8Map(ColumnType.LONG, new SingleColumnType(ColumnType.LONG), 65_536, 0.5, 8)
                            : new Unordered4Map(ColumnType.INT, new SingleColumnType(ColumnType.LONG), 65_536, 0.5, 8)) {
                        int mask = map.getKeyCapacity() - 1;
                        int k = 1;
                        // Leave a long empty run before or after the sole occupied slot.
                        while (((wide ? Hash.hashLong64(k) : Hash.hashInt64(k)) & mask) != (prefix ? mask : 0)) {
                            k++;
                        }
                        MapKey key = map.withKey();
                        if (wide) {
                            key.putLong(k);
                        } else {
                            key.putInt(k);
                        }
                        key.createValue().putLong(0, 42);
                        CountingSqlExecutionCircuitBreaker breaker = new CountingSqlExecutionCircuitBreaker(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER) {
                            @Override
                            public void statefulThrowExceptionIfTripped() {
                                super.statefulThrowExceptionIfTripped();
                                if (getCheckCount() == 16) {
                                    throw CairoException.queryCancelled(-1);
                                }
                            }
                        };
                        try {
                            map.getCursor(breaker).hasNext();
                            Assert.fail("expected interruption inside the sparse slot scan");
                        } catch (CairoException ex) {
                            Assert.assertTrue(ex.isCancellation());
                        }
                        Assert.assertEquals(16, breaker.getCheckCount());
                        try (MapRecordCursor cursor = map.getCursor(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(42, cursor.getRecord().getLong(0));
                            Assert.assertFalse(cursor.hasNext());
                            cursor.toTop();
                            Assert.assertTrue(cursor.hasNext());
                        }
                        // The ordinary overload clears any previous query binding as well.
                        Assert.assertTrue(map.getCursor().hasNext());
                        try (ShardedMapCursor cursor = new ShardedMapCursor()) {
                            ObjList<Map> shards = new ObjList<>();
                            shards.add(map);
                            cursor.of(shards, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertFalse(cursor.hasNext());
                        }
                    }
                }
            }
        });
    }
}
