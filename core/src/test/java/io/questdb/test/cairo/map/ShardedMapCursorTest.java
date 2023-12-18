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

package io.questdb.test.cairo.map;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.*;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ShardedMapCursorTest extends AbstractCairoTest {

    @Test
    public void testRowIdAccess() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ColumnTypes types = new SingleColumnType(ColumnType.INT);

            final int N = 100;
            final int M = 1000;
            final Rnd rnd = new Rnd();
            try (
                    FastMap mapA = new FastMap(Numbers.SIZE_1MB, types, types, 64, 0.5, 1);
                    FastMap mapB = new FastMap(Numbers.SIZE_1MB, types, types, 64, 0.5, 1)
            ) {
                for (int i = 0; i < N; i++) {
                    MapKey key = mapA.withKey();
                    key.putInt(rnd.nextInt());
                    MapValue values = key.createValue();
                    Assert.assertTrue(values.isNew());
                    values.putInt(0, i + 1);
                }

                for (int i = N; i < N + M; i++) {
                    MapKey key = mapB.withKey();
                    key.putInt(rnd.nextInt());
                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, i + 1);
                }

                // reset random generator and iterate map to double the value
                rnd.reset();

                LongList list = new LongList();
                try (ShardedMapCursor cursor = new ShardedMapCursor()) {
                    ObjList<Map> shards = new ObjList<>();
                    shards.add(mapA);
                    shards.add(mapB);
                    cursor.of(shards);

                    final MapRecord recordA = cursor.getRecord();
                    while (cursor.hasNext()) {
                        list.add(recordA.getRowId());
                        Assert.assertEquals(rnd.nextInt(), recordA.getInt(1));
                        MapValue value = recordA.getValue();
                        value.putInt(0, value.getInt(0) * 2);
                    }

                    final MapRecord recordB = cursor.getRecordB();
                    Assert.assertNotSame(recordB, recordA);

                    rnd.reset();
                    for (int i = 0, n = list.size(); i < n; i++) {
                        cursor.recordAt(recordB, list.getQuick(i));
                        Assert.assertEquals((i + 1) * 2, recordB.getInt(0));
                        Assert.assertEquals(rnd.nextInt(), recordB.getInt(1));
                    }
                }
            }
        });
    }

    @Test
    public void testSmoke() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ColumnTypes types = new SingleColumnType(ColumnType.INT);

            final int aSize = 1000;
            final int cSize = 420;
            final Rnd rnd = new Rnd();
            try (
                    FastMap mapA = new FastMap(Numbers.SIZE_1MB, types, types, 64, 0.5, 1);
                    FastMap mapB = new FastMap(Numbers.SIZE_1MB, types, types, 64, 0.5, 1); // mapB will be empty
                    FastMap mapC = new FastMap(Numbers.SIZE_1MB, types, types, 64, 0.5, 1)
            ) {
                for (int i = 0; i < aSize; i++) {
                    MapKey key = mapA.withKey();
                    key.putInt(rnd.nextInt());
                    MapValue values = key.createValue();
                    Assert.assertTrue(values.isNew());
                    values.putInt(0, i + 1);
                }

                for (int i = 0; i < cSize; i++) {
                    MapKey key = mapC.withKey();
                    key.putInt(rnd.nextInt());
                    MapValue value = key.createValue();
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, i + 1);
                }

                try (ShardedMapCursor cursor = new ShardedMapCursor()) {
                    ObjList<Map> shards = new ObjList<>();
                    shards.add(mapA);
                    shards.add(mapB);
                    shards.add(mapC);
                    cursor.of(shards);

                    Assert.assertEquals(aSize + cSize, cursor.size());

                    for (int i = 0; i < 10; i++) {
                        rnd.reset();
                        cursor.toTop();

                        int totalSize = 0;
                        final MapRecord record = cursor.getRecord();
                        while (cursor.hasNext()) {
                            Assert.assertEquals(rnd.nextInt(), record.getInt(1));
                            if (totalSize < aSize) {
                                Assert.assertEquals(totalSize + 1, record.getInt(0));
                            } else {
                                Assert.assertEquals(totalSize - aSize + 1, record.getInt(0));
                            }
                            totalSize++;
                        }
                        Assert.assertEquals(aSize + cSize, totalSize);
                    }
                }
            }
        });
    }
}
