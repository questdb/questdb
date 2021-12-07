/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cairo;

import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

public class ColumnVersionWriterTest extends AbstractCairoTest {
    public static void assertEqual(LongList expected, LongList actual) {
        Assert.assertEquals(expected.size(), actual.size());
        for (int i = 0, n = expected.size(); i < n; i++) {
            Assert.assertEquals(expected.getQuick(i), actual.getQuick(i));
        }
    }

    @Test
    public void testFuzz() {
        final Rnd rnd = new Rnd();
        final int N = 100_000;
        try (
                Path path = new Path();
                ColumnVersionWriter w = new ColumnVersionWriter(FilesFacadeImpl.INSTANCE, path.of(root).concat("_cv").$(), 0);
                ColumnVersionReader r = new ColumnVersionReader(FilesFacadeImpl.INSTANCE, path, 0)
        ) {
            w.upsert(1, 2, 3);

            for (int i = 0; i < N; i++) {
                // increment from 0 to 4 columns
                int increment = rnd.nextInt(32);

                for (int j = 0; j < increment; j++) {
                    w.upsert(rnd.nextLong(20), rnd.nextInt(10), rnd.nextLong());
                }

                w.commit();
                final long offset = w.getOffset();
                final long size = w.getSize();
                r.readUnsafe(offset, size);
                Assert.assertTrue(w.getCachedList().size() > 0);
                assertEqual(w.getCachedList(), r.getCachedList());
                // assert list is ordered by (timestamp,column_index)

                LongList list = r.getCachedList();
                long prevTimestamp = -1;
                long prevColumnIndex = -1;
                for (int j = 0, n = list.size(); j < n; j += ColumnVersionWriter.BLOCK_SIZE) {
                    long timestamp = list.getQuick(j);
                    long columnIndex = list.getQuick(j + 1);

                    if (prevTimestamp < timestamp) {
                        prevTimestamp = timestamp;
                        prevColumnIndex = columnIndex;
                        continue;
                    }

                    if (prevTimestamp == timestamp) {
                        Assert.assertTrue(prevColumnIndex < columnIndex);
                        prevColumnIndex = columnIndex;
                        continue;
                    }

                    Assert.fail();
                }
            }
        }
    }
}