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
        final int N = 10_000;
        try (
                Path path = new Path();
                ColumnVersionWriter w = new ColumnVersionWriter(FilesFacadeImpl.INSTANCE, path.of(root).concat("_cv").$(), 0);
                ColumnVersionReader r = new ColumnVersionReader(FilesFacadeImpl.INSTANCE, path, 0)
        ) {
            final LongList writtenColumns = new LongList();

            writtenColumns.extendAndSet(0, 1); // timestamp
            writtenColumns.extendAndSet(1, 2); // column index
            writtenColumns.extendAndSet(2, 3); // version
            writtenColumns.extendAndSet(3, 0); // reserved

            int lastSize = 4; // number of items in last batch; 4 items per column entry

            for (int i = 0; i < N; i++) {
                // increment from 0 to 4 columns
                int increment = rnd.nextInt(4);

                writtenColumns.setPos(lastSize + increment * 4);
                for (int j = 0; j < increment; j++) {
                    writtenColumns.setQuick(lastSize++, rnd.nextLong()); // timestamp
                    writtenColumns.setQuick(lastSize++, rnd.nextLong()); // column index
                    writtenColumns.setQuick(lastSize++, rnd.nextLong()); // version
                    writtenColumns.setQuick(lastSize++, 0); // reserved
                }

                w.commit(writtenColumns);
                final long offset = w.getOffset();
                final long size = w.getSize();
                r.load(offset, size);
                assertEqual(writtenColumns, r.getColumnVersions());
            }
        }
    }
}