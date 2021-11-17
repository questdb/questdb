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
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

public class ColumnVersionWriterTest extends AbstractCairoTest {
    @Test
    public void testSimple() {
        try (
                Path path = new Path();
                ColumnVersionWriter w = new ColumnVersionWriter(FilesFacadeImpl.INSTANCE, path.of(root).concat("_cv").$(), 0);
                ColumnVersionReader r = new ColumnVersionReader(FilesFacadeImpl.INSTANCE, path, 0)
        ) {
            final LongList writtenColumns = new LongList();
            final LongList readColumns = new LongList();

            writtenColumns.extendAndSet(0, 1); // timestamp
            writtenColumns.extendAndSet(1, 2); // column index
            writtenColumns.extendAndSet(2, 3); // version
            writtenColumns.extendAndSet(3, 0); // reserved
            w.commit(writtenColumns);

            Assert.assertFalse(w.isB());

            long offset;
            long size;

            offset = w.getOffsetA();
            size = w.getSizeA();

            // this is the reading pattern
            // first size up the reader
            r.resize(offset + size);

            // make sure reader sees the same things as writer
            Assert.assertFalse(r.isB());
            Assert.assertEquals(offset, r.getOffsetA());
            Assert.assertEquals(size, r.getSizeA());
            r.load(readColumns, offset, size);
            assertEqual(writtenColumns, readColumns);

            // another transaction
            // change version and update again
            // this should be regular append of 'B' area
            writtenColumns.extendAndSet(2, 4); // version
            w.commit(writtenColumns);
            Assert.assertTrue(w.isB());

            offset = w.getOffsetB();
            size = w.getSizeB();

            r.resize(offset + size);
            Assert.assertTrue(r.isB());
            Assert.assertEquals(offset, r.getOffsetB());
            Assert.assertEquals(size, r.getSizeB());
            r.load(readColumns, offset, size);
            assertEqual(writtenColumns, readColumns);

            // change 'A' area such it still fits at top of the file
            writtenColumns.extendAndSet(2, 5); // version
            w.commit(writtenColumns);
            Assert.assertFalse(w.isB());
            offset = w.getOffsetA();
            size = w.getSizeA();

            r.resize(offset + size);
            Assert.assertFalse(r.isB());
            Assert.assertEquals(offset, r.getOffsetA());
            Assert.assertEquals(size, r.getSizeA());
            r.load(readColumns, offset, size);
            assertEqual(writtenColumns, readColumns);

            // change 'B' area such it still fits at top of the file
            // nothing to dramatic, we need to reach 'A' when 'A' does not fit
            writtenColumns.extendAndSet(2, 6); // version
            w.commit(writtenColumns);
            Assert.assertTrue(w.isB());
            offset = w.getOffsetB();
            size = w.getSizeB();

            // verify
            r.resize(offset + size);
            Assert.assertTrue(r.isB());
            Assert.assertEquals(offset, r.getOffsetB());
            Assert.assertEquals(size, r.getSizeB());
            r.load(readColumns, offset, size);
            assertEqual(writtenColumns, readColumns);

        }
    }

    public static void assertEqual(LongList expected, LongList actual) {
        Assert.assertEquals(expected.size(), actual.size());
        for (int i = 0, n = expected.size(); i < n; i++) {
            Assert.assertEquals(expected.getQuick(i), actual.getQuick(i));
        }
    }
}