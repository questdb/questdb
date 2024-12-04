/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.std.ndarr;

import io.questdb.std.DirectIntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ndarr.NdArrayRowMajorTraversal;
import org.junit.Assert;
import org.junit.Test;

public class NdArrayRowMajorTraversalTest {
    @Test
    public void test2x3() {
        try (
                DirectIntList shape = new DirectIntList(0, MemoryTag.NATIVE_ND_ARRAY);
                NdArrayRowMajorTraversal traversal = new NdArrayRowMajorTraversal()
        ) {
            shape.add(2);  // rows
            shape.add(3);  // columns
            NdArrayRowMajorTraversal t = traversal.of(shape.asSlice());
            Assert.assertSame(traversal, t);

            // {0, 0}:
            //   X - -
            //   - - -
            Assert.assertArrayEquals(new int[]{0, 0}, t.next().toArray());

            // {0, 1}:
            //   - X -
            //   - - -
            Assert.assertArrayEquals(new int[]{0, 1}, t.next().toArray());

            // {0, 2}:
            //   - - X
            //   - - -
            Assert.assertArrayEquals(new int[]{0, 2}, t.next().toArray());

            // {1, 0}:
            //   - - -
            //   X - -
            Assert.assertArrayEquals(new int[]{1, 0}, t.next().toArray());

            // {1, 1}:
            //   - - -
            //   - X -
            Assert.assertArrayEquals(new int[]{1, 1}, t.next().toArray());

            // {1, 2}:
            //   - - -
            //   - - X
            Assert.assertArrayEquals(new int[]{1, 2}, t.next().toArray());

            // End of iteration.
            Assert.assertNull(t.next());
        }
    }

    @Test
    public void test2x3x4() {
        try (
                DirectIntList shape = new DirectIntList(0, MemoryTag.NATIVE_ND_ARRAY);
                NdArrayRowMajorTraversal traversal = new NdArrayRowMajorTraversal()
        ) {
            shape.add(2);
            shape.add(3);
            shape.add(4);
            traversal.of(shape.asSlice());

            Assert.assertArrayEquals(new int[]{0, 0, 0}, traversal.next().toArray());
            Assert.assertArrayEquals(new int[]{0, 0, 1}, traversal.next().toArray());
            Assert.assertArrayEquals(new int[]{0, 0, 2}, traversal.next().toArray());
            Assert.assertArrayEquals(new int[]{0, 0, 3}, traversal.next().toArray());
            Assert.assertArrayEquals(new int[]{0, 1, 0}, traversal.next().toArray());
            Assert.assertArrayEquals(new int[]{0, 1, 1}, traversal.next().toArray());
            Assert.assertArrayEquals(new int[]{0, 1, 2}, traversal.next().toArray());
            Assert.assertArrayEquals(new int[]{0, 1, 3}, traversal.next().toArray());
            Assert.assertArrayEquals(new int[]{0, 2, 0}, traversal.next().toArray());
            Assert.assertArrayEquals(new int[]{0, 2, 1}, traversal.next().toArray());
            Assert.assertArrayEquals(new int[]{0, 2, 2}, traversal.next().toArray());
            Assert.assertArrayEquals(new int[]{0, 2, 3}, traversal.next().toArray());
            Assert.assertArrayEquals(new int[]{1, 0, 0}, traversal.next().toArray());
            Assert.assertArrayEquals(new int[]{1, 0, 1}, traversal.next().toArray());
            Assert.assertArrayEquals(new int[]{1, 0, 2}, traversal.next().toArray());
            Assert.assertArrayEquals(new int[]{1, 0, 3}, traversal.next().toArray());
            Assert.assertArrayEquals(new int[]{1, 1, 0}, traversal.next().toArray());
            Assert.assertArrayEquals(new int[]{1, 1, 1}, traversal.next().toArray());
            Assert.assertArrayEquals(new int[]{1, 1, 2}, traversal.next().toArray());
            Assert.assertArrayEquals(new int[]{1, 1, 3}, traversal.next().toArray());
            Assert.assertArrayEquals(new int[]{1, 2, 0}, traversal.next().toArray());
            Assert.assertArrayEquals(new int[]{1, 2, 1}, traversal.next().toArray());
            Assert.assertArrayEquals(new int[]{1, 2, 2}, traversal.next().toArray());
            Assert.assertArrayEquals(new int[]{1, 2, 3}, traversal.next().toArray());
            Assert.assertNull(traversal.next());
        }
    }

    @Test
    public void testNull() {
        try (
                DirectIntList shape = new DirectIntList(0, MemoryTag.NATIVE_ND_ARRAY);
                NdArrayRowMajorTraversal traversal = new NdArrayRowMajorTraversal()
        ) {
            Assert.assertNull(traversal.of(shape.asSlice()).next());
            Assert.assertNull(traversal.of(shape.asSlice()).next());
            Assert.assertNull(traversal.of(shape.asSlice()).next());
        }
    }
}
