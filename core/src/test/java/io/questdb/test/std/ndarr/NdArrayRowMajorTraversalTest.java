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

import io.questdb.cairo.ndarr.NdArrayRowMajorTraversal;
import io.questdb.std.DirectIntList;
import io.questdb.std.MemoryTag;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class NdArrayRowMajorTraversalTest {

    @Test
    public void test1() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    DirectIntList shape = new DirectIntList(1, MemoryTag.NATIVE_ND_ARRAY);
                    NdArrayRowMajorTraversal traversal = new NdArrayRowMajorTraversal()
            ) {
                shape.add(1);
                traversal.of(shape.asSlice());

                Assert.assertArrayEquals(new int[]{0}, traversal.next().toArray());
                Assert.assertEquals(1, traversal.getIn());
                Assert.assertEquals(1, traversal.getOut());
                Assert.assertFalse(traversal.hasNext());
                Assert.assertNull(traversal.next());
            }
        });
    }

    @Test
    public void test1x1() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    DirectIntList shape = new DirectIntList(1, MemoryTag.NATIVE_ND_ARRAY);
                    NdArrayRowMajorTraversal traversal = new NdArrayRowMajorTraversal()
            ) {
                shape.add(1);
                shape.add(1);
                traversal.of(shape.asSlice());

                Assert.assertArrayEquals(new int[]{0, 0}, traversal.next().toArray());
                Assert.assertEquals(2, traversal.getIn());
                Assert.assertEquals(2, traversal.getOut());
                Assert.assertFalse(traversal.hasNext());
                Assert.assertNull(traversal.next());
            }
        });
    }

    @Test
    public void test2x3() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
                Assert.assertEquals(2, t.getIn());
                Assert.assertEquals(0, t.getOut());
                Assert.assertTrue(t.hasNext());

                // {0, 1}:
                //   - X -
                //   - - -
                Assert.assertArrayEquals(new int[]{0, 1}, t.next().toArray());
                Assert.assertEquals(0, t.getIn());
                Assert.assertEquals(0, t.getOut());
                Assert.assertTrue(t.hasNext());

                // {0, 2}:
                //   - - X
                //   - - -
                Assert.assertArrayEquals(new int[]{0, 2}, t.next().toArray());
                Assert.assertEquals(0, t.getIn());
                Assert.assertEquals(1, t.getOut());
                Assert.assertTrue(t.hasNext());

                // {1, 0}:
                //   - - -
                //   X - -
                Assert.assertArrayEquals(new int[]{1, 0}, t.next().toArray());
                Assert.assertEquals(1, t.getIn());
                Assert.assertEquals(0, t.getOut());
                Assert.assertTrue(t.hasNext());

                // {1, 1}:
                //   - - -
                //   - X -
                Assert.assertArrayEquals(new int[]{1, 1}, t.next().toArray());
                Assert.assertEquals(0, t.getIn());
                Assert.assertEquals(0, t.getOut());
                Assert.assertTrue(t.hasNext());

                // {1, 2}:
                //   - - -
                //   - - X
                Assert.assertArrayEquals(new int[]{1, 2}, t.next().toArray());
                Assert.assertEquals(0, t.getIn());
                Assert.assertEquals(2, t.getOut());
                Assert.assertFalse(t.hasNext());

                // End of iteration.
                Assert.assertNull(t.next());

                // Reaching the end of the iterator is idempotent.
                Assert.assertNull(t.next());
                Assert.assertNull(t.next());

                // Reusability.
                traversal.of(shape.asSlice());
                Assert.assertArrayEquals(new int[]{0, 0}, t.next().toArray());
                Assert.assertArrayEquals(new int[]{0, 1}, t.next().toArray());
                Assert.assertArrayEquals(new int[]{0, 2}, t.next().toArray());
                Assert.assertArrayEquals(new int[]{1, 0}, t.next().toArray());
                Assert.assertArrayEquals(new int[]{1, 1}, t.next().toArray());
                Assert.assertArrayEquals(new int[]{1, 2}, t.next().toArray());
                Assert.assertNull(t.next());
            }
        });
    }

    @Test
    public void test2x3x4() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    DirectIntList shape = new DirectIntList(0, MemoryTag.NATIVE_ND_ARRAY);
                    NdArrayRowMajorTraversal traversal = new NdArrayRowMajorTraversal()
            ) {
                shape.add(2);
                shape.add(3);
                shape.add(4);
                traversal.of(shape.asSlice());

                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{0, 0, 0}, traversal.next().toArray());
                Assert.assertEquals(3, traversal.getIn());
                Assert.assertEquals(0, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{0, 0, 1}, traversal.next().toArray());
                Assert.assertEquals(0, traversal.getIn());
                Assert.assertEquals(0, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{0, 0, 2}, traversal.next().toArray());
                Assert.assertEquals(0, traversal.getIn());
                Assert.assertEquals(0, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{0, 0, 3}, traversal.next().toArray());
                Assert.assertEquals(0, traversal.getIn());
                Assert.assertEquals(1, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{0, 1, 0}, traversal.next().toArray());
                Assert.assertEquals(1, traversal.getIn());
                Assert.assertEquals(0, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{0, 1, 1}, traversal.next().toArray());
                Assert.assertEquals(0, traversal.getIn());
                Assert.assertEquals(0, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{0, 1, 2}, traversal.next().toArray());
                Assert.assertEquals(0, traversal.getIn());
                Assert.assertEquals(0, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{0, 1, 3}, traversal.next().toArray());
                Assert.assertEquals(0, traversal.getIn());
                Assert.assertEquals(1, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{0, 2, 0}, traversal.next().toArray());
                Assert.assertEquals(1, traversal.getIn());
                Assert.assertEquals(0, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{0, 2, 1}, traversal.next().toArray());
                Assert.assertEquals(0, traversal.getIn());
                Assert.assertEquals(0, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{0, 2, 2}, traversal.next().toArray());
                Assert.assertEquals(0, traversal.getIn());
                Assert.assertEquals(0, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{0, 2, 3}, traversal.next().toArray());
                Assert.assertEquals(0, traversal.getIn());
                Assert.assertEquals(2, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{1, 0, 0}, traversal.next().toArray());
                Assert.assertEquals(2, traversal.getIn());
                Assert.assertEquals(0, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{1, 0, 1}, traversal.next().toArray());
                Assert.assertEquals(0, traversal.getIn());
                Assert.assertEquals(0, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{1, 0, 2}, traversal.next().toArray());
                Assert.assertEquals(0, traversal.getIn());
                Assert.assertEquals(0, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{1, 0, 3}, traversal.next().toArray());
                Assert.assertEquals(0, traversal.getIn());
                Assert.assertEquals(1, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{1, 1, 0}, traversal.next().toArray());
                Assert.assertEquals(1, traversal.getIn());
                Assert.assertEquals(0, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{1, 1, 1}, traversal.next().toArray());
                Assert.assertEquals(0, traversal.getIn());
                Assert.assertEquals(0, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{1, 1, 2}, traversal.next().toArray());
                Assert.assertEquals(0, traversal.getIn());
                Assert.assertEquals(0, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{1, 1, 3}, traversal.next().toArray());
                Assert.assertEquals(0, traversal.getIn());
                Assert.assertEquals(1, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{1, 2, 0}, traversal.next().toArray());
                Assert.assertEquals(1, traversal.getIn());
                Assert.assertEquals(0, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{1, 2, 1}, traversal.next().toArray());
                Assert.assertEquals(0, traversal.getIn());
                Assert.assertEquals(0, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{1, 2, 2}, traversal.next().toArray());
                Assert.assertEquals(0, traversal.getIn());
                Assert.assertEquals(0, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertArrayEquals(new int[]{1, 2, 3}, traversal.next().toArray());
                Assert.assertEquals(0, traversal.getIn());
                Assert.assertEquals(3, traversal.getOut());
                Assert.assertFalse(traversal.hasNext());
                Assert.assertNull(traversal.next());
            }
        });
    }

    @Test
    public void test4() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    DirectIntList shape = new DirectIntList(1, MemoryTag.NATIVE_ND_ARRAY);
                    NdArrayRowMajorTraversal traversal = new NdArrayRowMajorTraversal()
            ) {
                shape.add(4);
                traversal.of(shape.asSlice());

                Assert.assertArrayEquals(new int[]{0}, traversal.next().toArray());
                Assert.assertEquals(1, traversal.getIn());
                Assert.assertEquals(0, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());

                Assert.assertArrayEquals(new int[]{1}, traversal.next().toArray());
                Assert.assertEquals(0, traversal.getIn());
                Assert.assertEquals(0, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());

                Assert.assertArrayEquals(new int[]{2}, traversal.next().toArray());
                Assert.assertEquals(0, traversal.getIn());
                Assert.assertEquals(0, traversal.getOut());
                Assert.assertTrue(traversal.hasNext());

                Assert.assertArrayEquals(new int[]{3}, traversal.next().toArray());
                Assert.assertEquals(0, traversal.getIn());
                Assert.assertEquals(1, traversal.getOut());
                Assert.assertFalse(traversal.hasNext());
                Assert.assertNull(traversal.next());
            }
        });
    }

    @Test
    public void testNull() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    DirectIntList shape = new DirectIntList(0, MemoryTag.NATIVE_ND_ARRAY);
                    NdArrayRowMajorTraversal traversal = new NdArrayRowMajorTraversal()
            ) {
                traversal.of(shape.asSlice());
                Assert.assertFalse(traversal.hasNext());
                Assert.assertNull(traversal.of(shape.asSlice()).next());
                Assert.assertFalse(traversal.hasNext());
                Assert.assertNull(traversal.of(shape.asSlice()).next());
                Assert.assertFalse(traversal.hasNext());
                Assert.assertNull(traversal.of(shape.asSlice()).next());
                Assert.assertFalse(traversal.hasNext());
                Assert.assertNull(traversal.of(shape.asSlice()).next());
            }
        });
    }
}
