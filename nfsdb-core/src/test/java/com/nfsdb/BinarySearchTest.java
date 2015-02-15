/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb;

import com.nfsdb.column.BSearchType;
import com.nfsdb.column.FixedColumn;
import com.nfsdb.column.MappedFileImpl;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

public class BinarySearchTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    private FixedColumn column;

    @Before
    public void setUp() throws Exception {
        column = new FixedColumn(new MappedFileImpl(temp.newFile(), 16, JournalMode.APPEND), 8);
    }

    @After
    public void tearDown() throws Exception {
        column.close();
    }

    @Test
    public void testSearchGreaterOrEquals() throws Exception {
        assertEquals(new long[]{1, 2, 3, 4, 5, 6, 9, 12, 17, 23}, BSearchType.NEWER_OR_SAME, 7, 6);
        assertEquals(new long[]{1, 2, 3, 4, 5, 6, 6, 6, 7, 12, 17, 23}, BSearchType.NEWER_OR_SAME, 6, 5);
        assertEquals(new long[]{2, 2, 3, 4, 5, 6, 6, 6, 7, 12, 17, 23}, BSearchType.NEWER_OR_SAME, 1, 0);
        assertEquals(new long[]{2, 2, 3, 4, 5, 6, 6, 6, 7, 12, 17, 23}, BSearchType.NEWER_OR_SAME, 2, 0);
        assertEquals(new long[]{2, 2, 3, 4, 5, 6, 6, 6, 7, 12, 17, 23}, BSearchType.NEWER_OR_SAME, 25, -2);
        assertEquals(new long[]{2, 2}, BSearchType.NEWER_OR_SAME, 25, -2);
    }

    @Test
    public void testSearchLessOrEquals() throws Exception {
        assertEquals(new long[]{1, 2, 3, 4, 5, 6, 9, 12, 17, 23}, BSearchType.OLDER_OR_SAME, 11, 6);
        assertEquals(new long[]{1, 2, 3, 4, 9, 9, 9, 12, 17, 23}, BSearchType.OLDER_OR_SAME, 9, 6);
        assertEquals(new long[]{1, 2, 3, 4, 9, 9, 9, 12, 17, 23}, BSearchType.OLDER_OR_SAME, 25, 9);
        assertEquals(new long[]{3, 3, 3, 4, 9, 9, 9, 12, 17, 23}, BSearchType.OLDER_OR_SAME, 1, -1);
        assertEquals(new long[]{3, 3}, BSearchType.OLDER_OR_SAME, 1, -1);
    }

    private void assertEquals(long[] src, BSearchType type, long val, long expected) {
        column.truncate(0);
        column.commit();
        for (long l : src) {
            column.putLong(l);
            column.commit();
        }

        Assert.assertEquals(expected, column.bsearchEdge(val, type));
    }
}
