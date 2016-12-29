/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb;

import com.questdb.store.BSearchType;
import com.questdb.store.FixedColumn;
import com.questdb.store.MemoryFile;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

public class BinarySearchTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    private FixedColumn column;

    @Before
    public void setUp() throws Exception {
        column = new FixedColumn(new MemoryFile(temp.newFile(), 16, JournalMode.APPEND, false), 8);
    }

    @After
    public void tearDown() {
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
