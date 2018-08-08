/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.store;

import com.questdb.std.LongList;
import com.questdb.std.ex.JournalException;
import com.questdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class KvIndexTest extends AbstractTest {
    private static final int totalKeys = 10;
    private static final int totalValues = 100;
    private File indexFile;

    @Before
    public void setUp() {
        indexFile = new File(getFactory().getConfiguration().getJournalBase(), "index-test");
    }

    @Test
    public void testAppendNullAfterTruncate() throws JournalException {
        int totalKeys = 2;
        int totalValues = 1;
        try (KVIndex index = new KVIndex(indexFile, totalKeys, totalValues, 1, JournalMode.APPEND, 0, false)) {
            for (int i = -1; i < totalKeys; i++) {
                index.add(i, i);
            }

            Assert.assertEquals(totalKeys, index.size());

            index.truncate(0);

            Assert.assertEquals(0, index.size());
            index.add(-1, 10);
            Assert.assertEquals(11, index.size());
        }
    }

    @Test
    public void testGetValueQuick() throws Exception {
        long expected[][] = {
                {0, 3, 5, 6, 8, 10, 12, 14, 16, 22},
                {1, 2, 3, 4, 6, 8, 9, 11, 16, 21, 33}
        };
        try (KVIndex index = new KVIndex(indexFile, 10, 60, 1, JournalMode.APPEND, 0, false)) {
            putValues(expected, index);

            for (int i = 0; i < expected.length; i++) {
                Assert.assertEquals(expected[i].length, index.getValueCount(i));
                for (int k = 0; k < expected[i].length; k++) {
                    Assert.assertEquals(expected[i][k], index.getValueQuick(i, k));
                }
            }
        }
    }

    @Test
    public void testGetValuesMultiBlock() throws Exception {
        long expected[][] = {
                {0, 3, 5, 6, 8, 10, 12, 14, 16, 22},
                {1, 2, 3, 4, 6, 8, 9, 11, 16, 21, 33}
        };
        try (KVIndex index = new KVIndex(indexFile, 10, 60, 1, JournalMode.APPEND, 0, false)) {
            putValues(expected, index);
            assertValues(expected, index);
        }
    }

    @Test
    public void testGetValuesPartialBlock() throws Exception {
        long expected[][] = {
                {0, 3, 5, 6, 8, 10, 12, 14, 16, 22},
                {1, 2, 3, 4, 6, 8, 9, 11, 16, 21, 33}
        };
        try (KVIndex index = new KVIndex(indexFile, 10, 200, 1, JournalMode.APPEND, 0, false)) {
            putValues(expected, index);
            assertValues(expected, index);
        }
    }

    @Test
    public void testIndexReadWrite() throws JournalException {
        KVIndex index = new KVIndex(indexFile, totalKeys, totalValues, 1, JournalMode.APPEND, 0, false);
        index.add(0, 0);
        index.add(1, 1);
        index.add(1, 2);
        index.add(0, 3);
        index.add(0, 4);
        index.add(1, 5);
        index.add(1, 6);
        index.add(0, 7);
        Assert.assertEquals(1, index.getValueQuick(1, 0));
        Assert.assertEquals(2, index.getValueQuick(1, 1));
        Assert.assertEquals(5, index.getValueQuick(1, 2));
        Assert.assertEquals(6, index.getValueQuick(1, 3));
        Assert.assertEquals(0, index.getValueQuick(0, 0));
        Assert.assertEquals(3, index.getValueQuick(0, 1));
        Assert.assertEquals(4, index.getValueQuick(0, 2));
        Assert.assertEquals(7, index.getValueQuick(0, 3));
        Assert.assertEquals(8, index.size());

        index.close();
    }

    @Test
    public void testIndexTx() throws Exception {
        long expected[][] = {
                {0, 3, 5, 6, 8, 10, 12, 14, 16, 22},
                {1, 2, 3, 4, 6, 8, 9, 11, 16, 21, 33}
        };
        try (KVIndex index = new KVIndex(indexFile, 10, 60, 1, JournalMode.APPEND, 0, false);
             KVIndex reader = new KVIndex(indexFile, 10, 60, 1, JournalMode.READ, 0, false)) {
            // populate writer
            putValues(expected, index);

            // check if writer looks good
            Assert.assertEquals(34, index.size());
            Assert.assertTrue(index.contains(0));
            Assert.assertTrue(index.contains(1));
            Assert.assertEquals(10, index.getValues(0).size());
            Assert.assertEquals(10, index.getValueCount(0));
            Assert.assertEquals(11, index.getValueCount(1));

            // check if reader #1 (opened prior to writer writing) sees empty index
            Assert.assertEquals(0, reader.size());
            Assert.assertEquals(0, reader.getValues(0).size());
            Assert.assertFalse(reader.contains(0));
            Assert.assertFalse(reader.contains(1));
            Assert.assertEquals(0, reader.getValueCount(0));
            Assert.assertEquals(0, reader.getValueCount(1));

            // commit writer would make changes visible to reader after reader either reopens or refreshes
            index.commit();
            // check that refreshed reader saw changes
            reader.setTxAddress(index.getTxAddress());

            // add some more uncommitted changes to make life more difficult
            index.add(0, 35);
            index.add(1, 46);

            // check if refreshed reader saw only committed changes
            Assert.assertEquals(34, reader.size());
            Assert.assertTrue(reader.contains(0));
            Assert.assertTrue(reader.contains(1));
            Assert.assertEquals(10, reader.getValues(0).size());
            Assert.assertEquals(10, reader.getValueCount(0));
            Assert.assertEquals(11, reader.getValueCount(1));

            // open new reader and check if it can see only committed changes
            try (KVIndex reader2 = new KVIndex(indexFile, 10, 60, 1, JournalMode.READ, reader.getTxAddress(), false)) {
                Assert.assertEquals(34, reader2.size());
                Assert.assertTrue(reader2.contains(0));
                Assert.assertTrue(reader2.contains(1));
                Assert.assertEquals(10, reader2.getValues(0).size());
                Assert.assertEquals(10, reader2.getValueCount(0));
                Assert.assertEquals(11, reader2.getValueCount(1));
            }

            index.commit();

            try (KVIndex reader2 = new KVIndex(indexFile, 10, 60, 1, JournalMode.READ, index.getTxAddress(), false)) {
                Assert.assertEquals(47, reader2.size());
                Assert.assertTrue(reader2.contains(0));
                Assert.assertTrue(reader2.contains(1));
                Assert.assertEquals(11, reader2.getValues(0).size());
                Assert.assertEquals(11, reader2.getValueCount(0));
                Assert.assertEquals(12, reader2.getValueCount(1));
            }
        }
    }

    @Test
    public void testKeyOutOfBounds() throws JournalException {
        try (KVIndex index = new KVIndex(indexFile, totalKeys, totalValues, 1, JournalMode.APPEND, 0, false)) {
            index.add(0, 0);
            index.add(1, 1);
            index.add(1, 2);
            Assert.assertEquals(0, index.getValues(2).size());
        }
    }

    @Test
    public void testSmallValueArray() throws JournalException {
        int totalKeys = 2;
        int totalValues = 1;
        try (KVIndex index = new KVIndex(indexFile, totalKeys, totalValues, 1, JournalMode.APPEND, 0, false)) {
            for (int i = 0; i < totalKeys; i++) {
                index.add(i, i);
            }

            Assert.assertEquals(totalKeys, index.size());

            index.truncate(0);

            Assert.assertEquals(0, index.size());
        }
    }

    @Test
    public void testTruncateAtTail() throws JournalException {
        try (KVIndex index = new KVIndex(indexFile, totalKeys, totalValues, 1, JournalMode.APPEND, 0, false)) {
            index.add(0, 0);
            index.add(1, 1);
            index.add(1, 2);
            index.add(0, 3);
            index.add(0, 4);
            index.add(1, 5);
            index.add(1, 6);
            index.add(0, 7);
            index.truncate(6);
            Assert.assertEquals(3, index.getValues(0).size());
            Assert.assertEquals(3, index.getValues(1).size());

            Assert.assertEquals(1, index.getValueQuick(1, 0));
            Assert.assertEquals(2, index.getValueQuick(1, 1));
            Assert.assertEquals(5, index.getValueQuick(1, 2));
            Assert.assertEquals(0, index.getValueQuick(0, 0));
            Assert.assertEquals(3, index.getValueQuick(0, 1));
            Assert.assertEquals(4, index.getValueQuick(0, 2));

            Assert.assertEquals(6, index.size());
        }
    }

    @Test
    public void testTruncateBeforeStart() throws JournalException {
        try (KVIndex index = new KVIndex(indexFile, totalKeys, totalValues, 1, JournalMode.APPEND, 0, false)) {
            index.add(0, 0);
            index.add(1, 1);
            index.add(1, 2);
            index.add(0, 3);
            index.add(0, 4);
            index.add(1, 5);
            index.add(1, 6);
            index.add(0, 7);
            index.truncate(0);
            Assert.assertFalse(index.contains(0));
            Assert.assertFalse(index.contains(1));
            Assert.assertEquals(0, index.size());
        }
    }

    @Test
    public void testTruncateBeyondTail() throws JournalException {
        try (KVIndex index = new KVIndex(indexFile, totalKeys, totalValues, 1, JournalMode.APPEND, 0, false)) {
            index.add(0, 0);
            index.add(1, 1);
            index.add(1, 2);
            index.add(0, 3);
            index.add(0, 4);
            index.add(1, 5);
            index.add(1, 6);
            index.add(0, 7);
            index.truncate(10);
            Assert.assertEquals(4, index.getValues(0).size());
            Assert.assertEquals(4, index.getValues(1).size());

            Assert.assertEquals(1, index.getValueQuick(1, 0));
            Assert.assertEquals(2, index.getValueQuick(1, 1));
            Assert.assertEquals(5, index.getValueQuick(1, 2));
            Assert.assertEquals(6, index.getValueQuick(1, 3));
            Assert.assertEquals(0, index.getValueQuick(0, 0));
            Assert.assertEquals(3, index.getValueQuick(0, 1));
            Assert.assertEquals(4, index.getValueQuick(0, 2));
            Assert.assertEquals(7, index.getValueQuick(0, 3));

            Assert.assertEquals(8, index.size());
        }
    }

    @Test
    public void testTruncateMiddle() throws JournalException {
        try (KVIndex index = new KVIndex(indexFile, totalKeys, totalValues, 1, JournalMode.APPEND, 0, false)) {
            index.add(0, 0);
            index.add(1, 1);
            index.add(1, 2);
            index.add(0, 3);
            index.add(0, 4);
            index.add(1, 5);
            index.add(1, 6);
            index.add(0, 7);
            index.truncate(5);
            Assert.assertEquals(3, index.getValues(0).size());
            Assert.assertEquals(2, index.getValues(1).size());

            Assert.assertEquals(1, index.getValueQuick(1, 0));
            Assert.assertEquals(2, index.getValueQuick(1, 1));
            Assert.assertEquals(0, index.getValueQuick(0, 0));
            Assert.assertEquals(3, index.getValueQuick(0, 1));
            Assert.assertEquals(4, index.getValueQuick(0, 2));
        }
    }

    @Test(expected = JournalRuntimeException.class)
    public void testValueOutOfBounds() throws JournalException {
        try (KVIndex index = new KVIndex(indexFile, totalKeys, totalValues, 1, JournalMode.APPEND, 0, false)) {
            index.add(0, 0);
            index.add(1, 1);
            index.add(1, 2);
            index.getValueQuick(0, 1);
        }
    }

    private void assertValues(long values[][], KVIndex index) {
        for (int i = 0; i < values.length; i++) {
            LongList array = index.getValues(i);
            Assert.assertEquals(values[i].length, array.size());
            for (int k = 0; k < values[i].length; k++) {
                Assert.assertEquals(values[i][k], array.get(k));
            }

            IndexCursor cursor = index.cursor(i);
            int k = (int) cursor.size();
            while (cursor.hasNext()) {
                Assert.assertEquals(values[i][--k], cursor.next());
            }

            IndexCursor c = index.fwdCursor(i);
            int n = 0;
            while (c.hasNext()) {
                long l = c.next();
                Assert.assertEquals(values[i][n++], l);
            }
        }
    }

    private void putValues(long values[][], KVIndex index) {
        for (int i = 0; i < values.length; i++) {
            for (int k = 0; k < values[i].length; k++) {
                index.add(i, values[i][k]);
            }
        }
    }
}
