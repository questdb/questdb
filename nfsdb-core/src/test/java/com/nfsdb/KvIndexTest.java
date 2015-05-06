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

import com.nfsdb.collections.DirectLongList;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.storage.IndexCursor;
import com.nfsdb.storage.KVIndex;
import com.nfsdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class KvIndexTest extends AbstractTest {
    private static final int totalKeys = 10;
    private static final int totalValues = 100;
    private File indexFile;

    @Before
    public void setup() {
        indexFile = new File(factory.getConfiguration().getJournalBase(), "index-test");
    }

    @Test
    public void testAppendNullAfterTruncate() throws JournalException {
        int totalKeys = 2;
        int totalValues = 1;
        try (KVIndex index = new KVIndex(indexFile, totalKeys, totalValues, 1, JournalMode.APPEND, 0)) {
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
        try (KVIndex index = new KVIndex(indexFile, 10, 60, 1, JournalMode.APPEND, 0)) {
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
        try (KVIndex index = new KVIndex(indexFile, 10, 60, 1, JournalMode.APPEND, 0)) {
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
        try (KVIndex index = new KVIndex(indexFile, 10, 200, 1, JournalMode.APPEND, 0)) {
            putValues(expected, index);
            assertValues(expected, index);
        }
    }

    @Test
    public void testIndexReadWrite() throws JournalException {
        KVIndex index = new KVIndex(indexFile, totalKeys, totalValues, 1, JournalMode.APPEND, 0);
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
        try (KVIndex index = new KVIndex(indexFile, 10, 60, 1, JournalMode.APPEND, 0); KVIndex reader = new KVIndex(indexFile, 10, 60, 1, JournalMode.READ, 0)) {
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
            try (KVIndex reader2 = new KVIndex(indexFile, 10, 60, 1, JournalMode.READ, reader.getTxAddress())) {
                Assert.assertEquals(34, reader2.size());
                Assert.assertTrue(reader2.contains(0));
                Assert.assertTrue(reader2.contains(1));
                Assert.assertEquals(10, reader2.getValues(0).size());
                Assert.assertEquals(10, reader2.getValueCount(0));
                Assert.assertEquals(11, reader2.getValueCount(1));
            }

            index.commit();

            try (KVIndex reader2 = new KVIndex(indexFile, 10, 60, 1, JournalMode.READ, index.getTxAddress())) {
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
        try (KVIndex index = new KVIndex(indexFile, totalKeys, totalValues, 1, JournalMode.APPEND, 0)) {
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
        try (KVIndex index = new KVIndex(indexFile, totalKeys, totalValues, 1, JournalMode.APPEND, 0)) {
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
        try (KVIndex index = new KVIndex(indexFile, totalKeys, totalValues, 1, JournalMode.APPEND, 0)) {
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
        try (KVIndex index = new KVIndex(indexFile, totalKeys, totalValues, 1, JournalMode.APPEND, 0)) {
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
        try (KVIndex index = new KVIndex(indexFile, totalKeys, totalValues, 1, JournalMode.APPEND, 0)) {
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
        try (KVIndex index = new KVIndex(indexFile, totalKeys, totalValues, 1, JournalMode.APPEND, 0)) {
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
        try (KVIndex index = new KVIndex(indexFile, totalKeys, totalValues, 1, JournalMode.APPEND, 0)) {
            index.add(0, 0);
            index.add(1, 1);
            index.add(1, 2);
            index.getValueQuick(0, 1);
        }
    }

    private void assertValues(long values[][], KVIndex index) {
        for (int i = 0; i < values.length; i++) {
            try (DirectLongList array = index.getValues(i)) {
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
                while (c.hasNext()) {
                    System.out.println(c.next());
                }
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
