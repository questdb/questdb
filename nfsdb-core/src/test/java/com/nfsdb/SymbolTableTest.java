/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalInvalidSymbolValueException;
import com.nfsdb.storage.SymbolTable;
import com.nfsdb.test.tools.AbstractTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class SymbolTableTest extends AbstractTest {

    private static final int DATA_SIZE = 500;
    private SymbolTable tab = null;

    @After
    public void tearDown() {
        if (tab != null) {
            tab.close();
        }
    }

    @Test
    public void testCachePreLoad() throws Exception {
        String data[] = createData();
        createTestTable(data);
        // check that values match keys after cache heat up
        try (SymbolTable tab = getReader().preLoad()) {
            for (int i = 0; i < data.length; i++) {
                Assert.assertEquals(i, tab.getQuick(data[i]));
            }
        }
    }

    @Test
    public void testKeyValueMatch() throws Exception {

        String data[] = createData();
        createTestTable(data);

        // check that keys match values
        try (SymbolTable tab = getReader()) {
            for (int i = 0; i < tab.size(); i++) {
                Assert.assertEquals(data[i], tab.value(i));
            }
        }
    }

    @Test(expected = JournalInvalidSymbolValueException.class)
    public void testLoudCheckKey() throws Exception {
        createTestTable(createData());
        try (SymbolTable tab = getReader()) {
            Assert.assertEquals(420, tab.get("TEST420"));
            // exception
            tab.get("650");
        }
    }

    @Test
    public void testNullValues() throws Exception {
        String data[] = {null, null};
        createTestTable(data);

        SymbolTable tab = getReader();
        try {
            Assert.assertEquals(0, tab.size());
        } finally {
            tab.close();
        }

        tab = getReader().preLoad();
        try {
            Assert.assertEquals(0, tab.size());
        } finally {
            tab.close();
        }
    }

    @Test
    public void testReload() throws Exception {
        String data[] = createData();
        createTestTable(data);

        // check that keys match values
        try (SymbolTable tab = getReader()) {
            for (int i = 0; i < tab.size(); i++) {
                Assert.assertEquals(data[i], tab.value(i));
            }
            for (int i = 0; i < data.length; i++) {
                Assert.assertEquals(i, tab.getQuick(data[i]));
            }

            int LEN2 = 50;
            String data2[] = new String[LEN2];
            for (int i = 0; i < data2.length; i++) {
                data2[i] = "ABC" + i;
            }

            createTestTable(data2);
            tab.applyTx(this.tab.size(), this.tab.getIndexTxAddress());

            Assert.assertEquals(data.length + data2.length, tab.size());

            for (int i = data.length; i < tab.size(); i++) {
                Assert.assertEquals(data2[i - data.length], tab.value(i));
            }

            for (int i = data2.length - 1; i >= 0; i--) {
                Assert.assertEquals(i + data.length, tab.getQuick(data2[i]));
            }

        }
    }

    @Test
    public void testRepeatedValues() throws Exception {
        String data[] = {"VAL1", null, "VAL2", "", "VAL2", "", null, "VAL1", "VAL3"};
        int expectedKeys[] = {0, -1, 1, 2, 1, 2, -1, 0, 3};

        createTestTable(data);

        try (SymbolTable tab = getReader()) {
            for (int i = 0; i < data.length; i++) {
                Assert.assertEquals(expectedKeys[i], tab.getQuick(data[i]));
            }
        }
    }

    @Test
    public void testTruncate() throws Exception {
        String data[] = createData();
        createTestTable(data);

        try (SymbolTable tab = getWriter()) {
            Assert.assertEquals(DATA_SIZE, tab.size());
            Assert.assertTrue(tab.valueExists("TEST25"));
            tab.truncate();
            Assert.assertEquals(0, tab.size());
            Assert.assertFalse(tab.valueExists("TEST25"));
        }
    }

    @Test
    public void testValueIterator() throws Exception {
        String data[] = createData();
        createTestTable(data);

        try (SymbolTable tab = getReader()) {
            int key = 0;
            for (String s : tab.values()) {
                Assert.assertEquals(data[key++], s);
            }
        }
    }

    @Test
    public void testValueKeyMatch() throws Exception {
        String data[] = createData();
        createTestTable(data);

        // check that values match keys
        try (SymbolTable tab = getReader()) {
            for (int i = 0; i < data.length; i++) {
                Assert.assertEquals(i, tab.getQuick(data[i]));
            }
        }
    }

    private String[] createData() {
        String data[] = new String[DATA_SIZE];
        {
            for (int i = 0; i < data.length; i++) {
                data[i] = "TEST" + i;
            }
        }
        return data;
    }

    private void createTestTable(String data[]) throws JournalException {
        if (tab == null) {
            tab = new SymbolTable(DATA_SIZE, 256, 1, factory.getConfiguration().getJournalBase(), "test", JournalMode.APPEND, 0, 0, false);
        }

        for (String s : data) {
            tab.put(s);
        }
        tab.commit();
    }

    private SymbolTable getReader() throws JournalException {
        return new SymbolTable(DATA_SIZE, 256, 1, factory.getConfiguration().getJournalBase(), "test", JournalMode.READ, tab.size(), tab.getIndexTxAddress(), false);
    }

    private SymbolTable getWriter() throws JournalException {
        return new SymbolTable(DATA_SIZE, 256, 1, factory.getConfiguration().getJournalBase(), "test", JournalMode.APPEND, tab.size(), tab.getIndexTxAddress(), false);
    }
}