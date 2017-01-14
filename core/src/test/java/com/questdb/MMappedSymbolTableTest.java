/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

import com.questdb.ex.JournalException;
import com.questdb.ex.JournalInvalidSymbolValueException;
import com.questdb.store.MMappedSymbolTable;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class MMappedSymbolTableTest extends AbstractTest {

    private static final int DATA_SIZE = 500;
    private MMappedSymbolTable tab = null;

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
        try (MMappedSymbolTable tab = getReader().preLoad()) {
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
        try (MMappedSymbolTable tab = getReader()) {
            for (int i = 0; i < tab.size(); i++) {
                Assert.assertEquals(data[i], tab.value(i));
            }
        }
    }

    @Test(expected = JournalInvalidSymbolValueException.class)
    public void testLoudCheckKey() throws Exception {
        createTestTable(createData());
        try (MMappedSymbolTable tab = getReader()) {
            Assert.assertEquals(420, tab.get("TEST420"));
            // exception
            tab.get("650");
        }
    }

    @Test
    public void testNullValues() throws Exception {
        String data[] = {null, null};
        createTestTable(data);

        MMappedSymbolTable tab = getReader();
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
        try (MMappedSymbolTable tab = getReader()) {
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

        try (MMappedSymbolTable tab = getReader()) {
            for (int i = 0; i < data.length; i++) {
                Assert.assertEquals(expectedKeys[i], tab.getQuick(data[i]));
            }
        }
    }

    @Test
    public void testTruncate() throws Exception {
        String data[] = createData();
        createTestTable(data);

        try (MMappedSymbolTable tab = getWriter()) {
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

        try (MMappedSymbolTable tab = getReader()) {
            for (MMappedSymbolTable.Entry e : tab.values()) {
                TestUtils.assertEquals(data[e.key], e.value);
            }
        }
    }

    @Test
    public void testValueKeyMatch() throws Exception {
        String data[] = createData();
        createTestTable(data);

        // check that values match keys
        try (MMappedSymbolTable tab = getReader()) {
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
            tab = new MMappedSymbolTable(DATA_SIZE, 256, 1, factoryContainer.getFactory().getConfiguration().getJournalBase(), "test", JournalMode.APPEND, 0, 0, false, false);
        }

        for (String s : data) {
            tab.put(s);
        }
        tab.commit();
    }

    private MMappedSymbolTable getReader() throws JournalException {
        return new MMappedSymbolTable(DATA_SIZE, 256, 1, factoryContainer.getFactory().getConfiguration().getJournalBase(), "test", JournalMode.READ, tab.size(), tab.getIndexTxAddress(), false, false);
    }

    private MMappedSymbolTable getWriter() throws JournalException {
        return new MMappedSymbolTable(DATA_SIZE, 256, 1, factoryContainer.getFactory().getConfiguration().getJournalBase(), "test", JournalMode.APPEND, tab.size(), tab.getIndexTxAddress(), false, false);
    }
}