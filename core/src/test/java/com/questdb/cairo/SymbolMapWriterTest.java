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

package com.questdb.cairo;

import com.questdb.std.Numbers;
import com.questdb.std.Rnd;
import com.questdb.std.ex.JournalException;
import com.questdb.store.JournalMode;
import com.questdb.store.MMappedSymbolTable;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class SymbolMapWriterTest extends AbstractCairoTest {

    @Test
    public void testSimpleAdd() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10000000;
            try (SymbolMapWriter writer = new SymbolMapWriter(configuration, "x", 48, Numbers.ceilPow2(N))) {
                Rnd rnd = new Rnd();
                long prev = -1L;
                for (int i = 0; i < N; i++) {
                    CharSequence cs = rnd.nextChars(10);
                    long key = writer.put(cs);
                    Assert.assertEquals(prev + 1, key);
//                    Assert.assertEquals(key, writer.put(cs));
                    prev = key;
                }
            }
        });
    }

    @Test
    public void testSymbolTable() throws JournalException {
        int N = 10000000;
        MMappedSymbolTable tab = new MMappedSymbolTable(N, 10, 1, new File(configuration.getRoot().toString()), "test", JournalMode.APPEND, 0, 0, true, true);
        Rnd rnd = new Rnd();
        int prev = -1;
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextChars(10);
            int key = tab.put(cs);
            Assert.assertEquals(prev + 1, key);
            Assert.assertEquals(key, tab.put(cs));
            prev = key;
        }
    }
}