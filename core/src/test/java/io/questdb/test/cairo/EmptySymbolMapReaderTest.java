/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.cairo;

import io.questdb.cairo.EmptySymbolMapReader;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.sql.SymbolTable;
import org.junit.Assert;
import org.junit.Test;

public class EmptySymbolMapReaderTest {
    @Test
    public void testInterface() {
        SymbolMapReader reader = EmptySymbolMapReader.INSTANCE;
        Assert.assertTrue(reader.isDeleted());
        reader.updateSymbolCount(1000);
        for (int i = 0; i < 10000; i++) {
            Assert.assertNull(reader.valueOf(i));
        }
        Assert.assertEquals(0, reader.getSymbolCount());
        Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.keyOf("abc"));

        Assert.assertEquals(0, reader.getSymbolCapacity());
        Assert.assertFalse(reader.isCached());
    }
}