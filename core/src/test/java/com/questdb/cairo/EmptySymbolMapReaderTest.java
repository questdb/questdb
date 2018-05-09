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

package com.questdb.cairo;

import com.questdb.common.SymbolTable;
import org.junit.Assert;
import org.junit.Test;

public class EmptySymbolMapReaderTest {
    @Test
    public void testInterface() {
        SymbolMapReader reader = EmptySymbolMapReader.INSTANCE;
        Assert.assertTrue(reader.isDeleted());
        reader.updateSymbolCount(1000);
        for (int i = 0; i < 10000; i++) {
            Assert.assertNull(reader.value(i));
        }
        Assert.assertEquals(0, reader.size());
        Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.getQuick("abc"));

        Assert.assertEquals(0, reader.getSymbolCapacity());
        Assert.assertFalse(reader.isCached());
    }
}