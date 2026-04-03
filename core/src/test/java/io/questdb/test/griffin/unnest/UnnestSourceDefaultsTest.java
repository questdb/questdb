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

package io.questdb.test.griffin.unnest;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.engine.join.OrdinalityUnnestSource;
import io.questdb.griffin.engine.join.UnnestSource;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verifies that all default methods on {@link UnnestSource} throw
 * {@link UnsupportedOperationException}. Uses {@link OrdinalityUnnestSource}
 * as the test subject since it only overrides getLong, getColumnCount, and init.
 */
public class UnnestSourceDefaultsTest {
    private final UnnestSource source = new OrdinalityUnnestSource();

    @Test
    public void testGetArrayThrows() {
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> source.getArray(0, 0, ColumnType.DOUBLE));
    }

    @Test
    public void testGetBoolThrows() {
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> source.getBool(0, 0));
    }

    @Test
    public void testGetDateThrows() {
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> source.getDate(0, 0));
    }

    @Test
    public void testGetDoubleThrows() {
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> source.getDouble(0, 0));
    }

    @Test
    public void testGetIntThrows() {
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> source.getInt(0, 0));
    }

    @Test
    public void testGetShortThrows() {
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> source.getShort(0, 0));
    }

    @Test
    public void testGetStrAThrows() {
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> source.getStrA(0, 0));
    }

    @Test
    public void testGetStrBThrows() {
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> source.getStrB(0, 0));
    }

    @Test
    public void testGetStrLenThrows() {
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> source.getStrLen(0, 0));
    }

    @Test
    public void testGetTimestampThrows() {
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> source.getTimestamp(0, 0));
    }

    @Test
    public void testGetVarcharAThrows() {
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> source.getVarcharA(0, 0));
    }

    @Test
    public void testGetVarcharBThrows() {
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> source.getVarcharB(0, 0));
    }

    @Test
    public void testGetVarcharSizeThrows() {
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> source.getVarcharSize(0, 0));
    }
}
