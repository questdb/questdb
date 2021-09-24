/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cutlass.text.types;

import io.questdb.cairo.ColumnType;
import org.junit.Assert;
import org.junit.Test;

public class UnsupportedProbeTest {
    @Test(expected = UnsupportedOperationException.class)
    public void testBadDate() {
        BadDateAdapter.INSTANCE.probe("xyz");
    }

    @Test
    public void testBadDateType() {
        Assert.assertEquals(ColumnType.DATE, BadDateAdapter.INSTANCE.getType());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testBadTimestamp() {
        BadTimestampAdapter.INSTANCE.probe("xyz");
    }

    @Test
    public void testBadTimestampType() {
        Assert.assertEquals(ColumnType.TIMESTAMP, BadTimestampAdapter.INSTANCE.getType());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testByte() {
        ByteAdapter.INSTANCE.probe("xyz");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testFloat() {
        FloatAdapter.INSTANCE.probe("xyz");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testShort() {
        ShortAdapter.INSTANCE.probe("xyz");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testString() {
        new StringAdapter(null).probe("xyz");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSymbol() {
        new SymbolAdapter(false).probe("xyz");
    }
}