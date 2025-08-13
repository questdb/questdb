/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin.engine.functions.constants;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.constants.ByteConstant;
import io.questdb.griffin.engine.functions.constants.CharConstant;
import io.questdb.griffin.engine.functions.constants.DateConstant;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.constants.FloatConstant;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.constants.Long256NullConstant;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.constants.NullBinConstant;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.griffin.engine.functions.constants.ShortConstant;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.constants.SymbolConstant;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

public class NullConstantTest {

    @Test
    public void testConstant() {
        Function constant = NullConstant.NULL;

        Assert.assertEquals(ColumnType.NULL, constant.getType());
        Assert.assertTrue(constant.isConstant());
        Assert.assertFalse(constant.isRuntimeConstant());
        Assert.assertTrue(constant.supportsRandomAccess());
        Assert.assertFalse(constant.isUndefined());

        Assert.assertEquals(TableUtils.NULL_LEN, constant.extendedOps().getArrayLength());
        Assert.assertEquals(StrConstant.NULL.getStrLen(null), constant.getStrLen(null));

        Assert.assertEquals(IntConstant.NULL.getInt(null), constant.getInt(null));
        Assert.assertEquals(StrConstant.NULL.getStrA(null), constant.getStrA(null));
        Assert.assertEquals(StrConstant.NULL.getStrB(null), constant.getStrB(null));
        Assert.assertEquals(SymbolConstant.NULL.getSymbol(null), constant.getSymbol(null));
        Assert.assertEquals(SymbolConstant.NULL.getSymbolB(null), constant.getSymbolB(null));
        Assert.assertEquals(LongConstant.NULL.getLong(null), constant.getLong(null));
        Assert.assertEquals(DateConstant.NULL.getDate(null), constant.getDate(null));
        Assert.assertEquals(TimestampConstant.TIMESTAMP_MICRO_NULL.getTimestamp(null), constant.getTimestamp(null));
        Assert.assertEquals(TimestampConstant.TIMESTAMP_NANO_NULL.getTimestamp(null), constant.getTimestamp(null));
        Assert.assertEquals(ByteConstant.ZERO.getByte(null), constant.getByte(null));
        Assert.assertEquals(ShortConstant.ZERO.getShort(null), constant.getShort(null));
        Assert.assertEquals(CharConstant.ZERO.getChar(null), constant.getChar(null));
        Assert.assertEquals(BooleanConstant.FALSE.getBool(null), constant.getBool(null));
        Assert.assertEquals(DoubleConstant.NULL.getDouble(null), constant.getDouble(null), 0.1);
        Assert.assertEquals(FloatConstant.NULL.getFloat(null), constant.getFloat(null), 0.1);
        Assert.assertEquals(NullBinConstant.INSTANCE.getBin(null), constant.getBin(null));
        Assert.assertEquals(NullBinConstant.INSTANCE.getBinLen(null), constant.getBinLen(null));
        Assert.assertEquals(Long256NullConstant.INSTANCE.getLong(null), constant.getLong(null));
        Assert.assertEquals(Long256NullConstant.INSTANCE.getLong256A(null), constant.getLong256A(null));
        Assert.assertEquals(Long256NullConstant.INSTANCE.getLong256B(null), constant.getLong256B(null));
        Assert.assertEquals(GeoHashes.NULL, constant.getGeoLong(null));
        Assert.assertEquals(GeoHashes.BYTE_NULL, constant.getGeoByte(null));
        Assert.assertEquals(GeoHashes.INT_NULL, constant.getGeoInt(null));
        Assert.assertEquals(GeoHashes.SHORT_NULL, constant.getGeoShort(null));
        Assert.assertNull(constant.extendedOps().getRecord(null));

        StringSink sink = new StringSink();
        constant.getLong256(null, sink);
        Assert.assertEquals(0, sink.length());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetRecordCursorFactory() {
        NullConstant.NULL.getRecordCursorFactory();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetStrBWithIndex() {
        NullConstant.NULL.getStrB(null, 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetStrLenWithIndex() {
        NullConstant.NULL.getStrLen(null, 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetStrWithIndex() {
        NullConstant.NULL.getStrA(null, 0);
    }
}
