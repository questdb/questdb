/*+*****************************************************************************
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

package io.questdb.test.griffin.engine.functions.conditional;

import io.questdb.cairo.TableUtils;
import io.questdb.griffin.engine.functions.conditional.NullCaseFunction;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

/**
 * A CASE whose every branch is NULL reads like the NULL constant. A cast such as
 * {@code (CASE WHEN x > 1 THEN null END)::SYMBOL} reads it through one of these getters.
 */
public class NullCaseFunctionTest {
    private static final NullCaseFunction function = new NullCaseFunction(null);
    private static final NullConstant nullConstant = NullConstant.NULL;

    @Test
    public void testGetDecimal() {
        final Decimal128 expected128 = new Decimal128();
        final Decimal128 actual128 = new Decimal128();
        nullConstant.getDecimal128(null, expected128);
        function.getDecimal128(null, actual128);
        Assert.assertEquals(expected128, actual128);

        final Decimal256 expected256 = new Decimal256();
        final Decimal256 actual256 = new Decimal256();
        nullConstant.getDecimal256(null, expected256);
        function.getDecimal256(null, actual256);
        Assert.assertEquals(expected256, actual256);

        Assert.assertEquals(nullConstant.getDecimal8(null), function.getDecimal8(null));
        Assert.assertEquals(nullConstant.getDecimal16(null), function.getDecimal16(null));
        Assert.assertEquals(nullConstant.getDecimal32(null), function.getDecimal32(null));
        Assert.assertEquals(nullConstant.getDecimal64(null), function.getDecimal64(null));
    }

    @Test
    public void testGetFixedSize() {
        // the NULL sentinels of each type; BOOLEAN, BYTE, SHORT and CHAR have none and read 0
        Assert.assertFalse(function.getBool(null));
        Assert.assertEquals(0, function.getByte(null));
        Assert.assertEquals(0, function.getChar(null));
        Assert.assertEquals(0, function.getShort(null));
        Assert.assertEquals(Numbers.INT_NULL, function.getInt(null));
        Assert.assertEquals(Numbers.LONG_NULL, function.getLong(null));
        Assert.assertTrue(Float.isNaN(function.getFloat(null)));
        Assert.assertTrue(Double.isNaN(function.getDouble(null)));
        Assert.assertEquals(Numbers.LONG_NULL, function.getDate(null));
        Assert.assertEquals(Numbers.LONG_NULL, function.getTimestamp(null));
        Assert.assertEquals(Numbers.IPv4_NULL, function.getIPv4(null));
        Assert.assertEquals(Numbers.LONG_NULL, function.getLong128Hi(null));
        Assert.assertEquals(Numbers.LONG_NULL, function.getLong128Lo(null));
        // the types the sentinel table leaves out read as the NULL constant reads them
        Assert.assertEquals(nullConstant.getGeoByte(null), function.getGeoByte(null));
        Assert.assertEquals(nullConstant.getGeoShort(null), function.getGeoShort(null));
        Assert.assertEquals(nullConstant.getGeoInt(null), function.getGeoInt(null));
        Assert.assertEquals(nullConstant.getGeoLong(null), function.getGeoLong(null));
    }

    @Test
    public void testGetLong256() {
        final StringSink expected = new StringSink();
        final StringSink actual = new StringSink();
        nullConstant.getLong256(null, expected);
        function.getLong256(null, actual);
        Assert.assertEquals(expected.toString(), actual.toString());
        Assert.assertEquals(nullConstant.getLong256A(null), function.getLong256A(null));
        Assert.assertEquals(nullConstant.getLong256B(null), function.getLong256B(null));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetRecordCursorFactory() {
        function.getRecordCursorFactory();
    }

    @Test
    public void testGetVarSize() {
        Assert.assertEquals(nullConstant.getArray(null), function.getArray(null));
        Assert.assertEquals(nullConstant.getBin(null), function.getBin(null));
        Assert.assertEquals(nullConstant.getBinLen(null), function.getBinLen(null));
        Assert.assertEquals(nullConstant.getInterval(null), function.getInterval(null));
        Assert.assertNull(function.getStrA(null));
        Assert.assertNull(function.getStrB(null));
        Assert.assertEquals(TableUtils.NULL_LEN, function.getStrLen(null));
        Assert.assertNull(function.getSymbol(null));
        Assert.assertNull(function.getSymbolB(null));
        Assert.assertNull(function.getVarcharA(null));
        Assert.assertNull(function.getVarcharB(null));
        Assert.assertEquals(TableUtils.NULL_LEN, function.getVarcharSize(null));
    }
}
