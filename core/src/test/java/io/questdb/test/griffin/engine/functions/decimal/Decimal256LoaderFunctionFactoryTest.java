package io.questdb.test.griffin.engine.functions.decimal;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.constants.ByteConstant;
import io.questdb.griffin.engine.functions.constants.DateConstant;
import io.questdb.griffin.engine.functions.constants.Decimal128Constant;
import io.questdb.griffin.engine.functions.constants.Decimal16Constant;
import io.questdb.griffin.engine.functions.constants.Decimal256Constant;
import io.questdb.griffin.engine.functions.constants.Decimal32Constant;
import io.questdb.griffin.engine.functions.constants.Decimal64Constant;
import io.questdb.griffin.engine.functions.constants.Decimal8Constant;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.constants.ShortConstant;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.functions.decimal.Decimal256LoaderFunctionFactory;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

public class Decimal256LoaderFunctionFactoryTest {

    @Test
    public void testDecimal256LoaderFromDecimal8() {
        int fromType = ColumnType.getDecimalType(ColumnType.DECIMAL8, 2, 1);
        Function source = new Decimal8Constant((byte) 12, fromType);

        Function loader = Decimal256LoaderFunctionFactory.getInstance(source);
        Assert.assertNotSame(source, loader);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL256, 2, 1), loader.getType());
        Decimal256 value = getDecimal256(loader);
        assertRaw(value, 0, 0, 0, 12);
        Assert.assertFalse(value.isNull());
        Assert.assertSame(source, Decimal256LoaderFunctionFactory.getParent(loader));

        Function nullSource = new Decimal8Constant(Decimals.DECIMAL8_NULL, fromType);
        Function nullLoader = Decimal256LoaderFunctionFactory.getInstance(nullSource);
        Decimal256 nullValue = getDecimal256(nullLoader);
        Assert.assertTrue(nullValue.isNull());
    }

    @Test
    public void testDecimal256LoaderFromDecimal16() {
        int fromType = ColumnType.getDecimalType(ColumnType.DECIMAL16, 4, 1);
        Function source = new Decimal16Constant((short) -1234, fromType);

        Function loader = Decimal256LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL256, 4, 1), loader.getType());
        Decimal256 value = getDecimal256(loader);
        assertRaw(value, -1, -1, -1, -1234);
        Assert.assertSame(source, Decimal256LoaderFunctionFactory.getParent(loader));

        Function nullSource = new Decimal16Constant(Decimals.DECIMAL16_NULL, fromType);
        Function nullLoader = Decimal256LoaderFunctionFactory.getInstance(nullSource);
        Decimal256 nullValue = getDecimal256(nullLoader);
        Assert.assertTrue(nullValue.isNull());
    }

    @Test
    public void testDecimal256LoaderFromDecimal32() {
        int fromType = ColumnType.getDecimalType(ColumnType.DECIMAL32, 9, 2);
        Function source = new Decimal32Constant(12345678, fromType);

        Function loader = Decimal256LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL256, 9, 2), loader.getType());
        Decimal256 value = getDecimal256(loader);
        assertRaw(value, 0, 0, 0, 12345678);

        Function nullSource = new Decimal32Constant(Decimals.DECIMAL32_NULL, fromType);
        Function nullLoader = Decimal256LoaderFunctionFactory.getInstance(nullSource);
        Assert.assertTrue(getDecimal256(nullLoader).isNull());
    }

    @Test
    public void testDecimal256LoaderFromDecimal64() {
        int fromType = ColumnType.getDecimalType(ColumnType.DECIMAL64, 18, 3);
        long raw = -9_876_543_210L;
        Function source = new Decimal64Constant(raw, fromType);

        Function loader = Decimal256LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL256, 18, 3), loader.getType());
        Decimal256 value = getDecimal256(loader);
        assertRaw(value, -1, -1, -1, raw);

        Function nullSource = new Decimal64Constant(Decimals.DECIMAL64_NULL, fromType);
        Assert.assertTrue(getDecimal256(Decimal256LoaderFunctionFactory.getInstance(nullSource)).isNull());
    }

    @Test
    public void testDecimal256LoaderFromDecimal128() {
        int fromType = ColumnType.getDecimalType(ColumnType.DECIMAL128, 30, 4);
        long hi = 0x0123456789ABCDEFL;
        long lo = 0x0FEDCBA987654321L;
        Function source = new Decimal128Constant(hi, lo, fromType);

        Function loader = Decimal256LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL256, 30, 4), loader.getType());
        Decimal256 value = getDecimal256(loader);
        assertRaw(value, hi < 0 ? -1 : 0, hi < 0 ? -1 : 0, hi, lo);
        Assert.assertFalse(loader.isThreadSafe());

        Function nullSource = new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, fromType);
        Assert.assertTrue(getDecimal256(Decimal256LoaderFunctionFactory.getInstance(nullSource)).isNull());
    }

    @Test
    public void testDecimal256LoaderFromByte() {
        Function source = ByteConstant.newInstance((byte) -7);

        Function loader = Decimal256LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL256, 3, 0), loader.getType());
        Decimal256 value = getDecimal256(loader);
        assertRaw(value, -1, -1, -1, -7);
        Assert.assertSame(source, Decimal256LoaderFunctionFactory.getParent(loader));
    }

    @Test
    public void testDecimal256LoaderFromShort() {
        Function source = ShortConstant.newInstance((short) 12345);

        Function loader = Decimal256LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL256, 5, 0), loader.getType());
        Decimal256 value = getDecimal256(loader);
        assertRaw(value, 0, 0, 0, 12345);
        Assert.assertSame(source, Decimal256LoaderFunctionFactory.getParent(loader));
    }

    @Test
    public void testDecimal256LoaderFromInt() {
        Function source = IntConstant.newInstance(987654321);

        Function loader = Decimal256LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL256, 10, 0), loader.getType());
        Decimal256 value = getDecimal256(loader);
        assertRaw(value, 0, 0, 0, 987654321);

        Function nullSource = IntConstant.newInstance(Numbers.INT_NULL);
        Assert.assertTrue(getDecimal256(Decimal256LoaderFunctionFactory.getInstance(nullSource)).isNull());
    }

    @Test
    public void testDecimal256LoaderFromLong() {
        Function source = LongConstant.newInstance(-9_223_372_036_854_775L);

        Function loader = Decimal256LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL256, 19, 0), loader.getType());
        Decimal256 value = getDecimal256(loader);
        assertRaw(value, -1, -1, -1, -9_223_372_036_854_775L);

        Function nullSource = LongConstant.newInstance(Numbers.LONG_NULL);
        Assert.assertTrue(getDecimal256(Decimal256LoaderFunctionFactory.getInstance(nullSource)).isNull());
    }

    @Test
    public void testDecimal256LoaderFromDate() {
        Function source = DateConstant.newInstance(172_800_000L);

        Function loader = Decimal256LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL256, 19, 0), loader.getType());
        Decimal256 value = getDecimal256(loader);
        assertRaw(value, 0, 0, 0, 172_800_000L);

        Function nullSource = DateConstant.NULL;
        Assert.assertTrue(getDecimal256(Decimal256LoaderFunctionFactory.getInstance(nullSource)).isNull());
    }

    @Test
    public void testDecimal256LoaderFromTimestamp() {
        Function source = TimestampConstant.newInstance(123_456_789L, ColumnType.TIMESTAMP);

        Function loader = Decimal256LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL256, 19, 0), loader.getType());
        Decimal256 value = getDecimal256(loader);
        assertRaw(value, 0, 0, 0, 123_456_789L);

        Function nullSource = TimestampConstant.TIMESTAMP_MICRO_NULL;
        Assert.assertTrue(getDecimal256(Decimal256LoaderFunctionFactory.getInstance(nullSource)).isNull());
    }

    @Test
    public void testDecimal256LoaderKeepsDecimal256Functions() {
        int type = ColumnType.getDecimalType(ColumnType.DECIMAL256, 40, 6);
        Function source = new Decimal256Constant(1L, 2L, 3L, 4L, type);

        Function loader = Decimal256LoaderFunctionFactory.getInstance(source);
        Assert.assertSame(source, loader);
        Assert.assertSame(source, Decimal256LoaderFunctionFactory.getParent(loader));
    }

    @Test
    public void testDecimal256LoaderKeepsUnsupportedFunctions() {
        Function source = DoubleConstant.ONE;

        Function loader = Decimal256LoaderFunctionFactory.getInstance(source);
        Assert.assertSame(source, loader);
        Assert.assertSame(source, Decimal256LoaderFunctionFactory.getParent(loader));
    }

    private static Decimal256 getDecimal256(Function function) {
        Decimal256 sink = new Decimal256();
        function.getDecimal256(null, sink);
        return sink;
    }

    private static void assertRaw(Decimal256 value, long hh, long hl, long lh, long ll) {
        Assert.assertEquals(hh, value.getHh());
        Assert.assertEquals(hl, value.getHl());
        Assert.assertEquals(lh, value.getLh());
        Assert.assertEquals(ll, value.getLl());
    }
}

