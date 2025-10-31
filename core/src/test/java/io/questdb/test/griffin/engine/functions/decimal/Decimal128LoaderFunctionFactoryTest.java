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
import io.questdb.griffin.engine.functions.decimal.Decimal128LoaderFunctionFactory;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimals;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

public class Decimal128LoaderFunctionFactoryTest {

    @Test
    public void testDecimal128LoaderFromDecimal8() {
        int fromType = ColumnType.getDecimalType(ColumnType.DECIMAL8, 2, 1);
        Function source = new Decimal8Constant((byte) 45, fromType);

        Function loader = Decimal128LoaderFunctionFactory.getInstance(source);
        Assert.assertNotSame(source, loader);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL128, 2, 1), loader.getType());
        Decimal128 value = getDecimal128(loader);
        assertRaw(value, 0, 45);

        Function nullSource = new Decimal8Constant(Decimals.DECIMAL8_NULL, fromType);
        Assert.assertTrue(getDecimal128(Decimal128LoaderFunctionFactory.getInstance(nullSource)).isNull());
    }

    @Test
    public void testDecimal128LoaderFromDecimal16() {
        int fromType = ColumnType.getDecimalType(ColumnType.DECIMAL16, 4, 1);
        Function source = new Decimal16Constant((short) -999, fromType);

        Function loader = Decimal128LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL128, 4, 1), loader.getType());
        Decimal128 value = getDecimal128(loader);
        assertRaw(value, -1, -999);

        Function nullSource = new Decimal16Constant(Decimals.DECIMAL16_NULL, fromType);
        Assert.assertTrue(getDecimal128(Decimal128LoaderFunctionFactory.getInstance(nullSource)).isNull());
    }

    @Test
    public void testDecimal128LoaderFromDecimal32() {
        int fromType = ColumnType.getDecimalType(ColumnType.DECIMAL32, 9, 2);
        Function source = new Decimal32Constant(1_234_567, fromType);

        Function loader = Decimal128LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL128, 9, 2), loader.getType());
        Decimal128 value = getDecimal128(loader);
        assertRaw(value, 0, 1_234_567);

        Function nullSource = new Decimal32Constant(Decimals.DECIMAL32_NULL, fromType);
        Assert.assertTrue(getDecimal128(Decimal128LoaderFunctionFactory.getInstance(nullSource)).isNull());
    }

    @Test
    public void testDecimal128LoaderFromDecimal64() {
        int fromType = ColumnType.getDecimalType(ColumnType.DECIMAL64, 18, 4);
        long raw = -123_456_789L;
        Function source = new Decimal64Constant(raw, fromType);

        Function loader = Decimal128LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL128, 18, 4), loader.getType());
        Decimal128 value = getDecimal128(loader);
        assertRaw(value, -1, raw);

        Function nullSource = new Decimal64Constant(Decimals.DECIMAL64_NULL, fromType);
        Assert.assertTrue(getDecimal128(Decimal128LoaderFunctionFactory.getInstance(nullSource)).isNull());
    }

    @Test
    public void testDecimal128LoaderFromDecimal256() {
        int fromType = ColumnType.getDecimalType(ColumnType.DECIMAL256, 40, 6);
        Function source = new Decimal256Constant(1L, -2L, 3L, -4L, fromType);

        Function loader = Decimal128LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL128, 40, 6), loader.getType());
        Decimal128 value = getDecimal128(loader);
        assertRaw(value, 3L, -4L);
        Assert.assertFalse(loader.isThreadSafe());

        Function nullSource = new Decimal256Constant(
                Decimals.DECIMAL256_HH_NULL,
                Decimals.DECIMAL256_HL_NULL,
                Decimals.DECIMAL256_LH_NULL,
                Decimals.DECIMAL256_LL_NULL,
                fromType
        );
        Assert.assertTrue(getDecimal128(Decimal128LoaderFunctionFactory.getInstance(nullSource)).isNull());
    }

    @Test
    public void testDecimal128LoaderFromByte() {
        Function source = ByteConstant.newInstance((byte) -11);

        Function loader = Decimal128LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL128, 3, 0), loader.getType());
        Decimal128 value = getDecimal128(loader);
        assertRaw(value, -1, -11);
    }

    @Test
    public void testDecimal128LoaderFromShort() {
        Function source = ShortConstant.newInstance((short) 123);

        Function loader = Decimal128LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL128, 5, 0), loader.getType());
        Decimal128 value = getDecimal128(loader);
        assertRaw(value, 0, 123);
    }

    @Test
    public void testDecimal128LoaderFromInt() {
        Function source = IntConstant.newInstance(1_000_001);

        Function loader = Decimal128LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL128, 10, 0), loader.getType());
        Decimal128 value = getDecimal128(loader);
        assertRaw(value, 0, 1_000_001);

        Function nullSource = IntConstant.newInstance(Numbers.INT_NULL);
        Assert.assertTrue(getDecimal128(Decimal128LoaderFunctionFactory.getInstance(nullSource)).isNull());
    }

    @Test
    public void testDecimal128LoaderFromLong() {
        Function source = LongConstant.newInstance(-9_000_000_000L);

        Function loader = Decimal128LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL128, 19, 0), loader.getType());
        Decimal128 value = getDecimal128(loader);
        assertRaw(value, -1, -9_000_000_000L);

        Function nullSource = LongConstant.newInstance(Numbers.LONG_NULL);
        Assert.assertTrue(getDecimal128(Decimal128LoaderFunctionFactory.getInstance(nullSource)).isNull());
    }

    @Test
    public void testDecimal128LoaderFromDate() {
        Function source = DateConstant.newInstance(604_800_000L);

        Function loader = Decimal128LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL128, 19, 0), loader.getType());
        Decimal128 value = getDecimal128(loader);
        assertRaw(value, 0, 604_800_000L);

        Function nullSource = DateConstant.NULL;
        Assert.assertTrue(getDecimal128(Decimal128LoaderFunctionFactory.getInstance(nullSource)).isNull());
    }

    @Test
    public void testDecimal128LoaderFromTimestamp() {
        Function source = TimestampConstant.newInstance(42_000_000L, ColumnType.TIMESTAMP);

        Function loader = Decimal128LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL128, 19, 0), loader.getType());
        Decimal128 value = getDecimal128(loader);
        assertRaw(value, 0, 42_000_000L);

        Function nullSource = TimestampConstant.TIMESTAMP_MICRO_NULL;
        Assert.assertTrue(getDecimal128(Decimal128LoaderFunctionFactory.getInstance(nullSource)).isNull());
    }

    @Test
    public void testDecimal128LoaderKeepsDecimal128Functions() {
        int type = ColumnType.getDecimalType(ColumnType.DECIMAL128, 28, 3);
        Function source = new Decimal128Constant(1L, 2L, type);

        Function loader = Decimal128LoaderFunctionFactory.getInstance(source);
        Assert.assertSame(source, loader);
    }

    @Test
    public void testDecimal128LoaderKeepsUnsupportedFunctions() {
        Function source = DoubleConstant.ONE;

        Function loader = Decimal128LoaderFunctionFactory.getInstance(source);
        Assert.assertSame(source, loader);
    }

    private static Decimal128 getDecimal128(Function function) {
        Decimal128 sink = new Decimal128();
        function.getDecimal128(null, sink);
        return sink;
    }

    private static void assertRaw(Decimal128 value, long high, long low) {
        Assert.assertEquals(high, value.getHigh());
        Assert.assertEquals(low, value.getLow());
    }
}

