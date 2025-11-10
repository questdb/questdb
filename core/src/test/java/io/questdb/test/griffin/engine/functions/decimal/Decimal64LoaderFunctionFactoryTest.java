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
import io.questdb.griffin.engine.functions.decimal.Decimal64LoaderFunctionFactory;
import io.questdb.std.Decimals;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

public class Decimal64LoaderFunctionFactoryTest {

    @Test
    public void testDecimal64LoaderFromByte() {
        Function source = ByteConstant.newInstance((byte) -8);

        Function loader = Decimal64LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL64, 3, 0), loader.getType());
        Assert.assertEquals(-8L, loader.getDecimal64(null));
    }

    @Test
    public void testDecimal64LoaderFromDate() {
        Function source = DateConstant.newInstance(86_400_000L);

        Function loader = Decimal64LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL64, 19, 0), loader.getType());
        Assert.assertEquals(86_400_000L, loader.getDecimal64(null));

        Function nullSource = DateConstant.NULL;
        Assert.assertEquals(Decimals.DECIMAL64_NULL, Decimal64LoaderFunctionFactory.getInstance(nullSource).getDecimal64(null));
    }

    @Test
    public void testDecimal64LoaderFromDecimal128() {
        int fromType = ColumnType.getDecimalType(ColumnType.DECIMAL128, 28, 4);
        long hi = 0x0000_0000_0000_0100L;
        long lo = 0x00FF_00FF_00FF_00FFL;
        Function source = new Decimal128Constant(hi, lo, fromType);

        Function loader = Decimal64LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL64, 28, 4), loader.getType());
        Assert.assertEquals(lo, loader.getDecimal64(null));
        Assert.assertFalse(loader.isThreadSafe());

        Function nullSource = new Decimal128Constant(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, fromType);
        Assert.assertEquals(Decimals.DECIMAL64_NULL, Decimal64LoaderFunctionFactory.getInstance(nullSource).getDecimal64(null));
    }

    @Test
    public void testDecimal64LoaderFromDecimal16() {
        int fromType = ColumnType.getDecimalType(ColumnType.DECIMAL16, 4, 1);
        Function source = new Decimal16Constant((short) -456, fromType);

        Function loader = Decimal64LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL64, 4, 1), loader.getType());
        Assert.assertEquals(-456L, loader.getDecimal64(null));

        Function nullSource = new Decimal16Constant(Decimals.DECIMAL16_NULL, fromType);
        Assert.assertEquals(Decimals.DECIMAL64_NULL, Decimal64LoaderFunctionFactory.getInstance(nullSource).getDecimal64(null));
    }

    @Test
    public void testDecimal64LoaderFromDecimal256() {
        int fromType = ColumnType.getDecimalType(ColumnType.DECIMAL256, 40, 6);
        Function source = new Decimal256Constant(1L, 2L, 3L, 4L, fromType);

        Function loader = Decimal64LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL64, 40, 6), loader.getType());
        Assert.assertEquals(4L, loader.getDecimal64(null));
        Assert.assertFalse(loader.isThreadSafe());

        Function nullSource = new Decimal256Constant(
                Decimals.DECIMAL256_HH_NULL,
                Decimals.DECIMAL256_HL_NULL,
                Decimals.DECIMAL256_LH_NULL,
                Decimals.DECIMAL256_LL_NULL,
                fromType
        );
        Assert.assertEquals(Decimals.DECIMAL64_NULL, Decimal64LoaderFunctionFactory.getInstance(nullSource).getDecimal64(null));
    }

    @Test
    public void testDecimal64LoaderFromDecimal32() {
        int fromType = ColumnType.getDecimalType(ColumnType.DECIMAL32, 9, 2);
        Function source = new Decimal32Constant(4_200_100, fromType);

        Function loader = Decimal64LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL64, 9, 2), loader.getType());
        Assert.assertEquals(4_200_100L, loader.getDecimal64(null));

        Function nullSource = new Decimal32Constant(Decimals.DECIMAL32_NULL, fromType);
        Assert.assertEquals(Decimals.DECIMAL64_NULL, Decimal64LoaderFunctionFactory.getInstance(nullSource).getDecimal64(null));
    }

    @Test
    public void testDecimal64LoaderFromDecimal8() {
        int fromType = ColumnType.getDecimalType(ColumnType.DECIMAL8, 2, 0);
        Function source = new Decimal8Constant((byte) 77, fromType);

        Function loader = Decimal64LoaderFunctionFactory.getInstance(source);
        Assert.assertNotSame(source, loader);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL64, 2, 0), loader.getType());
        Assert.assertEquals(77L, loader.getDecimal64(null));

        Function nullSource = new Decimal8Constant(Decimals.DECIMAL8_NULL, fromType);
        Assert.assertEquals(Decimals.DECIMAL64_NULL, Decimal64LoaderFunctionFactory.getInstance(nullSource).getDecimal64(null));
    }

    @Test
    public void testDecimal64LoaderFromInt() {
        Function source = IntConstant.newInstance(-2_000_000_001);

        Function loader = Decimal64LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL64, 10, 0), loader.getType());
        Assert.assertEquals(-2_000_000_001L, loader.getDecimal64(null));

        Function nullSource = IntConstant.newInstance(Numbers.INT_NULL);
        Assert.assertEquals(Decimals.DECIMAL64_NULL, Decimal64LoaderFunctionFactory.getInstance(nullSource).getDecimal64(null));
    }

    @Test
    public void testDecimal64LoaderFromLong() {
        Function source = LongConstant.newInstance(9_000_000_123_456_789L);

        Function loader = Decimal64LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL64, 19, 0), loader.getType());
        Assert.assertEquals(9_000_000_123_456_789L, loader.getDecimal64(null));

        Function nullSource = LongConstant.newInstance(Numbers.LONG_NULL);
        Assert.assertEquals(Decimals.DECIMAL64_NULL, Decimal64LoaderFunctionFactory.getInstance(nullSource).getDecimal64(null));
    }

    @Test
    public void testDecimal64LoaderFromShort() {
        Function source = ShortConstant.newInstance((short) 32_000);

        Function loader = Decimal64LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL64, 5, 0), loader.getType());
        Assert.assertEquals(32_000L, loader.getDecimal64(null));
    }

    @Test
    public void testDecimal64LoaderFromTimestamp() {
        Function source = TimestampConstant.newInstance(777_777_777L, ColumnType.TIMESTAMP);

        Function loader = Decimal64LoaderFunctionFactory.getInstance(source);
        Assert.assertEquals(ColumnType.getDecimalType(ColumnType.DECIMAL64, 19, 0), loader.getType());
        Assert.assertEquals(777_777_777L, loader.getDecimal64(null));

        Function nullSource = TimestampConstant.TIMESTAMP_MICRO_NULL;
        Assert.assertEquals(Decimals.DECIMAL64_NULL, Decimal64LoaderFunctionFactory.getInstance(nullSource).getDecimal64(null));
    }

    @Test
    public void testDecimal64LoaderKeepsDecimal64Functions() {
        int type = ColumnType.getDecimalType(ColumnType.DECIMAL64, 18, 2);
        Function source = new Decimal64Constant(123L, type);

        Function loader = Decimal64LoaderFunctionFactory.getInstance(source);
        Assert.assertSame(source, loader);
    }

    @Test
    public void testDecimal64LoaderKeepsUnsupportedFunctions() {
        Function source = DoubleConstant.ONE;

        Function loader = Decimal64LoaderFunctionFactory.getInstance(source);
        Assert.assertSame(source, loader);
    }
}
