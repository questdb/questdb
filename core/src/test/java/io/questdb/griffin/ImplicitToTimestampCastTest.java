package io.questdb.griffin;

import org.junit.Assert;
import org.junit.Test;

/**
 * Checks that implicit string/symbol -> timestamp conversion works only for literals.
 */
public class ImplicitToTimestampCastTest extends AbstractGriffinTest {

    @Test
    public void testImplicitIntegerToSymbolConversionFails() throws Exception {
        try {
            assertQuery("",
                    "select * from balances where cust_id = 1",
                    "CREATE TABLE balances ( " +
                            "    cust_id SYMBOL, " +
                            "    ts TIMESTAMP " +
                            ") TIMESTAMP(ts) PARTITION BY DAY;",
                    "k", false, true, true
            );
            Assert.fail("error should be thrown");
        } catch (SqlException e) {
            Assert.assertEquals(e.getMessage(), "[37] unexpected argument for function: =. expected args: (STRING,STRING). actual args: (SYMBOL,INT constant)");
        }
    }

    @Test
    public void testImplicitStringLiteralToTimestampConversionWorks() throws Exception {
        assertQuery("cust_id\tts\n" +
                        "abc\t2022-03-23T00:00:00.000000Z\n",
                "select * from balances where ts = '2022-03-23'",
                "CREATE TABLE balances as (" +
                        "select cast('abc' as symbol) as cust_id, cast('2022-03-23' as timestamp) as ts from long_sequence(1) " +
                        ");", null, true, true, false);
    }

    @Test
    public void testImplicitSymbolLiteralToTimestampConversionWorks() throws Exception {
        assertQuery("cust_id\tts\n" +
                        "abc\t2022-03-23T00:00:00.000000Z\n",
                "select * from balances where ts = cast('2022-03-23' as symbol)",
                "CREATE TABLE balances as (" +
                        "select cast('abc' as symbol) as cust_id, cast('2022-03-23' as timestamp) as ts from long_sequence(1) " +
                        ");", null, true, true, false);
    }

    @Test
    public void testImplicitStringConstExpressionToTimestampCastWorks() throws Exception {
        assertQuery("cust_id\tts\n" +
                        "abc\t2022-03-23T00:00:00.000000Z\n",
                "select * from balances where ts = '2022-03-23' || ' 00:00'",
                "CREATE TABLE balances as (" +
                        "select cast('abc' as symbol) as cust_id, cast('2022-03-23' as timestamp) as ts from long_sequence(1) " +
                        ");", null, true, true, false);
    }

    @Test
    public void testImplicitSymbolConstExpressionToTimestampCastWorks() throws Exception {
        assertQuery("cust_id\tts\n" +
                        "abc\t2022-03-23T00:00:00.000000Z\n",
                "select * from balances where ts = cast(('2022-03-23' || ' 00:00') as symbol)",
                "CREATE TABLE balances as (" +
                        "select cast('abc' as symbol) as cust_id, cast('2022-03-23' as timestamp) as ts from long_sequence(1) " +
                        ");", null, true, true, false);
    }

    @Test
    public void testImplicitNonConstStringExpressionToTimestampConversionFails() throws Exception {
        try {
            assertQuery("cust_id\tts\n" +
                            "abc\t2022-03-23T00:00:00.000000Z\n",
                    "select * from balances where ts = rnd_str('2022-03-23')",
                    "CREATE TABLE balances as (" +
                            "select cast('abc' as symbol) as cust_id, cast('2022-03-23' as timestamp) as ts from long_sequence(1) " +
                            ");", null, true, true, false);
            Assert.fail("Exception should be thrown");
        } catch (SqlException e) {
            Assert.assertEquals(e.getMessage(), "[32] unexpected argument for function: =. expected args: (STRING,STRING). actual args: (TIMESTAMP,STRING)");
        }
    }

    @Test
    public void testImplicitNonConstSymbolExpressionToTimestampConversionFails() throws Exception {
        try {
            assertQuery("cust_id\tts\n" +
                            "abc\t2022-03-23T00:00:00.000000Z\n",
                    "select * from balances where ts = rnd_symbol('2022-03-23')",
                    "CREATE TABLE balances as (" +
                            "select cast('abc' as symbol) as cust_id, cast('2022-03-23' as timestamp) as ts from long_sequence(1) " +
                            ");",
                    null, true, true, false);
            Assert.fail("Exception should be thrown");
        } catch (SqlException e) {
            Assert.assertEquals(e.getMessage(), "[32] unexpected argument for function: =. expected args: (STRING,STRING). actual args: (TIMESTAMP,SYMBOL)");
        }
    }

}
