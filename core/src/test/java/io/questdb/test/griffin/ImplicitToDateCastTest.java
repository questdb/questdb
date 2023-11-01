/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.griffin;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Checks that implicit string/symbol -> date conversion works only for literals.
 */
public class ImplicitToDateCastTest extends AbstractCairoTest {

    @Test
    public void testImplicitNonConstSymbolExpressionToDateConversionFails() throws Exception {
        // we do not want to support general implicit conversion of symbol to date, implicit conversions symbol -> date are reserved for literals
        try {
            assertQuery("cust_id\tts\n" +
                            "abc\t2022-03-23T00:00:00.000000Z\n",
                    "select * from balances where date = rnd_symbol('2022-03-23')",
                    "CREATE TABLE balances as (" +
                            "select cast('abc' as symbol) as cust_id, cast('2022-03-23' as date) as date from long_sequence(1) " +
                            ");",
                    null, true, false);
            Assert.fail("Exception should be thrown");
        } catch (SqlException e) {
            Assert.assertEquals(e.getMessage(), "[34] unexpected argument for function: =. expected args: (STRING,STRING). actual args: (DATE,SYMBOL)");
        }
    }

    @Test
    public void testImplicitStringConstExpressionToDateCastWorks() throws Exception {
        assertQuery("cust_id\tdate\n" +
                        "abc\t2022-03-23T00:00:00.000Z\n",
                "select * from balances where date = '2022-03-23' || ' 00:00'",
                "CREATE TABLE balances as (" +
                        "select cast('abc' as symbol) as cust_id, cast('2022-03-23' as date) as date from long_sequence(1) " +
                        ");", null, true, false);
    }

    @Test
    public void testImplicitStringLiteralToDateConversionWorks() throws Exception {
        assertQuery("cust_id\tdate\n" +
                        "abc\t2022-03-23T00:00:00.000Z\n",
                "select * from balances where date = '2022-03-23'",
                "CREATE TABLE balances as (" +
                        "select cast('abc' as symbol) as cust_id, cast('2022-03-23' as date) as date from long_sequence(1) " +
                        ");", null, true, false);
    }

    @Test
    public void testImplicitSymbolConstExpressionToDateCastWorks() throws Exception {
        assertQuery("cust_id\tdate\n" +
                        "abc\t2022-03-23T00:00:00.000Z\n",
                "select * from balances where date = cast(('2022-03-23' || ' 00:00') as symbol)",
                "CREATE TABLE balances as (" +
                        "select cast('abc' as symbol) as cust_id, cast('2022-03-23' as date) as date from long_sequence(1) " +
                        ");", null, true, false);
    }

    @Test
    public void testImplicitSymbolLiteralToDateConversionWorks() throws Exception {
        assertQuery("cust_id\tdate\n" +
                        "abc\t2022-03-23T00:00:00.000Z\n",
                "select * from balances where date = cast('2022-03-23' as symbol)",
                "CREATE TABLE balances as (" +
                        "select cast('abc' as symbol) as cust_id, cast('2022-03-23' as date) as date from long_sequence(1) " +
                        ");", null, true, false);
    }
}
