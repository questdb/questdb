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

package io.questdb.test.griffin.engine.functions.rnd;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.rnd.RndSymbolZipfNFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class RndSymbolZipfNFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testBasicFunction() throws Exception {
        execute("""
                create table abc as (
                  select rnd_symbol_zipf(5, 1.5) as testCol
                  from long_sequence(100)
                )
                """);

        // Should return all 5 symbols, but with different frequencies (sym0 most common)
        assertSql(
                """
                        testCol	cnt
                        sym0	53
                        sym1	20
                        sym2	12
                        sym3	9
                        sym4	6
                        """,
                """
                        select testCol, count() as cnt from abc order by 1
                        """
        );
    }

    @Test
    public void testCountsSkewedToFirstSymbol() throws Exception {
        execute("""
                create table abc as (
                  select rnd_symbol_zipf(5, 2.0) as testCol
                  from long_sequence(1000)
                )
                """);

        // The first symbol should have significantly more occurrences
        assertSql("""
                        testCol\tcnt
                        sym0\t666
                        sym1\t185
                        sym2\t76
                        sym3\t49
                        sym4\t24
                        """,
                "select testCol, count() as cnt from abc order by 1"
        );
    }

    @Test
    public void testExplainPlan() throws Exception {
        assertSql(
                """
                        QUERY PLAN
                        VirtualRecord
                          functions: [rnd_symbol_zipf(10,1.5)]
                            long_sequence count: 100
                        """,
                "explain select rnd_symbol_zipf(10, 1.5) from long_sequence(100)"
        );
    }

    @Test
    public void testExplainPlanLowAlpha() throws Exception {
        assertSql(
                """
                        QUERY PLAN
                        VirtualRecord
                          functions: [rnd_symbol_zipf(3,0.5)]
                            long_sequence count: 10
                        """,
                "explain select rnd_symbol_zipf(3, 0.5) from long_sequence(10)"
        );
    }

    @Test
    public void testExplainPlanTwoSymbols() throws Exception {
        assertSql(
                """
                        QUERY PLAN
                        VirtualRecord
                          functions: [rnd_symbol_zipf(2,2.0)]
                            long_sequence count: 5
                        """,
                "explain select rnd_symbol_zipf(2, 2.0) from long_sequence(5)"
        );
    }

    @Test
    public void testHighAlphaConcentration() throws Exception {
        execute("""
                create table abc as (
                  select rnd_symbol_zipf(5, 5.0) as testCol
                  from long_sequence(100)
                )
                """);

        // With alpha=5.0, sym0 should dominate
        assertSql("""
                testCol\tcnt
                sym0\t98
                sym1\t2
                """, "select testCol, count() as cnt from abc order by 1");
    }

    @Test
    public void testInsufficientArgs() throws Exception {
        // Need exactly 2 arguments: symbol count and alpha
        assertException(
                "select rnd_symbol_zipf(10) as testCol from long_sequence(10)",
                7,
                "expected at least 2 arguments: symbol list and alpha parameter"
        );
    }

    @Test
    public void testLargeSymbolCount() throws Exception {
        execute("""
                create table abc as (
                  select rnd_symbol_zipf(1000, 2.0) as testCol
                  from long_sequence(10000)
                )
                """);

        // Verify we get multiple distinct symbols and first symbol is most common
        assertSql("""
                        testCol	cnt
                        sym0	6078
                        """,
                """
                        select testCol, count() as cnt from abc order by 2 desc limit 1
                        """);
    }

    @Test
    public void testLowAlphaModerateDist() throws Exception {
        execute("""
                create table abc as (
                  select rnd_symbol_zipf(5, 0.5) as testCol
                  from long_sequence(100)
                )
                """);

        // With alpha=0.5, distribution should be more even
        assertSql("""
                testCol\tcnt
                sym0\t26
                sym1\t22
                sym2\t20
                sym3\t14
                sym4\t18
                """, "select testCol, count() as cnt from abc order by 1");
    }

    @Test
    public void testNanAlpha() throws Exception {
        assertException(
                "select rnd_symbol_zipf(1_000_000, nan)",
                23,
                "non-null value expected"
        );
    }

    @Test
    public void testNegativeAlpha() throws Exception {
        assertException(
                "select rnd_symbol_zipf(5, -1.0) as testCol from long_sequence(10)",
                26,
                "alpha must be positive"
        );
    }

    @Test
    public void testNegativeSymbolCount() throws Exception {
        assertException(
                "select rnd_symbol_zipf(-5, 1.5) as testCol from long_sequence(10)",
                23,
                "symbol count must be positive"
        );
    }

    @Test
    public void testTwoSymbols() throws Exception {
        execute("""
                create table abc as (
                  select rnd_symbol_zipf(2, 1.0) as testCol
                  from long_sequence(100)
                )
                """);

        assertSql("""
                testCol\tcnt
                sym0\t63
                sym1\t37
                """, "select testCol, count() as cnt from abc order by 1");
    }

    @Test
    public void testZeroAlpha() throws Exception {
        assertException(
                "select rnd_symbol_zipf(5, 0.0) as testCol from long_sequence(10)",
                26,
                "alpha must be positive"
        );
    }

    @Test
    public void testZeroSymbolCount() throws Exception {
        assertException(
                "select rnd_symbol_zipf(0, 1.5) as testCol from long_sequence(10)",
                23,
                "symbol count must be positive"
        );
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RndSymbolZipfNFunctionFactory();
    }
}
