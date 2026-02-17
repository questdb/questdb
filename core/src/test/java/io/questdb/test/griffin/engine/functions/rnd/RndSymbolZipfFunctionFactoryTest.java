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
import io.questdb.griffin.engine.functions.rnd.RndSymbolZipfFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class RndSymbolZipfFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testBasicFunction() throws Exception {
        execute("""
                create table abc as (
                  select rnd_symbol_zipf('1AAPL', '2MSFT', '2GOOGL', '4TSLA', '5AMZN', 1.5) as testCol
                  from long_sequence(100)
                )
                """);

        // Should return all 5 symbols, but with different frequencies (AAPL most common)
        assertSql(
                """
                        testCol	cnt
                        1AAPL	53
                        2GOOGL	12
                        2MSFT	20
                        4TSLA	9
                        5AMZN	6
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
                  select rnd_symbol_zipf('A', 'B', 'C', 'D', 'E', 2.0) as testCol
                  from long_sequence(1000)
                )
                """);

        // The first symbol should have significantly more occurrences
        assertSql("""
                        testCol\tcnt
                        A\t666
                        B\t185
                        C\t76
                        D\t49
                        E\t24
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
                          functions: [rnd_symbol_zipf([AAPL,MSFT,GOOGL],1.5)]
                            long_sequence count: 10
                        """,
                "explain select rnd_symbol_zipf('AAPL', 'MSFT', 'GOOGL', 1.5) from long_sequence(10)"
        );
    }

    @Test
    public void testExplainPlanLowAlpha() throws Exception {
        assertSql(
                """
                        QUERY PLAN
                        VirtualRecord
                          functions: [rnd_symbol_zipf([X,Y,Z],0.5)]
                            long_sequence count: 3
                        """,
                "explain select rnd_symbol_zipf('X', 'Y', 'Z', 0.5) from long_sequence(3)"
        );
    }

    @Test
    public void testExplainPlanTwoSymbols() throws Exception {
        assertSql(
                """
                        QUERY PLAN
                        VirtualRecord
                          functions: [rnd_symbol_zipf([A,B],2.0)]
                            long_sequence count: 5
                        """,
                "explain select rnd_symbol_zipf('A', 'B', 2.0) from long_sequence(5)"
        );
    }

    @Test
    public void testHighAlphaConcentration() throws Exception {
        execute("""
                create table abc as (
                  select rnd_symbol_zipf('1AAPL', '2MSFT', '3GOOGL', '4TSLA', '5AMZN', 5.0) as testCol
                  from long_sequence(100)
                )
                """);

        // With alpha=5.0, AAPL should dominate
        assertSql("""
                testCol\tcnt
                1AAPL\t98
                2MSFT\t2
                """, "select testCol, count() as cnt from abc order by 1");
    }

    @Test
    public void testInsufficientArgs() throws Exception {
        // Need at least 2 arguments: one symbol and alpha
        assertException(
                "select rnd_symbol_zipf('AAPL') as testCol from long_sequence(10)",
                7,
                "expected at least 2 arguments: symbol list and alpha parameter"
        );
    }

    @Test
    public void testLowAlphaModerateDist() throws Exception {
        execute("""
                create table abc as (
                  select rnd_symbol_zipf('1AAPL', '2MSFT', '3GOOGL', '4TSLA', '5AMZN', 0.5) as testCol
                  from long_sequence(100)
                )
                """);

        // With alpha=0.5, distribution should be more even
        assertSql("""
                testCol\tcnt
                1AAPL\t26
                2MSFT\t22
                3GOOGL\t20
                4TSLA\t14
                5AMZN\t18
                """, "select testCol, count() as cnt from abc order by 1");
    }

    @Test
    public void testNegativeAlpha() throws Exception {
        assertException(
                "select rnd_symbol_zipf('AAPL', 'MSFT', -1.0) as testCol from long_sequence(10)",
                39,
                "alpha must be positive"
        );
    }

    @Test
    public void testTwoSymbols() throws Exception {
        execute("""
                create table abc as (
                  select rnd_symbol_zipf('A', 'B', 1.0) as testCol
                  from long_sequence(100)
                )
                """);

        assertSql("""
                testCol\tcnt
                A\t63
                B\t37
                """, "select testCol, count() as cnt from abc order by 1");
    }

    @Test
    public void testTwoSymbolsByteAlpha() throws Exception {
        execute("""
                create table abc as (
                  select rnd_symbol_zipf('A', 'B', 1::byte) as testCol
                  from long_sequence(100)
                )
                """);

        assertSql("""
                testCol\tcnt
                A\t63
                B\t37
                """, "select testCol, count() as cnt from abc order by 1");
    }

    @Test
    public void testTwoSymbolsFloatAlpha() throws Exception {
        execute("""
                create table abc as (
                  select rnd_symbol_zipf('A', 'B', 1::float) as testCol
                  from long_sequence(100)
                )
                """);

        assertSql("""
                testCol\tcnt
                A\t63
                B\t37
                """, "select testCol, count() as cnt from abc order by 1");
    }

    @Test
    public void testTwoSymbolsIntAlpha() throws Exception {
        execute("""
                create table abc as (
                  select rnd_symbol_zipf('A', 'B', 1) as testCol
                  from long_sequence(100)
                )
                """);

        assertSql("""
                testCol\tcnt
                A\t63
                B\t37
                """, "select testCol, count() as cnt from abc order by 1");
    }

    @Test
    public void testTwoSymbolsLongAlpha() throws Exception {
        execute("""
                create table abc as (
                  select rnd_symbol_zipf('A', 'B', 1L) as testCol
                  from long_sequence(100)
                )
                """);

        assertSql("""
                testCol\tcnt
                A\t63
                B\t37
                """, "select testCol, count() as cnt from abc order by 1");
    }

    @Test
    public void testTwoSymbolsShortAlpha() throws Exception {
        execute("""
                create table abc as (
                  select rnd_symbol_zipf('A', 'B', 1::short) as testCol
                  from long_sequence(100)
                )
                """);

        assertSql("""
                testCol\tcnt
                A\t63
                B\t37
                """, "select testCol, count() as cnt from abc order by 1");
    }

    @Test
    public void testZeroAlpha() throws Exception {
        assertException(
                "select rnd_symbol_zipf('AAPL', 'MSFT', 0.0) as testCol from long_sequence(10)",
                39,
                "alpha must be positive"
        );
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RndSymbolZipfFunctionFactory();
    }
}
