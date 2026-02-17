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
import io.questdb.griffin.engine.functions.rnd.RndSymbolWeightedFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class RndSymbolWeightedFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testBasicFunction() throws Exception {
        execute("""
                create table abc as (
                  select rnd_symbol_weighted('AAPL', 50, 'MSFT', 30, 'GOOGL', 15, 'TSLA', 5) as testCol
                  from long_sequence(100)
                )
                """);

        // Weights: AAPL=50, MSFT=30, GOOGL=15, TSLA=5 (total=100)
        // Expected distribution: AAPL=50%, MSFT=30%, GOOGL=15%, TSLA=5%
        assertSql("""
                        testCol\tcnt
                        AAPL\t46
                        GOOGL\t19
                        MSFT\t29
                        TSLA\t6
                        """,
                "select testCol, count() as cnt from abc order by 1"
        );
    }

    @Test
    public void testDecimalWeights() throws Exception {
        execute("""
                create table abc as (
                  select rnd_symbol_weighted('A', 2.5, 'B', 1.5, 'C', 1.0) as testCol
                  from long_sequence(100)
                )
                """);

        // Weights sum to 5.0: A=50%, B=30%, C=20%
        assertSql("""
                        testCol	cnt
                        A	46
                        B	29
                        C	25
                        """,
                "select testCol, count() as cnt from abc order by 1"
        );
    }

    @Test
    public void testEqualWeights() throws Exception {
        execute("""
                create table abc as (
                  select rnd_symbol_weighted('A', 1, 'B', 1, 'C', 1) as testCol
                  from long_sequence(99)
                )
                """);

        // Equal weights should produce roughly equal distribution
        assertSql("""
                        testCol	cnt
                        A	27
                        B	35
                        C	37
                        """,
                "select testCol, count() as cnt from abc order by 1"
        );
    }

    @Test
    public void testHighlySkewedWeights() throws Exception {
        execute("""
                create table abc as (
                  select rnd_symbol_weighted('A', 95, 'B', 3, 'C', 2) as testCol
                  from long_sequence(100)
                )
                """);

        // A should dominate with 95% probability
        assertSql("""
                        testCol	cnt
                        A	94
                        B	5
                        C	1
                        """,
                "select testCol, count() as cnt from abc order by 1"
        );
    }

    @Test
    public void testInsufficientArgs() {
        assertFailure("[7] expected at least one symbol-weight pair",
                "select rnd_symbol_weighted('AAPL') as testCol from long_sequence(10)");
    }

    @Test
    public void testMixedZeroAndNonZeroWeights() throws Exception {
        execute("""
                create table abc as (
                  select rnd_symbol_weighted('A', 100, 'B', 0, 'C', 0) as testCol
                  from long_sequence(50)
                )
                """);

        // Only A should appear (weight=100, others=0)
        assertSql("""
                        testCol\tcnt
                        A\t50
                        """,
                "select testCol, count() as cnt from abc order by 1"
        );
    }

    @Test
    public void testNegativeWeight() {
        assertFailure("[47] weight must be non-negative",
                "select rnd_symbol_weighted('AAPL', 50, 'MSFT', -10) as testCol from long_sequence(10)");
    }

    @Test
    public void testOddNumberOfArgs() {
        assertFailure("[7] expected even number of arguments (symbol-weight pairs)",
                "select rnd_symbol_weighted('AAPL', 50, 'MSFT') as testCol from long_sequence(10)");
    }

    @Test
    public void testTwoSymbols() throws Exception {
        execute("""
                create table abc as (
                  select rnd_symbol_weighted('A', 70, 'B', 30) as testCol
                  from long_sequence(100)
                )
                """);

        assertSql("""
                        testCol\tcnt
                        A\t68
                        B\t32
                        """,
                "select testCol, count() as cnt from abc order by 1"
        );
    }

    @Test
    public void testZeroWeights() throws Exception {
        // All zero weights should fail at runtime when constructing the function
        assertException(
                "select rnd_symbol_weighted('A', 0, 'B', 0) as testCol from long_sequence(10)",
                7,
                "total weight must be positive"
        );
    }

    @Test
    public void testNonConstantSymbol() {
        // Symbol must be constant
        assertFailure("[27] constant expected",
                "select rnd_symbol_weighted(cast(x as string), 50) as testCol from long_sequence(10)");
    }

    @Test
    public void testNullSymbol() {
        // Symbol must not be null
        assertFailure("[27] STRING constant expected",
                "select rnd_symbol_weighted(cast(null as string), 50) as testCol from long_sequence(10)");
    }

    @Test
    public void testNonConstantWeight() {
        // Weight must be constant
        assertFailure("[35] constant weight expected",
                "select rnd_symbol_weighted('AAPL', x) as testCol from long_sequence(10)");
    }

    @Test
    public void testNonConstantSecondSymbol() {
        // Second symbol must also be constant
        assertFailure("[39] constant expected",
                "select rnd_symbol_weighted('AAPL', 50, cast(x as string), 30) as testCol from long_sequence(10)");
    }

    @Test
    public void testNonConstantSecondWeight() {
        // Second weight must also be constant
        assertFailure("[47] constant weight expected",
                "select rnd_symbol_weighted('AAPL', 50, 'MSFT', x) as testCol from long_sequence(10)");
    }

    @Test
    public void testExplainPlan() throws Exception {
        assertSql(
                """
                        QUERY PLAN
                        VirtualRecord
                          functions: [rnd_symbol_weighted(AAPL,50.0,MSFT,30.0,GOOGL,20.0)]
                            long_sequence count: 10
                        """,
                "explain select rnd_symbol_weighted('AAPL', 50, 'MSFT', 30, 'GOOGL', 20) from long_sequence(10)"
        );
    }

    @Test
    public void testExplainPlanTwoSymbols() throws Exception {
        assertSql(
                """
                        QUERY PLAN
                        VirtualRecord
                          functions: [rnd_symbol_weighted(A,70.0,B,30.0)]
                            long_sequence count: 5
                        """,
                "explain select rnd_symbol_weighted('A', 70, 'B', 30) from long_sequence(5)"
        );
    }

    @Test
    public void testExplainPlanWithDecimalWeights() throws Exception {
        assertSql(
                """
                        QUERY PLAN
                        VirtualRecord
                          functions: [rnd_symbol_weighted(X,2.5,Y,1.5)]
                            long_sequence count: 3
                        """,
                "explain select rnd_symbol_weighted('X', 2.5, 'Y', 1.5) from long_sequence(3)"
        );
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RndSymbolWeightedFunctionFactory();
    }
}
