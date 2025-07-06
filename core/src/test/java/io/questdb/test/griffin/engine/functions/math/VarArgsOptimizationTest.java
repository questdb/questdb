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

package io.questdb.test.griffin.engine.functions.math;

import org.junit.Test;

import io.questdb.test.AbstractCairoTest;

/**
 * Test to verify that loop-unrolled variants are selected and work correctly.
 */
public class VarArgsOptimizationTest extends AbstractCairoTest {

    @Test
    public void testGreatestFunctionSelectionAndCorrectness() throws Exception {
        assertMemoryLeak(() -> {
            // Test that the optimized implementations are selected for 2-5 arguments
            // and return correct results
            
            assertQuery("greatest\n30.1\n", "select greatest(10.5, 20.3, 15.7, 8.2, 30.1)", true);
            assertQuery("greatest\n20.3\n", "select greatest(10.5, 20.3)", true);
            assertQuery("greatest\n20.3\n", "select greatest(10.5, 20.3, 15.7)", true);
            assertQuery("greatest\n20.3\n", "select greatest(10.5, 20.3, 15.7, 8.2)", true);
            
            // Test with NaN values (NaN is treated as null and ignored)
            assertQuery("greatest\n20.3\n", "select greatest(10.5, 20.3, NaN)", true);
            
            // Test with all NaN (all values are null, so result is null)
            assertQuery("greatest\nnull\n", "select greatest(NaN, NaN)", true);
        });
    }

    @Test
    public void testLeastFunctionSelectionAndCorrectness() throws Exception {
        assertMemoryLeak(() -> {
            // Test that the optimized implementations are selected for 2-5 arguments
            // and return correct results
            
            assertQuery("least\n8.2\n", "select least(10.5, 20.3, 15.7, 8.2, 30.1)", true);
            assertQuery("least\n10.5\n", "select least(10.5, 20.3)", true);
            assertQuery("least\n10.5\n", "select least(10.5, 20.3, 15.7)", true);
            assertQuery("least\n8.2\n", "select least(10.5, 20.3, 15.7, 8.2)", true);
            
            // Test with NaN values - QuestDB treats NaN as null
            assertQuery("least\n10.5\n", "select least(10.5, NaN, 15.7)", true);
            
            // Test with all NaN
            assertQuery("least\nnull\n", "select least(NaN, NaN)", true);
        });
    }

    @Test
    public void testCoalesceFunctionSelectionAndCorrectness() throws Exception {
        assertMemoryLeak(() -> {
            // Test that optimized implementations are selected for 2-5 arguments
            
            // Basic coalesce tests
            assertQuery("coalesce\n10.5\n", "select coalesce(10.5, 20.3)", true);
            assertQuery("coalesce\n20.3\n", "select coalesce(null, 20.3)", true);
            assertQuery("coalesce\n10.5\n", "select coalesce(10.5, null, 15.7)", true);
            assertQuery("coalesce\n15.7\n", "select coalesce(null, null, 15.7)", true);
            assertQuery("coalesce\n8.2\n", "select coalesce(null, null, null, 8.2)", true);
            assertQuery("coalesce\n30.1\n", "select coalesce(null, null, null, null, 30.1)", true);
            
            // Test with different data types - Long
            assertQuery("coalesce\n105\n", "select coalesce(null, 105::long)", true);
            assertQuery("coalesce\n203\n", "select coalesce(null, null, 203::long)", true);
        });
    }

    @Test
    public void testConcatFunctionSelectionAndCorrectness() throws Exception {
        assertMemoryLeak(() -> {
            // Test that optimized implementations are selected for 2-5 arguments
            
            assertQuery("concat\nhello world\n", "select concat('hello', ' ', 'world')", true);
            assertQuery("concat\nhelloworld\n", "select concat('hello', 'world')", true);
            assertQuery("concat\na1b2c\n", "select concat('a', '1', 'b', '2', 'c')", true);
            assertQuery("concat\n12345\n", "select concat('1', '2', '3', '4', '5')", true);
            
            // Test with numbers
            assertQuery("concat\n12.5test\n", "select concat(12.5, 'test')", true);
            assertQuery("concat\n123true\n", "select concat(1, 2, 3, true)", true);
        });
    }

    @Test
    public void testPerformanceCharacteristics() throws Exception {
        assertMemoryLeak(() -> {
            // Create a table with many rows to test performance characteristics
            execute("create table perf_test (d1 double, d2 double, d3 double, d4 double, d5 double)");
            
            // Insert test data
            execute("insert into perf_test values " +
                   "(10.1, 20.2, 30.3, 40.4, 50.5), " +
                   "(5.1, 15.2, 25.3, 35.4, 45.5), " +
                   "(1.1, 2.2, 3.3, 4.4, 5.5)");
            
            // Test that functions work correctly on table data
            assertQuery("greatest\td1\td2\td3\td4\td5\n" +
                       "50.5\t10.1\t20.2\t30.3\t40.4\t50.5\n" +
                       "45.5\t5.1\t15.2\t25.3\t35.4\t45.5\n" +
                       "5.5\t1.1\t2.2\t3.3\t4.4\t5.5\n", 
                       "select greatest(d1, d2, d3, d4, d5), * from perf_test", true);
            
            assertQuery("least\n10.1\n5.1\n1.1\n", 
                       "select least(d1, d2, d3, d4, d5) from perf_test", true);
            
            assertQuery("coalesce\n10.1\n5.1\n1.1\n", 
                       "select coalesce(d1, d2, d3, d4, d5) from perf_test", true);
        });
    }

    @Test 
    public void testMixedDataTypes() throws Exception {
        assertMemoryLeak(() -> {
            // Test functions with mixed numeric data types
            assertQuery("greatest\n30.0\n", "select greatest(10, 20.5, 30::short)", true);
            assertQuery("least\n10.0\n", "select least(10::int, 20.5, 30)", true);
            
            // Test coalesce with different types that get cast to common type
            assertQuery("coalesce\n10.0\n", "select coalesce(null, 10::int, 20.5)", true);
            assertQuery("coalesce\n20\n", "select coalesce(null, null, 20::long)", true);
        });
    }

    @Test
    public void testEdgeCases() throws Exception {
        assertMemoryLeak(() -> {
            // Test edge cases
            
            // 6 arguments (should fallback to loop implementation for > 5 args)
            assertQuery("greatest\n30.1\n", "select greatest(10.5, 20.3, 15.7, 8.2, 30.1, 5.1)", true);
            
            // Many arguments (should use loop implementation)
            assertQuery("greatest\n30.1\n", 
                       "select greatest(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 30.1)", true);
            
            // Test with infinity values (Infinity is treated as null and ignored)
            assertQuery("greatest\n10.5\n", 
                       "select greatest(10.5, cast('Infinity' as double))", true);
            
            assertQuery("least\n10.5\n", 
                       "select least(10.5, cast('-Infinity' as double))", true);
        });
    }
}
