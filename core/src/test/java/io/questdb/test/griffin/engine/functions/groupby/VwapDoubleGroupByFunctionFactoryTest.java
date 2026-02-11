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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class VwapDoubleGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAll() throws Exception {
        assertMemoryLeak(() -> assertSql("""
                        vwap
                        0.4601797676425299
                        """,
                "select vwap(rnd_double(), rnd_double()) from long_sequence(10)"
        ));
    }

    @Test
    public void testIgnoreNullAndZeroOrNegativeQty() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (p double, q double)");
            execute("insert into tab values (null,null),(1,null),(100,10),(null,1),(105,40),(1,0),(1,-1)");
            assertSql(
                    """
                            vwap
                            104.0
                            """,
                    "select vwap(p, q) from tab"
            );
            // make sure they are the same
            assertSql("""
                            same_vwap
                            true
                            """,
                    "select (new_vwap=old_vwap) same_vwap " +
                            "from (" +
                            "      (select vwap(p, q) new_vwap from tab) a," +
                            "      (select (sum(p*q)/sum(q)) old_vwap from tab where p != null and q != null and q > 0) b" +
                            ")"
            );
        });
    }

    @Test
    public void testNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (a0 double, a1 double)");
            execute("insert into tab values (null,null),(null,1),(1,null)");
            assertSql("""
                            vwap
                            null
                            """,
                    "select vwap(a0, a1) from tab"
            );
        });
    }

    @Test
    public void testVwap() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (a0 double, a1 double)");
            execute("insert into tab values (100,10),(105,40)");
            assertSql("""
                            vwap
                            104.0
                            """,
                    "select vwap(a0, a1) from tab"
            );
        });
    }

    @Test
    public void testVwapGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (price double, volume double, ticker symbol)");
            execute("insert into tab values (100,10,'a'),(105,40,'a'),(102,20,'b'),(103,60,'b')");
            assertSql("""
                            ticker\tvwap
                            a\t104.0
                            b\t102.75
                            """,
                    "select ticker,vwap(price, volume) from tab order by ticker"
            );
        });
    }

    @Test
    public void testVwapInterpolation() throws Exception {
        assertMemoryLeak(() -> {
            // Create table with timestamp, price, volume and symbol columns
            execute("create table trades (" +
                    "ts timestamp, " +
                    "price double, " +
                    "volume double, " +
                    "ticker symbol" +
                    ") timestamp(ts)");

            // Insert data with some gaps in the time series
            execute("insert into trades values " +
                    "('2024-01-01T00:00:00', 100.0, 50.0, 'AAPL'), " +  // First bucket
                    "('2024-01-01T00:00:00', 101.0, 30.0, 'AAPL'), " +
                    "('2024-01-01T00:00:00', 99.0, 20.0, 'GOOGL'), " +
                    // Skip 01:00
                    "('2024-01-01T02:00:00', 105.0, 40.0, 'AAPL'), " +  // Third bucket
                    "('2024-01-01T02:00:00', 102.0, 25.0, 'GOOGL'), " +
                    // Skip 03:00
                    "('2024-01-01T04:00:00', 110.0, 60.0, 'AAPL'), " +  // Fifth bucket
                    "('2024-01-01T04:00:00', 108.0, 35.0, 'GOOGL')");

            // Calculate VWAP with 1-hour buckets and linear interpolation
            // Expected results:
            // - First bucket (00:00): Actual calculations
            // - Second bucket (01:00): Interpolated
            // - Third bucket (02:00): Actual calculations
            // - Fourth bucket (03:00): Interpolated
            // - Fifth bucket (04:00): Actual calculations
            String expected = "ts\tticker\tvwap\n" +
                    "2024-01-01T00:00:00.000000Z\tAAPL\t100.375\n" +
                    "2024-01-01T00:00:00.000000Z\tGOOGL\t99.0\n" +
                    "2024-01-01T01:00:00.000000Z\tAAPL\t102.6875\n" +  // Interpolated
                    "2024-01-01T01:00:00.000000Z\tGOOGL\t100.5\n" +    // Interpolated
                    "2024-01-01T02:00:00.000000Z\tAAPL\t105.0\n" +
                    "2024-01-01T02:00:00.000000Z\tGOOGL\t102.0\n" +
                    "2024-01-01T03:00:00.000000Z\tAAPL\t107.5\n" +     // Interpolated
                    "2024-01-01T03:00:00.000000Z\tGOOGL\t105.0\n" +    // Interpolated
                    "2024-01-01T04:00:00.000000Z\tAAPL\t110.0\n" +
                    "2024-01-01T04:00:00.000000Z\tGOOGL\t108.0\n";

            assertQuery(
                    expected,
                    "select ts, ticker, vwap(price, volume) " +
                            "from trades " +
                            "sample by 1h fill(linear) ",
                    "ts",
                    true,
                    true
            );
        });
    }
}
