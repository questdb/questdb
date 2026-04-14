/*+*****************************************************************************
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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class BarFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBasicEmptyBar() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "bar\n" +
                        "\n",
                "SELECT bar(0, 0, 100, 10)"
        ));
    }

    @Test
    public void testBasicFullBar() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "bar\n" +
                        "\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\n",
                "SELECT bar(100, 0, 100, 10)"
        ));
    }

    @Test
    public void testBasicHalfBar() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "bar\n" +
                        "\u2588\u2588\u2588\u2588\u2588\n",
                "SELECT bar(50, 0, 100, 10)"
        ));
    }

    @Test
    public void testClampAboveMax() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "bar\n" +
                        "\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\n",
                "SELECT bar(200, 0, 100, 10)"
        ));
    }

    @Test
    public void testClampBelowMin() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "bar\n" +
                        "\n",
                "SELECT bar(-50, 0, 100, 10)"
        ));
    }

    @Test
    public void testFractionalBlock() throws Exception {
        // 75/100 * 10 = 7.5 chars -> 7 full + half block (index 3 = U+258C)
        assertMemoryLeak(() -> assertSql(
                "bar\n" +
                        "\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u258C\n",
                "SELECT bar(75, 0, 100, 10)"
        ));
    }

    @Test
    public void testIntegerColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (50, '2024-01-01T00:00:00.000000Z'),
                    (100, '2024-01-01T01:00:00.000000Z')
                    """);
            assertSql(
                    "val\tbar\n" +
                            "50\t\u2588\u2588\u2588\u2588\u2588\n" +
                            "100\t\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\n",
                    "SELECT val, bar(val, 0, 100, 10) FROM t"
            );
        });
    }

    @Test
    public void testMinEqualsMax() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "bar\n" +
                        "\n",
                "SELECT bar(50, 50, 50, 10)"
        ));
    }

    @Test
    public void testNegativeRange() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "bar\n" +
                        "\u2588\u2588\u2588\u2588\u2588\n",
                "SELECT bar(-50, -100, 0, 10)"
        ));
    }

    @Test
    public void testNullMax() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "bar\n" +
                        "\n",
                "SELECT bar(50, 0, NULL, 10)"
        ));
    }

    @Test
    public void testNullMin() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "bar\n" +
                        "\n",
                "SELECT bar(50, NULL, 100, 10)"
        ));
    }

    @Test
    public void testNullValue() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "bar\n" +
                        "\n",
                "SELECT bar(NULL, 0, 100, 10)"
        ));
    }

    @Test
    public void testWithSampleByAndSum() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:10:00.000000Z'),
                    (20.0, '2024-01-01T00:20:00.000000Z'),
                    (30.0, '2024-01-01T01:10:00.000000Z'),
                    (70.0, '2024-01-01T01:20:00.000000Z')
                    """);
            assertSql(
                    "ts\ttotal\tbar\n" +
                            "2024-01-01T00:00:00.000000Z\t30.0\t\u2588\u2588\u2588\u2588\u2588\u2588\n" +
                            "2024-01-01T01:00:00.000000Z\t100.0\t\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\n",
                    "SELECT ts, sum(amount) total, bar(sum(amount), 0, 100, 20) FROM t SAMPLE BY 1h"
            );
        });
    }

    @Test
    public void testWithTableData() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, '2024-01-01T00:00:00.000000Z'),
                    (25.0, '2024-01-01T01:00:00.000000Z'),
                    (50.0, '2024-01-01T02:00:00.000000Z'),
                    (75.0, '2024-01-01T03:00:00.000000Z'),
                    (100.0, '2024-01-01T04:00:00.000000Z')
                    """);
            assertSql(
                    "val\tbar\n" +
                            "0.0\t\n" +
                            "25.0\t\u2588\u2588\u258C\n" +
                            "50.0\t\u2588\u2588\u2588\u2588\u2588\n" +
                            "75.0\t\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u258C\n" +
                            "100.0\t\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\n",
                    "SELECT val, bar(val, 0, 100, 10) FROM t"
            );
        });
    }

    @Test
    public void testWithWindowFunctionGlobalScale() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (amount DOUBLE, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 'A', '2024-01-01T00:00:00.000000Z'),
                    (30.0, 'A', '2024-01-01T01:00:00.000000Z'),
                    (50.0, 'B', '2024-01-01T00:00:00.000000Z'),
                    (100.0, 'B', '2024-01-01T01:00:00.000000Z')
                    """);
            // Global OVER (): min=10, max=100, range=90
            // A: 10 -> 0 chars, 30 -> 20/90*20=4.44 -> 4 full + frac(3)=U+258D
            // B: 50 -> 40/90*20=8.88 -> 8 full + frac(7)=U+2589, 100 -> 20 full
            assertSql(
                    "symbol\tamount\tbar\n" +
                            "A\t10.0\t\n" +
                            "A\t30.0\t\u2588\u2588\u2588\u2588\u258D\n" +
                            "B\t50.0\t\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2589\n" +
                            "B\t100.0\t\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\n",
                    "SELECT symbol, amount, bar(amount, min(amount) OVER (), max(amount) OVER (), 20) FROM t ORDER BY symbol, amount"
            );
        });
    }

    @Test
    public void testWithWindowFunctionPerSymbolScale() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (amount DOUBLE, symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 'A', '2024-01-01T00:00:00.000000Z'),
                    (30.0, 'A', '2024-01-01T01:00:00.000000Z'),
                    (50.0, 'B', '2024-01-01T00:00:00.000000Z'),
                    (100.0, 'B', '2024-01-01T01:00:00.000000Z')
                    """);
            // OVER (PARTITION BY symbol): each symbol scales independently
            // A: min=10, max=30. 10->0 chars, 30->20 chars (full)
            // B: min=50, max=100. 50->0 chars, 100->20 chars (full)
            assertSql(
                    "symbol\tamount\tbar\n" +
                            "A\t10.0\t\n" +
                            "A\t30.0\t\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\n" +
                            "B\t50.0\t\n" +
                            "B\t100.0\t\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2588\n",
                    "SELECT symbol, amount, bar(amount, min(amount) OVER (PARTITION BY symbol), max(amount) OVER (PARTITION BY symbol), 20) FROM t ORDER BY symbol, amount"
            );
        });
    }

    @Test
    public void testWidthExceedsMaxLimit() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_STR_FUNCTION_BUFFER_MAX_SIZE, 30);
        assertMemoryLeak(() -> assertException(
                "SELECT bar(50, 0, 100, 100)",
                0,
                "breached memory limit set for bar(DDDI)"
        ));
    }

    @Test
    public void testZeroWidth() throws Exception {
        // width <= 0 returns null
        assertMemoryLeak(() -> assertSql(
                "bar\n" +
                        "\n",
                "SELECT bar(50, 0, 100, 0)"
        ));
    }

    @Test
    public void testNegativeWidth() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "bar\n" +
                        "\n",
                "SELECT bar(50, 0, 100, -5)"
        ));
    }

    @Test
    public void testMinGreaterThanMax() throws Exception {
        // min > max returns null
        assertMemoryLeak(() -> assertSql(
                "bar\n" +
                        "\n",
                "SELECT bar(50, 100, 0, 10)"
        ));
    }
}
