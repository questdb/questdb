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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class DepthChartFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllZeroBothSides() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "depth_chart\n" +
                        "\u2581\u2581\u2581\u254E\u2581\u2581\u2581\n",
                "SELECT depth_chart(ARRAY[0.0, 0.0, 0.0]::DOUBLE[], ARRAY[0.0, 0.0, 0.0]::DOUBLE[])"
        ));
    }

    @Test
    public void testAsymmetricArrays() throws Exception {
        // 5 bids, 3 asks - different lengths. 9 chars total.
        // Bid cumulative: 10,30,60,100,150. Ask cumulative: 10,30,60.
        // Bids reversed: 150,100,60,30,10 | Asks: 10,30,60
        assertMemoryLeak(() -> {
            String result = queryResultString(
                    "SELECT depth_chart(ARRAY[10.0, 20.0, 30.0, 40.0, 50.0]::DOUBLE[], ARRAY[10.0, 20.0, 30.0]::DOUBLE[])"
            );
            String chart = result.split("\n")[1];
            Assert.assertEquals(9, chart.length());
            Assert.assertEquals('\u254E', chart.charAt(5));
            // Bids should descend from left to spread
            Assert.assertTrue(chart.charAt(0) >= chart.charAt(4));
        });
    }

    @Test
    public void testAsymmetricWithExplicitWidth() throws Exception {
        // 10 bids, 3 asks, width=7: bidChars=3, askChars=3
        // Bids: 10 levels compressed to 3 chars. Asks: 3 levels fit in 3 chars.
        assertMemoryLeak(() -> {
            String result = queryResultString(
                    "SELECT depth_chart(" +
                            "ARRAY[10.0,20.0,30.0,40.0,50.0,60.0,70.0,80.0,90.0,100.0]::DOUBLE[], " +
                            "ARRAY[10.0, 20.0, 30.0]::DOUBLE[], 7)"
            );
            String chart = result.split("\n")[1];
            Assert.assertEquals(7, chart.length());
            Assert.assertEquals('\u254E', chart.charAt(3));
            // Bids descend from left to spread
            Assert.assertTrue(chart.charAt(0) >= chart.charAt(2));
            // Asks ascend from spread to right
            Assert.assertTrue(chart.charAt(4) <= chart.charAt(6));
        });
    }

    @Test
    public void testBasicSymmetric() throws Exception {
        // Equal bid/ask arrays with simple values
        assertMemoryLeak(() -> {
            String result = queryResultString(
                    "SELECT depth_chart(ARRAY[100.0, 200.0, 300.0]::DOUBLE[], ARRAY[100.0, 200.0, 300.0]::DOUBLE[])"
            );
            // Should be symmetric around spread marker
            Assert.assertTrue(result.contains("\u254E"));
            // 3 bids + spread + 3 asks = 7 chars
            String chart = result.split("\n")[1];
            Assert.assertEquals(7, chart.length());
        });
    }

    @Test
    public void testConstantPerLevelVolumes() throws Exception {
        // All levels have volume 100. Cumulative: 100, 200, 300, 400, 500.
        // This is a RISING cumulative curve, not flat.
        assertMemoryLeak(() -> {
            String result = queryResultString(
                    "SELECT depth_chart(ARRAY[100.0, 100.0, 100.0, 100.0, 100.0]::DOUBLE[], " +
                            "ARRAY[100.0, 100.0, 100.0, 100.0, 100.0]::DOUBLE[])"
            );
            String chart = result.split("\n")[1];
            // Bids reversed: worst(highest cum) on left, best(lowest cum) near spread
            // First char should be highest, last bid char should be lowest
            Assert.assertEquals('\u254E', chart.charAt(5)); // spread at center
            // Left edge (worst bid) should be taller than right-of-center (best bid)
            Assert.assertTrue(chart.charAt(0) >= chart.charAt(4));
        });
    }

    @Test
    public void testCumulativeSumOverflowThrows() throws Exception {
        assertMemoryLeak(() -> assertException(
                "SELECT depth_chart(ARRAY[1.7976931348623157E308, 1.7976931348623157E308]::DOUBLE[], ARRAY[1.0]::DOUBLE[])",
                72,
                "volume must be finite and non-negative"
        ));
    }

    @Test
    public void testDefaultWidthExceedsMaxBuffer() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_STR_FUNCTION_BUFFER_MAX_SIZE, 15);
        assertMemoryLeak(() -> assertException(
                "SELECT depth_chart(ARRAY[1.0,2.0,3.0,4.0,5.0]::DOUBLE[], ARRAY[1.0,2.0,3.0,4.0,5.0]::DOUBLE[])",
                45,
                "breached memory limit"
        ));
    }

    @Test
    public void testDimensionality2DRejected() throws Exception {
        assertMemoryLeak(() -> assertException(
                "SELECT depth_chart(ARRAY[[1.0, 2.0], [3.0, 4.0]]::DOUBLE[][], ARRAY[1.0]::DOUBLE[])",
                48,
                "not a one-dimensional array"
        ));
    }

    @Test
    public void testEmptyArrayReturnsNull() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "depth_chart\n\n",
                "SELECT depth_chart(ARRAY[]::DOUBLE[], ARRAY[1.0, 2.0]::DOUBLE[])"
        ));
    }

    @Test
    public void testEvenWidth() throws Exception {
        // Width 10: askChars = 5, bidChars = 4
        assertMemoryLeak(() -> {
            String result = queryResultString(
                    "SELECT depth_chart(ARRAY[10.0,20.0,30.0,40.0,50.0,60.0,70.0,80.0]::DOUBLE[], " +
                            "ARRAY[10.0,20.0,30.0,40.0,50.0,60.0,70.0,80.0]::DOUBLE[], 10)"
            );
            String chart = result.split("\n")[1];
            Assert.assertEquals(10, chart.length());
            Assert.assertEquals('\u254E', chart.charAt(4)); // spread at position 4
        });
    }

    @Test
    public void testInfinityElementThrows() throws Exception {
        assertMemoryLeak(() -> assertException(
                "SELECT depth_chart(ARRAY[1.0, 1.0/0.0]::DOUBLE[], ARRAY[1.0]::DOUBLE[])",
                38,
                "volume must be finite and non-negative"
        ));
    }

    @Test
    public void testLabelsBasic() throws Exception {
        assertMemoryLeak(() -> {
            String result = queryResultString(
                    "SELECT depth_chart_labels(ARRAY[100.0, 200.0, 300.0]::DOUBLE[], ARRAY[150.0, 250.0, 350.0]::DOUBLE[])"
            );
            Assert.assertTrue(result.contains("bb:100.0"));
            Assert.assertTrue(result.contains("ba:150.0"));
            Assert.assertTrue(result.contains("tb:600.0"));
            Assert.assertTrue(result.contains("ta:750.0"));
        });
    }

    @Test
    public void testLabelsExceedsMaxBuffer() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_STR_FUNCTION_BUFFER_MAX_SIZE, 30);
        assertMemoryLeak(() -> assertException(
                "SELECT depth_chart_labels(ARRAY[12345.6789]::DOUBLE[], ARRAY[12345.6789]::DOUBLE[], 3)",
                84,
                "breached memory limit"
        ));
    }

    @Test
    public void testLabelsWithWidth() throws Exception {
        // Combined labels + width path
        assertMemoryLeak(() -> {
            String result = queryResultString(
                    "SELECT depth_chart_labels(ARRAY[100.0, 200.0, 300.0, 400.0, 500.0]::DOUBLE[], " +
                            "ARRAY[150.0, 250.0, 350.0, 450.0, 550.0]::DOUBLE[], 5)"
            );
            Assert.assertTrue(result.contains("\u254E"));
            Assert.assertTrue(result.contains("bb:100.0"));
            Assert.assertTrue(result.contains("ta:1750.0"));
        });
    }

    @Test
    public void testLargeVolumes() throws Exception {
        // Billions - log scale handles the range
        assertMemoryLeak(() -> {
            String result = queryResultString(
                    "SELECT depth_chart(ARRAY[73339.0, 124978.0, 141805.0, 900378480.0]::DOUBLE[], " +
                            "ARRAY[80708.0, 112517.0, 129269.0, 978250899.0]::DOUBLE[])"
            );
            Assert.assertTrue(result.contains("\u254E"));
        });
    }

    @Test
    public void testNaNElementAskSideThrows() throws Exception {
        assertMemoryLeak(() -> assertException(
                "SELECT depth_chart(ARRAY[1.0]::DOUBLE[], ARRAY[1.0, NaN]::DOUBLE[])",
                56,
                "volume must be finite and non-negative"
        ));
    }

    @Test
    public void testNaNElementBidSideThrows() throws Exception {
        assertMemoryLeak(() -> assertException(
                "SELECT depth_chart(ARRAY[1.0, NaN]::DOUBLE[], ARRAY[1.0]::DOUBLE[])",
                34,
                "volume must be finite and non-negative"
        ));
    }

    @Test
    public void testNegativeVolumeAskSideThrows() throws Exception {
        assertMemoryLeak(() -> assertException(
                "SELECT depth_chart(ARRAY[1.0]::DOUBLE[], ARRAY[1.0, -5.0]::DOUBLE[])",
                57,
                "volume must be finite and non-negative"
        ));
    }

    @Test
    public void testNegativeVolumeBidSideThrows() throws Exception {
        assertMemoryLeak(() -> assertException(
                "SELECT depth_chart(ARRAY[1.0, -5.0]::DOUBLE[], ARRAY[1.0]::DOUBLE[])",
                35,
                "volume must be finite and non-negative"
        ));
    }

    @Test
    public void testNullArrayReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (bids DOUBLE[], asks DOUBLE[], ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (NULL, ARRAY[1.0]::DOUBLE[], '2024-01-01T00:00:00.000000Z')");
            assertSql(
                    "depth_chart\n\n",
                    "SELECT depth_chart(bids, asks) FROM t"
            );
        });
    }

    @Test
    public void testOddWidth() throws Exception {
        // Width 11: askChars = 5, bidChars = 5
        assertMemoryLeak(() -> {
            String result = queryResultString(
                    "SELECT depth_chart(ARRAY[10.0,20.0,30.0,40.0,50.0,60.0,70.0,80.0]::DOUBLE[], " +
                            "ARRAY[10.0,20.0,30.0,40.0,50.0,60.0,70.0,80.0]::DOUBLE[], 11)"
            );
            String chart = result.split("\n")[1];
            Assert.assertEquals(11, chart.length());
            Assert.assertEquals('\u254E', chart.charAt(5)); // spread at center
        });
    }

    @Test
    public void testOrderByDepthChart() throws Exception {
        // A/B flyweight independence
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (bids DOUBLE[], asks DOUBLE[], ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (ARRAY[100.0, 200.0]::DOUBLE[], ARRAY[100.0, 200.0]::DOUBLE[], '2024-01-01T00:00:00.000000Z'),
                    (ARRAY[50.0, 100.0]::DOUBLE[], ARRAY[50.0, 100.0]::DOUBLE[], '2024-01-01T01:00:00.000000Z')
                    """);
            String result = queryResultString("SELECT depth_chart(bids, asks) dc FROM t ORDER BY dc");
            Assert.assertTrue(result.contains("\u254E"));
        });
    }

    @Test
    public void testSingleLevelPositive() throws Exception {
        // Single level per side with positive values
        assertMemoryLeak(() -> assertSql(
                "depth_chart\n" +
                        "\u2588\u254E\u2588\n",
                "SELECT depth_chart(ARRAY[10.0]::DOUBLE[], ARRAY[10.0]::DOUBLE[])"
        ));
    }

    @Test
    public void testSingleLevelZero() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "depth_chart\n" +
                        "\u2581\u254E\u2581\n",
                "SELECT depth_chart(ARRAY[0.0]::DOUBLE[], ARRAY[0.0]::DOUBLE[])"
        ));
    }

    @Test
    public void testWidthExceedsMaxBuffer() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_STR_FUNCTION_BUFFER_MAX_SIZE, 30);
        assertMemoryLeak(() -> assertException(
                "SELECT depth_chart(ARRAY[1.0]::DOUBLE[], ARRAY[1.0]::DOUBLE[], 100)",
                63,
                "breached memory limit"
        ));
    }

    @Test
    public void testWidthLargerThanArrays() throws Exception {
        // Width 9 but only 2 levels per side. Pads with lowest char.
        // bidChars = 4, askChars = 4. Bids pad on left, asks pad on right.
        assertMemoryLeak(() -> {
            String result = queryResultString(
                    "SELECT depth_chart(ARRAY[100.0, 200.0]::DOUBLE[], ARRAY[100.0, 200.0]::DOUBLE[], 9)"
            );
            String chart = result.split("\n")[1];
            Assert.assertEquals(9, chart.length());
            Assert.assertEquals('\u254E', chart.charAt(4)); // spread
            // Padding chars should be lowest
            Assert.assertEquals('\u2581', chart.charAt(0)); // left pad
            Assert.assertEquals('\u2581', chart.charAt(1)); // left pad
            Assert.assertEquals('\u2581', chart.charAt(7)); // right pad
            Assert.assertEquals('\u2581', chart.charAt(8)); // right pad
        });
    }

    @Test
    public void testWidthMinimum() throws Exception {
        // Width = 3: 1 bid + spread + 1 ask
        assertMemoryLeak(() -> {
            String result = queryResultString(
                    "SELECT depth_chart(ARRAY[100.0, 200.0, 300.0]::DOUBLE[], ARRAY[100.0, 200.0, 300.0]::DOUBLE[], 3)"
            );
            String chart = result.split("\n")[1];
            Assert.assertEquals(3, chart.length());
            Assert.assertEquals('\u254E', chart.charAt(1)); // spread in middle
        });
    }

    @Test
    public void testWidthTooSmallThrows() throws Exception {
        assertMemoryLeak(() -> assertException(
                "SELECT depth_chart(ARRAY[1.0]::DOUBLE[], ARRAY[1.0]::DOUBLE[], 2)",
                63,
                "width must be at least 3"
        ));
    }

    @Test
    public void testZeroVolumesValid() throws Exception {
        // Cumulative: 0, 100, 300. Bids reversed: 300,100,0 | Asks: 0,100,300
        // Near spread (best levels) has zero cum vol -> lowest char
        assertMemoryLeak(() -> {
            String result = queryResultString(
                    "SELECT depth_chart(ARRAY[0.0, 100.0, 200.0]::DOUBLE[], ARRAY[0.0, 100.0, 200.0]::DOUBLE[])"
            );
            String chart = result.split("\n")[1];
            Assert.assertEquals(7, chart.length());
            Assert.assertEquals('\u254E', chart.charAt(3));
            // Best bid (pos 2) and best ask (pos 4) have cum=0, render as lowest
            Assert.assertEquals('\u2581', chart.charAt(2));
            Assert.assertEquals('\u2581', chart.charAt(4));
            // Worst levels (edges) should be highest
            Assert.assertEquals(chart.charAt(0), chart.charAt(6));
        });
    }

    private String queryResultString(String sql) throws Exception {
        printSql(sql);
        return sink.toString();
    }
}
