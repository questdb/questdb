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

package io.questdb.test.griffin.engine.functions.bool;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class GeoWithinBoxFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testInWhereClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double)");
            execute("insert into points values (5.0, 5.0)");
            execute("insert into points values (-1.0, 5.0)");
            execute("insert into points values (5.0, 11.0)");
            execute("insert into points values (0.0, 0.0)");

            assertSql(
                    "x\ty\n" +
                            "5.0\t5.0\n" +
                            "0.0\t0.0\n",
                    "select x, y from points where geo_within_box(x, y, 0.0, 0.0, 10.0, 10.0)"
            );
        });
    }

    @Test
    public void testInfinity() throws Exception {
        // Note: QuestDB's division converts Infinity to NaN (see DivDoubleFunctionFactory),
        // so 1.0/0.0 produces NaN, not Infinity. These tests verify NaN handling.
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(1.0/0.0, 5.0, 0.0, 0.0, 10.0, 10.0)");
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(-1.0/0.0, 5.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testInvertedBoxX() throws Exception {
        // min_x > max_x should return false
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(5.0, 5.0, 10.0, 0.0, 0.0, 10.0)");
    }

    @Test
    public void testInvertedBoxY() throws Exception {
        // min_y > max_y should return false
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(5.0, 5.0, 0.0, 10.0, 10.0, 0.0)");
    }

    @Test
    public void testNaNMaxX() throws Exception {
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(5.0, 5.0, 0.0, 0.0, NaN, 10.0)");
    }

    @Test
    public void testNaNMaxY() throws Exception {
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(5.0, 5.0, 0.0, 0.0, 10.0, NaN)");
    }

    @Test
    public void testNaNMinX() throws Exception {
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(5.0, 5.0, NaN, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testNaNMinY() throws Exception {
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(5.0, 5.0, 0.0, NaN, 10.0, 10.0)");
    }

    @Test
    public void testNaNX() throws Exception {
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(NaN, 5.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testNaNY() throws Exception {
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(5.0, NaN, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testNegativeCoordinates() throws Exception {
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(-5.0, -5.0, -10.0, -10.0, 0.0, 0.0)");
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(-15.0, -5.0, -10.0, -10.0, 0.0, 0.0)");
    }

    @Test
    public void testNegativeZero() throws Exception {
        // -0.0 should be treated as 0.0 for boundary checks
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(-0.0, 5.0, 0.0, 0.0, 10.0, 10.0)");
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(0.0, 5.0, -0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testPointInsideBox() throws Exception {
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(5.0, 5.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testPointOnCorner() throws Exception {
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(0.0, 0.0, 0.0, 0.0, 10.0, 10.0)");
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(10.0, 10.0, 0.0, 0.0, 10.0, 10.0)");
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(0.0, 10.0, 0.0, 0.0, 10.0, 10.0)");
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(10.0, 0.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testPointOnMaxXBoundary() throws Exception {
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(10.0, 5.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testPointOnMaxYBoundary() throws Exception {
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(5.0, 10.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testPointOnMinXBoundary() throws Exception {
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(0.0, 5.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testPointOnMinYBoundary() throws Exception {
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(5.0, 0.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testPointOutsideBoxAbove() throws Exception {
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(5.0, 11.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testPointOutsideBoxBelow() throws Exception {
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(5.0, -1.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testPointOutsideBoxLeft() throws Exception {
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(-1.0, 5.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testPointOutsideBoxRight() throws Exception {
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(11.0, 5.0, 0.0, 0.0, 10.0, 10.0)");
    }

    @Test
    public void testVerySmallDifferences() throws Exception {
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(1.0000000001, 1.0, 1.0, 1.0, 2.0, 2.0)");
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(0.9999999999, 1.0, 1.0, 1.0, 2.0, 2.0)");
    }

    @Test
    public void testWithNullValuesInTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double)");
            execute("insert into points values (5.0, 5.0)");
            execute("insert into points values (null, 5.0)");
            execute("insert into points values (5.0, null)");

            assertSql(
                    "x\ty\tinside\n" +
                            "5.0\t5.0\ttrue\n" +
                            "null\t5.0\tfalse\n" +
                            "5.0\tnull\tfalse\n",
                    "select x, y, geo_within_box(x, y, 0.0, 0.0, 10.0, 10.0) as inside from points"
            );
        });
    }

    @Test
    public void testWithTableData() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table points (x double, y double)");
            execute("insert into points values (5.0, 5.0)");
            execute("insert into points values (-1.0, 5.0)");
            execute("insert into points values (5.0, 11.0)");
            execute("insert into points values (0.0, 0.0)");

            assertSql(
                    "x\ty\tinside\n" +
                            "5.0\t5.0\ttrue\n" +
                            "-1.0\t5.0\tfalse\n" +
                            "5.0\t11.0\tfalse\n" +
                            "0.0\t0.0\ttrue\n",
                    "select x, y, geo_within_box(x, y, 0.0, 0.0, 10.0, 10.0) as inside from points"
            );
        });
    }

    @Test
    public void testZeroSizedBox() throws Exception {
        // Point exactly at zero-sized box location
        assertSql("geo_within_box\ntrue\n", "select geo_within_box(5.0, 5.0, 5.0, 5.0, 5.0, 5.0)");
        // Point not at zero-sized box location
        assertSql("geo_within_box\nfalse\n", "select geo_within_box(5.0, 5.0, 6.0, 6.0, 6.0, 6.0)");
    }
}
