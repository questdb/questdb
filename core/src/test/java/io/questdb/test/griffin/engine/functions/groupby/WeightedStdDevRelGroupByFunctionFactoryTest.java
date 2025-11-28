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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class WeightedStdDevRelGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testWeightedStddevRelAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango as (SELECT 17.2151921 value, rnd_double() weight FROM long_sequence(100))");
            assertSql(
                    "weighted_stddev_rel\n0.0\n",
                    "SELECT weighted_stddev_rel(value, weight) FROM tango"
            );
            assertSql(
                    "weighted_stddev\n0.0\n",
                    "SELECT weighted_stddev(value, weight) FROM tango"
            );
        });
    }

    @Test
    public void testWeightedStddevRelDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango as (SELECT x::DOUBLE x FROM long_sequence(100))");
            assertSql(
                    "weighted_stddev_rel\n23.84414071334121\n",
                    "SELECT weighted_stddev_rel(x, 101.0 - x) FROM tango"
            );
            assertSql(
                    "weighted_stddev\n23.84414071334121\n",
                    "SELECT weighted_stddev(x, 101.0 - x) FROM tango"
            );
        });
    }

    @Test
    public void testWeightedStddevRelFirstNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(x DOUBLE)");
            execute("INSERT INTO 'tango' VALUES (null)");
            execute("INSERT INTO 'tango' SELECT x FROM long_sequence(100)");
            assertSql(
                    "weighted_stddev_rel\n23.84414071334121\n",
                    "SELECT weighted_stddev_rel(x, 101.0 - x) FROM tango"
            );
            assertSql(
                    "weighted_stddev\n23.84414071334121\n",
                    "SELECT weighted_stddev(x, 101.0 - x) FROM tango"
            );
        });
    }

    @Test
    public void testWeightedStddevRelFloatValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango as (SELECT x::FLOAT value, (101.0 - x)::FLOAT weight FROM long_sequence(100))");
            assertSql(
                    "weighted_stddev_rel\n23.84414071334121\n",
                    "SELECT weighted_stddev_rel(value, weight) FROM tango"
            );
            assertSql(
                    "weighted_stddev\n23.84414071334121\n",
                    "SELECT weighted_stddev(value, weight) FROM tango"
            );
        });
    }

    @Test
    public void testWeightedStddevRelHugeValuesAndWeights() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango as (SELECT 100_000_000 * x x FROM long_sequence(1_000_000))");
            assertSql(
                    "weighted_stddev_rel\n2.3570253538446816E13\n", "SELECT weighted_stddev_rel(x, x) FROM tango"
            );
            assertSql(
                    "weighted_stddev\n2.3570253538446816E13\n", "SELECT weighted_stddev(x, x) FROM tango"
            );
        });
    }

    @Test
    public void testWeightedStddevRelIntValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango as (SELECT x::INT value, (101 - x) weight FROM long_sequence(100))");
            assertSql(
                    "weighted_stddev_rel\n23.84414071334121\n",
                    "SELECT weighted_stddev_rel(value, weight) FROM tango"
            );
            assertSql(
                    "weighted_stddev\n23.84414071334121\n",
                    "SELECT weighted_stddev(value, weight) FROM tango"
            );
        });
    }

    @Test
    public void testWeightedStddevRelNoValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(x int)");
            assertSql(
                    "weighted_stddev_rel\nnull\n", "SELECT weighted_stddev_rel(x, x) FROM tango"
            );
            assertSql(
                    "weighted_stddev\nnull\n", "SELECT weighted_stddev(x, x) FROM tango"
            );
        });
    }

    @Test
    public void testWeightedStddevRelNullValues() throws Exception {
        assertMemoryLeak(() -> {
            assertSql(
                    "weighted_stddev_rel\nnull\n",
                    "SELECT weighted_stddev_rel(x, 1.0) FROM (SELECT null::double x FROM long_sequence(100))"
            );
            assertSql(
                    "weighted_stddev\nnull\n",
                    "SELECT weighted_stddev(x, 1.0) FROM (SELECT null::double x FROM long_sequence(100))"
            );
        });
    }

    @Test
    public void testWeightedStddevRelNullValuesAndWeights() throws Exception {
        assertMemoryLeak(() -> {
            assertSql(
                    "weighted_stddev_rel\nnull\n",
                    "SELECT weighted_stddev_rel(x, x) FROM (SELECT null::double x FROM long_sequence(100))"
            );
            assertSql(
                    "weighted_stddev\nnull\n",
                    "SELECT weighted_stddev(x, x) FROM (SELECT null::double x FROM long_sequence(100))"
            );
        });
    }

    @Test
    public void testWeightedStddevRelNullWeights() throws Exception {
        assertMemoryLeak(() -> {
            assertSql(
                    "weighted_stddev_rel\nnull\n",
                    "SELECT weighted_stddev_rel(1.0, x) FROM (SELECT null::double x FROM long_sequence(100))"
            );
            assertSql(
                    "weighted_stddev\nnull\n",
                    "SELECT weighted_stddev(1.0, x) FROM (SELECT null::double x FROM long_sequence(100))"
            );
        });
    }

    @Test
    public void testWeightedStddevRelOneRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango(x double)");
            execute("INSERT INTO 'tango' VALUES (17.2151920)");
            assertSql(
                    "weighted_stddev_rel\nnull\n", "SELECT weighted_stddev_rel(x, x) FROM tango"
            );
            assertSql(
                    "weighted_stddev\nnull\n", "SELECT weighted_stddev(x, x) FROM tango"
            );
        });
    }

    @Test
    public void testWeightedStddevRelSomeNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango as (SELECT x::DOUBLE x FROM long_sequence(100))");
            execute("INSERT INTO 'tango' VALUES (null)");
            assertSql(
                    "weighted_stddev_rel\n23.84414071334121\n",
                    "SELECT weighted_stddev_rel(x, 101.0 - x) FROM tango"
            );
            assertSql(
                    "weighted_stddev\n23.84414071334121\n",
                    "SELECT weighted_stddev(x, 101.0 - x) FROM tango"
            );
        });
    }

    @Test
    public void testWeightedStddevRelZeroAndNegativeWeight() throws Exception {
        assertMemoryLeak(() -> {
            assertSql(
                    """
                            weighted_stddev_rel
                            null
                            """,
                    "SELECT weighted_stddev_rel(x, 0) FROM long_sequence(10)"
            );
            assertSql(
                    """
                            weighted_stddev
                            null
                            """,
                    "SELECT weighted_stddev(x, 0) FROM long_sequence(10)"
            );
            assertSql(
                    """
                            weighted_stddev_rel
                            0.7071067811865476
                            """,
                    """
                            SELECT weighted_stddev_rel(
                                x,
                                CASE WHEN x < 3 THEN 1 ELSE 0 END
                            ) FROM long_sequence(10)
                            """
            );
            assertSql(
                    """
                            weighted_stddev
                            0.7071067811865476
                            """,
                    """
                            SELECT weighted_stddev(
                                x,
                                CASE WHEN x < 3 THEN 1 ELSE 0 END
                            ) FROM long_sequence(10)
                            """
            );
            // Weights sum to zero
            assertSql(
                    """
                            weighted_stddev_rel
                            null
                            """,
                    "SELECT weighted_stddev_rel(x, x - 6) FROM long_sequence(11)"
            );
            assertSql(
                    """
                            weighted_stddev
                            null
                            """,
                    "SELECT weighted_stddev(x, x - 6) FROM long_sequence(11)"
            );
        });
    }
}
