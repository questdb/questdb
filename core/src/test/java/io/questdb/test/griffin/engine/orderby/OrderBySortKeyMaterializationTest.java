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

package io.questdb.test.griffin.engine.orderby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class OrderBySortKeyMaterializationTest extends AbstractCairoTest {

    @Test
    public void testDelegateArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v DOUBLE[], ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, ARRAY[1.0, 2.0], '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, ARRAY[3.0, 4.0], '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, ARRAY[5.0, 6.0], '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\t[1.0,2.0]
                    15.0\t[3.0,4.0]
                    20.0\t[5.0,6.0]
                    """, query);
        });
    }

    @Test
    public void testDelegateBool() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, true, '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, false, '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, true, '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\ttrue
                    15.0\tfalse
                    20.0\ttrue
                    """, query);
        });
    }

    @Test
    public void testDelegateByte() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v BYTE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, 10, '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, 20, '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, 30, '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\t10
                    15.0\t20
                    20.0\t30
                    """, query);
        });
    }

    @Test
    public void testDelegateChar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v CHAR, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, 'A', '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, 'B', '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, 'C', '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\tA
                    15.0\tB
                    20.0\tC
                    """, query);
        });
    }

    @Test
    public void testDelegateDate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v DATE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, '2024-01-01T00:00:00.000Z', '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, '2024-06-15T00:00:00.000Z', '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, '2024-12-31T00:00:00.000Z', '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\t2024-01-01T00:00:00.000Z
                    15.0\t2024-06-15T00:00:00.000Z
                    20.0\t2024-12-31T00:00:00.000Z
                    """, query);
        });
    }

    @Test
    public void testDelegateDecimal128() throws Exception {
        assertMemoryLeak(() -> {
            // DECIMAL(30,2) uses DECIMAL128 storage (16 bytes, precision 19-38)
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v DECIMAL(30,2), ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, '1.23', '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, '4.56', '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, '7.89', '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\t1.23
                    15.0\t4.56
                    20.0\t7.89
                    """, query);
        });
    }

    @Test
    public void testDelegateDecimal16() throws Exception {
        assertMemoryLeak(() -> {
            // DECIMAL(4,2) uses DECIMAL16 storage (2 bytes, precision 3-4)
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v DECIMAL(4,2), ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, '12.34', '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, '56.78', '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, '90.12', '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\t12.34
                    15.0\t56.78
                    20.0\t90.12
                    """, query);
        });
    }

    @Test
    public void testDelegateDecimal256() throws Exception {
        assertMemoryLeak(() -> {
            // DECIMAL(50,2) uses DECIMAL256 storage (32 bytes, precision 39-76)
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v DECIMAL(50,2), ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, '1.23', '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, '4.56', '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, '7.89', '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\t1.23
                    15.0\t4.56
                    20.0\t7.89
                    """, query);
        });
    }

    @Test
    public void testDelegateDecimal32() throws Exception {
        assertMemoryLeak(() -> {
            // DECIMAL(8,2) uses DECIMAL32 storage (4 bytes, precision 5-9)
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v DECIMAL(8,2), ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, '1234.56', '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, '7890.12', '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, '3456.78', '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\t1234.56
                    15.0\t7890.12
                    20.0\t3456.78
                    """, query);
        });
    }

    @Test
    public void testDelegateDecimal64() throws Exception {
        assertMemoryLeak(() -> {
            // DECIMAL(15,2) uses DECIMAL64 storage (8 bytes, precision 10-18)
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v DECIMAL(15,2), ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, '1234567890.12', '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, '9876543210.34', '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, '5555555555.56', '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\t1234567890.12
                    15.0\t9876543210.34
                    20.0\t5555555555.56
                    """, query);
        });
    }

    @Test
    public void testDelegateDecimal8() throws Exception {
        assertMemoryLeak(() -> {
            // DECIMAL(2,1) uses DECIMAL8 storage (1 byte, precision 1-2)
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v DECIMAL(2,1), ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, '1.2', '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, '3.4', '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, '5.6', '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\t1.2
                    15.0\t3.4
                    20.0\t5.6
                    """, query);
        });
    }

    @Test
    public void testDelegateDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, 1.11, '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, 2.22, '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, 3.33, '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\t1.11
                    15.0\t2.22
                    20.0\t3.33
                    """, query);
        });
    }

    @Test
    public void testDelegateFloat() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v FLOAT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, 1.5, '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, 2.5, '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, 3.5, '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\t1.5
                    15.0\t2.5
                    20.0\t3.5
                    """, query);
        });
    }

    @Test
    public void testDelegateGeoByte() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v GEOHASH(1c), ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, 's', '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, 'u', '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, 'z', '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\ts
                    15.0\tu
                    20.0\tz
                    """, query);
        });
    }

    @Test
    public void testDelegateGeoInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v GEOHASH(6c), ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, 'sp052w', '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, 'u33d8b', '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, 'zzzzzz', '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\tsp052w
                    15.0\tu33d8b
                    20.0\tzzzzzz
                    """, query);
        });
    }

    @Test
    public void testDelegateGeoLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v GEOHASH(12c), ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, 'sp052w92p1p8', '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, 'u33d8b12b5s0', '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, 'zzzzzzzzzzzz', '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\tsp052w92p1p8
                    15.0\tu33d8b12b5s0
                    20.0\tzzzzzzzzzzzz
                    """, query);
        });
    }

    @Test
    public void testDelegateGeoShort() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v GEOHASH(3c), ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, 'sp0', '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, 'u33', '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, 'zzz', '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\tsp0
                    15.0\tu33
                    20.0\tzzz
                    """, query);
        });
    }

    @Test
    public void testDelegateIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v IPv4, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, '192.168.1.1', '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, '10.0.0.1', '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, '172.16.0.1', '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\t192.168.1.1
                    15.0\t10.0.0.1
                    20.0\t172.16.0.1
                    """, query);
        });
    }

    @Test
    public void testDelegateLong128() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v UUID, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, '11111111-1111-1111-1111-111111111111', '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, '22222222-2222-2222-2222-222222222222', '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, '33333333-3333-3333-3333-333333333333', '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\t11111111-1111-1111-1111-111111111111
                    15.0\t22222222-2222-2222-2222-222222222222
                    20.0\t33333333-3333-3333-3333-333333333333
                    """, query);
        });
    }

    @Test
    public void testDelegateLong256() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v LONG256, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, '0x01', '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, '0x02', '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, '0x03', '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\t0x01
                    15.0\t0x02
                    20.0\t0x03
                    """, query);
        });
    }

    @Test
    public void testDelegateShort() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v SHORT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, 100, '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, 200, '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, 300, '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\t100
                    15.0\t200
                    20.0\t300
                    """, query);
        });
    }

    @Test
    public void testDelegateStr() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v STRING, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, 'hello', '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, 'world', '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, 'foo', '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\thello
                    15.0\tworld
                    20.0\tfoo
                    """, query);
        });
    }

    @Test
    public void testDelegateSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, 'alpha', '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, 'beta', '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, 'gamma', '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\talpha
                    15.0\tbeta
                    20.0\tgamma
                    """, query);
        });
    }

    @Test
    public void testDelegateTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, '2024-06-01T12:00:00.000000Z', '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, '2024-07-15T08:30:00.000000Z', '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, '2024-08-20T16:45:00.000000Z', '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\t2024-06-01T12:00:00.000000Z
                    15.0\t2024-07-15T08:30:00.000000Z
                    20.0\t2024-08-20T16:45:00.000000Z
                    """, query);
        });
    }

    @Test
    public void testDelegateVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, 'hello', '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, 'world', '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, 'foo', '2024-01-01T00:00:02.000000Z')
                    """);
            // v is VARCHAR → delegated to baseRecord (getVarcharA/getVarcharB)
            String query = "SELECT (a + b) * (c + d) AS x, v FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,v]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\tv
                    10.0\thello
                    15.0\tworld
                    20.0\tfoo
                    """, query);
        });
    }

    @Test
    public void testMaterializeComplexIntExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, c INT, d INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (3, 10, 2, 1, '2024-01-01T00:00:00.000000Z'),
                    (1, 20, 3, 2, '2024-01-01T00:00:01.000000Z'),
                    (2, 30, 1, 3, '2024-01-01T00:00:02.000000Z'),
                    (5, 5, 4, 1, '2024-01-01T00:00:03.000000Z'),
                    (4, 15, 2, 3, '2024-01-01T00:00:04.000000Z')
                    """);
            // (a+b)*(c+d) has high complexity (above threshold 3)
            // Two sort keys to avoid radix sort path (single INT key triggers radix sort)
            String query = "SELECT (a + b) * (c + d) AS x, a FROM t ORDER BY x, a";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, a]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,a]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            // (3+10)*(2+1)=39, (1+20)*(3+2)=105, (2+30)*(1+3)=128, (5+5)*(4+1)=50, (4+15)*(2+3)=95
            assertQueryNoLeakCheck("""
                    x\ta
                    39\t3
                    50\t5
                    95\t4
                    105\t1
                    128\t2
                    """, query);
        });
    }

    @Test
    public void testMaterializeDescending() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (3.0, 10.0, 2.0, 1.0, '2024-01-01T00:00:00.000000Z'),
                    (1.0, 20.0, 3.0, 2.0, '2024-01-01T00:00:01.000000Z'),
                    (2.0, 30.0, 1.0, 3.0, '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x FROM t ORDER BY x DESC";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x desc]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            // (3+10)*(2+1)=39.0, (1+20)*(3+2)=105.0, (2+30)*(1+3)=128.0
            assertQueryNoLeakCheck("""
                    x
                    128.0
                    105.0
                    39.0
                    """, query);
        });
    }

    @Test
    public void testMaterializeDoubleType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            // (2+3)*(1+1)=10.0, (1+2)*(4+1)=15.0, (3+1)*(2+3)=20.0
            assertQueryNoLeakCheck("""
                    x
                    10.0
                    15.0
                    20.0
                    """, query);
        });
    }

    @Test
    public void testMaterializeEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            String query = "SELECT (a + b) * (c + d) AS x FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("x\n", query);
        });
    }

    @Test
    public void testMaterializeFloatType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a FLOAT, b FLOAT, c FLOAT, d FLOAT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, 3.0, 1.0, 1.0, '2024-01-01T00:00:00.000000Z'),
                    (1.0, 2.0, 4.0, 1.0, '2024-01-01T00:00:01.000000Z'),
                    (3.0, 1.0, 2.0, 3.0, '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT (a + b) * (c + d) AS x FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            // (2+3)*(1+1)=10.0, (1+2)*(4+1)=15.0, (3+1)*(2+3)=20.0
            assertQueryNoLeakCheck("""
                    x
                    10.0
                    15.0
                    20.0
                    """, query);
        });
    }

    @Test
    public void testMaterializeLongType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a LONG, b LONG, c LONG, d LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (3, 10, 2, 1, '2024-01-01T00:00:00.000000Z'),
                    (1, 20, 3, 2, '2024-01-01T00:00:01.000000Z'),
                    (2, 30, 1, 3, '2024-01-01T00:00:02.000000Z')
                    """);
            // Two sort keys to avoid radix sort path (single LONG key triggers radix sort)
            String query = "SELECT (a + b) * (c + d) AS x, a FROM t ORDER BY x, a";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, a]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,a]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            // (3+10)*(2+1)=39, (1+20)*(3+2)=105, (2+30)*(1+3)=128
            assertQueryNoLeakCheck("""
                    x\ta
                    39\t3
                    105\t1
                    128\t2
                    """, query);
        });
    }

    @Test
    public void testMaterializeMixedSortKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, c INT, d INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (3, 10, 2, 1, '2024-01-01T00:00:00.000000Z'),
                    (1, 12, 2, 1, '2024-01-01T00:00:01.000000Z'),
                    (2, 30, 1, 3, '2024-01-01T00:00:02.000000Z')
                    """);
            // x = (a+b)*(c+d) has high complexity → materialized; a has complexity 1 → not materialized
            String query = "SELECT (a + b) * (c + d) AS x, a FROM t ORDER BY x, a";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, a]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,a]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            // (3+10)*(2+1)=39, (1+12)*(2+1)=39, (2+30)*(1+3)=128
            // Secondary sort by a: 1 before 3
            assertQueryNoLeakCheck("""
                    x\ta
                    39\t1
                    39\t3
                    128\t2
                    """, query);
        });
    }

    @Test
    public void testMaterializeMultipleSortKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, c INT, d INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (1, 2, 10, 5, '2024-01-01T00:00:00.000000Z'),
                    (1, 2, 3, 4, '2024-01-01T00:00:01.000000Z'),
                    (3, 4, 10, 5, '2024-01-01T00:00:02.000000Z'),
                    (3, 4, 3, 4, '2024-01-01T00:00:03.000000Z')
                    """);
            // Both (a+b)*(c+d) and a*c+b*d have high complexity → both materialized
            String query = "SELECT (a + b) * (c + d) AS x, a * c + b * d AS y FROM t ORDER BY x, y";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, y]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,a*c+b*d]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            // (1+2)*(10+5)=45 y=10+10=20, (1+2)*(3+4)=21 y=3+8=11,
            // (3+4)*(10+5)=105 y=30+20=50, (3+4)*(3+4)=49 y=9+16=25
            assertQueryNoLeakCheck("""
                    x\ty
                    21\t11
                    45\t20
                    49\t25
                    105\t50
                    """, query);
        });
    }

    @Test
    public void testMaterializeSingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (3.0, 10.0, 2.0, 1.0, '2024-01-01T00:00:00.000000Z')");
            String query = "SELECT (a + b) * (c + d) AS x FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            // (3+10)*(2+1)=39.0
            assertQueryNoLeakCheck("""
                    x
                    39.0
                    """, query);
        });
    }

    @Test
    public void testMaterializeWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, c INT, d INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (3, 10, 2, 1, '2024-01-01T00:00:00.000000Z'),
                    (NULL, 20, 3, 2, '2024-01-01T00:00:01.000000Z'),
                    (2, 30, 1, 3, '2024-01-01T00:00:02.000000Z')
                    """);
            // Two sort keys to avoid radix sort path
            String query = "SELECT (a + b) * (c + d) AS x, a FROM t ORDER BY x, a";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, a]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,a]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            // (3+10)*(2+1)=39, (NULL+20)*(3+2)=null, (2+30)*(1+3)=128
            assertQueryNoLeakCheck("""
                    x\ta
                    null\tnull
                    39\t3
                    128\t2
                    """, query);
        });
    }

    @Test
    public void testMaterializeWithSymbolSortKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    ('beta', 2.0, 3.0, 1.0, 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('alpha', 2.0, 3.0, 1.0, 1.0, '2024-01-01T00:00:01.000000Z'),
                    ('gamma', 1.0, 2.0, 4.0, 1.0, '2024-01-01T00:00:02.000000Z')
                    """);
            // x = (a+b)*(c+d) has high complexity → materialized
            // s is SYMBOL (not fixed-size) → not materialized, sorted lexicographically via getSym()
            String query = "SELECT (a + b) * (c + d) AS x, s FROM t ORDER BY x, s";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, s]
                        Materialize sort keys
                            VirtualRecord
                              functions: [a+b*c+d,s]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            // (2+3)*(1+1)=10.0, (2+3)*(1+1)=10.0, (1+2)*(4+1)=15.0
            // Secondary sort by s: alpha before beta
            assertQueryNoLeakCheck("""
                    x\ts
                    10.0\talpha
                    10.0\tbeta
                    15.0\tgamma
                    """, query);
        });
    }

    // --- Type routing tests: verify which types go through materialization ---

    @Test
    public void testNoMaterializeSimpleExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            // a + 1 has complexity ARITHMETIC(2) + COLUMN(1) + NONE(0) = 3,
            // at the default threshold (3) but not above → no materialization
            assertPlanNoLeakCheck(
                    "SELECT a + 1 AS x FROM t ORDER BY x",
                    """
                            Sort light
                              keys: [x]
                                VirtualRecord
                                  functions: [a+1]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testNoMaterializeVariableLengthType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a VARCHAR, b VARCHAR, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    ('c', 'c', '2024-01-01T00:00:00.000000Z'),
                    ('a', 'a', '2024-01-01T00:00:01.000000Z'),
                    ('b', 'b', '2024-01-01T00:00:02.000000Z')
                    """);
            // concat returns VARCHAR (variable-length) → not eligible for materialization
            // Plan should NOT contain "Materialize sort keys"
            String query = "SELECT concat(a, b) AS x FROM t ORDER BY x";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x]
                        VirtualRecord
                          functions: [concat([a,b])]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x
                    aa
                    bb
                    cc
                    """, query);
        });
    }

    @Test
    public void testRouteBooleanMaterialized() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a BOOLEAN, b BOOLEAN, e INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (true, false, 1, '2024-01-01T00:00:00.000000Z'),
                    (false, true, -1, '2024-01-01T00:00:01.000000Z'),
                    (false, false, 2, '2024-01-01T00:00:02.000000Z')
                    """);
            // CASE has self-complexity 5 → exceeds threshold (3). BOOLEAN is fixed 1 byte → materialized
            String query = "SELECT CASE WHEN e > 0 THEN a ELSE b END AS x, e FROM t ORDER BY x, e";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, e]
                        Materialize sort keys
                            VirtualRecord
                              functions: [case([0<e,a,b]),e]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            // e=1→a=true, e=-1→b=true, e=2→a=false. Sorted: false < true
            assertQueryNoLeakCheck("""
                    x\te
                    false\t2
                    true\t-1
                    true\t1
                    """, query);
        });
    }

    @Test
    public void testRouteByteMaterialized() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a BYTE, b BYTE, e INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10, 20, 1, '2024-01-01T00:00:00.000000Z'),
                    (30, 5, -1, '2024-01-01T00:00:01.000000Z'),
                    (15, 25, 2, '2024-01-01T00:00:02.000000Z')
                    """);
            // BYTE is fixed 1 byte → materialized
            String query = "SELECT CASE WHEN e > 0 THEN a ELSE b END AS x, e FROM t ORDER BY x, e";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, e]
                        Materialize sort keys
                            VirtualRecord
                              functions: [case([0<e,a,b]),e]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            // e=1→a=10, e=-1→b=5, e=2→a=15
            assertQueryNoLeakCheck("""
                    x\te
                    5\t-1
                    10\t1
                    15\t2
                    """, query);
        });
    }

    @Test
    public void testRouteCharMaterialized() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a CHAR, b CHAR, e INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    ('x', 'a', 1, '2024-01-01T00:00:00.000000Z'),
                    ('z', 'b', -1, '2024-01-01T00:00:01.000000Z'),
                    ('y', 'c', 2, '2024-01-01T00:00:02.000000Z')
                    """);
            // CHAR is fixed 2 bytes → materialized
            String query = "SELECT CASE WHEN e > 0 THEN a ELSE b END AS x, e FROM t ORDER BY x, e";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, e]
                        Materialize sort keys
                            VirtualRecord
                              functions: [case([0<e,a,b]),e]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            // e=1→'x', e=-1→'b', e=2→'y'
            assertQueryNoLeakCheck("""
                    x\te
                    b\t-1
                    x\t1
                    y\t2
                    """, query);
        });
    }

    @Test
    public void testRouteDateMaterialized() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (d1 DATE, d2 DATE, e INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:01.000Z', '2024-01-01T00:00:04.000Z', 1, '2024-01-01T00:00:00.000000Z'),
                    ('2024-01-01T00:00:03.000Z', '2024-01-01T00:00:02.000Z', -1, '2024-01-01T00:00:01.000000Z'),
                    ('2024-01-01T00:00:05.000Z', '2024-01-01T00:00:06.000Z', 2, '2024-01-01T00:00:02.000000Z')
                    """);
            // DATE is fixed 8 bytes → materialized
            String query = "SELECT CASE WHEN e > 0 THEN d1 ELSE d2 END AS x, e FROM t ORDER BY x, e";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, e]
                        Materialize sort keys
                            VirtualRecord
                              functions: [case([0<e,d1,d2]),e]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            // e=1→d1='01', e=-1→d2='02', e=2→d1='05'
            assertQueryNoLeakCheck("""
                    x\te
                    2024-01-01T00:00:01.000Z\t1
                    2024-01-01T00:00:02.000Z\t-1
                    2024-01-01T00:00:05.000Z\t2
                    """, query);
        });
    }

    @Test
    public void testRouteDecimal128Materialized() throws Exception {
        assertMemoryLeak(() -> {
            // DECIMAL(30,2) uses DECIMAL128 storage (16 bytes)
            execute("CREATE TABLE t (a DECIMAL(30,2), b DECIMAL(30,2), e INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    ('10.00', '20.00', 1, '2024-01-01T00:00:00.000000Z'),
                    ('30.00', '5.00', -1, '2024-01-01T00:00:01.000000Z'),
                    ('15.00', '25.00', 2, '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT CASE WHEN e > 0 THEN a ELSE b END AS x, e FROM t ORDER BY x, e";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, e]
                        Materialize sort keys
                            VirtualRecord
                              functions: [case([0<e,a,b]),e]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            // e=1→a=10.00, e=-1→b=5.00, e=2→a=15.00
            assertQueryNoLeakCheck("""
                    x\te
                    5.00\t-1
                    10.00\t1
                    15.00\t2
                    """, query);
        });
    }

    @Test
    public void testRouteDecimal16Materialized() throws Exception {
        assertMemoryLeak(() -> {
            // DECIMAL(4,2) uses DECIMAL16 storage (2 bytes)
            execute("CREATE TABLE t (a DECIMAL(4,2), b DECIMAL(4,2), e INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    ('10.00', '20.00', 1, '2024-01-01T00:00:00.000000Z'),
                    ('30.00', '5.00', -1, '2024-01-01T00:00:01.000000Z'),
                    ('15.00', '25.00', 2, '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT CASE WHEN e > 0 THEN a ELSE b END AS x, e FROM t ORDER BY x, e";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, e]
                        Materialize sort keys
                            VirtualRecord
                              functions: [case([0<e,a,b]),e]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\te
                    5.00\t-1
                    10.00\t1
                    15.00\t2
                    """, query);
        });
    }

    @Test
    public void testRouteDecimal256Materialized() throws Exception {
        assertMemoryLeak(() -> {
            // DECIMAL(50,2) uses DECIMAL256 storage (32 bytes)
            execute("CREATE TABLE t (a DECIMAL(50,2), b DECIMAL(50,2), e INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    ('10.00', '20.00', 1, '2024-01-01T00:00:00.000000Z'),
                    ('30.00', '5.00', -1, '2024-01-01T00:00:01.000000Z'),
                    ('15.00', '25.00', 2, '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT CASE WHEN e > 0 THEN a ELSE b END AS x, e FROM t ORDER BY x, e";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, e]
                        Materialize sort keys
                            VirtualRecord
                              functions: [case([0<e,a,b]),e]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            // e=1→a=10.00, e=-1→b=5.00, e=2→a=15.00
            assertQueryNoLeakCheck("""
                    x\te
                    5.00\t-1
                    10.00\t1
                    15.00\t2
                    """, query);
        });
    }

    @Test
    public void testRouteDecimal32Materialized() throws Exception {
        assertMemoryLeak(() -> {
            // DECIMAL(8,2) uses DECIMAL32 storage (4 bytes)
            execute("CREATE TABLE t (a DECIMAL(8,2), b DECIMAL(8,2), e INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    ('10.00', '20.00', 1, '2024-01-01T00:00:00.000000Z'),
                    ('30.00', '5.00', -1, '2024-01-01T00:00:01.000000Z'),
                    ('15.00', '25.00', 2, '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT CASE WHEN e > 0 THEN a ELSE b END AS x, e FROM t ORDER BY x, e";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, e]
                        Materialize sort keys
                            VirtualRecord
                              functions: [case([0<e,a,b]),e]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\te
                    5.00\t-1
                    10.00\t1
                    15.00\t2
                    """, query);
        });
    }

    @Test
    public void testRouteDecimal64Materialized() throws Exception {
        assertMemoryLeak(() -> {
            // DECIMAL(15,2) uses DECIMAL64 storage (8 bytes)
            execute("CREATE TABLE t (a DECIMAL(15,2), b DECIMAL(15,2), e INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    ('10.00', '20.00', 1, '2024-01-01T00:00:00.000000Z'),
                    ('30.00', '5.00', -1, '2024-01-01T00:00:01.000000Z'),
                    ('15.00', '25.00', 2, '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT CASE WHEN e > 0 THEN a ELSE b END AS x, e FROM t ORDER BY x, e";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, e]
                        Materialize sort keys
                            VirtualRecord
                              functions: [case([0<e,a,b]),e]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            // e=1→a=10.00, e=-1→b=5.00, e=2→a=15.00
            assertQueryNoLeakCheck("""
                    x\te
                    5.00\t-1
                    10.00\t1
                    15.00\t2
                    """, query);
        });
    }

    @Test
    public void testRouteDecimal8Materialized() throws Exception {
        assertMemoryLeak(() -> {
            // DECIMAL(2,1) uses DECIMAL8 storage (1 byte)
            execute("CREATE TABLE t (a DECIMAL(2,1), b DECIMAL(2,1), e INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    ('1.0', '2.0', 1, '2024-01-01T00:00:00.000000Z'),
                    ('3.0', '0.5', -1, '2024-01-01T00:00:01.000000Z'),
                    ('1.5', '2.5', 2, '2024-01-01T00:00:02.000000Z')
                    """);
            String query = "SELECT CASE WHEN e > 0 THEN a ELSE b END AS x, e FROM t ORDER BY x, e";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, e]
                        Materialize sort keys
                            VirtualRecord
                              functions: [case([0<e,a,b]),e]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            assertQueryNoLeakCheck("""
                    x\te
                    0.5\t-1
                    1.0\t1
                    1.5\t2
                    """, query);
        });
    }

    @Test
    public void testRouteGeoHashByteMaterialized() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, e INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 20.0, 30.0, 10.0, 1, '2024-01-01T00:00:00.000000Z'),
                    (-30.0, 10.0, -20.0, 15.0, 2, '2024-01-01T00:00:01.000000Z'),
                    (50.0, 60.0, 20.0, 5.0, 3, '2024-01-01T00:00:02.000000Z')
                    """);
            // make_geohash(lon, lat, 5) returns GEOHASH(1c) = GEOBYTE (1 byte, fixed)
            // args include computed expressions (a+b, c+d) → high complexity, above threshold → materialized
            String query = "SELECT make_geohash(a + b, c + d, 5) AS x, e FROM t ORDER BY x, e";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, e]
                        Materialize sort keys
                            VirtualRecord
                              functions: [make_geohash(a+b,c+d,5),e]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            // Verify geohash values are read through MaterializedRecord
            // lon=a+b, lat=c+d: row1(30,40)→s, row2(-20,-5)→7, row3(110,25)→w; sorted: 7,s,w
            assertQueryNoLeakCheck("""
                    x\te
                    7\t2
                    s\t1
                    w\t3
                    """, query);
        });
    }

    @Test
    public void testRouteGeoHashIntMaterialized() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, e INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 20.0, 30.0, 10.0, 1, '2024-01-01T00:00:00.000000Z'),
                    (-30.0, 10.0, -20.0, 15.0, 2, '2024-01-01T00:00:01.000000Z'),
                    (50.0, 60.0, 20.0, 5.0, 3, '2024-01-01T00:00:02.000000Z')
                    """);
            // make_geohash(lon, lat, 30) returns GEOHASH(6c) = GEOINT (4 bytes, fixed)
            String query = "SELECT make_geohash(a + b, c + d, 30) AS x, e FROM t ORDER BY x, e";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, e]
                        Materialize sort keys
                            VirtualRecord
                              functions: [make_geohash(a+b,c+d,30),e]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            // Read x to force getGeoInt through MaterializedRecord; verify sort order via e
            assertQueryNoLeakCheck("""
                    e
                    2
                    1
                    3
                    """, "SELECT e FROM (" + query + ")");
        });
    }

    @Test
    public void testRouteGeoHashLongMaterialized() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, e INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 20.0, 30.0, 10.0, 1, '2024-01-01T00:00:00.000000Z'),
                    (-30.0, 10.0, -20.0, 15.0, 2, '2024-01-01T00:00:01.000000Z'),
                    (50.0, 60.0, 20.0, 5.0, 3, '2024-01-01T00:00:02.000000Z')
                    """);
            // make_geohash(lon, lat, 60) returns GEOHASH(12c) = GEOLONG (8 bytes, fixed)
            String query = "SELECT make_geohash(a + b, c + d, 60) AS x, e FROM t ORDER BY x, e";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, e]
                        Materialize sort keys
                            VirtualRecord
                              functions: [make_geohash(a+b,c+d,60),e]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            // Read x to force getGeoLong through MaterializedRecord; verify sort order via e
            assertQueryNoLeakCheck("""
                    e
                    2
                    1
                    3
                    """, "SELECT e FROM (" + query + ")");
        });
    }

    @Test
    public void testRouteGeoHashShortMaterialized() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, e INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, 20.0, 30.0, 10.0, 1, '2024-01-01T00:00:00.000000Z'),
                    (-30.0, 10.0, -20.0, 15.0, 2, '2024-01-01T00:00:01.000000Z'),
                    (50.0, 60.0, 20.0, 5.0, 3, '2024-01-01T00:00:02.000000Z')
                    """);
            // make_geohash(lon, lat, 15) returns GEOHASH(3c) = GEOSHORT (2 bytes, fixed)
            String query = "SELECT make_geohash(a + b, c + d, 15) AS x, e FROM t ORDER BY x, e";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, e]
                        Materialize sort keys
                            VirtualRecord
                              functions: [make_geohash(a+b,c+d,15),e]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            // Read x to force getGeoShort through MaterializedRecord; verify sort order via e
            assertQueryNoLeakCheck("""
                    e
                    2
                    1
                    3
                    """, "SELECT e FROM (" + query + ")");
        });
    }

    @Test
    public void testRouteIPv4NotMaterialized() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ip1 IPv4, ip2 IPv4, e INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    ('1.1.1.1', '4.4.4.4', 1, '2024-01-01T00:00:00.000000Z'),
                    ('3.3.3.3', '2.2.2.2', -1, '2024-01-01T00:00:01.000000Z'),
                    ('5.5.5.5', '6.6.6.6', 2, '2024-01-01T00:00:02.000000Z')
                    """);
            // IPv4 is excluded from materialization (no arithmetic on this type)
            String query = "SELECT CASE WHEN e > 0 THEN ip1 ELSE ip2 END AS x, e FROM t ORDER BY x, e";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, e]
                        VirtualRecord
                          functions: [case([0<e,ip1,ip2]),e]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: t
                    """);
            // e=1→'1.1.1.1', e=-1→'2.2.2.2', e=2→'5.5.5.5'
            assertQueryNoLeakCheck("""
                    x\te
                    1.1.1.1\t1
                    2.2.2.2\t-1
                    5.5.5.5\t2
                    """, query);
        });
    }

    @Test
    public void testRouteShortMaterialized() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a SHORT, b SHORT, e INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (100, 200, 1, '2024-01-01T00:00:00.000000Z'),
                    (300, 50, -1, '2024-01-01T00:00:01.000000Z'),
                    (150, 250, 2, '2024-01-01T00:00:02.000000Z')
                    """);
            // SHORT is fixed 2 bytes → materialized
            String query = "SELECT CASE WHEN e > 0 THEN a ELSE b END AS x, e FROM t ORDER BY x, e";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, e]
                        Materialize sort keys
                            VirtualRecord
                              functions: [case([0<e,a,b]),e]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            // e=1→a=100, e=-1→b=50, e=2→a=150
            assertQueryNoLeakCheck("""
                    x\te
                    50\t-1
                    100\t1
                    150\t2
                    """, query);
        });
    }

    @Test
    public void testRouteStringNotMaterialized() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s1 STRING, s2 STRING, e INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    ('hello', 'world', 1, '2024-01-01T00:00:00.000000Z'),
                    ('foo', 'bar', -1, '2024-01-01T00:00:01.000000Z'),
                    ('abc', 'xyz', 2, '2024-01-01T00:00:02.000000Z')
                    """);
            // STRING is variable-length → NOT materialized
            String query = "SELECT CASE WHEN e > 0 THEN s1 ELSE s2 END AS x, e FROM t ORDER BY x, e";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, e]
                        VirtualRecord
                          functions: [case([0<e,s1,s2]),e]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: t
                    """);
            // e=1→'hello', e=-1→'bar', e=2→'abc'
            assertQueryNoLeakCheck("""
                    x\te
                    abc\t2
                    bar\t-1
                    hello\t1
                    """, query);
        });
    }

    // --- Delegation tests: non-materialized columns read through MaterializedRecord ---

    @Test
    public void testRouteTimestampMaterialized() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (t1 TIMESTAMP, t2 TIMESTAMP, e INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:01.000000Z', '2024-01-01T00:00:04.000000Z', 1, '2024-01-01T00:00:00.000000Z'),
                    ('2024-01-01T00:00:03.000000Z', '2024-01-01T00:00:02.000000Z', -1, '2024-01-01T00:00:01.000000Z'),
                    ('2024-01-01T00:00:05.000000Z', '2024-01-01T00:00:06.000000Z', 2, '2024-01-01T00:00:02.000000Z')
                    """);
            // TIMESTAMP is fixed 8 bytes → materialized
            String query = "SELECT CASE WHEN e > 0 THEN t1 ELSE t2 END AS x, e FROM t ORDER BY x, e";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, e]
                        Materialize sort keys
                            VirtualRecord
                              functions: [case([0<e,t1,t2]),e]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                    """);
            // e=1→t1='01', e=-1→t2='02', e=2→t1='05'
            assertQueryNoLeakCheck("""
                            x\te
                            2024-01-01T00:00:01.000000Z\t1
                            2024-01-01T00:00:02.000000Z\t-1
                            2024-01-01T00:00:05.000000Z\t2
                            """,
                    query,
                    "x",
                    true,
                    true
            );
        });
    }

    @Test
    public void testRouteUuidNotMaterialized() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (u1 UUID, u2 UUID, e INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    ('11111111-1111-1111-1111-111111111111', '44444444-4444-4444-4444-444444444444', 1, '2024-01-01T00:00:00.000000Z'),
                    ('33333333-3333-3333-3333-333333333333', '22222222-2222-2222-2222-222222222222', -1, '2024-01-01T00:00:01.000000Z'),
                    ('55555555-5555-5555-5555-555555555555', '66666666-6666-6666-6666-666666666666', 2, '2024-01-01T00:00:02.000000Z')
                    """);
            // UUID is 16 bytes (> Long.BYTES) → NOT materialized
            String query = "SELECT CASE WHEN e > 0 THEN u1 ELSE u2 END AS x, e FROM t ORDER BY x, e";
            assertPlanNoLeakCheck(query, """
                    Sort light
                      keys: [x, e]
                        VirtualRecord
                          functions: [case([0<e,u1,u2]),e]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: t
                    """);
            // e=1→u1='111...', e=-1→u2='222...', e=2→u1='555...'
            assertQueryNoLeakCheck("""
                    x\te
                    11111111-1111-1111-1111-111111111111\t1
                    22222222-2222-2222-2222-222222222222\t-1
                    55555555-5555-5555-5555-555555555555\t2
                    """, query);
        });
    }
}
