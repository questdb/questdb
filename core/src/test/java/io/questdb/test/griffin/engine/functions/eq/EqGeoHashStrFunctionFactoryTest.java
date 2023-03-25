/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.griffin.engine.functions.eq;

import io.questdb.test.AbstractGriffinTest;
import org.junit.Test;

public class EqGeoHashStrFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testEq1() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table geohash as (" +
                            "select " +
                            "    cast('sp052w92p1' as GeOhAsH(50b)) geohash1, " +
                            "    cast(null as GeOhAsH(50b)) geohash2 " +
                            "from long_sequence(1)" +
                            ")",
                    sqlExecutionContext);
            assertSql(
                    "geohash where 'sp052w92p1' = geohash1",
                    "geohash1\tgeohash2\n" +
                            "sp052w92p1\t\n"
            );
        });
    }

    @Test
    public void testEq2() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table geohash as (" +
                            "select " +
                            "    cast('sp052w92p1' as GeOhAsH(50b)) geohash1, " +
                            "    cast(null as GeOhAsH(50b)) geohash2 " +
                            "from long_sequence(1)" +
                            ")",
                    sqlExecutionContext);
            assertSql(
                    "geohash where geohash1 = 'sp052w92p1'",
                    "geohash1\tgeohash2\n" +
                            "sp052w92p1\t\n"
            );
        });
    }

    @Test
    public void testEq3() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table geohash as (" +
                            "select " +
                            "    cast('sp052w92p1' as GeOhAsH(50b)) geohash1, " +
                            "    cast(null as GeOhAsH(50b)) geohash2 " +
                            "from long_sequence(1)" +
                            ")",
                    sqlExecutionContext);
            assertSql(
                    "geohash where geohash2 = null",
                    "geohash1\tgeohash2\n" +
                            "sp052w92p1\t\n"
            );
        });
    }

    @Test
    public void testNoEq1() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table geohash as (" +
                            "select " +
                            "    cast('sp052w92p1' as GeOhAsH(50b)) geohash1, " +
                            "    cast(null as GeOhAsH(50b)) geohash2 " +
                            "from long_sequence(1)" +
                            ")",
                    sqlExecutionContext);
            assertSql(
                    "geohash where 'sp052w92p0' != geohash1",
                    "geohash1\tgeohash2\n" +
                            "sp052w92p1\t\n"
            );
        });
    }

    @Test
    public void testNoEq2() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table geohash as (" +
                            "select " +
                            "    cast('sp052w92p1' as GeOhAsH(50b)) geohash1, " +
                            "    cast(null as GeOhAsH(50b)) geohash2 " +
                            "from long_sequence(1)" +
                            ")",
                    sqlExecutionContext);
            assertSql(
                    "geohash where geohash1 != 'sp052w92p0'",
                    "geohash1\tgeohash2\n" +
                            "sp052w92p1\t\n"
            );
        });
    }

    @Test
    public void testNoEq3() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table geohash as (" +
                            "select " +
                            "    cast('sp052w92p1' as GeOhAsH(50b)) geohash1, " +
                            "    cast(null as GeOhAsH(50b)) geohash2 " +
                            "from long_sequence(1)" +
                            ")",
                    sqlExecutionContext);
            assertSql(
                    "geohash where geohash2 != null",
                    "geohash1\tgeohash2\n"
            );
        });
    }
}
