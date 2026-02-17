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

public class LastArrayGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testNotKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (arr double[])");
            execute("insert into tab values (ARRAY[1.0, 2.0])");
            execute("insert into tab values (ARRAY[3.0, 4.0])");
            assertQuery(
                    "arr\n" +
                            "[3.0,4.0]\n",
                    "select last(arr) arr from tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (grp int, arr double[])");
            execute("insert into tab values (1, ARRAY[10.0, 11.0])");
            execute("insert into tab values (1, ARRAY[20.0, 21.0])");
            execute("insert into tab values (1, ARRAY[30.0, 31.0])");
            execute("insert into tab values (2, ARRAY[40.0, 41.0])");
            execute("insert into tab values (2, ARRAY[50.0, 51.0])");
            assertQuery(
                    "grp\tarr\n" +
                            "1\t[30.0,31.0]\n" +
                            "2\t[50.0,51.0]\n",
                    "select grp, last(arr) arr from tab order by grp",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testNullArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (arr double[])");
            execute("insert into tab values (ARRAY[1.0, 2.0])");
            execute("insert into tab values (null)");
            assertQuery(
                    "arr\n" +
                            "null\n",
                    "select last(arr) arr from tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmptyArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (arr double[])");
            execute("insert into tab values (ARRAY[1.0, 2.0])");
            execute("insert into tab values (ARRAY[])");
            assertQuery(
                    "arr\n" +
                            "[]\n",
                    "select last(arr) arr from tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testArrayWithNullElements() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (arr double[])");
            execute("insert into tab values (ARRAY[1.0, 2.0])");
            execute("insert into tab values (ARRAY[3.0, null, 5.0])");
            assertQuery(
                    "arr\n" +
                            "[3.0,null,5.0]\n",
                    "select last(arr) arr from tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testSingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (arr double[])");
            execute("insert into tab values (ARRAY[1.0, 2.0])");
            assertQuery(
                    "arr\n" +
                            "[1.0,2.0]\n",
                    "select last(arr) arr from tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (arr double[])");
            execute("insert into tab values (null)");
            execute("insert into tab values (null)");
            assertQuery(
                    "arr\n" +
                            "null\n",
                    "select last(arr) arr from tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testKeyedWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (grp int, arr double[])");
            execute("insert into tab values (1, ARRAY[10.0, 11.0])");
            execute("insert into tab values (1, null)");
            execute("insert into tab values (2, null)");
            execute("insert into tab values (2, ARRAY[20.0, 21.0])");
            assertQuery(
                    "grp\tarr\n" +
                            "1\tnull\n" +
                            "2\t[20.0,21.0]\n",
                    "select grp, last(arr) arr from tab order by grp",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testDifferentArraySizes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (arr double[])");
            execute("insert into tab values (ARRAY[1.0])");
            execute("insert into tab values (ARRAY[2.0, 3.0, 4.0])");
            assertQuery(
                    "arr\n" +
                            "[2.0,3.0,4.0]\n",
                    "select last(arr) arr from tab",
                    null,
                    false,
                    true
            );
        });
    }

}
