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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class LastNotNullArrayGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testNullArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (arr double[])");
            execute("insert into tab values (ARRAY[1.0, 2.0])");
            execute("insert into tab values (null)");
            assertQuery("select last_not_null(arr) arr from tab")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            arr
                            [1.0,2.0]
                            """);
        });
    }

    @Test
    public void testArrayWithNullElements() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (arr double[])");
            execute("insert into tab values (ARRAY[1.0, 2.0])");
            execute("insert into tab values (ARRAY[3.0, null, 5.0])");
            assertQuery("select last_not_null(arr) arr from tab")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            arr
                            [3.0,null,5.0]
                            """);
        });
    }

    @Test
    public void testEmptyArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (arr double[])");
            execute("insert into tab values (ARRAY[1.0, 2.0])");
            execute("insert into tab values (ARRAY[])");
            execute("insert into tab values (null)");
            assertQuery("select last_not_null(arr) arr from tab")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            arr
                            []
                            """);
        });
    }

    @Test
    public void testSingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (arr double[])");
            execute("insert into tab values (ARRAY[1.0, 2.0])");
            assertQuery("select last_not_null(arr) arr from tab")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            arr
                            [1.0,2.0]
                            """);
        });
    }

    @Test
    public void testAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (arr double[])");
            execute("insert into tab values (null)");
            execute("insert into tab values (null)");
            assertQuery("select last_not_null(arr) arr from tab")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            arr
                            null
                            """);
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
            assertQuery("select grp, last_not_null(arr) arr from tab order by grp")
                    .expectSize()
                    .returns("""
                            grp\tarr
                            1\t[10.0,11.0]
                            2\t[20.0,21.0]
                            """);
        });
    }

    @Test
    public void testNullsAfterNonNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (arr double[])");
            execute("insert into tab values (ARRAY[1.0, 2.0])");
            execute("insert into tab values (ARRAY[3.0, 4.0])");
            execute("insert into tab values (null)");
            execute("insert into tab values (null)");
            assertQuery("select last_not_null(arr) arr from tab")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            arr
                            [3.0,4.0]
                            """);
        });
    }

    @Test
    public void testKeyedWithAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (grp int, arr double[])");
            execute("insert into tab values (1, null)");
            execute("insert into tab values (1, null)");
            execute("insert into tab values (2, ARRAY[20.0, 21.0])");
            assertQuery("select grp, last_not_null(arr) arr from tab order by grp")
                    .expectSize()
                    .returns("""
                            grp\tarr
                            1\tnull
                            2\t[20.0,21.0]
                            """);
        });
    }
}
