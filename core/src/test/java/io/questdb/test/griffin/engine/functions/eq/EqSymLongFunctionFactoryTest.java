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

package io.questdb.test.griffin.engine.functions.eq;

import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class EqSymLongFunctionFactoryTest extends AbstractCairoTest {
    @Test
    public void testDynamicCast() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('1','3','5') a, abs(rnd_long())%5 b  from long_sequence(10))");
            assertQuery("select a,b from x where a = b")
                    .noLeakCheck()
                    .returns("""
                            a\tb
                            1\t1
                            1\t1
                            1\t1
                            """);
        });
    }

    @Test
    public void testDynamicCastNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('1','3','5', null) a, abs(rnd_long())%5 b  from long_sequence(20))");
            assertQuery("select a,b from x where a = null::long")
                    .noLeakCheck()
                    .returns("""
                            a\tb
                            \t3
                            \t1
                            \t2
                            \t0
                            \t3
                            """);
        });
    }

    @Test
    public void testDynamicCastNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('1','3','5', null) a, abs(rnd_long())%5 b  from long_sequence(20))");
            assertQuery("select a,b from x where a = b")
                    .noLeakCheck()
                    .returns("""
                            a\tb
                            1\t1
                            3\t3
                            1\t1
                            1\t1
                            3\t3
                            1\t1
                            """);
        });
    }

    @Test
    public void testDynamicSymbolTable() throws Exception {
        assertQuery("select x from long_sequence(10) where rnd_symbol('1','3','5') = 3")
                .returnsOnce("""
                        x
                        3
                        8
                        10
                        """);
    }

    @Test
    public void testStaticSymbolTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('1','3','5') a from long_sequence(10))");
            assertQuery("select a from x where a = 3")
                    .noLeakCheck()
                    .returns("""
                            a
                            3
                            3
                            3
                            """);
        });
    }

    @Test
    public void testIndexedSymbolBindKeys() throws Exception {
        assertMemoryLeak(() -> {
            createKeyTable("t", "s SYMBOL INDEX");
            bindVariableService.clear();
            bindVariableService.setLong(0, 7);
            assertKeyQuery("SELECT v FROM t WHERE s = $1 ORDER BY v", "1\n6\n");
            bindVariableService.clear();
            bindVariableService.setInt(0, -7);
            assertKeyQuery("SELECT v FROM t WHERE s = $1 ORDER BY v", "4\n");
            bindVariableService.clear();
            bindVariableService.setShort(0, (short) 10);
            assertKeyQuery("SELECT v FROM t WHERE s = $1 ORDER BY v", "3\n");
            bindVariableService.clear();
            bindVariableService.setInt(0, Numbers.INT_NULL);
            assertKeyQuery("SELECT v FROM t WHERE s = $1 ORDER BY v", "5\n");
            bindVariableService.clear();
            bindVariableService.setLong(0, 7);
            bindVariableService.setInt(1, 10);
            assertException("SELECT v FROM t WHERE s IN ($1, $2) ORDER BY v", 28, "STRING constant expected");
            assertException("SELECT v FROM t WHERE n IN ($1, $2) ORDER BY v", 28, "STRING constant expected");
            bindVariableService.clear();
        });
    }

    @Test
    public void testIndexedSymbolCoveringKeys() throws Exception {
        assertMemoryLeak(() -> {
            createKeyTable("c", "s SYMBOL INDEX TYPE POSTING INCLUDE (v)");
            final String[][] cases = {
                    {"SELECT v FROM c WHERE s = 7", "1\n6\n"},
                    {"SELECT v FROM c WHERE s = '5'::SYMBOL", "2\n"},
            };
            for (String[] c : cases) {
                assertQuery(c[0])
                        .noLeakCheck()
                        .sizeMayVary()
                        .noRandomAccess()
                        .withPlanContaining("CoveringIndex")
                        .returns("v\n" + c[1]);
            }
            assertQuery("SELECT v FROM c WHERE s IN (07, 1_0) ORDER BY v")
                    .noLeakCheck()
                    .sizeMayVary()
                    .withPlanContaining("CoveringIndex")
                    .returns("v\n1\n3\n6\n");
        });
    }

    @Test
    public void testIndexedSymbolTextKeys() throws Exception {
        assertMemoryLeak(() -> {
            createKeyTable("t", "s SYMBOL INDEX");
            final String[][] cases = {
                    {"%s = 7", "1\n6\n"},
                    {"7 = %s", "1\n6\n"},
                    {"%s = 7L", "1\n6\n"},
                    {"%s = 07", "1\n6\n"},
                    {"%s = 1_0", "3\n"},
                    {"%s = -7", "4\n"},
                    {"%s = 7::BYTE", "1\n6\n"},
                    {"%s = 7::SHORT", "1\n6\n"},
                    {"%s = 7::INT", "1\n6\n"},
                    {"%s = NULL::INT", "5\n"},
                    {"%s = NULL::LONG", "5\n"},
                    {"%s = '5'::SYMBOL", "2\n"},
                    {"%s = '07'", "7\n"},
                    {"%s = 7 AND %s = '7'", "1\n6\n"},
                    {"%s IN (7, 10)", "1\n3\n6\n"},
                    {"%s IN (07, 1_0, -7)", "1\n3\n4\n6\n"},
                    {"%s IN ('5'::SYMBOL, '7')", "1\n2\n6\n"},
                    {"%s IN (7, NULL::INT)", "1\n5\n6\n"},
                    {"%s IN (7, 10) AND %s != 10", "1\n6\n"},
            };
            for (String[] c : cases) {
                assertKeyQuery("SELECT v FROM t WHERE " + c[0].replace("%s", "s") + " ORDER BY v", c[1]);
                assertQuery("SELECT v FROM t WHERE " + c[0].replace("%s", "n") + " ORDER BY v")
                        .noLeakCheck()
                        .sizeMayVary()
                        .returns("v\n" + c[1]);
            }
            final String[][] unindexed = {
                    {"%s = 7 AND %s = 10", ""},
                    {"%s = 7 OR %s = 10", "1\n3\n6\n"},
                    {"%s = '5'::SYMBOL OR %s = 'x'", "2\n"},
                    {"%s = 7 OR %s = '5'::SYMBOL OR %s = '10'", "1\n2\n3\n6\n"},
                    {"%s = 07 OR %s = NULL::INT", "1\n5\n6\n"},
                    {"%s = (SELECT 7)", "1\n6\n"},
                    {"(SELECT 7L) = %s", "1\n6\n"},
                    {"%s = (SELECT -7::SHORT)", "4\n"},
                    {"%s = (SELECT 10::BYTE)", "3\n"},
                    {"%s = (SELECT NULL::INT)", "5\n"},
                    {"%s != (SELECT 7)", "2\n3\n4\n5\n7\n"},
                    {"%s != 7", "2\n3\n4\n5\n7\n"},
                    {"%s NOT IN (7, 10)", "2\n4\n5\n7\n"},
                    {"%s NOT IN ('5'::SYMBOL, -7)", "1\n3\n5\n6\n7\n"},
            };
            for (String[] c : unindexed) {
                assertQuery("SELECT v FROM t WHERE " + c[0].replace("%s", "s") + " ORDER BY v")
                        .noLeakCheck()
                        .sizeMayVary()
                        .returns("v\n" + c[1]);
                assertQuery("SELECT v FROM t WHERE " + c[0].replace("%s", "n") + " ORDER BY v")
                        .noLeakCheck()
                        .sizeMayVary()
                        .returns("v\n" + c[1]);
            }
            assertException("SELECT v FROM t WHERE s IN (7.0)", 28, "STRING constant expected");
            assertException("SELECT v FROM t WHERE n IN (7.0)", 28, "STRING constant expected");
            assertException("SELECT v FROM t WHERE s = 7.0", 24, "there is no matching operator `=` with the argument types: SYMBOL = DOUBLE");
            assertException("SELECT v FROM t WHERE n = 7.0", 24, "there is no matching operator `=` with the argument types: SYMBOL = DOUBLE");
            assertQuery("SELECT v FROM t WHERE s = 7 ORDER BY ts DESC LIMIT 1")
                    .noLeakCheck()
                    .sizeMayVary()
                    .withPlanContaining("Index backward scan on: s")
                    .returns("v\n6\n");
            assertQuery("SELECT v, s FROM t WHERE s = 7 LATEST ON ts PARTITION BY s")
                    .noLeakCheck()
                    .sizeMayVary()
                    .withPlanContaining("Index backward scan on: s")
                    .returns("v\ts\n6\t7\n");
            assertQuery("SELECT v, s FROM t WHERE s IN (7, '5'::SYMBOL) LATEST ON ts PARTITION BY s ORDER BY v")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("v\ts\n2\t5\n6\t7\n");
            assertQuery("SELECT v, s FROM t WHERE s = 7 OR s = '5'::SYMBOL LATEST ON ts PARTITION BY s ORDER BY v")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("v\ts\n2\t5\n6\t7\n");
            assertQuery("SELECT v, s FROM t WHERE n IN (7, '5'::SYMBOL) LATEST ON ts PARTITION BY s ORDER BY v")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("v\ts\n2\t5\n6\t7\n");
        });
    }

    @Test
    public void testStaticSymbolTableNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('1','3','5', null) a from long_sequence(30))");
            assertQuery("select a from x where a = null::long")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            a
                            
                            
                            
                            
                            
                            
                            
                            
                            """);
        });
    }

    private static void createKeyTable(String name, String keyColumn) throws Exception {
        execute("CREATE TABLE " + name + " (" + keyColumn + ", n SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO " + name + """
                 VALUES
                ('7', '7', 1, '2024-01-01T01'),
                ('5', '5', 2, '2024-01-01T02'),
                ('10', '10', 3, '2024-01-01T03'),
                ('-7', '-7', 4, '2024-01-01T04'),
                (NULL, NULL, 5, '2024-01-01T05'),
                ('7', '7', 6, '2024-01-01T06'),
                ('07', '07', 7, '2024-01-01T07')
                """);
    }

    private void assertKeyQuery(String sql, String expected) throws Exception {
        assertQuery(sql)
                .noLeakCheck()
                .sizeMayVary()
                .withPlanContaining("Index forward scan on: s")
                .returns("v\n" + expected);
    }
}