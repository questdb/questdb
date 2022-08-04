/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import org.junit.Test;

public class OrderByExpressionTest extends AbstractGriffinTest {

    @Test//fails with [0] Duplicate column [name=column] 
    public void testOrderByTwoExpressions() throws Exception {
        assertQuery("x\n6\n7\n8\n",
                "select x from long_sequence(10) order by x/2 desc, x*8 desc limit 5", null, null, true, true, true);
    }

    @Test//fails with io.questdb.griffin.SqlException: [0] Duplicate column [name=column]
    public void testOrderByTwoExpressionsInNestedQuery() throws Exception {
        assertQuery("x\n6\n7\n8\n",
                "select * from \n" +
                        "(\n" +
                        "  select x from long_sequence(10) order by x/2 desc, x*8 desc limit 5 \n" +
                        ")\n" +
                        "order by x*2 asc\n" +
                        "limit 3", null, null, true, true, true);
    }

    @Test
    public void testOrderByExpressionInNestedQuery() throws Exception {
        assertQuery("x\n6\n7\n8\n",
                "select * from \n" +
                        "(\n" +
                        "  select x from long_sequence(10) order by x/2 desc limit 5 \n" +
                        ")\n" +
                        "order by x*2 asc\n" +
                        "limit 3", null, null, true, true, true);
    }

    @Test
    public void testOrderByExpressionWithFunctionCallInNestedQuery() throws Exception {
        assertQuery("x\n6\n7\n8\n",
                "select * from \n" +
                        "(\n" +
                        "    select x from long_sequence(10) order by x+rnd_int(1,10,0)*0 desc limit 5 \n" +
                        ")\n" +
                        "order by x*2 asc\n" +
                        "limit 3", null, null, true, true, true);
    }

    @Test
    public void testOrderByExpressionWithFunctionCallInWithClause() throws Exception {
        assertQuery("x\n6\n7\n8\n",
                "with q as (select x from long_sequence(10) order by x+rnd_int(1,10,0)*0 desc limit 5 ) \n" +
                        "select * from q\n" +
                        "order by x*2 asc\n" +
                        "limit 3", null, null, true, true, true);
    }

    @Test
    //fails with duplicate column : column because alias created for 'x*x' clashes with one created for x+rnd_int(1,10,0)*0
    public void testOrderByExpressionInJoinedSubquery() throws Exception {
        assertQuery("x\n6\n7\n8\n",
                "select * from \n" +
                        "(\n" +
                        "  select x from long_sequence(10) \n" +
                        ")\n" +
                        "cross join \n" +
                        "(\n" +
                        "    select x*x from long_sequence(10) order by x+rnd_int(1,10,0)*0 desc limit 5 \n" +
                        ")\n" +
                        "order by x*2 asc\n" +
                        "limit 3", null, null, true, true, true);
    }

    @Test //fails on io.questdb.griffin.SqlException: [123] Invalid column: +
    public void testOrderByExpressionWhenColumnHasAliasInJoinedSubquery() throws Exception {
        assertQuery("x\n6\n7\n8\n",
                "select * from \n" +
                        "(\n" +
                        "  select x from long_sequence(10) \n" +
                        ")\n" +
                        "cross join \n" +
                        "(\n" +
                        "    select x*x as ext from long_sequence(10) order by x+rnd_int(1,10,0)*0 desc limit 5 \n" +
                        ")\n" +
                        "order by x*2 asc\n" +
                        "limit 3", null, null, true, true, true);
    }

    @Test
    public void testOrderByColumnInJoinedSubquery() throws Exception {
        assertQuery("x\toth\n" +
                        "1\t100\n" +
                        "1\t81\n" +
                        "1\t64\n",
                "select * from \n" +
                        "(\n" +
                        "  selecT x from long_sequence(10) \n" +
                        ")\n" +
                        "cross join \n" +
                        "(\n" +
                        "  select * from \n" +
                        "  (\n" +
                        "    selecT x*x as oth from long_sequence(10) order by x desc limit 5 \n" +
                        "  )\n" +
                        ")\n" +
                        "order by x*2 asc\n" +
                        "limit 3", null, null, true, true, true);
    }
}
