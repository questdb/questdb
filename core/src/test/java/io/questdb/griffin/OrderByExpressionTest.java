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

import org.junit.Ignore;
import org.junit.Test;

public class OrderByExpressionTest extends AbstractGriffinTest {

    @Test
    public void testOrderByTwoExpressions() throws Exception {
        assertQuery("x\n10\n9\n8\n7\n6\n",
                "select x from long_sequence(10) order by x/100, x*x desc  limit 5", null, null, true, true, true);
    }

    @Test
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

    @Test//TODO: test with order by x*2 in outer query
    @Ignore
    // fails with duplicate column : column because alias created for 'x*x' clashes with one created for x+rnd_int(1,10,0)*0
    public void testOrderByExpressionInJoinedSubquery() throws Exception {
        assertQuery("x\tcolumn\tcolumn1\n" +
                        "1\t100\t50\n" +
                        "1\t81\t45\n" +
                        "1\t64\t40\n",
                "select * from \n" +
                        "(\n" +
                        "  select x from long_sequence(10) \n" +
                        ")\n" +
                        "cross join \n" +
                        "(\n" +
                        "    select x*x,5*x from long_sequence(10) order by x+rnd_int(1,10,0)*0 desc limit 5 \n" +
                        ")\n" +
                        "order by x*2  asc\n" +
                        "limit 3", null, null, true, true, true);
    }

    @Test
    public void testOrderByExpressionWhenColumnHasAliasInJoinedSubquery() throws Exception {
        assertQuery("x\text\n1\t100\n1\t81\n1\t64\n",
                "select * from \n" +
                        "(\n" +
                        "  select x from long_sequence(10) \n" +
                        ")\n" +
                        "cross join \n" +
                        "(\n" +
                        "    select x*x as ext from long_sequence(10) order by x+rnd_int(1,10,0)*0 desc limit 5 \n" +
                        ")\n" +
                        "order by x*2 asc, ext desc\n" +
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
