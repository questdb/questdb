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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class VwapDoubleGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAll() throws Exception {
        assertSql("vwap\n" +
                        "0.4601797676425299\n",
                "select vwap(rnd_double(), rnd_double()) from long_sequence(10)"
        );
    }

    @Test
    public void testIgnoreNullAndZeroOrNegativeQty() throws Exception {
        ddl("create table tab (p double, q double)");
        insert("insert into tab values (null,null),(1,null),(100,10),(null,1),(105,40),(1,0),(1,-1)");
        assertSql(
                "vwap\n" +
                        "104.0\n",
                "select vwap(p, q) from tab"
        );
        // make sure they are the same
        assertSql("same_vwap\n" +
                        "true\n",
                "select (new_vwap=old_vwap) same_vwap " +
                        "from (" +
                        "      (select vwap(p, q) new_vwap from tab) a," +
                        "      (select (sum(p*q)/sum(q)) old_vwap from tab where p != null and q != null and q > 0) b" +
                        ")"
        );
    }

    @Test
    public void testNull() throws Exception {
        ddl("create table tab (a0 double, a1 double)");
        insert("insert into tab values (null,null),(null,1),(1,null)");
        assertSql("vwap\n" +
                        "NaN\n",
                "select vwap(a0, a1) from tab"
        );

    }

    @Test
    public void testVwap() throws Exception {
        ddl("create table tab (a0 double, a1 double)");
        insert("insert into tab values (100,10),(105,40)");
        assertSql("vwap\n" +
                        "104.0\n",
                "select vwap(a0, a1) from tab"
        );
    }

    @Test
    public void testVwapGroupBy() throws Exception {
        ddl("create table tab (price double, volume double, ticker symbol)");
        insert("insert into tab values (100,10,'a'),(105,40,'a'),(102,20,'b'),(103,60,'b')");
        assertSql("ticker\tvwap\n" +
                        "a\t104.0\n" +
                        "b\t102.75\n",
                "select ticker,vwap(price, volume) from tab order by ticker"
        );
    }
}