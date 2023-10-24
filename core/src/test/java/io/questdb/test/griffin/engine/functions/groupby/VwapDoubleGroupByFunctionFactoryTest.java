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
    public void testNull() throws Exception {
        ddl("create table tab (a0 double, a1 double)");
        insert("insert into tab values (null,null)");
        insert("insert into tab values (null,1)");
        insert("insert into tab values (1,null)");
        assertSql("vwap\n" +
                        "NaN\n",
                "select vwap(a0, a1) from tab"
        );

    }
    @Test
    public void testAll() throws Exception {
        assertSql("vwap\n" +
                        "0.4601797676425299\n",
                "select vwap(rnd_double(), rnd_double()) from long_sequence(10)"
        );
    }

    @Test
    public void testVwap() throws Exception {
        ddl("create table tab (a0 double, a1 double)");
        insert("insert into tab values (100,10)");
        insert("insert into tab values (105,40)");
        assertSql("vwap\n" +
                        "104.0\n",
                "select vwap(a0, a1) from tab"
        );
    }

    @Test
    public void testIgnoreNullAndZeroQty() throws Exception {
        ddl("create table tab (a0 double, a1 double)");
        insert("insert into tab values (null,null)");
        insert("insert into tab values (1,null)");
        insert("insert into tab values (100,10)");
        insert("insert into tab values (null,1)");
        insert("insert into tab values (105,40)");
        insert("insert into tab values (1,0)");
        assertSql("vwap\n" +
                        "104.0\n",
                "select vwap(a0, a1) from tab"
        );

    }

}