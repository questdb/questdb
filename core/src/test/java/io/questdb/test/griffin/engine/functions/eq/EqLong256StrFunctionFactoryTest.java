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

package io.questdb.test.griffin.engine.functions.eq;

import io.questdb.cairo.ImplicitCastException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class EqLong256StrFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testIndexedBindVariableInEqFilter() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x (l long256)");
            insert("insert into x values('0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650')");
            insert("insert into x values('0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9651')");
            insert("insert into x values('0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9652')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9651");
            assertSql(
                    "l\n" +
                            "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9651\n",
                    "x where l = $1"
            );
        });
    }

    @Test
    public void testInvalidConstantInEqFilter() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x (l long256)");
            assertException("x where l = 'foobar'", 0, "inconvertible value: `foobar` [STRING -> LONG256]");
        });
    }

    @Test
    public void testLong256Decode1() throws Exception {
        assertQuery(
                "rnd_long256\n0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\n",
                "xxxx where rnd_long256='0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650'",
                "create table xxxx as (select rnd_long256() from long_sequence(200));",
                null,
                true
        );
    }

    @Test
    public void testLong256Decode2() throws Exception {
        assertQuery(
                "rnd_long256\n0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\n",
                "xxxx where rnd_long256!='0x056'",
                "create table xxxx as (select rnd_long256() from long_sequence(1));",
                null,
                true
        );
    }

    @Test
    public void testLong256Decode3() throws Exception {
        assertQuery(
                "rnd_long256\n0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\n",
                "xxxx where '0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650'=rnd_long256",
                "create table xxxx as (select rnd_long256() from long_sequence(200));",
                null,
                true
        );
    }

    @Test
    public void testLong256GarbageDecode1() throws Exception {
        try {
            assertQuery(
                    "rnd_long256\n0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\n",
                    "xxxx where rnd_long256!='0xG56'",
                    "create table xxxx as (select rnd_long256() from long_sequence(1));",
                    null,
                    true
            );
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `0xG56` [STRING -> LONG256]");
        }
    }

    @Test
    public void testLong256NotNull() throws Exception {
        assertQuery(
                "rnd_long256\n0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\n",
                "xxxx where null!=rnd_long256 limit 1",
                "create table xxxx as (select rnd_long256() from long_sequence(200));",
                null,
                true
        );
    }

    @Test
    public void testLong256Null() throws Exception {
        assertQuery(
                "rnd_long256\n",
                "xxxx where rnd_long256=null",
                "create table xxxx as (select rnd_long256() from long_sequence(200));",
                null,
                true
        );
    }

    @Test
    public void testNamedBindVariableInEqFilter() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x (l long256)");
            insert("insert into x values('0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650')");
            insert("insert into x values('0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9651')");
            insert("insert into x values('0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9652')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr("l256", "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9652");
            assertSql(
                    "l\n" +
                            "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\n" +
                            "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9651\n",
                    "x where l != :l256"
            );
        });
    }

    @Test
    public void testStrColumnInEqFilter() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x (l long256, a string)");
            assertException("x where l = a", 12, "STRING constant expected");
        });
    }
}
