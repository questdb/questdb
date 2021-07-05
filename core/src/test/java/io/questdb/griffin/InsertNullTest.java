/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.CairoException;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

public class InsertNullTest extends AbstractGriffinTest {

    private static final int NULL_INSERTS = 3;

    private static final String[][] TYPES = {
            // type name, null value
            {"boolean", "false"},
            {"byte", "0"},
            {"short", "0"},
            {"char", ""},
            {"int", "NaN"},
            {"long", "NaN"},
            {"date", ""},
            {"timestamp", ""},
            {"float", "NaN"},
            {"double", "NaN"},
            {"string", ""},
            {"symbol", ""},
            {"long256", ""},
            {"binary", ""}
    };

    @Test
    public void testInsertNull() throws Exception {
        for (int i = 0; i < TYPES.length; i++) {
            if (i > 0) {
                setUp();
            }
            try {
                final String[] type = TYPES[i];
                assertQuery(
                        "value\n",
                        "x",
                        String.format("create table x (value %s)", type[0]),
                        null,
                        String.format("insert into x select null from long_sequence(%d)", NULL_INSERTS),
                        expectedNullInserts(type[1]),
                        true,
                        true,
                        true
                );
            } finally {
                tearDown();
            }
        }
    }

    @Test
    public void testInsertNullThenFilterEq() throws Exception {
        for (int i = 0; i < TYPES.length; i++) {
            if (i > 0) {
                setUp();
            }
            try {
                final String[] type = TYPES[i];
                assertQuery(
                        "value\n",
                        "x where value = null",
                        String.format("create table x (value %s)", type[0]),
                        null,
                        String.format("insert into x select null from long_sequence(%d)", NULL_INSERTS),
                        expectedNullInserts(type[1]),
                        true,
                        true,
                        type[0].equals("long256")
                );
            } finally {
                tearDown();
            }
        }
    }

    @Test
    public void testInsertNullThenFilterNotEq() throws Exception {
        for (int i = 0; i < TYPES.length; i++) {
            if (i > 0) {
                setUp();
            }
            try {
                final String[] type = TYPES[i];
                assertQuery(
                        "value\n",
                        "x where value != null",
                        String.format("create table x (value %s)", type[0]),
                        null,
                        String.format("insert into x select null from long_sequence(%d)", NULL_INSERTS),
                        "value\n",
                        !type[0].equals("long256"),
                        true,
                        false
                );
            } finally {
                tearDown();
            }
        }
    }

    @Test
    public void testInsertNullFromSelectOnDesignatedColumnMustFail() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertQuery(
                        "sym\ty\n",
                        "xx",
                        "create table xx (sym symbol, y timestamp) timestamp(y)",
                        "y",
                        "insert into xx select 'AA', null from long_sequence(1)",
                        "y\n",
                        true,
                        true,
                        false
                );
                Assert.fail();
            } catch (CairoException expected) {
                Assert.assertTrue(expected.getMessage().contains("Cannot insert rows before 1970-01-01"));
            }
        });
    }

    @Test
    public void testInsertNullFromValuesOnDesignatedColumnMustFail() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertQuery(
                        "sym\ty\n",
                        "xx",
                        "create table xx (sym symbol, y timestamp) timestamp(y)",
                        "y",
                        "insert into xx values('AA', null)",
                        "y\n",
                        true,
                        true,
                        false
                );
                Assert.fail();
            } catch (SqlException expected) {
                Assert.assertEquals("[0] insert statement must populate timestamp", expected.getMessage());
            }
        });
    }

    private static String expectedNullInserts(final String nullValue) {
        StringSink sb = Misc.getThreadLocalBuilder();
        sb.put("value\n");
        for (int i = 0; i < NULL_INSERTS; i++) {
            sb.put(nullValue);
            sb.put("\n");
        }
        return sb.toString();
    }
}
