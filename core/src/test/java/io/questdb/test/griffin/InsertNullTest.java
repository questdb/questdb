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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoException;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class InsertNullTest extends AbstractCairoTest {

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
            {"binary", ""},
            {"geohash(5b)", ""},
            {"geohash(15b)", ""},
            {"geohash(31b)", ""},
            {"geohash(60b)", ""}
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
                        expectedNullInserts("value\n", type[1], NULL_INSERTS, true),
                        true,
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
                        false,
                        false
                );
                Assert.fail();
            } catch (CairoException expected) {
                Assert.assertTrue(expected.getMessage().contains("timestamp before 1970-01-01 is not allowed"));
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
                        false,
                        false
                );
                Assert.fail();
            } catch (SqlException expected) {
                Assert.assertEquals("[0] insert statement must populate timestamp", expected.getMessage());
            }
        });
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
                        expectedNullInserts("value\n", type[1], NULL_INSERTS, !isNotNullable(type[0])),
                        !isNotNullable(type[0]),
                        false,
                        false
                );
            } finally {
                tearDown();
            }
        }
    }

    @Test
    public void testInsertNullThenFilterIsNotNull() throws Exception {
        for (int i = 0; i < TYPES.length; i++) {
            if (i > 0) {
                setUp();
            }
            try {
                final String[] type = TYPES[i];
                assertQuery(
                        "value\n",
                        "x where value is not null",
                        String.format("create table x (value %s)", type[0]),
                        null,
                        String.format("insert into x select null from long_sequence(%d)", NULL_INSERTS),
                        expectedNullInserts("value\n", type[1], NULL_INSERTS, isNotNullable(type[0])),
                        true,
                        isNotNullable(type[0]),
                        false
                );
            } finally {
                tearDown();
            }
        }
    }

    @Test
    public void testInsertNullThenFilterIsNull() throws Exception {
        for (int i = 0; i < TYPES.length; i++) {
            if (i > 0) {
                setUp();
            }
            try {
                final String[] type = TYPES[i];
                assertQuery(
                        "value\n",
                        "x where value is null",
                        String.format("create table x (value %s)", type[0]),
                        null,
                        String.format("insert into x select null from long_sequence(%d)", NULL_INSERTS),
                        expectedNullInserts("value\n", type[1], NULL_INSERTS, !isNotNullable(type[0])),
                        !isNotNullable(type[0]),
                        false,
                        false
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
                        expectedNullInserts("value\n", type[1], NULL_INSERTS, isNotNullable(type[0])),
                        true,
                        isNotNullable(type[0]),
                        false
                );
            } finally {
                tearDown();
            }
        }
    }

    private static boolean isNotNullable(String type) {
        return Chars.equalsLowerCaseAscii(type, "short") ||
                Chars.equalsLowerCaseAscii(type, "byte") ||
                Chars.equalsLowerCaseAscii(type, "boolean");
    }

    static String expectedNullInserts(String header, String nullValue, int count, boolean expectsOutput) {
        StringSink sb = Misc.getThreadLocalSink();
        sb.put(header);
        if (expectsOutput) {
            for (int i = 0; i < count; i++) {
                sb.put(nullValue).put("\n");
            }
        }
        return sb.toString();
    }
}
