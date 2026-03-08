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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class NullLiteralsTest extends AbstractCairoTest {

    @Test
    public void testBooleanIsNotNull() throws Exception {
        assertQuery("v\n" +
                "false\n" +
                "false\n" +
                "false\n" +
                "true\n" +
                "false\n" +
                "false\n" +
                "true\n" +
                "true\n" +
                "true\n", "tab_boolean where v is NOT NULL", "create table tab_boolean as (\n" +
                "    select rnd_boolean() as v from long_sequence(9)\n" +
                ");", null, "insert into tab_boolean values(null)", "v\n" +
                "false\n" +
                "false\n" +
                "false\n" +
                "true\n" +
                "false\n" +
                "false\n" +
                "true\n" +
                "true\n" +
                "true\n" +
                "false\n", true, true, false);
    }

    @Test
    public void testBooleanIsNull() throws Exception {
        assertQuery("v\n", "tab_boolean where v is NULL", "create table tab_boolean as (\n" +
                "    select rnd_boolean() as v from long_sequence(9)\n" +
                ");", null, "insert into tab_boolean values(NULL)", "v\n", false, true, false);
    }

    @Test
    public void testBooleanSelectCast() throws Exception {
        assertSql("column\nfalse\n", "select cast(0 AS BOOLEAN) IS NULL");
        assertSql("column\nfalse\n", "select cast(0L AS BOOLEAN) IS NULL");
        assertSql("column\nfalse\n", "select cast('' AS BOOLEAN) IS NULL");
        assertSql("column\nfalse\n", "select cast(NULL AS BOOLEAN) IS NULL");
        assertSql("column\nfalse\n", "select cast(false AS BOOLEAN) IS NULL");
        assertSql("column\ntrue\n", "select cast(0 AS BOOLEAN) IS NOT NULL");
        assertSql("column\ntrue\n", "select cast(0L AS BOOLEAN) IS NOT NULL");
        assertSql("column\ntrue\n", "select cast('' AS BOOLEAN) IS NOT NULL");
        assertSql("column\ntrue\n", "select cast(NULL AS BOOLEAN) IS NOT NULL");
        assertSql("column\ntrue\n", "select cast(false AS BOOLEAN) IS NOT NULL");
    }

    @Test
    public void testByteIsNotNull() throws Exception {
        assertQuery("v\n" +
                "76\n" +
                "102\n" +
                "27\n" +
                "87\n" +
                "79\n" +
                "79\n" +
                "122\n" +
                "83\n" +
                "90\n", "tab_byte where v is NOT NULL", "create table tab_byte as (\n" +
                "    select rnd_byte() as v from long_sequence(9)\n" +
                ");", null, "insert into tab_byte values(null)", "v\n" +
                "76\n" +
                "102\n" +
                "27\n" +
                "87\n" +
                "79\n" +
                "79\n" +
                "122\n" +
                "83\n" +
                "90\n" +
                "0\n", true, true, false);
    }

    @Test
    public void testByteIsNull() throws Exception {
        assertQuery("v\n", "tab_byte where v is NULL", "create table tab_byte as (\n" +
                "    select rnd_byte() as v from long_sequence(9)\n" +
                ");", null, "insert into tab_byte values(NULL)", "v\n", false, true, false);
    }

    @Test
    public void testByteSelectCast() throws Exception {
        assertSql("column\nfalse\n", "select cast(0 AS BYTE) IS NULL");
        assertSql("column\nfalse\n", "select cast(0L AS BYTE) IS NULL");
        assertSql("column\nfalse\n", "select cast('' AS BYTE) IS NULL");
        assertSql("column\nfalse\n", "select cast(NULL AS BYTE) IS NULL");
        assertSql("column\nfalse\n", "select cast(false AS BYTE) IS NULL");
        assertSql("column\ntrue\n", "select cast(0 AS BYTE) IS NOT NULL");
        assertSql("column\ntrue\n", "select cast(0L AS BYTE) IS NOT NULL");
        assertSql("column\ntrue\n", "select cast('' AS BYTE) IS NOT NULL");
        assertSql("column\ntrue\n", "select cast(NULL AS BYTE) IS NOT NULL");
        assertSql("column\ntrue\n", "select cast(false AS BYTE) IS NOT NULL");
    }

    @Test
    public void testCharIsNotNull() throws Exception {
        assertQuery("v\n" +
                "V\n" +
                "T\n" +
                "J\n" +
                "W\n" +
                "C\n" +
                "P\n" +
                "S\n" +
                "W\n" +
                "H\n", "tab_char where v is NOT NULL", "create table tab_char as (\n" +
                "    select rnd_char() as v from long_sequence(9)\n" +
                ");", null, "insert into tab_char values(null)", "v\n" +
                "V\n" +
                "T\n" +
                "J\n" +
                "W\n" +
                "C\n" +
                "P\n" +
                "S\n" +
                "W\n" +
                "H\n", true, false, false);
    }

    @Test
    public void testCharIsNull() throws Exception {
        assertQuery("v\n", "tab_char where v is NULL", "create table tab_char as (\n" +
                "    select rnd_char() as v from long_sequence(9)\n" +
                ");", null, "insert into tab_char values(NULL)", "v\n\n", true, false, false);
    }

    @Test
    public void testCharSelectCast() throws Exception {
        assertSql("column\ntrue\n", "select cast(0 AS CHAR) IS NULL");
        assertSql("column\ntrue\n", "select cast(0L AS CHAR) IS NULL");
        assertSql("column\ntrue\n", "select cast('' AS CHAR) IS NULL");
        assertSql("column\ntrue\n", "select cast(NULL AS CHAR) IS NULL");
        assertSql("column\nfalse\n", "select cast(false AS CHAR) IS NULL");
        assertSql("column\nfalse\n", "select cast(0 AS CHAR) IS NOT NULL");
        assertSql("column\nfalse\n", "select cast(0L AS CHAR) IS NOT NULL");
        assertSql("column\nfalse\n", "select cast('' AS CHAR) IS NOT NULL");
        assertSql("column\nfalse\n", "select cast(NULL AS CHAR) IS NOT NULL");
        assertSql("column\ntrue\n", "select cast(false AS CHAR) IS NOT NULL");
    }

    @Test
    public void testShortIsNotNull() throws Exception {
        assertQuery("v\n" +
                "-27056\n" +
                "24814\n" +
                "-11455\n" +
                "-13027\n" +
                "-21227\n" +
                "-22955\n" +
                "-1398\n" +
                "21015\n" +
                "30202\n", "tab_short where v is NOT NULL", "create table tab_short as (\n" +
                "    select rnd_short() as v from long_sequence(9)\n" +
                ");", null, "insert into tab_short values(null)", "v\n" +
                "-27056\n" +
                "24814\n" +
                "-11455\n" +
                "-13027\n" +
                "-21227\n" +
                "-22955\n" +
                "-1398\n" +
                "21015\n" +
                "30202\n" +
                "0\n", true, true, false);
    }

    @Test
    public void testShortIsNull() throws Exception {
        assertQuery("v\n", "tab_short where v is NULL", "create table tab_short as (\n" +
                "    select rnd_short() as v from long_sequence(9)\n" +
                ");", null, "insert into tab_short values(NULL)", "v\n", false, true, false);
    }

    @Test
    public void testShortSelectCast() throws Exception {
        assertSql("column\nfalse\n", "select cast(0 AS SHORT) IS NULL");
        assertSql("column\nfalse\n", "select cast(0L AS SHORT) IS NULL");
        assertSql("column\nfalse\n", "select cast('' AS SHORT) IS NULL");
        assertSql("column\nfalse\n", "select cast(NULL AS SHORT) IS NULL");
        assertSql("column\nfalse\n", "select cast(false AS SHORT) IS NULL");
        assertSql("column\ntrue\n", "select cast(0 AS SHORT) IS NOT NULL");
        assertSql("column\ntrue\n", "select cast(0L AS SHORT) IS NOT NULL");
        assertSql("column\ntrue\n", "select cast('' AS SHORT) IS NOT NULL");
        assertSql("column\ntrue\n", "select cast(NULL AS SHORT) IS NOT NULL");
        assertSql("column\ntrue\n", "select cast(false AS SHORT) IS NOT NULL");
    }
}
