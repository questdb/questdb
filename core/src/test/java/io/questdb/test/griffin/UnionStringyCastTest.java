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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class UnionStringyCastTest extends AbstractCairoTest {

    private final String castTableDdl = "create table cast_table as (select" +
            " rnd_boolean() a_boolean," +
            " rnd_byte(0,1) a_byte," +
            " rnd_short(0,1) a_short," +
            " rnd_int(0,1,0) a_int," +
            " rnd_long(0,1,0) a_long," +
            " rnd_long256() a_long256," +
            " rnd_float() a_float," +
            " rnd_double() a_double," +
            " rnd_date(to_date('2024','yyyy'),to_date('2025','yyyy'),0) a_date," +
            " rnd_timestamp(to_timestamp('2024','yyyy'),to_timestamp('2025','yyyy'),0) a_timestamp," +
            " rnd_char() a_char," +
            " rnd_symbol('sym1') a_symbol," +
            " rnd_uuid4() a_uuid4," +
            " rnd_str(3,3,0) a_string" +
            " from long_sequence(1))";
    private final String expectedCastValue;
    private final String expectedColTypeOf;
    private final String expectedStringyValue;
    private final String selectFromCastTable;
    private final String stringyType;

    public UnionStringyCastTest(String stringyType, String castType, String expectedCastValue) {
        this.stringyType = stringyType;
        selectFromCastTable = "(select a_" + castType + " as a from cast_table)";
        this.expectedCastValue = expectedCastValue;
        if (stringyType.equals("str")) {
            expectedStringyValue = "TJW";
            expectedColTypeOf = "STRING";
        } else {
            expectedStringyValue = "\u1755\uDA1F\uDE98|";
            if (castType.equals("string")) {
                expectedColTypeOf = "STRING";
            } else {
                expectedColTypeOf = "VARCHAR";
            }
        }
    }

    @Parameterized.Parameters(name = "{0}-{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"varchar", "boolean", "false"},
                {"varchar", "byte", "1"},
                {"varchar", "short", "1"},
                {"varchar", "int", "0"},
                {"varchar", "long", "1"},
                {"varchar", "long256", "0x568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db4b0f595f143e5d72"},
                {"varchar", "float", "0.46218354"},
                {"varchar", "double", "0.5243722859289777"},
                {"varchar", "date", "2024-08-25T20:37:19.653Z"},
                {"varchar", "timestamp", "2024-09-23T10:11:44.610041Z"},
                {"varchar", "char", "G"},
                {"varchar", "symbol", "sym1"},
                {"varchar", "uuid4", "c718ab5c-bb3f-4261-81bf-6c24be538768"},
                {"varchar", "string", "UDE"},
                {"str", "boolean", "false"},
                {"str", "byte", "1"},
                {"str", "short", "0"},
                {"str", "int", "0"},
                {"str", "long", "0"},
                {"str", "long256", "0xc72bfc5230158059980eca62a219a0f16846d7a3aa5aecce322a2198864beb14"},
                {"str", "float", "0.19202209"},
                {"str", "double", "0.22452340856088226"},
                {"str", "date", "2024-07-16T06:10:13.449Z"},
                {"str", "timestamp", "2024-06-21T14:36:33.471269Z"},
                {"str", "char", "U"},
                {"str", "symbol", "sym1"},
                {"str", "uuid4", "8d076bf9-91c0-4e88-88b1-863d4316f9c7"},
                {"str", "string", "TGP"},
        });
    }

    @Test
    public void testUnionAllStringyLeft() throws Exception {
        testUnionStringyLeft0(" all");
    }

    @Test
    public void testUnionAllStringyRight() throws Exception {
        testUnionStringyRight0(" all");
    }

    @Test
    public void testUnionStringyLeft() throws Exception {
        testUnionStringyLeft0("");
    }

    @Test
    public void testUnionStringyRight() throws Exception {
        testUnionStringyRight0("");
    }

    private static String stringyTableDdl(String typeName) {
        return "create table " + typeName + "_table as (select rnd_" + typeName + "(3,3,0) a from long_sequence(1))";
    }

    private void testUnionStringyLeft0(String allOrEmpty) throws Exception {
        execute(stringyTableDdl(stringyType));
        engine.releaseAllWriters();
        String query = String.format("select a, typeOf(a) from (%s_table union%s %s)",
                stringyType, allOrEmpty, selectFromCastTable);
        String expected = "a\ttypeOf\n" +
                expectedStringyValue + '\t' + expectedColTypeOf + '\n' +
                expectedCastValue + '\t' + expectedColTypeOf + '\n';
        assertQuery(expected, query, castTableDdl, null, false, !allOrEmpty.isEmpty());
    }

    private void testUnionStringyRight0(String allOrEmpty) throws Exception {
        execute(stringyTableDdl(stringyType));
        engine.releaseAllWriters();
        String query = String.format("select a, typeOf(a) from (%s union%s %s_table)", selectFromCastTable, allOrEmpty, stringyType);
        String expected = "a\ttypeOf\n" +
                expectedCastValue + '\t' + expectedColTypeOf + '\n' +
                expectedStringyValue + '\t' + expectedColTypeOf + '\n';
        assertQuery(expected, query, castTableDdl, null, false, !allOrEmpty.isEmpty());
    }
}
