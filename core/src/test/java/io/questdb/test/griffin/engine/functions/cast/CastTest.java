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

package io.questdb.test.griffin.engine.functions.cast;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CastTest extends AbstractCairoTest {

    @Test
    public void testBooleanToBoolean() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(rnd_boolean() as boolean) from long_sequence(10)",
                "a\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "true\n" +
                        "false\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testBooleanToByte() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a byte)",
                null,
                "insert into tab select cast(rnd_boolean() as byte) from long_sequence(10)",
                "a\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "1\n" +
                        "0\n" +
                        "0\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testBooleanToChar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a char)",
                null,
                "insert into tab select cast(rnd_boolean() as char) from long_sequence(10)",
                "a\n" +
                        "F\n" +
                        "F\n" +
                        "F\n" +
                        "T\n" +
                        "F\n" +
                        "F\n" +
                        "T\n" +
                        "T\n" +
                        "T\n" +
                        "T\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testBooleanToDate() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a date)",
                null,
                "insert into tab select cast(rnd_boolean() as date) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.001Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.001Z\n" +
                        "1970-01-01T00:00:00.001Z\n" +
                        "1970-01-01T00:00:00.001Z\n" +
                        "1970-01-01T00:00:00.001Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testBooleanToDouble() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a double)",
                null,
                "insert into tab select cast(rnd_boolean() as double) from long_sequence(10)",
                "a\n" +
                        "0.0\n" +
                        "0.0\n" +
                        "0.0\n" +
                        "1.0\n" +
                        "0.0\n" +
                        "0.0\n" +
                        "1.0\n" +
                        "1.0\n" +
                        "1.0\n" +
                        "1.0\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testBooleanToDoubleConstant() throws Exception {
        assertQueryNoLeakCheck(
                "cast\n0.0\n",
                "select cast(false as double)",
                null,
                true,
                true,
                true
        );

        assertQueryNoLeakCheck(
                "cast\n0.0\n",
                "select cast((150 < 0) as double)",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testBooleanToFloat() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a float)",
                null,
                "insert into tab select cast(rnd_boolean() as float) from long_sequence(10)",
                "a\n" +
                        "0.0\n" +
                        "0.0\n" +
                        "0.0\n" +
                        "1.0\n" +
                        "0.0\n" +
                        "0.0\n" +
                        "1.0\n" +
                        "1.0\n" +
                        "1.0\n" +
                        "1.0\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testBooleanToFloatConstant() throws Exception {
        assertQueryNoLeakCheck(
                "cast\n0.0\n",
                "select cast(false as float)",
                null,
                true,
                true,
                true
        );

        assertQueryNoLeakCheck(
                "cast\n0.0\n",
                "select cast((150 < 100) as float)",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testBooleanToInt() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a int)",
                null,
                "insert into tab select cast(rnd_boolean() as int) from long_sequence(10)",
                "a\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "1\n" +
                        "0\n" +
                        "0\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testBooleanToIntConstant() throws Exception {
        assertQueryNoLeakCheck(
                "cast\n0\n",
                "select cast(false as int)",
                null,
                true,
                true,
                true
        );

        assertQueryNoLeakCheck(
                "cast\n0\n",
                "select cast((150 < 0) as int)",
                null,
                true,
                true,
                true
        );

        assertQueryNoLeakCheck(
                "cast\n1\n",
                "select cast((150 < 250) as int)",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testBooleanToLong() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long)",
                null,
                "insert into tab select cast(rnd_boolean() as long) from long_sequence(10)",
                "a\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "1\n" +
                        "0\n" +
                        "0\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testBooleanToLong256() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long256)",
                null,
                "insert into tab select cast(rnd_boolean() as long256) from long_sequence(10)",
                "a\n" +
                        "0x00\n" +
                        "0x00\n" +
                        "0x00\n" +
                        "0x01\n" +
                        "0x00\n" +
                        "0x00\n" +
                        "0x01\n" +
                        "0x01\n" +
                        "0x01\n" +
                        "0x01\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testBooleanToLong256Sort() throws Exception {
        assertQuery(
                "x\n" +
                        "0x00\n" +
                        "0x00\n" +
                        "0x00\n" +
                        "0x00\n" +
                        "0x00\n" +
                        "0x00\n" +
                        "0x00\n" +
                        "0x00\n" +
                        "0x00\n" +
                        "0x00\n" +
                        "0x00\n" +
                        "0x01\n" +
                        "0x01\n" +
                        "0x01\n" +
                        "0x01\n" +
                        "0x01\n" +
                        "0x01\n" +
                        "0x01\n" +
                        "0x01\n" +
                        "0x01\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select rnd_boolean() a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testBooleanToShort() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a short)",
                null,
                "insert into tab select cast(rnd_boolean() as short) from long_sequence(10)",
                "a\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "1\n" +
                        "0\n" +
                        "0\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testBooleanToStr() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(rnd_boolean() as string) from long_sequence(10)",
                "a\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "true\n" +
                        "false\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testBooleanToStrConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(34=34 as string) from long_sequence(10)",
                "a\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testBooleanToStrSort() throws Exception {
        assertQuery(
                "x\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                "select cast(a as string) x from tt order by x",
                "create table tt as (select rnd_boolean() a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testBooleanToSymbol() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(rnd_boolean() as symbol) from long_sequence(10)",
                "a\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "true\n" +
                        "false\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testBooleanToSymbolConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(14=14 as symbol) from long_sequence(10)",
                "a\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testBooleanToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "false\tfalse\n" +
                        "false\tfalse\n" +
                        "false\tfalse\n" +
                        "true\ttrue\n" +
                        "false\tfalse\n" +
                        "false\tfalse\n" +
                        "true\ttrue\n" +
                        "true\ttrue\n" +
                        "true\ttrue\n" +
                        "true\ttrue\n" +
                        "false\tfalse\n" +
                        "false\tfalse\n" +
                        "false\tfalse\n" +
                        "true\ttrue\n" +
                        "true\ttrue\n" +
                        "true\ttrue\n" +
                        "true\ttrue\n" +
                        "false\tfalse\n" +
                        "false\tfalse\n" +
                        "false\tfalse\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_boolean() a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testBooleanToTimestamp() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp)",
                null,
                "insert into tab select cast(rnd_boolean() as timestamp) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000001Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testBooleanToTimestampNano() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp_ns)",
                null,
                "insert into tab select cast(rnd_boolean() as timestamp_ns) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.000000000Z\n" +
                        "1970-01-01T00:00:00.000000000Z\n" +
                        "1970-01-01T00:00:00.000000000Z\n" +
                        "1970-01-01T00:00:00.000000001Z\n" +
                        "1970-01-01T00:00:00.000000000Z\n" +
                        "1970-01-01T00:00:00.000000000Z\n" +
                        "1970-01-01T00:00:00.000000001Z\n" +
                        "1970-01-01T00:00:00.000000001Z\n" +
                        "1970-01-01T00:00:00.000000001Z\n" +
                        "1970-01-01T00:00:00.000000001Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testBooleanToVarchar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(rnd_boolean() as varchar) from long_sequence(10)",
                "a\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "true\n" +
                        "false\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testBooleanToVarcharConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(34=34 as varchar) from long_sequence(10)",
                "a\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testBooleanToVarcharSort() throws Exception {
        assertQuery(
                "x\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                "select cast(a as varchar) x from tt order by x",
                "create table tt as (select rnd_boolean() a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testByteToBoolean() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(rnd_byte() as boolean) from long_sequence(10)",
                "a\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true,
                true,
                false
        );

        assertQueryNoLeakCheck(
                "cast\nfalse\n",
                "select cast(0x00 as boolean)",
                null,
                true,
                true,
                true
        );
        assertQueryNoLeakCheck(
                "cast\ntrue\n",
                "select cast(0xF0 as boolean)",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testByteToBooleanConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(cast(1 as byte) as boolean) from long_sequence(10)",
                "a\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testByteToByte() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a byte)",
                null,
                "insert into tab select cast(rnd_byte() as byte) from long_sequence(10)",
                "a\n" +
                        "76\n" +
                        "102\n" +
                        "27\n" +
                        "87\n" +
                        "79\n" +
                        "79\n" +
                        "122\n" +
                        "83\n" +
                        "90\n" +
                        "76\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testByteToChar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a char)",
                null,
                "insert into tab select cast(rnd_byte() as char) from long_sequence(10)",
                "a\n" +
                        "L\n" +
                        "f\n" +
                        "\u001B\n" +
                        "W\n" +
                        "O\n" +
                        "O\n" +
                        "z\n" +
                        "S\n" +
                        "Z\n" +
                        "L\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testByteToDate() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a date)",
                null,
                "insert into tab select cast(rnd_byte() as date) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.076Z\n" +
                        "1970-01-01T00:00:00.102Z\n" +
                        "1970-01-01T00:00:00.027Z\n" +
                        "1970-01-01T00:00:00.087Z\n" +
                        "1970-01-01T00:00:00.079Z\n" +
                        "1970-01-01T00:00:00.079Z\n" +
                        "1970-01-01T00:00:00.122Z\n" +
                        "1970-01-01T00:00:00.083Z\n" +
                        "1970-01-01T00:00:00.090Z\n" +
                        "1970-01-01T00:00:00.076Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testByteToDouble() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a double)",
                null,
                "insert into tab select cast(rnd_byte() as double) from long_sequence(10)",
                "a\n" +
                        "76.0\n" +
                        "102.0\n" +
                        "27.0\n" +
                        "87.0\n" +
                        "79.0\n" +
                        "79.0\n" +
                        "122.0\n" +
                        "83.0\n" +
                        "90.0\n" +
                        "76.0\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testByteToFloat() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a float)",
                null,
                "insert into tab select cast(rnd_byte() as float) from long_sequence(10)",
                "a\n" +
                        "76.0\n" +
                        "102.0\n" +
                        "27.0\n" +
                        "87.0\n" +
                        "79.0\n" +
                        "79.0\n" +
                        "122.0\n" +
                        "83.0\n" +
                        "90.0\n" +
                        "76.0\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testByteToInt() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a int)",
                null,
                "insert into tab select cast(rnd_byte() as int) from long_sequence(10)",
                "a\n" +
                        "76\n" +
                        "102\n" +
                        "27\n" +
                        "87\n" +
                        "79\n" +
                        "79\n" +
                        "122\n" +
                        "83\n" +
                        "90\n" +
                        "76\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testByteToLong() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long)",
                null,
                "insert into tab select cast(rnd_byte() as long) from long_sequence(10)",
                "a\n" +
                        "76\n" +
                        "102\n" +
                        "27\n" +
                        "87\n" +
                        "79\n" +
                        "79\n" +
                        "122\n" +
                        "83\n" +
                        "90\n" +
                        "76\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testByteToLong256() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long256)",
                null,
                "insert into tab select cast(rnd_byte() as long256) from long_sequence(10)",
                "a\n" +
                        "0x4c\n" +
                        "0x66\n" +
                        "0x1b\n" +
                        "0x57\n" +
                        "0x4f\n" +
                        "0x4f\n" +
                        "0x7a\n" +
                        "0x53\n" +
                        "0x5a\n" +
                        "0x4c\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testByteToLong256Sort() throws Exception {
        assertQuery(
                "x\n" +
                        "0x15\n" +
                        "0x1b\n" +
                        "0x20\n" +
                        "0x37\n" +
                        "0x4a\n" +
                        "0x4a\n" +
                        "0x4c\n" +
                        "0x4c\n" +
                        "0x4f\n" +
                        "0x4f\n" +
                        "0x53\n" +
                        "0x53\n" +
                        "0x54\n" +
                        "0x54\n" +
                        "0x57\n" +
                        "0x58\n" +
                        "0x5a\n" +
                        "0x5b\n" +
                        "0x66\n" +
                        "0x7a\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select rnd_byte() a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testByteToShort() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a short)",
                null,
                "insert into tab select cast(rnd_byte() as short) from long_sequence(10)",
                "a\n" +
                        "76\n" +
                        "102\n" +
                        "27\n" +
                        "87\n" +
                        "79\n" +
                        "79\n" +
                        "122\n" +
                        "83\n" +
                        "90\n" +
                        "76\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testByteToStr() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(rnd_byte() as string) from long_sequence(10)",
                "a\n" +
                        "76\n" +
                        "102\n" +
                        "27\n" +
                        "87\n" +
                        "79\n" +
                        "79\n" +
                        "122\n" +
                        "83\n" +
                        "90\n" +
                        "76\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testByteToStrConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(cast(34 as byte) as string) from long_sequence(10)",
                "a\n" +
                        "34\n" +
                        "34\n" +
                        "34\n" +
                        "34\n" +
                        "34\n" +
                        "34\n" +
                        "34\n" +
                        "34\n" +
                        "34\n" +
                        "34\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testByteToStrSort() throws Exception {
        assertQuery(
                "x\n" +
                        "102\n" +
                        "122\n" +
                        "21\n" +
                        "27\n" +
                        "32\n" +
                        "55\n" +
                        "74\n" +
                        "74\n" +
                        "76\n" +
                        "76\n" +
                        "79\n" +
                        "79\n" +
                        "83\n" +
                        "83\n" +
                        "84\n" +
                        "84\n" +
                        "87\n" +
                        "88\n" +
                        "90\n" +
                        "91\n",
                "select cast(a as string) x from tt order by x",
                "create table tt as (select rnd_byte() a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testByteToSymbol() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(rnd_byte() as symbol) from long_sequence(10)",
                "a\n" +
                        "76\n" +
                        "102\n" +
                        "27\n" +
                        "87\n" +
                        "79\n" +
                        "79\n" +
                        "122\n" +
                        "83\n" +
                        "90\n" +
                        "76\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testByteToSymbolConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(cast(14 as byte) as symbol) from long_sequence(10)",
                "a\n" +
                        "14\n" +
                        "14\n" +
                        "14\n" +
                        "14\n" +
                        "14\n" +
                        "14\n" +
                        "14\n" +
                        "14\n" +
                        "14\n" +
                        "14\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testByteToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "76\t76\n" +
                        "102\t102\n" +
                        "27\t27\n" +
                        "87\t87\n" +
                        "79\t79\n" +
                        "79\t79\n" +
                        "122\t122\n" +
                        "83\t83\n" +
                        "90\t90\n" +
                        "76\t76\n" +
                        "84\t84\n" +
                        "84\t84\n" +
                        "74\t74\n" +
                        "55\t55\n" +
                        "83\t83\n" +
                        "88\t88\n" +
                        "32\t32\n" +
                        "21\t21\n" +
                        "91\t91\n" +
                        "74\t74\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_byte() a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testByteToTimestamp() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp)",
                null,
                "insert into tab select cast(rnd_byte() as timestamp) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.000076Z\n" +
                        "1970-01-01T00:00:00.000102Z\n" +
                        "1970-01-01T00:00:00.000027Z\n" +
                        "1970-01-01T00:00:00.000087Z\n" +
                        "1970-01-01T00:00:00.000079Z\n" +
                        "1970-01-01T00:00:00.000079Z\n" +
                        "1970-01-01T00:00:00.000122Z\n" +
                        "1970-01-01T00:00:00.000083Z\n" +
                        "1970-01-01T00:00:00.000090Z\n" +
                        "1970-01-01T00:00:00.000076Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testByteToTimestampNano() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp_ns)",
                null,
                "insert into tab select cast(rnd_byte() as timestamp_ns) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.000000076Z\n" +
                        "1970-01-01T00:00:00.000000102Z\n" +
                        "1970-01-01T00:00:00.000000027Z\n" +
                        "1970-01-01T00:00:00.000000087Z\n" +
                        "1970-01-01T00:00:00.000000079Z\n" +
                        "1970-01-01T00:00:00.000000079Z\n" +
                        "1970-01-01T00:00:00.000000122Z\n" +
                        "1970-01-01T00:00:00.000000083Z\n" +
                        "1970-01-01T00:00:00.000000090Z\n" +
                        "1970-01-01T00:00:00.000000076Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testByteToVarchar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(rnd_byte() as varchar) from long_sequence(10)",
                "a\n" +
                        "76\n" +
                        "102\n" +
                        "27\n" +
                        "87\n" +
                        "79\n" +
                        "79\n" +
                        "122\n" +
                        "83\n" +
                        "90\n" +
                        "76\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testByteToVarcharConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(cast(34 as byte) as varchar) from long_sequence(10)",
                "a\n" +
                        "34\n" +
                        "34\n" +
                        "34\n" +
                        "34\n" +
                        "34\n" +
                        "34\n" +
                        "34\n" +
                        "34\n" +
                        "34\n" +
                        "34\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testByteToVarcharSort() throws Exception {
        assertQuery(
                "x\n" +
                        "102\n" +
                        "122\n" +
                        "21\n" +
                        "27\n" +
                        "32\n" +
                        "55\n" +
                        "74\n" +
                        "74\n" +
                        "76\n" +
                        "76\n" +
                        "79\n" +
                        "79\n" +
                        "83\n" +
                        "83\n" +
                        "84\n" +
                        "84\n" +
                        "87\n" +
                        "88\n" +
                        "90\n" +
                        "91\n",
                "select cast(a as varchar) x from tt order by x",
                "create table tt as (select rnd_byte() a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testCharToBoolean() throws Exception {
        assertQuery(
                "cast\n",
                "select a::boolean from tab",
                "create table tab (a char)",
                null,
                "insert into tab values('0'), ('1'), ('T'), ('t'), ('F'), ('f')",
                "cast\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "false\n" +
                        "false\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testCharToBooleanTrue() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(cast(116 as char) as boolean) from long_sequence(10)",
                "a\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testCharToByte() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a byte)",
                null,
                "insert into tab select cast(cast(rnd_int(0,9,0)+47 as char) as byte) from long_sequence(10)",
                "a\n" +
                        "7\n" +
                        "0\n" +
                        "8\n" +
                        "0\n" +
                        "7\n" +
                        "1\n" +
                        "2\n" +
                        "1\n" +
                        "6\n" +
                        "3\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testCharToChar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a char)",
                null,
                "insert into tab select cast(rnd_char() as char) from long_sequence(10)",
                "a\n" +
                        "V\n" +
                        "T\n" +
                        "J\n" +
                        "W\n" +
                        "C\n" +
                        "P\n" +
                        "S\n" +
                        "W\n" +
                        "H\n" +
                        "Y\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testCharToDate() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a date)",
                null,
                "insert into tab select cast(cast(rnd_int(0,9,0)+47 as char) as date) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.007Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.008Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.007Z\n" +
                        "1970-01-01T00:00:00.001Z\n" +
                        "1970-01-01T00:00:00.002Z\n" +
                        "1970-01-01T00:00:00.001Z\n" +
                        "1970-01-01T00:00:00.006Z\n" +
                        "1970-01-01T00:00:00.003Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testCharToDouble() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a double)",
                null,
                "insert into tab select cast(cast(rnd_int(0,9,0)+47 as char) as double) from long_sequence(10)",
                "a\n" +
                        "7.0\n" +
                        "0.0\n" +
                        "8.0\n" +
                        "0.0\n" +
                        "7.0\n" +
                        "1.0\n" +
                        "2.0\n" +
                        "1.0\n" +
                        "6.0\n" +
                        "3.0\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testCharToFloat() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a float)",
                null,
                "insert into tab select cast(cast(rnd_int(0,9,0)+47 as char) as float) from long_sequence(10)",
                "a\n" +
                        "7.0\n" +
                        "0.0\n" +
                        "8.0\n" +
                        "0.0\n" +
                        "7.0\n" +
                        "1.0\n" +
                        "2.0\n" +
                        "1.0\n" +
                        "6.0\n" +
                        "3.0\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testCharToInt() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a int)",
                null,
                "insert into tab select cast(cast(rnd_int(0,9,0)+47 as char) as int) from long_sequence(10)",
                "a\n" +
                        "7\n" +
                        "0\n" +
                        "8\n" +
                        "0\n" +
                        "7\n" +
                        "1\n" +
                        "2\n" +
                        "1\n" +
                        "6\n" +
                        "3\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testCharToLong() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long)",
                null,
                "insert into tab select cast(cast(rnd_int(0,9,0)+47 as char) as long) from long_sequence(10)",
                "a\n" +
                        "7\n" +
                        "0\n" +
                        "8\n" +
                        "0\n" +
                        "7\n" +
                        "1\n" +
                        "2\n" +
                        "1\n" +
                        "6\n" +
                        "3\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testCharToLong256() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long256)",
                null,
                "insert into tab select cast(cast(rnd_int(0,9,0)+47 as char) as long256) from long_sequence(10)",
                "a\n" +
                        "0x07\n" +
                        "0x00\n" +
                        "0x08\n" +
                        "0x00\n" +
                        "0x07\n" +
                        "0x01\n" +
                        "0x02\n" +
                        "0x01\n" +
                        "0x06\n" +
                        "0x03\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testCharToLong256Sort() throws Exception {
        assertQuery(
                "x\n" +
                        "0x30\n" +
                        "0x31\n" +
                        "0x31\n" +
                        "0x31\n" +
                        "0x32\n" +
                        "0x32\n" +
                        "0x33\n" +
                        "0x34\n" +
                        "0x34\n" +
                        "0x37\n" +
                        "0x37\n" +
                        "0x37\n" +
                        "0x38\n" +
                        "0x38\n" +
                        "0x38\n" +
                        "0x38\n" +
                        "0x38\n" +
                        "0x39\n" +
                        "0x39\n" +
                        "0x39\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select cast(rnd_int(0,9,0)+48 as char) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testCharToNumbersException() throws Exception {
        assertException(
                "select 'm'::byte",
                0,
                "inconvertible value: m [CHAR -> BYTE]"
        );
        assertException(
                "select 'm'::short",
                0,
                "inconvertible value: m [CHAR -> SHORT]"
        );
        assertException(
                "select 'm'::int",
                0,
                "inconvertible value: m [CHAR -> INT]"
        );
        assertException(
                "select 'm'::long",
                0,
                "inconvertible value: m [CHAR -> LONG]"
        );
        assertException(
                "select 'm'::float",
                0,
                "inconvertible value: m [CHAR -> DOUBLE]"
        );
        assertException(
                "select 'm'::double",
                0,
                "inconvertible value: m [CHAR -> DOUBLE]"
        );
        assertException(
                "select 'm'::date - 1",
                0,
                "inconvertible value: m [CHAR -> DATE]"
        );
        assertException(
                "select 'm'::timestamp - 1",
                0,
                "inconvertible value: m [CHAR -> TIMESTAMP]"
        );
        assertException(
                "select 'm'::timestamp_ns - 1",
                0,
                "inconvertible value: m [CHAR -> TIMESTAMP_NS]"
        );
        assertException(
                "select 'm'::boolean",
                0,
                "inconvertible value: m [CHAR -> BOOLEAN]"
        );
    }

    @Test
    public void testCharToShort() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a short)",
                null,
                "insert into tab select cast(cast(rnd_int(0,9,0)+47 as char) as short) from long_sequence(10)",
                "a\n" +
                        "7\n" +
                        "0\n" +
                        "8\n" +
                        "0\n" +
                        "7\n" +
                        "1\n" +
                        "2\n" +
                        "1\n" +
                        "6\n" +
                        "3\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testCharToStr() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(rnd_char() as string) from long_sequence(10)",
                "a\n" +
                        "V\n" +
                        "T\n" +
                        "J\n" +
                        "W\n" +
                        "C\n" +
                        "P\n" +
                        "S\n" +
                        "W\n" +
                        "H\n" +
                        "Y\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testCharToStrConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast('A' as string) from long_sequence(10)",
                "a\n" +
                        "A\n" +
                        "A\n" +
                        "A\n" +
                        "A\n" +
                        "A\n" +
                        "A\n" +
                        "A\n" +
                        "A\n" +
                        "A\n" +
                        "A\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testCharToStrSort() throws Exception {
        assertQuery(
                "x\n" +
                        "C\n" +
                        "E\n" +
                        "G\n" +
                        "H\n" +
                        "H\n" +
                        "J\n" +
                        "N\n" +
                        "P\n" +
                        "P\n" +
                        "R\n" +
                        "R\n" +
                        "S\n" +
                        "T\n" +
                        "V\n" +
                        "W\n" +
                        "W\n" +
                        "X\n" +
                        "X\n" +
                        "Y\n" +
                        "Z\n",
                "select cast(a as string) x from tt order by x",
                "create table tt as (select rnd_char() a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testCharToSymbol() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(rnd_char() as symbol) from long_sequence(10)",
                "a\n" +
                        "V\n" +
                        "T\n" +
                        "J\n" +
                        "W\n" +
                        "C\n" +
                        "P\n" +
                        "S\n" +
                        "W\n" +
                        "H\n" +
                        "Y\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testCharToSymbolConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast('X' as symbol) from long_sequence(10)",
                "a\n" +
                        "X\n" +
                        "X\n" +
                        "X\n" +
                        "X\n" +
                        "X\n" +
                        "X\n" +
                        "X\n" +
                        "X\n" +
                        "X\n" +
                        "X\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testCharToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "V\tV\n" +
                        "T\tT\n" +
                        "J\tJ\n" +
                        "W\tW\n" +
                        "C\tC\n" +
                        "P\tP\n" +
                        "S\tS\n" +
                        "W\tW\n" +
                        "H\tH\n" +
                        "Y\tY\n" +
                        "R\tR\n" +
                        "X\tX\n" +
                        "P\tP\n" +
                        "E\tE\n" +
                        "H\tH\n" +
                        "N\tN\n" +
                        "R\tR\n" +
                        "X\tX\n" +
                        "G\tG\n" +
                        "Z\tZ\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_char() a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testCharToTimestamp() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp)",
                null,
                "insert into tab select cast(cast(rnd_int(0,9,0)+47 as char) as timestamp) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.000007Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000008Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000007Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000002Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000006Z\n" +
                        "1970-01-01T00:00:00.000003Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testCharToTimestampNano() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp_ns)",
                null,
                "insert into tab select cast(cast(rnd_int(0,9,0)+47 as char) as timestamp_ns) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.000000007Z\n" +
                        "1970-01-01T00:00:00.000000000Z\n" +
                        "1970-01-01T00:00:00.000000008Z\n" +
                        "1970-01-01T00:00:00.000000000Z\n" +
                        "1970-01-01T00:00:00.000000007Z\n" +
                        "1970-01-01T00:00:00.000000001Z\n" +
                        "1970-01-01T00:00:00.000000002Z\n" +
                        "1970-01-01T00:00:00.000000001Z\n" +
                        "1970-01-01T00:00:00.000000006Z\n" +
                        "1970-01-01T00:00:00.000000003Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testCharToVarchar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(rnd_char() as varchar) from long_sequence(10)",
                "a\n" +
                        "V\n" +
                        "T\n" +
                        "J\n" +
                        "W\n" +
                        "C\n" +
                        "P\n" +
                        "S\n" +
                        "W\n" +
                        "H\n" +
                        "Y\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testCharToVarcharConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast('A' as varchar) from long_sequence(10)",
                "a\n" +
                        "A\n" +
                        "A\n" +
                        "A\n" +
                        "A\n" +
                        "A\n" +
                        "A\n" +
                        "A\n" +
                        "A\n" +
                        "A\n" +
                        "A\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testCharToVarcharSort() throws Exception {
        assertQuery(
                "x\n" +
                        "C\n" +
                        "E\n" +
                        "G\n" +
                        "H\n" +
                        "H\n" +
                        "J\n" +
                        "N\n" +
                        "P\n" +
                        "P\n" +
                        "R\n" +
                        "R\n" +
                        "S\n" +
                        "T\n" +
                        "V\n" +
                        "W\n" +
                        "W\n" +
                        "X\n" +
                        "X\n" +
                        "Y\n" +
                        "Z\n",
                "select cast(a as varchar) x from tt order by x",
                "create table tt as (select rnd_char() a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testDateToBoolean() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(rnd_date() as boolean) from long_sequence(10)",
                "a\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDateToBooleanConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(cast(1 as date) as boolean) from long_sequence(10)",
                "a\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDateToByte() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a byte)",
                null,
                "insert into tab select cast(rnd_date(96,100, 2) as byte) from long_sequence(10)",
                "a\n" +
                        "97\n" +
                        "0\n" +
                        "100\n" +
                        "99\n" +
                        "0\n" +
                        "97\n" +
                        "97\n" +
                        "98\n" +
                        "0\n" +
                        "96\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDateToChar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a char)",
                null,
                "insert into tab select cast(rnd_date(34,66,2) as char) from long_sequence(10)",
                "a\n" +
                        "7\n" +
                        "\n" +
                        "9\n" +
                        "0\n" +
                        "\n" +
                        "2\n" +
                        "+\n" +
                        "4\n" +
                        "\n" +
                        "-\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDateToDate() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a date)",
                null,
                "insert into tab select cast(rnd_date(1000000,10000000,2) as date) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T01:30:37.285Z\n" +
                        "\n" +
                        "1970-01-01T01:39:20.648Z\n" +
                        "1970-01-01T02:26:19.401Z\n" +
                        "\n" +
                        "1970-01-01T01:31:53.880Z\n" +
                        "1970-01-01T01:32:15.804Z\n" +
                        "1970-01-01T02:38:18.131Z\n" +
                        "\n" +
                        "1970-01-01T00:42:01.595Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDateToDouble() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a double)",
                null,
                "insert into tab select cast(rnd_date(1,150,2) as double) from long_sequence(10)",
                "a\n" +
                        "67.0\n" +
                        "null\n" +
                        "30.0\n" +
                        "99.0\n" +
                        "null\n" +
                        "137.0\n" +
                        "127.0\n" +
                        "58.0\n" +
                        "null\n" +
                        "111.0\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDateToFloat() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a float)",
                null,
                "insert into tab select cast(rnd_date(1,150,2) as float) from long_sequence(10)",
                "a\n" +
                        "67.0\n" +
                        "null\n" +
                        "30.0\n" +
                        "99.0\n" +
                        "null\n" +
                        "137.0\n" +
                        "127.0\n" +
                        "58.0\n" +
                        "null\n" +
                        "111.0\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDateToInt() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a int)",
                null,
                "insert into tab select cast(rnd_date(1000000L, 1000000000L, 2) as int) from long_sequence(10)",
                "a\n" +
                        "985257636\n" +
                        "null\n" +
                        "968130026\n" +
                        "555619965\n" +
                        "null\n" +
                        "712286238\n" +
                        "215755333\n" +
                        "720037886\n" +
                        "null\n" +
                        "129724714\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDateToLong() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long)",
                null,
                "insert into tab select cast(rnd_date(1,15000000,2) as long) from long_sequence(10)",
                "a\n" +
                        "13992367\n" +
                        "null\n" +
                        "7587030\n" +
                        "11082999\n" +
                        "null\n" +
                        "602537\n" +
                        "5112277\n" +
                        "5361808\n" +
                        "null\n" +
                        "8600061\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDateToLong256Sort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "0x08\n" +
                        "0x11\n" +
                        "0x1e\n" +
                        "0x34\n" +
                        "0x3d\n" +
                        "0x4d\n" +
                        "0x57\n" +
                        "0x63\n" +
                        "0x80\n" +
                        "0x89\n" +
                        "0xa7\n" +
                        "0xc0\n" +
                        "0xc7\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select rnd_date(1,200,1) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testDateToShort() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a short)",
                null,
                "insert into tab select cast(rnd_date(23,56,2) as short) from long_sequence(10)",
                "a\n" +
                        "31\n" +
                        "0\n" +
                        "54\n" +
                        "23\n" +
                        "0\n" +
                        "29\n" +
                        "33\n" +
                        "24\n" +
                        "0\n" +
                        "51\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDateToStr() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(rnd_date(34,66,100) as string) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.055Z\n" +
                        "1970-01-01T00:00:00.048Z\n" +
                        "1970-01-01T00:00:00.055Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.045Z\n" +
                        "1970-01-01T00:00:00.036Z\n" +
                        "1970-01-01T00:00:00.034Z\n" +
                        "1970-01-01T00:00:00.058Z\n" +
                        "1970-01-01T00:00:00.045Z\n" +
                        "1970-01-01T00:00:00.061Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDateToStrConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(cast(334l as date) as string) from long_sequence(10)",
                "a\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDateToStrSort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "1970-01-01T00:00:00.008Z\n" +
                        "1970-01-01T00:00:00.017Z\n" +
                        "1970-01-01T00:00:00.030Z\n" +
                        "1970-01-01T00:00:00.052Z\n" +
                        "1970-01-01T00:00:00.061Z\n" +
                        "1970-01-01T00:00:00.077Z\n" +
                        "1970-01-01T00:00:00.087Z\n" +
                        "1970-01-01T00:00:00.099Z\n" +
                        "1970-01-01T00:00:00.128Z\n" +
                        "1970-01-01T00:00:00.137Z\n" +
                        "1970-01-01T00:00:00.167Z\n" +
                        "1970-01-01T00:00:00.192Z\n" +
                        "1970-01-01T00:00:00.199Z\n",
                "select cast(a as string) x from tt order by x",
                "create table tt as (select rnd_date(1,200,1) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testDateToSymbol() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(rnd_date(1,150,2) as symbol) from long_sequence(10)",
                "a\n" +
                        "67\n" +
                        "\n" +
                        "30\n" +
                        "99\n" +
                        "\n" +
                        "137\n" +
                        "127\n" +
                        "58\n" +
                        "\n" +
                        "111\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDateToSymbolConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(cast(601l as date) as symbol) from long_sequence(10)",
                "a\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDateToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "20\t1970-01-01T00:00:00.020Z\n" +
                        "\t\n" +
                        "11\t1970-01-01T00:00:00.011Z\n" +
                        "13\t1970-01-01T00:00:00.013Z\n" +
                        "\t\n" +
                        "15\t1970-01-01T00:00:00.015Z\n" +
                        "19\t1970-01-01T00:00:00.019Z\n" +
                        "17\t1970-01-01T00:00:00.017Z\n" +
                        "\t\n" +
                        "10\t1970-01-01T00:00:00.010Z\n" +
                        "\t\n" +
                        "17\t1970-01-01T00:00:00.017Z\n" +
                        "\t\n" +
                        "17\t1970-01-01T00:00:00.017Z\n" +
                        "18\t1970-01-01T00:00:00.018Z\n" +
                        "18\t1970-01-01T00:00:00.018Z\n" +
                        "\t\n" +
                        "12\t1970-01-01T00:00:00.012Z\n" +
                        "11\t1970-01-01T00:00:00.011Z\n" +
                        "15\t1970-01-01T00:00:00.015Z\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_date(10, 20, 2) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testDateToTimestamp() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp)",
                null,
                "insert into tab select cast(rnd_date(1000,150000,1) as timestamp) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:01:38.083000Z\n" +
                        "\n" +
                        "1970-01-01T00:00:06.240000Z\n" +
                        "1970-01-01T00:00:53.076000Z\n" +
                        "\n" +
                        "1970-01-01T00:01:03.779000Z\n" +
                        "1970-01-01T00:01:23.737000Z\n" +
                        "1970-01-01T00:02:23.935000Z\n" +
                        "\n" +
                        "1970-01-01T00:01:15.474000Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDateToTimestampNano() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp_ns)",
                null,
                "insert into tab select cast(rnd_date(1000,150000,1) as timestamp_ns) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:01:38.083000000Z\n" +
                        "\n" +
                        "1970-01-01T00:00:06.240000000Z\n" +
                        "1970-01-01T00:00:53.076000000Z\n" +
                        "\n" +
                        "1970-01-01T00:01:03.779000000Z\n" +
                        "1970-01-01T00:01:23.737000000Z\n" +
                        "1970-01-01T00:02:23.935000000Z\n" +
                        "\n" +
                        "1970-01-01T00:01:15.474000000Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDateToVarchar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(rnd_date(34,66,100) as varchar) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.055Z\n" +
                        "1970-01-01T00:00:00.048Z\n" +
                        "1970-01-01T00:00:00.055Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.045Z\n" +
                        "1970-01-01T00:00:00.036Z\n" +
                        "1970-01-01T00:00:00.034Z\n" +
                        "1970-01-01T00:00:00.058Z\n" +
                        "1970-01-01T00:00:00.045Z\n" +
                        "1970-01-01T00:00:00.061Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDateToVarcharConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(cast(334l as date) as varchar) from long_sequence(10)",
                "a\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDateToVarcharSort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "1970-01-01T00:00:00.008Z\n" +
                        "1970-01-01T00:00:00.017Z\n" +
                        "1970-01-01T00:00:00.030Z\n" +
                        "1970-01-01T00:00:00.052Z\n" +
                        "1970-01-01T00:00:00.061Z\n" +
                        "1970-01-01T00:00:00.077Z\n" +
                        "1970-01-01T00:00:00.087Z\n" +
                        "1970-01-01T00:00:00.099Z\n" +
                        "1970-01-01T00:00:00.128Z\n" +
                        "1970-01-01T00:00:00.137Z\n" +
                        "1970-01-01T00:00:00.167Z\n" +
                        "1970-01-01T00:00:00.192Z\n" +
                        "1970-01-01T00:00:00.199Z\n",
                "select cast(a as varchar) x from tt order by x",
                "create table tt as (select rnd_date(1,200,1) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testDoubleToBoolean() throws Exception {
        assertQueryNoLeakCheck(
                "cast\nfalse\n",
                "select cast(0.0 as boolean)",
                null,
                true,
                true,
                true
        );
        assertQueryNoLeakCheck(
                "cast\ntrue\n",
                "select cast(0.123 as boolean)",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testDoubleToByte() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a byte)",
                null,
                "insert into tab select cast(rnd_double(4)*10 as byte) from long_sequence(10)",
                "a\n" +
                        "8\n" +
                        "0\n" +
                        "0\n" +
                        "6\n" +
                        "7\n" +
                        "2\n" +
                        "3\n" +
                        "7\n" +
                        "4\n" +
                        "0\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDoubleToChar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a char)",
                null,
                "insert into tab select cast(33 + (rnd_double() * 100) % 25 as char) from long_sequence(10)",
                "a\n" +
                        "1\n" +
                        "7\n" +
                        ")\n" +
                        "%\n" +
                        "5\n" +
                        "0\n" +
                        "*\n" +
                        "8\n" +
                        "7\n" +
                        "!\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDoubleToDate() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a date)",
                null,
                "insert into tab select cast(rnd_double(2)*10000 as date) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:08.043Z\n" +
                        "1970-01-01T00:00:00.848Z\n" +
                        "1970-01-01T00:00:00.843Z\n" +
                        "1970-01-01T00:00:06.508Z\n" +
                        "1970-01-01T00:00:07.905Z\n" +
                        "1970-01-01T00:00:02.245Z\n" +
                        "1970-01-01T00:00:03.491Z\n" +
                        "1970-01-01T00:00:07.611Z\n" +
                        "1970-01-01T00:00:04.217Z\n" +
                        "\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDoubleToDouble() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a double)",
                null,
                "insert into tab select cast(rnd_double(2) as double) from long_sequence(10)",
                "a\n" +
                        "0.8043224099968393\n" +
                        "0.08486964232560668\n" +
                        "0.0843832076262595\n" +
                        "0.6508594025855301\n" +
                        "0.7905675319675964\n" +
                        "0.22452340856088226\n" +
                        "0.3491070363730514\n" +
                        "0.7611029514995744\n" +
                        "0.4217768841969397\n" +
                        "null\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDoubleToFloat() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a float)",
                null,
                "insert into tab select cast(rnd_double(2) as float) from long_sequence(10)",
                "a\n" +
                        "0.8043224\n" +
                        "0.084869646\n" +
                        "0.084383205\n" +
                        "0.6508594\n" +
                        "0.7905675\n" +
                        "0.22452341\n" +
                        "0.34910703\n" +
                        "0.761103\n" +
                        "0.4217769\n" +
                        "null\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDoubleToInt() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a int)",
                null,
                "insert into tab select cast(rnd_double()*100 as int) from long_sequence(10)",
                "a\n" +
                        "66\n" +
                        "22\n" +
                        "8\n" +
                        "29\n" +
                        "20\n" +
                        "65\n" +
                        "84\n" +
                        "98\n" +
                        "22\n" +
                        "50\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDoubleToLong() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long)",
                null,
                "insert into tab select cast(rnd_double(2)*1000 as long) from long_sequence(10)",
                "a\n" +
                        "804\n" +
                        "84\n" +
                        "84\n" +
                        "650\n" +
                        "790\n" +
                        "224\n" +
                        "349\n" +
                        "761\n" +
                        "421\n" +
                        "null\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDoubleToLong256() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long256)",
                null,
                "insert into tab select cast(rnd_double(2)*1000000 as long256) from long_sequence(10)",
                "a\n" +
                        "0x0c45e2\n" +
                        "0x014b85\n" +
                        "0x01499f\n" +
                        "0x09ee6b\n" +
                        "0x0c1027\n" +
                        "0x036d0b\n" +
                        "0x0553b3\n" +
                        "0x0b9d0e\n" +
                        "0x066f90\n" +
                        "\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDoubleToLong256Sort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "0x0f52\n" +
                        "0x016749\n" +
                        "0x01695b\n" +
                        "0x03bbfa\n" +
                        "0x042050\n" +
                        "0x05780b\n" +
                        "0x05ce6e\n" +
                        "0x0668f5\n" +
                        "0x0703d8\n" +
                        "0x0706a6\n" +
                        "0x0a9d2b\n" +
                        "0x0ad33c\n" +
                        "0x0bcca5\n" +
                        "0x0c13a7\n" +
                        "0x0ca8a2\n" +
                        "0x0d2616\n" +
                        "0x0d60a7\n" +
                        "0x10405a\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select rnd_double(2)*1090000 a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testDoubleToShort() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a short)",
                null,
                "insert into tab select cast(rnd_double(5)*10 as short) from long_sequence(10)",
                "a\n" +
                        "8\n" +
                        "0\n" +
                        "0\n" +
                        "6\n" +
                        "7\n" +
                        "2\n" +
                        "3\n" +
                        "7\n" +
                        "4\n" +
                        "0\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDoubleToStr() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(rnd_double() as string) from long_sequence(10)",
                "a\n" +
                        "0.6607777894187332\n" +
                        "0.2246301342497259\n" +
                        "0.08486964232560668\n" +
                        "0.299199045961845\n" +
                        "0.20447441837877756\n" +
                        "0.6508594025855301\n" +
                        "0.8423410920883345\n" +
                        "0.9856290845874263\n" +
                        "0.22452340856088226\n" +
                        "0.5093827001617407\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDoubleToStrConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(1.34 as string) from long_sequence(10)",
                "a\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDoubleToStrSort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "0.0035983672154330515\n" +
                        "0.0843832076262595\n" +
                        "0.08486964232560668\n" +
                        "0.22452340856088226\n" +
                        "0.24808812376657652\n" +
                        "0.3288176907679504\n" +
                        "0.3491070363730514\n" +
                        "0.38539947865244994\n" +
                        "0.4217768841969397\n" +
                        "0.4224356661645131\n" +
                        "0.6381607531178513\n" +
                        "0.6508594025855301\n" +
                        "0.7094360487171202\n" +
                        "0.7261136209823622\n" +
                        "0.7611029514995744\n" +
                        "0.7905675319675964\n" +
                        "0.8043224099968393\n" +
                        "0.9771103146051203\n",
                "select cast(a as string) x from tt order by x",
                "create table tt as (select rnd_double(2) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testDoubleToSymbol() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(rnd_double(2) as symbol) from long_sequence(10)",
                "a\n" +
                        "0.8043224099968393\n" +
                        "0.08486964232560668\n" +
                        "0.0843832076262595\n" +
                        "0.6508594025855301\n" +
                        "0.7905675319675964\n" +
                        "0.22452340856088226\n" +
                        "0.3491070363730514\n" +
                        "0.7611029514995744\n" +
                        "0.4217768841969397\n" +
                        "\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDoubleToSymbolConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(1.5 as symbol) from long_sequence(10)",
                "a\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDoubleToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "0.8043224099968393\t0.8043224099968393\n" +
                        "0.08486964232560668\t0.08486964232560668\n" +
                        "0.0843832076262595\t0.0843832076262595\n" +
                        "0.6508594025855301\t0.6508594025855301\n" +
                        "0.7905675319675964\t0.7905675319675964\n" +
                        "0.22452340856088226\t0.22452340856088226\n" +
                        "0.3491070363730514\t0.3491070363730514\n" +
                        "0.7611029514995744\t0.7611029514995744\n" +
                        "0.4217768841969397\t0.4217768841969397\n" +
                        "\tnull\n" +
                        "0.7261136209823622\t0.7261136209823622\n" +
                        "0.4224356661645131\t0.4224356661645131\n" +
                        "0.7094360487171202\t0.7094360487171202\n" +
                        "0.38539947865244994\t0.38539947865244994\n" +
                        "0.0035983672154330515\t0.0035983672154330515\n" +
                        "0.3288176907679504\t0.3288176907679504\n" +
                        "\tnull\n" +
                        "0.9771103146051203\t0.9771103146051203\n" +
                        "0.24808812376657652\t0.24808812376657652\n" +
                        "0.6381607531178513\t0.6381607531178513\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_double(2) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testDoubleToTimestamp() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp)",
                null,
                "insert into tab select cast(rnd_double(2)*100000000 as timestamp) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:01:20.432240Z\n" +
                        "1970-01-01T00:00:08.486964Z\n" +
                        "1970-01-01T00:00:08.438320Z\n" +
                        "1970-01-01T00:01:05.085940Z\n" +
                        "1970-01-01T00:01:19.056753Z\n" +
                        "1970-01-01T00:00:22.452340Z\n" +
                        "1970-01-01T00:00:34.910703Z\n" +
                        "1970-01-01T00:01:16.110295Z\n" +
                        "1970-01-01T00:00:42.177688Z\n" +
                        "\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDoubleToTimestampNano() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp_ns)",
                null,
                "insert into tab select cast(rnd_double(2)*100000000 as timestamp_ns) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.080432240Z\n" +
                        "1970-01-01T00:00:00.008486964Z\n" +
                        "1970-01-01T00:00:00.008438320Z\n" +
                        "1970-01-01T00:00:00.065085940Z\n" +
                        "1970-01-01T00:00:00.079056753Z\n" +
                        "1970-01-01T00:00:00.022452340Z\n" +
                        "1970-01-01T00:00:00.034910703Z\n" +
                        "1970-01-01T00:00:00.076110295Z\n" +
                        "1970-01-01T00:00:00.042177688Z\n" +
                        "\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDoubleToVarchar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(rnd_double() as varchar) from long_sequence(10)",
                "a\n" +
                        "0.6607777894187332\n" +
                        "0.2246301342497259\n" +
                        "0.08486964232560668\n" +
                        "0.299199045961845\n" +
                        "0.20447441837877756\n" +
                        "0.6508594025855301\n" +
                        "0.8423410920883345\n" +
                        "0.9856290845874263\n" +
                        "0.22452340856088226\n" +
                        "0.5093827001617407\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDoubleToVarcharConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(1.34 as varchar) from long_sequence(10)",
                "a\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testDoubleToVarcharSort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "0.0035983672154330515\n" +
                        "0.0843832076262595\n" +
                        "0.08486964232560668\n" +
                        "0.22452340856088226\n" +
                        "0.24808812376657652\n" +
                        "0.3288176907679504\n" +
                        "0.3491070363730514\n" +
                        "0.38539947865244994\n" +
                        "0.4217768841969397\n" +
                        "0.4224356661645131\n" +
                        "0.6381607531178513\n" +
                        "0.6508594025855301\n" +
                        "0.7094360487171202\n" +
                        "0.7261136209823622\n" +
                        "0.7611029514995744\n" +
                        "0.7905675319675964\n" +
                        "0.8043224099968393\n" +
                        "0.9771103146051203\n",
                "select cast(a as varchar) x from tt order by x",
                "create table tt as (select rnd_double(2) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testFloatToBoolean() throws Exception {
        assertQueryNoLeakCheck(
                "cast\ntrue\n",
                "select cast(0.0002 as boolean)",
                null,
                true,
                true,
                true
        );
        assertQueryNoLeakCheck(
                "cast\nfalse\n",
                "select cast(0.0000 as boolean)",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testFloatToByte() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a byte)",
                null,
                "insert into tab select cast(rnd_float()*100 as byte) from long_sequence(10)",
                "a\n" +
                        "66\n" +
                        "80\n" +
                        "22\n" +
                        "12\n" +
                        "8\n" +
                        "28\n" +
                        "29\n" +
                        "8\n" +
                        "20\n" +
                        "93\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testFloatToChar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a char)",
                null,
                "insert into tab select cast(33 + (rnd_float() * 100) % 25 as char) from long_sequence(10)",
                "a\n" +
                        "1\n" +
                        "&\n" +
                        "7\n" +
                        "-\n" +
                        ")\n" +
                        "$\n" +
                        "%\n" +
                        ")\n" +
                        "5\n" +
                        "3\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testFloatToDate() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a date)",
                null,
                "insert into tab select cast(rnd_float(2)*1000000 as date) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:13:24.322Z\n" +
                        "\n" +
                        "1970-01-01T00:01:24.869Z\n" +
                        "1970-01-01T00:04:59.199Z\n" +
                        "\n" +
                        "1970-01-01T00:15:34.460Z\n" +
                        "1970-01-01T00:02:11.233Z\n" +
                        "1970-01-01T00:13:10.567Z\n" +
                        "\n" +
                        "1970-01-01T00:03:44.523Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testFloatToDouble() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a double)",
                null,
                "insert into tab select cast(rnd_float(2) as double) from long_sequence(10)",
                "a\n" +
                        "0.804322361946106\n" +
                        "null\n" +
                        "0.0848696231842041\n" +
                        "0.29919904470443726\n" +
                        "null\n" +
                        "0.934460461139679\n" +
                        "0.1312335729598999\n" +
                        "0.7905675172805786\n" +
                        "null\n" +
                        "0.2245233654975891\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testFloatToFloat() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a float)",
                null,
                "insert into tab select cast(rnd_float(2) as float) from long_sequence(10)",
                "a\n" +
                        "0.80432236\n" +
                        "null\n" +
                        "0.08486962\n" +
                        "0.29919904\n" +
                        "null\n" +
                        "0.93446046\n" +
                        "0.13123357\n" +
                        "0.7905675\n" +
                        "null\n" +
                        "0.22452337\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testFloatToInt() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a int)",
                null,
                "insert into tab select cast(rnd_float(2)*10000 as int) from long_sequence(10)",
                "a\n" +
                        "8043\n" +
                        "null\n" +
                        "848\n" +
                        "2991\n" +
                        "null\n" +
                        "9344\n" +
                        "1312\n" +
                        "7905\n" +
                        "null\n" +
                        "2245\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testFloatToLong() throws Exception {
        execute("create table rndfloat as (select rnd_float(2) fl from long_sequence(10))");
        engine.releaseAllReaders();
        engine.releaseAllWriters();

        assertQuery(
                "fl\ta\n",
                "tab",
                "create table tab (fl DOUBLE, a LONG)",
                null,
                "insert into tab select fl, cast(fl*10000000000l as long) from rndfloat",
                "fl\ta\n" +
                        "0.804322361946106\t8043223619\n" +
                        "null\tnull\n" +
                        "0.0848696231842041\t848696231\n" +
                        "0.29919904470443726\t2991990447\n" +
                        "null\tnull\n" +
                        "0.934460461139679\t9344604611\n" +
                        "0.1312335729598999\t1312335729\n" +
                        "0.7905675172805786\t7905675172\n" +
                        "null\tnull\n" +
                        "0.2245233654975891\t2245233654\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testFloatToLong256() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long256)",
                null,
                "insert into tab select cast(rnd_float(2)*1000000 as long256) from long_sequence(10)",
                "a\n" +
                        "0x0c45e2\n" +
                        "\n" +
                        "0x014b85\n" +
                        "0x0490bf\n" +
                        "\n" +
                        "0x0e423c\n" +
                        "0x0200a1\n" +
                        "0x0c1027\n" +
                        "\n" +
                        "0x036d0b\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testFloatToLong256Sort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "0x01695b\n" +
                        "0x022ec4\n" +
                        "0x03bbfa\n" +
                        "0x04f9ee\n" +
                        "0x05ce6e\n" +
                        "0x08b8ad\n" +
                        "0x095004\n" +
                        "0x0a709b\n" +
                        "0x0b221c\n" +
                        "0x0c13a7\n" +
                        "0x0ca8a2\n" +
                        "0x0d2616\n" +
                        "0x0d60a7\n" +
                        "0x0f8ac1\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select rnd_float(2)*1090000 a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testFloatToShort() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a short)",
                null,
                "insert into tab select cast(rnd_float()*1000 as short) from long_sequence(10)",
                "a\n" +
                        "660\n" +
                        "804\n" +
                        "224\n" +
                        "129\n" +
                        "84\n" +
                        "284\n" +
                        "299\n" +
                        "84\n" +
                        "204\n" +
                        "934\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testFloatToStr() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(rnd_float() as string) from long_sequence(10)",
                "a\n" +
                        "0.66077775\n" +
                        "0.80432236\n" +
                        "0.22463012\n" +
                        "0.12966657\n" +
                        "0.08486962\n" +
                        "0.28455776\n" +
                        "0.29919904\n" +
                        "0.08438319\n" +
                        "0.20447439\n" +
                        "0.93446046\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testFloatToStrConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(cast(1.34 as float) as string) from long_sequence(10)",
                "a\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testFloatToStrSort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "0.08486962\n" +
                        "0.13123357\n" +
                        "0.22452337\n" +
                        "0.29919904\n" +
                        "0.34910703\n" +
                        "0.5243723\n" +
                        "0.55991614\n" +
                        "0.6276954\n" +
                        "0.6693837\n" +
                        "0.7261136\n" +
                        "0.7611029\n" +
                        "0.7905675\n" +
                        "0.80432236\n" +
                        "0.93446046\n",
                "select cast(a as string) x from tt order by x",
                "create table tt as (select rnd_float(2) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testFloatToSymbol() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(rnd_float(2) as symbol) from long_sequence(10)",
                "a\n" +
                        "0.80432236\n" +
                        "\n" +
                        "0.08486962\n" +
                        "0.29919904\n" +
                        "\n" +
                        "0.93446046\n" +
                        "0.13123357\n" +
                        "0.7905675\n" +
                        "\n" +
                        "0.22452337\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testFloatToSymbolConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(cast(1.5 as float) as symbol) from long_sequence(10)",
                "a\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testFloatToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "0.80432236\t0.80432236\n" +
                        "\tnull\n" +
                        "0.08486962\t0.08486962\n" +
                        "0.29919904\t0.29919904\n" +
                        "\tnull\n" +
                        "0.93446046\t0.93446046\n" +
                        "0.13123357\t0.13123357\n" +
                        "0.7905675\t0.7905675\n" +
                        "\tnull\n" +
                        "0.22452337\t0.22452337\n" +
                        "\tnull\n" +
                        "0.34910703\t0.34910703\n" +
                        "\tnull\n" +
                        "0.7611029\t0.7611029\n" +
                        "0.5243723\t0.5243723\n" +
                        "0.55991614\t0.55991614\n" +
                        "\tnull\n" +
                        "0.7261136\t0.7261136\n" +
                        "0.6276954\t0.6276954\n" +
                        "0.6693837\t0.6693837\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_float(2) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testFloatToTimestampNanoViaDouble() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp_ns)",
                null,
                "insert into tab select cast(rnd_float(2)*100000000 as timestamp_ns) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.080432240Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.008486962Z\n" +
                        "1970-01-01T00:00:00.029919904Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.093446048Z\n" +
                        "1970-01-01T00:00:00.013123357Z\n" +
                        "1970-01-01T00:00:00.079056752Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.022452336Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testFloatToTimestampViaDouble() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp)",
                null,
                "insert into tab select cast(rnd_float(2)*100000000 as timestamp) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:01:20.432240Z\n" +
                        "\n" +
                        "1970-01-01T00:00:08.486962Z\n" +
                        "1970-01-01T00:00:29.919904Z\n" +
                        "\n" +
                        "1970-01-01T00:01:33.446048Z\n" +
                        "1970-01-01T00:00:13.123357Z\n" +
                        "1970-01-01T00:01:19.056752Z\n" +
                        "\n" +
                        "1970-01-01T00:00:22.452336Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testFloatToVarchar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(rnd_float() as varchar) from long_sequence(10)",
                "a\n" +
                        "0.66077775\n" +
                        "0.80432236\n" +
                        "0.22463012\n" +
                        "0.12966657\n" +
                        "0.08486962\n" +
                        "0.28455776\n" +
                        "0.29919904\n" +
                        "0.08438319\n" +
                        "0.20447439\n" +
                        "0.93446046\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testFloatToVarcharConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(cast(1.34 as float) as varchar) from long_sequence(10)",
                "a\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testFloatToVarcharSort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "0.08486962\n" +
                        "0.13123357\n" +
                        "0.22452337\n" +
                        "0.29919904\n" +
                        "0.34910703\n" +
                        "0.5243723\n" +
                        "0.55991614\n" +
                        "0.6276954\n" +
                        "0.6693837\n" +
                        "0.7261136\n" +
                        "0.7611029\n" +
                        "0.7905675\n" +
                        "0.80432236\n" +
                        "0.93446046\n",
                "select cast(a as varchar) x from tt order by x",
                "create table tt as (select rnd_float(2) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testGeoHashToStr() throws Exception {
        assertQuery(
                "a\n",
                "tab",
                "create table tab (a geohash(12c))",
                null,
                "insert into tab select cast(rnd_geohash(60) as varchar) as a from long_sequence(10)",
                "a\n" +
                        "9v1s8hm7wpks\n" +
                        "46swgj10r88k\n" +
                        "jnw97u4yuquw\n" +
                        "zfuqd3bf8hbu\n" +
                        "hp4muv5tgg3q\n" +
                        "wh4b6vntdq1c\n" +
                        "s2z2fydsjq5n\n" +
                        "1cjjwk6r9jfe\n" +
                        "mmt89425bhff\n" +
                        "71ftmpy5v1uy\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testGeoHashToVarchar() throws Exception {
        assertQuery(
                "a\n",
                "tab",
                "create table tab (a geohash(12c))",
                null,
                "insert into tab select cast(rnd_geohash(60) as string) as a from long_sequence(10)",
                "a\n" +
                        "9v1s8hm7wpks\n" +
                        "46swgj10r88k\n" +
                        "jnw97u4yuquw\n" +
                        "zfuqd3bf8hbu\n" +
                        "hp4muv5tgg3q\n" +
                        "wh4b6vntdq1c\n" +
                        "s2z2fydsjq5n\n" +
                        "1cjjwk6r9jfe\n" +
                        "mmt89425bhff\n" +
                        "71ftmpy5v1uy\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testIPv4ToVarchar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(rnd_varchar('171.30.189.77','111.221.228.130','201.100.238.229',null) as varchar) from long_sequence(10)",
                "a\n" +
                        "171.30.189.77\n" +
                        "201.100.238.229\n" +
                        "111.221.228.130\n" +
                        "\n" +
                        "111.221.228.130\n" +
                        "\n" +
                        "201.100.238.229\n" +
                        "\n" +
                        "201.100.238.229\n" +
                        "171.30.189.77\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testIPv4ToVarcharConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(cast('192.168.0.1' as IPv4) as varchar) from long_sequence(10)",
                "a\n" +
                        "192.168.0.1\n" +
                        "192.168.0.1\n" +
                        "192.168.0.1\n" +
                        "192.168.0.1\n" +
                        "192.168.0.1\n" +
                        "192.168.0.1\n" +
                        "192.168.0.1\n" +
                        "192.168.0.1\n" +
                        "192.168.0.1\n" +
                        "192.168.0.1\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testInfinity() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast('Infinity' as double) = cast('Infinity' as double)," +
                        "cast('Infinity' as float) = cast('Infinity' as double)," +
                        "cast('Infinity' as float) = cast('Infinity' as float)," +
                        "cast('-Infinity' as double) = cast('-Infinity' as double)," +
                        "cast('-Infinity' as double) = cast('-Infinity' as float)," +
                        "cast('-Infinity' as float) = cast('-Infinity' as float)," +
                        "cast('Infinity' as double) != cast('-Infinity' as double)," +
                        "cast('Infinity' as float) > 1 " +
                        "from long_sequence(8)",
                "a\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testInfinityNonConstant() throws Exception {
        assertQuery(
                "column\n",
                "select a = b from tab ",
                "create table tab (a double, b float)",
                null,
                "insert into tab values (cast('Infinity' as double), cast('Infinity' as float))",
                "column\n" +
                        "true\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testIntConstToBoolean() throws Exception {
        assertQueryNoLeakCheck(
                "cast\tcast1\tcast2\tcast3\ntrue\tfalse\ttrue\ttrue\n",
                "select cast(-1 as boolean), cast(0 as boolean), cast(1 as boolean), cast(2 as boolean)",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testIntToBoolean() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(rnd_int(0,66,100) as boolean) from long_sequence(10)",
                "a\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testIntToBooleanConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(1 as boolean) from long_sequence(10)",
                "a\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testIntToByte() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a byte)",
                null,
                "insert into tab select cast(rnd_int(2,100, 10) as byte) from long_sequence(10)",
                "a\n" +
                        "41\n" +
                        "28\n" +
                        "0\n" +
                        "100\n" +
                        "5\n" +
                        "72\n" +
                        "72\n" +
                        "24\n" +
                        "53\n" +
                        "50\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testIntToChar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a char)",
                null,
                "insert into tab select cast(rnd_int(34,66,100) as char) from long_sequence(10)",
                "a\n" +
                        "(\n" +
                        "<\n" +
                        "9\n" +
                        "\n" +
                        "%\n" +
                        "&\n" +
                        "&\n" +
                        "8\n" +
                        "4\n" +
                        "1\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testIntToDate() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a date)",
                null,
                "insert into tab select cast(rnd_int(1,150,100) as date) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.019Z\n" +
                        "1970-01-01T00:00:00.072Z\n" +
                        "1970-01-01T00:00:00.090Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.007Z\n" +
                        "1970-01-01T00:00:00.017Z\n" +
                        "1970-01-01T00:00:00.065Z\n" +
                        "1970-01-01T00:00:00.032Z\n" +
                        "1970-01-01T00:00:00.067Z\n" +
                        "1970-01-01T00:00:00.106Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testIntToDouble() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a double)",
                null,
                "insert into tab select cast(rnd_int(1,150,100) as double) from long_sequence(10)",
                "a\n" +
                        "19.0\n" +
                        "72.0\n" +
                        "90.0\n" +
                        "null\n" +
                        "7.0\n" +
                        "17.0\n" +
                        "65.0\n" +
                        "32.0\n" +
                        "67.0\n" +
                        "106.0\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testIntToFloat() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a float)",
                null,
                "insert into tab select cast(rnd_int(1,150,100) as float) from long_sequence(10)",
                "a\n" +
                        "19.0\n" +
                        "72.0\n" +
                        "90.0\n" +
                        "null\n" +
                        "7.0\n" +
                        "17.0\n" +
                        "65.0\n" +
                        "32.0\n" +
                        "67.0\n" +
                        "106.0\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testIntToInt() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a int)",
                null,
                "insert into tab select cast(rnd_int(34,66,100) as int) from long_sequence(10)",
                "a\n" +
                        "40\n" +
                        "60\n" +
                        "57\n" +
                        "null\n" +
                        "37\n" +
                        "38\n" +
                        "38\n" +
                        "56\n" +
                        "52\n" +
                        "49\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testIntToLong() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long)",
                null,
                "insert into tab select cast(rnd_int(1,150,100) as long) from long_sequence(10)",
                "a\n" +
                        "19\n" +
                        "72\n" +
                        "90\n" +
                        "null\n" +
                        "7\n" +
                        "17\n" +
                        "65\n" +
                        "32\n" +
                        "67\n" +
                        "106\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testIntToLong256() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long256)",
                null,
                "insert into tab select cast(rnd_int(34,66,100) as long256) from long_sequence(10)",
                "a\n" +
                        "0x28\n" +
                        "0x3c\n" +
                        "0x39\n" +
                        "\n" +
                        "0x25\n" +
                        "0x26\n" +
                        "0x26\n" +
                        "0x38\n" +
                        "0x34\n" +
                        "0x31\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testIntToLong256Sort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "0x2b\n" +
                        "0x31\n" +
                        "0x33\n" +
                        "0x38\n" +
                        "0x4b\n" +
                        "0x66\n" +
                        "0x68\n" +
                        "0x69\n" +
                        "0x6a\n" +
                        "0x75\n" +
                        "0x77\n" +
                        "0xad\n" +
                        "0xc6\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select rnd_int(1,200,1) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntToShort() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a short)",
                null,
                "insert into tab select cast(rnd_int(23,56,100) as short) from long_sequence(10)",
                "a\n" +
                        "37\n" +
                        "48\n" +
                        "30\n" +
                        "0\n" +
                        "55\n" +
                        "51\n" +
                        "53\n" +
                        "54\n" +
                        "23\n" +
                        "34\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testIntToStr() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(rnd_int(34,66,100) as string) from long_sequence(10)",
                "a\n" +
                        "40\n" +
                        "60\n" +
                        "57\n" +
                        "\n" +
                        "37\n" +
                        "38\n" +
                        "38\n" +
                        "56\n" +
                        "52\n" +
                        "49\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testIntToStrConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(334 as string) from long_sequence(10)",
                "a\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testIntToStrSort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "102\n" +
                        "104\n" +
                        "105\n" +
                        "106\n" +
                        "117\n" +
                        "119\n" +
                        "173\n" +
                        "198\n" +
                        "43\n" +
                        "49\n" +
                        "51\n" +
                        "56\n" +
                        "75\n",
                "select cast(a as string) x from tt order by x",
                "create table tt as (select rnd_int(1,200,1) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntToSymbol() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(rnd_int(1,150,100) as symbol) from long_sequence(10)",
                "a\n" +
                        "19\n" +
                        "72\n" +
                        "90\n" +
                        "\n" +
                        "7\n" +
                        "17\n" +
                        "65\n" +
                        "32\n" +
                        "67\n" +
                        "106\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testIntToSymbolConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(601 as symbol) from long_sequence(10)",
                "a\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testIntToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "16\t16\n" +
                        "\tnull\n" +
                        "11\t11\n" +
                        "20\t20\n" +
                        "\tnull\n" +
                        "11\t11\n" +
                        "12\t12\n" +
                        "18\t18\n" +
                        "\tnull\n" +
                        "17\t17\n" +
                        "\tnull\n" +
                        "16\t16\n" +
                        "\tnull\n" +
                        "19\t19\n" +
                        "15\t15\n" +
                        "15\t15\n" +
                        "\tnull\n" +
                        "12\t12\n" +
                        "15\t15\n" +
                        "18\t18\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_int(10, 20, 2) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntToTimestamp() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp)",
                null,
                "insert into tab select cast(rnd_int(1,150,100) as timestamp) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.000019Z\n" +
                        "1970-01-01T00:00:00.000072Z\n" +
                        "1970-01-01T00:00:00.000090Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000007Z\n" +
                        "1970-01-01T00:00:00.000017Z\n" +
                        "1970-01-01T00:00:00.000065Z\n" +
                        "1970-01-01T00:00:00.000032Z\n" +
                        "1970-01-01T00:00:00.000067Z\n" +
                        "1970-01-01T00:00:00.000106Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testIntToTimestampNano() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp_ns)",
                null,
                "insert into tab select cast(rnd_int(1,150,100) as timestamp_ns) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.000000019Z\n" +
                        "1970-01-01T00:00:00.000000072Z\n" +
                        "1970-01-01T00:00:00.000000090Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000000007Z\n" +
                        "1970-01-01T00:00:00.000000017Z\n" +
                        "1970-01-01T00:00:00.000000065Z\n" +
                        "1970-01-01T00:00:00.000000032Z\n" +
                        "1970-01-01T00:00:00.000000067Z\n" +
                        "1970-01-01T00:00:00.000000106Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testIntToVarchar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(rnd_int(34,66,100) as varchar) from long_sequence(10)",
                "a\n" +
                        "40\n" +
                        "60\n" +
                        "57\n" +
                        "\n" +
                        "37\n" +
                        "38\n" +
                        "38\n" +
                        "56\n" +
                        "52\n" +
                        "49\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testIntToVarcharConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(334 as varchar) from long_sequence(10)",
                "a\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testIntToVarcharSort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "102\n" +
                        "104\n" +
                        "105\n" +
                        "106\n" +
                        "117\n" +
                        "119\n" +
                        "173\n" +
                        "198\n" +
                        "43\n" +
                        "49\n" +
                        "51\n" +
                        "56\n" +
                        "75\n",
                "select cast(a as varchar) x from tt order by x",
                "create table tt as (select rnd_int(1,200,1) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntervalToStr() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "cast\n" +
                        "('1991-11-08T09:53:57.643Z', '1995-12-22T06:21:36.636Z')\n" +
                        "('1987-02-10T05:53:36.329Z', '1992-01-23T11:30:00.830Z')\n" +
                        "('1976-02-09T03:40:32.587Z', '1979-08-18T11:00:20.247Z')\n" +
                        "('2028-07-16T17:59:01.082Z', '2029-03-05T12:49:29.174Z')\n" +
                        "('2014-08-18T15:50:20.864Z', '2016-02-17T19:58:31.466Z')\n" +
                        "('1998-08-22T09:31:10.281Z', '2003-08-09T14:59:20.394Z')\n" +
                        "('2031-04-13T21:30:51.977Z', '2033-04-24T06:22:27.339Z')\n" +
                        "('1998-08-25T13:53:59.100Z', '2000-06-21T03:19:31.403Z')\n" +
                        "('2031-02-04T05:18:53.600Z', '2033-08-21T03:18:57.217Z')\n" +
                        "('1975-05-26T13:57:40.478Z', '1977-07-09T21:00:52.129Z')\n",
                "select cast(rnd_interval() as string) from long_sequence(10)"
        ));
    }

    @Test
    public void testLong256ToBoolean() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(rnd_long256() as boolean) from long_sequence(10)",
                "a\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true,
                true,
                false
        );
        assertQueryNoLeakCheck(
                "cast\nfalse\n",
                "select cast(cast(0 as long) as boolean)",
                null,
                true,
                true,
                true
        );
        assertQueryNoLeakCheck(
                "cast\ntrue\n",
                "select cast(cast(100 as long) as boolean)",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testLong256ToBooleanConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(cast(1 as long256) as boolean) from long_sequence(10)",
                "a\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLong256ToInt() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a int)",
                null,
                "insert into tab select cast(rnd_long256() as int) from long_sequence(10)",
                "a\n" +
                        "-1148479920\n" +
                        "73575701\n" +
                        "1868723706\n" +
                        "-1436881714\n" +
                        "1569490116\n" +
                        "1530831067\n" +
                        "1125579207\n" +
                        "-85170055\n" +
                        "-1101822104\n" +
                        "-1125169127\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLong256ToLong() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long)",
                null,
                "insert into tab select cast(rnd_long256() as long) from long_sequence(10)",
                "a\n" +
                        "4689592037643856\n" +
                        "8260188555232587029\n" +
                        "-2653407051020864006\n" +
                        "7513930126251977934\n" +
                        "-6943924477733600060\n" +
                        "7953532976996720859\n" +
                        "-3985256597569472057\n" +
                        "-8671107786057422727\n" +
                        "-4485747798769957016\n" +
                        "375856366519011353\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLong256ToStr() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(rnd_long256() as string) from long_sequence(10)",
                "a\n" +
                        "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\n" +
                        "0xb5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa65572a215ba0462ad15\n" +
                        "0x322a2198864beb14797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fa\n" +
                        "0xc1e631285c1ab288c72bfc5230158059980eca62a219a0f16846d7a3aa5aecce\n" +
                        "0x4b0f595f143e5d722f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4\n" +
                        "0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\n" +
                        "0xa0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88c8b1863d4316f9c7\n" +
                        "0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879\n" +
                        "0x523eb59d99c647af9840ad8800156d26c718ab5cbb3fd261c1bf6c24be538768\n" +
                        "0x5b9832d4b5522a9474ce62a98a4516952705e02c613acfc405374f5fbcef4819\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLong256ToVarchar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(rnd_long256() as varchar) from long_sequence(10)",
                "a\n" +
                        "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\n" +
                        "0xb5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa65572a215ba0462ad15\n" +
                        "0x322a2198864beb14797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fa\n" +
                        "0xc1e631285c1ab288c72bfc5230158059980eca62a219a0f16846d7a3aa5aecce\n" +
                        "0x4b0f595f143e5d722f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4\n" +
                        "0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\n" +
                        "0xa0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88c8b1863d4316f9c7\n" +
                        "0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879\n" +
                        "0x523eb59d99c647af9840ad8800156d26c718ab5cbb3fd261c1bf6c24be538768\n" +
                        "0x5b9832d4b5522a9474ce62a98a4516952705e02c613acfc405374f5fbcef4819\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToBoolean() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(rnd_long(0,66,100) as boolean) from long_sequence(10)",
                "a\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToBooleanConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(1l as boolean) from long_sequence(10)",
                "a\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToByte() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a byte)",
                null,
                "insert into tab select cast(rnd_long(96,100, 10) as byte) from long_sequence(10)",
                "a\n" +
                        "97\n" +
                        "96\n" +
                        "0\n" +
                        "99\n" +
                        "97\n" +
                        "98\n" +
                        "100\n" +
                        "100\n" +
                        "96\n" +
                        "97\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToChar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a char)",
                null,
                "insert into tab select cast(rnd_long(34,66,100) as char) from long_sequence(10)",
                "a\n" +
                        "7\n" +
                        "0\n" +
                        "7\n" +
                        "\n" +
                        "-\n" +
                        "$\n" +
                        "\"\n" +
                        ":\n" +
                        "-\n" +
                        "=\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToDate() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a date)",
                null,
                "insert into tab select cast(rnd_long(1,150,100) as date) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.067Z\n" +
                        "1970-01-01T00:00:00.126Z\n" +
                        "1970-01-01T00:00:00.124Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.057Z\n" +
                        "1970-01-01T00:00:00.033Z\n" +
                        "1970-01-01T00:00:00.085Z\n" +
                        "1970-01-01T00:00:00.040Z\n" +
                        "1970-01-01T00:00:00.111Z\n" +
                        "1970-01-01T00:00:00.112Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToDouble() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a double)",
                null,
                "insert into tab select cast(rnd_long(1,150,100) as double) from long_sequence(10)",
                "a\n" +
                        "67.0\n" +
                        "126.0\n" +
                        "124.0\n" +
                        "null\n" +
                        "57.0\n" +
                        "33.0\n" +
                        "85.0\n" +
                        "40.0\n" +
                        "111.0\n" +
                        "112.0\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToFloat() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a float)",
                null,
                "insert into tab select cast(rnd_long(1,150,100) as float) from long_sequence(10)",
                "a\n" +
                        "67.0\n" +
                        "126.0\n" +
                        "124.0\n" +
                        "null\n" +
                        "57.0\n" +
                        "33.0\n" +
                        "85.0\n" +
                        "40.0\n" +
                        "111.0\n" +
                        "112.0\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToInt() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a int)",
                null,
                "insert into tab select cast(rnd_long(34,66,100) as int) from long_sequence(10)",
                "a\n" +
                        "55\n" +
                        "48\n" +
                        "55\n" +
                        "null\n" +
                        "45\n" +
                        "36\n" +
                        "34\n" +
                        "58\n" +
                        "45\n" +
                        "61\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToLong() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long)",
                null,
                "insert into tab select cast(rnd_long(1,150,100) as long) from long_sequence(10)",
                "a\n" +
                        "67\n" +
                        "126\n" +
                        "124\n" +
                        "null\n" +
                        "57\n" +
                        "33\n" +
                        "85\n" +
                        "40\n" +
                        "111\n" +
                        "112\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToLong256() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long256)",
                null,
                "insert into tab select cast(rnd_long(34,66,100) as long256) from long_sequence(10)",
                "a\n" +
                        "0x37\n" +
                        "0x30\n" +
                        "0x37\n" +
                        "\n" +
                        "0x2d\n" +
                        "0x24\n" +
                        "0x22\n" +
                        "0x3a\n" +
                        "0x2d\n" +
                        "0x3d\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToLong256Sort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "0x08\n" +
                        "0x11\n" +
                        "0x1e\n" +
                        "0x34\n" +
                        "0x3d\n" +
                        "0x4d\n" +
                        "0x57\n" +
                        "0x63\n" +
                        "0x80\n" +
                        "0x89\n" +
                        "0xa7\n" +
                        "0xc0\n" +
                        "0xc7\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select rnd_long(1,200,1) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testLongToShort() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a short)",
                null,
                "insert into tab select cast(rnd_long(23,56,100) as short) from long_sequence(10)",
                "a\n" +
                        "31\n" +
                        "26\n" +
                        "38\n" +
                        "0\n" +
                        "47\n" +
                        "41\n" +
                        "53\n" +
                        "46\n" +
                        "51\n" +
                        "46\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToStr() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(rnd_long(34,66,100) as string) from long_sequence(10)",
                "a\n" +
                        "55\n" +
                        "48\n" +
                        "55\n" +
                        "\n" +
                        "45\n" +
                        "36\n" +
                        "34\n" +
                        "58\n" +
                        "45\n" +
                        "61\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToStrConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(334l as string) from long_sequence(10)",
                "a\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToStrSort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "128\n" +
                        "137\n" +
                        "167\n" +
                        "17\n" +
                        "192\n" +
                        "199\n" +
                        "30\n" +
                        "52\n" +
                        "61\n" +
                        "77\n" +
                        "8\n" +
                        "87\n" +
                        "99\n",
                "select cast(a as string) x from tt order by x",
                "create table tt as (select rnd_long(1,200,1) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testLongToSymbol() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(rnd_long(1,150,100) as symbol) from long_sequence(10)",
                "a\n" +
                        "67\n" +
                        "126\n" +
                        "124\n" +
                        "\n" +
                        "57\n" +
                        "33\n" +
                        "85\n" +
                        "40\n" +
                        "111\n" +
                        "112\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToSymbolConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(601l as symbol) from long_sequence(10)",
                "a\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "20\t20\n" +
                        "\tnull\n" +
                        "11\t11\n" +
                        "13\t13\n" +
                        "\tnull\n" +
                        "15\t15\n" +
                        "19\t19\n" +
                        "17\t17\n" +
                        "\tnull\n" +
                        "10\t10\n" +
                        "\tnull\n" +
                        "17\t17\n" +
                        "\tnull\n" +
                        "17\t17\n" +
                        "18\t18\n" +
                        "18\t18\n" +
                        "\tnull\n" +
                        "12\t12\n" +
                        "11\t11\n" +
                        "15\t15\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_long(10, 20, 2) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testLongToTimestamp() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp)",
                null,
                "insert into tab select cast(rnd_long(1,150,100) as timestamp) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.000067Z\n" +
                        "1970-01-01T00:00:00.000126Z\n" +
                        "1970-01-01T00:00:00.000124Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000057Z\n" +
                        "1970-01-01T00:00:00.000033Z\n" +
                        "1970-01-01T00:00:00.000085Z\n" +
                        "1970-01-01T00:00:00.000040Z\n" +
                        "1970-01-01T00:00:00.000111Z\n" +
                        "1970-01-01T00:00:00.000112Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToTimestampNano() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp_ns)",
                null,
                "insert into tab select cast(rnd_long(1,150,100) as timestamp_ns) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.000000067Z\n" +
                        "1970-01-01T00:00:00.000000126Z\n" +
                        "1970-01-01T00:00:00.000000124Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000000057Z\n" +
                        "1970-01-01T00:00:00.000000033Z\n" +
                        "1970-01-01T00:00:00.000000085Z\n" +
                        "1970-01-01T00:00:00.000000040Z\n" +
                        "1970-01-01T00:00:00.000000111Z\n" +
                        "1970-01-01T00:00:00.000000112Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToVarchar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(rnd_long(34,66,100) as varchar) from long_sequence(10)",
                "a\n" +
                        "55\n" +
                        "48\n" +
                        "55\n" +
                        "\n" +
                        "45\n" +
                        "36\n" +
                        "34\n" +
                        "58\n" +
                        "45\n" +
                        "61\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToVarcharConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(334l as varchar) from long_sequence(10)",
                "a\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToVarcharSort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "128\n" +
                        "137\n" +
                        "167\n" +
                        "17\n" +
                        "192\n" +
                        "199\n" +
                        "30\n" +
                        "52\n" +
                        "61\n" +
                        "77\n" +
                        "8\n" +
                        "87\n" +
                        "99\n",
                "select cast(a as varchar) x from tt order by x",
                "create table tt as (select rnd_long(1,200,1) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testNullToBinary() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a binary)",
                null,
                "insert into tab select cast(null as binary) from long_sequence(10)",
                "a\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testShortToBoolean() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(rnd_short() as boolean) from long_sequence(10)",
                "a\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true,
                true,
                false
        );

        assertQueryNoLeakCheck(
                "cast\nfalse\n",
                "select cast(cast(0 as short) as boolean)",
                null,
                true,
                true,
                true
        );
        assertQueryNoLeakCheck(
                "cast\ntrue\n",
                "select cast(cast(256 as short) as boolean)",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testShortToBooleanTrue() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(cast(1 as short) as boolean) from long_sequence(10)",
                "a\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testShortToByte() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a byte)",
                null,
                "insert into tab select cast(rnd_short() as byte) from long_sequence(10)",
                "a\n" +
                        "80\n" +
                        "-18\n" +
                        "65\n" +
                        "29\n" +
                        "21\n" +
                        "85\n" +
                        "-118\n" +
                        "23\n" +
                        "-6\n" +
                        "-40\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testShortToChar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a char)",
                null,
                "insert into tab select cast(rnd_short(34,66) as char) from long_sequence(10)",
                "a\n" +
                        "?\n" +
                        "A\n" +
                        "&\n" +
                        ";\n" +
                        "*\n" +
                        "6\n" +
                        ".\n" +
                        "=\n" +
                        ")\n" +
                        "<\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testShortToDate() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a date)",
                null,
                "insert into tab select cast(rnd_short() as date) from long_sequence(10)",
                "a\n" +
                        "1969-12-31T23:59:32.944Z\n" +
                        "1970-01-01T00:00:24.814Z\n" +
                        "1969-12-31T23:59:48.545Z\n" +
                        "1969-12-31T23:59:46.973Z\n" +
                        "1969-12-31T23:59:38.773Z\n" +
                        "1969-12-31T23:59:37.045Z\n" +
                        "1969-12-31T23:59:58.602Z\n" +
                        "1970-01-01T00:00:21.015Z\n" +
                        "1970-01-01T00:00:30.202Z\n" +
                        "1969-12-31T23:59:40.504Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testShortToDouble() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a double)",
                null,
                "insert into tab select cast(rnd_short() as double) from long_sequence(10)",
                "a\n" +
                        "-27056.0\n" +
                        "24814.0\n" +
                        "-11455.0\n" +
                        "-13027.0\n" +
                        "-21227.0\n" +
                        "-22955.0\n" +
                        "-1398.0\n" +
                        "21015.0\n" +
                        "30202.0\n" +
                        "-19496.0\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testShortToFloat() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a float)",
                null,
                "insert into tab select cast(rnd_short() as float) from long_sequence(10)",
                "a\n" +
                        "-27056.0\n" +
                        "24814.0\n" +
                        "-11455.0\n" +
                        "-13027.0\n" +
                        "-21227.0\n" +
                        "-22955.0\n" +
                        "-1398.0\n" +
                        "21015.0\n" +
                        "30202.0\n" +
                        "-19496.0\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testShortToInt() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a int)",
                null,
                "insert into tab select cast(rnd_short() as int) from long_sequence(10)",
                "a\n" +
                        "-27056\n" +
                        "24814\n" +
                        "-11455\n" +
                        "-13027\n" +
                        "-21227\n" +
                        "-22955\n" +
                        "-1398\n" +
                        "21015\n" +
                        "30202\n" +
                        "-19496\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testShortToLong() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long)",
                null,
                "insert into tab select cast(rnd_short() as long) from long_sequence(10)",
                "a\n" +
                        "-27056\n" +
                        "24814\n" +
                        "-11455\n" +
                        "-13027\n" +
                        "-21227\n" +
                        "-22955\n" +
                        "-1398\n" +
                        "21015\n" +
                        "30202\n" +
                        "-19496\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testShortToLong256() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long256)",
                null,
                "insert into tab select cast(rnd_short() as long256) from long_sequence(10)",
                "a\n" +
                        "0xffffffffffff9650\n" +
                        "0x60ee\n" +
                        "0xffffffffffffd341\n" +
                        "0xffffffffffffcd1d\n" +
                        "0xffffffffffffad15\n" +
                        "0xffffffffffffa655\n" +
                        "0xfffffffffffffa8a\n" +
                        "0x5217\n" +
                        "0x75fa\n" +
                        "0xffffffffffffb3d8\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testShortToLong256Sort() throws Exception {
        assertQuery(
                "x\n" +
                        "0x1e3b\n" +
                        "0x2d91\n" +
                        "0x5217\n" +
                        "0x5d72\n" +
                        "0x60ee\n" +
                        "0x75fa\n" +
                        "0xffffffffffff8059\n" +
                        "0xffffffffffff84c4\n" +
                        "0xffffffffffff9650\n" +
                        "0xffffffffffffa0f1\n" +
                        "0xffffffffffffa655\n" +
                        "0xffffffffffffad15\n" +
                        "0xffffffffffffb288\n" +
                        "0xffffffffffffb3d8\n" +
                        "0xffffffffffffc6cc\n" +
                        "0xffffffffffffcd1d\n" +
                        "0xffffffffffffd341\n" +
                        "0xffffffffffffeb14\n" +
                        "0xffffffffffffecce\n" +
                        "0xfffffffffffffa8a\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select rnd_short() a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testShortToShort() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a short)",
                null,
                "insert into tab select cast(rnd_short() as short) from long_sequence(10)",
                "a\n" +
                        "-27056\n" +
                        "24814\n" +
                        "-11455\n" +
                        "-13027\n" +
                        "-21227\n" +
                        "-22955\n" +
                        "-1398\n" +
                        "21015\n" +
                        "30202\n" +
                        "-19496\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testShortToStr() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(rnd_short() as string) from long_sequence(10)",
                "a\n" +
                        "-27056\n" +
                        "24814\n" +
                        "-11455\n" +
                        "-13027\n" +
                        "-21227\n" +
                        "-22955\n" +
                        "-1398\n" +
                        "21015\n" +
                        "30202\n" +
                        "-19496\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testShortToStrConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(cast(10 as short) as string) from long_sequence(10)",
                "a\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testShortToStrSort() throws Exception {
        assertQuery(
                "x\n" +
                        "-11455\n" +
                        "-13027\n" +
                        "-1398\n" +
                        "-14644\n" +
                        "-19496\n" +
                        "-19832\n" +
                        "-21227\n" +
                        "-22955\n" +
                        "-24335\n" +
                        "-27056\n" +
                        "-31548\n" +
                        "-32679\n" +
                        "-4914\n" +
                        "-5356\n" +
                        "11665\n" +
                        "21015\n" +
                        "23922\n" +
                        "24814\n" +
                        "30202\n" +
                        "7739\n",
                "select cast(a as string) x from tt order by x",
                "create table tt as (select rnd_short() a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testShortToSymbol() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(rnd_short() as symbol) from long_sequence(10)",
                "a\n" +
                        "-27056\n" +
                        "24814\n" +
                        "-11455\n" +
                        "-13027\n" +
                        "-21227\n" +
                        "-22955\n" +
                        "-1398\n" +
                        "21015\n" +
                        "30202\n" +
                        "-19496\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testShortToSymbolConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(cast(99 as short) as symbol) from long_sequence(10)",
                "a\n" +
                        "99\n" +
                        "99\n" +
                        "99\n" +
                        "99\n" +
                        "99\n" +
                        "99\n" +
                        "99\n" +
                        "99\n" +
                        "99\n" +
                        "99\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testShortToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "-27056\t-27056\n" +
                        "24814\t24814\n" +
                        "-11455\t-11455\n" +
                        "-13027\t-13027\n" +
                        "-21227\t-21227\n" +
                        "-22955\t-22955\n" +
                        "-1398\t-1398\n" +
                        "21015\t21015\n" +
                        "30202\t30202\n" +
                        "-19496\t-19496\n" +
                        "-14644\t-14644\n" +
                        "-5356\t-5356\n" +
                        "-4914\t-4914\n" +
                        "-24335\t-24335\n" +
                        "-32679\t-32679\n" +
                        "-19832\t-19832\n" +
                        "-31548\t-31548\n" +
                        "11665\t11665\n" +
                        "7739\t7739\n" +
                        "23922\t23922\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_short() a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testShortToTimestamp() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp)",
                null,
                "insert into tab select cast(rnd_short() as timestamp) from long_sequence(10)",
                "a\n" +
                        "1969-12-31T23:59:59.972944Z\n" +
                        "1970-01-01T00:00:00.024814Z\n" +
                        "1969-12-31T23:59:59.988545Z\n" +
                        "1969-12-31T23:59:59.986973Z\n" +
                        "1969-12-31T23:59:59.978773Z\n" +
                        "1969-12-31T23:59:59.977045Z\n" +
                        "1969-12-31T23:59:59.998602Z\n" +
                        "1970-01-01T00:00:00.021015Z\n" +
                        "1970-01-01T00:00:00.030202Z\n" +
                        "1969-12-31T23:59:59.980504Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testShortToTimestampNano() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp_ns)",
                null,
                "insert into tab select cast(rnd_short() as timestamp_ns) from long_sequence(10)",
                "a\n" +
                        "1969-12-31T23:59:59.999972944Z\n" +
                        "1970-01-01T00:00:00.000024814Z\n" +
                        "1969-12-31T23:59:59.999988545Z\n" +
                        "1969-12-31T23:59:59.999986973Z\n" +
                        "1969-12-31T23:59:59.999978773Z\n" +
                        "1969-12-31T23:59:59.999977045Z\n" +
                        "1969-12-31T23:59:59.999998602Z\n" +
                        "1970-01-01T00:00:00.000021015Z\n" +
                        "1970-01-01T00:00:00.000030202Z\n" +
                        "1969-12-31T23:59:59.999980504Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testShortToVarchar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(rnd_short() as varchar) from long_sequence(10)",
                "a\n" +
                        "-27056\n" +
                        "24814\n" +
                        "-11455\n" +
                        "-13027\n" +
                        "-21227\n" +
                        "-22955\n" +
                        "-1398\n" +
                        "21015\n" +
                        "30202\n" +
                        "-19496\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testShortToVarcharConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(cast(10 as short) as varchar) from long_sequence(10)",
                "a\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testShortToVarcharSort() throws Exception {
        assertQuery(
                "x\n" +
                        "-11455\n" +
                        "-13027\n" +
                        "-1398\n" +
                        "-14644\n" +
                        "-19496\n" +
                        "-19832\n" +
                        "-21227\n" +
                        "-22955\n" +
                        "-24335\n" +
                        "-27056\n" +
                        "-31548\n" +
                        "-32679\n" +
                        "-4914\n" +
                        "-5356\n" +
                        "11665\n" +
                        "21015\n" +
                        "23922\n" +
                        "24814\n" +
                        "30202\n" +
                        "7739\n",
                "select cast(a as varchar) x from tt order by x",
                "create table tt as (select rnd_short() a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testStrConstZeroToChar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a char)",
                null,
                "insert into tab select cast('' as char) from long_sequence(10)",
                "a\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testStrToBoolean() throws Exception {
        assertQuery(
                "boolean\n" +
                        "false\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "false\n" +
                        "true\n" +
                        "false\n" +
                        "false\n",
                "select boolean from tab",
                "create table tab as (" +
                        "select cast(rnd_str('28', 'TRuE', '', null, 'false', 'true') as boolean) boolean from long_sequence(10)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testStrToByte() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a byte)",
                null,
                "insert into tab select cast(rnd_str('23','56','100', null) as byte) from long_sequence(10)",
                "a\n" +
                        "23\n" +
                        "100\n" +
                        "56\n" +
                        "0\n" +
                        "56\n" +
                        "0\n" +
                        "100\n" +
                        "0\n" +
                        "100\n" +
                        "23\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testStrToChar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a char)",
                null,
                "insert into tab select cast(rnd_str('A', 'BC', 'K', null) as char) from long_sequence(10)",
                "a\n" +
                        "A\n" +
                        "K\n" +
                        "B\n" +
                        "\n" +
                        "B\n" +
                        "\n" +
                        "K\n" +
                        "\n" +
                        "K\n" +
                        "A\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testStrToDate() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a date)",
                null,
                "insert into tab select cast(rnd_str('2019-03-11T10:20:33.123Z', '2019-03-24T14:20:33.123Z', 'ABC', null) as date) from long_sequence(10)",
                "a\n" +
                        "2019-03-11T10:20:33.123Z\n" +
                        "\n" +
                        "2019-03-24T14:20:33.123Z\n" +
                        "\n" +
                        "2019-03-24T14:20:33.123Z\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "2019-03-11T10:20:33.123Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testStrToDouble() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a double)",
                null,
                "insert into tab select cast(rnd_str('1234.556', '988.223', 'abc', null) as double) from long_sequence(15)",
                "a\n" +
                        "1234.556\n" +
                        "null\n" +
                        "988.223\n" +
                        "null\n" +
                        "988.223\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "1234.556\n" +
                        "1234.556\n" +
                        "1234.556\n" +
                        "null\n" +
                        "null\n" +
                        "988.223\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testStrToDouble_doubleParserTakingSlowPath() throws Exception {
        assertQuery(
                "a\n",
                "select cast(a as double) as a from tab",
                "create table tab (a string)",
                null,
                "insert into tab values ('4.9E-324')",
                "a\n" +
                        "4.9E-324\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testStrToFloat() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a float)",
                null,
                "insert into tab select cast(rnd_str('9.23', '4.15', 'xyz', null) as float) from long_sequence(15)",
                "a\n" +
                        "9.23\n" +
                        "null\n" +
                        "4.15\n" +
                        "null\n" +
                        "4.15\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "9.23\n" +
                        "9.23\n" +
                        "9.23\n" +
                        "null\n" +
                        "null\n" +
                        "4.15\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testStrToInt() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a int)",
                null,
                "insert into tab select cast(rnd_str('90092', '2203', null) as int) from long_sequence(10)",
                "a\n" +
                        "90092\n" +
                        "90092\n" +
                        "2203\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "2203\n" +
                        "90092\n" +
                        "2203\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testStrToLong() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long)",
                null,
                "insert into tab select cast(rnd_str('2334l', '99002', null) as long) from long_sequence(10)",
                "a\n" +
                        "2334\n" +
                        "2334\n" +
                        "99002\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "99002\n" +
                        "2334\n" +
                        "99002\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testStrToLong256Sort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "0x123455\n" +
                        "0x123455\n" +
                        "0x123455\n" +
                        "0x123455\n" +
                        "0x123455\n" +
                        "0x123455\n" +
                        "0x8802ff90\n" +
                        "0x8802ff90\n" +
                        "0x8802ff90\n" +
                        "0x8802ff90\n" +
                        "0x8802ff90\n" +
                        "0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\n" +
                        "0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\n" +
                        "0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\n" +
                        "0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select rnd_str('0x00123455', '0x8802ff90', null, '0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926') a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testStrToShort() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a short)",
                null,
                "insert into tab select cast(rnd_str('23','56','100', null, 'y') as short) from long_sequence(10)",
                "a\n" +
                        "23\n" +
                        "0\n" +
                        "0\n" +
                        "56\n" +
                        "56\n" +
                        "0\n" +
                        "100\n" +
                        "56\n" +
                        "56\n" +
                        "0\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testStrToSymbolConst() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "abc\tJWCP\n" +
                        "abc\t\n" +
                        "abc\tYRXP\n" +
                        "abc\tNRXGZ\n" +
                        "abc\tUXIBB\n" +
                        "abc\tPGWFF\n" +
                        "abc\tDEYYQ\n" +
                        "abc\tBHFOW\n" +
                        "abc\tDXYS\n" +
                        "abc\tOUOJ\n" +
                        "abc\tRUED\n" +
                        "abc\tQULO\n" +
                        "abc\tGETJ\n" +
                        "abc\tZSRYR\n" +
                        "abc\tVTMHG\n" +
                        "abc\tZZVD\n" +
                        "abc\tMYICC\n" +
                        "abc\tOUIC\n" +
                        "abc\tKGHV\n" +
                        "abc\tSDOTS\n",
                "select cast('abc' as symbol) b, a from tab",
                "create table tab as (select rnd_str(4,5,100) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testStrToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "JWCP\tJWCP\n" +
                        "\t\n" +
                        "YRXP\tYRXP\n" +
                        "NRXGZ\tNRXGZ\n" +
                        "UXIBB\tUXIBB\n" +
                        "PGWFF\tPGWFF\n" +
                        "DEYYQ\tDEYYQ\n" +
                        "BHFOW\tBHFOW\n" +
                        "DXYS\tDXYS\n" +
                        "OUOJ\tOUOJ\n" +
                        "RUED\tRUED\n" +
                        "QULO\tQULO\n" +
                        "GETJ\tGETJ\n" +
                        "ZSRYR\tZSRYR\n" +
                        "VTMHG\tVTMHG\n" +
                        "ZZVD\tZZVD\n" +
                        "MYICC\tMYICC\n" +
                        "OUIC\tOUIC\n" +
                        "KGHV\tKGHV\n" +
                        "SDOTS\tSDOTS\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_str(4,5,100) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testStrToSymbolSort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "a\n" +
                        "a\n" +
                        "a\n" +
                        "a\n" +
                        "a\n" +
                        "a\n" +
                        "b\n" +
                        "b\n" +
                        "b\n" +
                        "b\n" +
                        "b\n" +
                        "c\n" +
                        "c\n" +
                        "c\n" +
                        "c\n" +
                        "c\n",
                "select cast(a as symbol) x from tt order by x",
                "create table tt as (select rnd_str('a','b','c', null) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testStrToTimestamp() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp)",
                null,
                "insert into tab select cast(rnd_str('2019-03-11T10:20:33.123897Z', '2019-03-24T14:20:33.123551Z', 'ABC', null) as timestamp) from long_sequence(10)",
                "a\n" +
                        "2019-03-11T10:20:33.123897Z\n" +
                        "\n" +
                        "2019-03-24T14:20:33.123551Z\n" +
                        "\n" +
                        "2019-03-24T14:20:33.123551Z\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "2019-03-11T10:20:33.123897Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testStrToTimestampNano() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp_ns)",
                null,
                "insert into tab select cast(rnd_str('2019-03-11T10:20:33.123897123Z', '2019-03-24T14:20:33.123551098Z', 'ABC', null) as timestamp_ns) from long_sequence(10)",
                "a\n" +
                        "2019-03-11T10:20:33.123897123Z\n" +
                        "\n" +
                        "2019-03-24T14:20:33.123551098Z\n" +
                        "\n" +
                        "2019-03-24T14:20:33.123551098Z\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "2019-03-11T10:20:33.123897123Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testStrToVarchar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(rnd_str('раз', 'два', 'три', null) as varchar) from long_sequence(10)",
                "a\n" +
                        "раз\n" +
                        "три\n" +
                        "два\n" +
                        "\n" +
                        "два\n" +
                        "\n" +
                        "три\n" +
                        "\n" +
                        "три\n" +
                        "раз\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testStringRegClass() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "2615\tpg_namespace\n" +
                        "2615\tpg_namespace\n" +
                        "1259\tpg_class\n" +
                        "1259\tpg_class\n" +
                        "1259\tpg_class\n",
                "select cast(a as string)::regclass b, a from tab",
                "create table tab as (select rnd_symbol('pg_namespace', 'pg_class') a from long_sequence(5))",
                null,
                true,
                true
        );
    }

    @Test
    public void testSymbolNocacheToLong256Sort() throws Exception {
        assertQuery(
                "x\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt (a symbol nocache)",
                null,
                "insert into tt select rnd_symbol('0x00123455', '0x8802ff90', null, '0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926', '0x880') a from long_sequence(20)",
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "0x123455\n" +
                        "0x123455\n" +
                        "0x8802ff90\n" +
                        "0x8802ff90\n" +
                        "0x8802ff90\n" +
                        "0x8802ff90\n" +
                        "0x8802ff90\n" +
                        "0x8802ff90\n" +
                        "0x8802ff90\n" +
                        "0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\n" +
                        "0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\n" +
                        "0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\n" +
                        "0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolNocacheToStrSort() throws Exception {
        assertQuery(
                "x\n",
                "select cast(a as string) x from tt order by x",
                "create table tt (a symbol nocache)",
                null,
                "insert into tt select rnd_symbol('1','200','221', null) from long_sequence(20)",
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "200\n" +
                        "200\n" +
                        "200\n" +
                        "200\n" +
                        "200\n" +
                        "221\n" +
                        "221\n" +
                        "221\n" +
                        "221\n" +
                        "221\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolToByte() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a byte)",
                null,
                "insert into tab select cast(rnd_symbol('23','56','100', null) as byte) from long_sequence(10)",
                "a\n" +
                        "23\n" +
                        "100\n" +
                        "56\n" +
                        "0\n" +
                        "56\n" +
                        "0\n" +
                        "100\n" +
                        "0\n" +
                        "100\n" +
                        "23\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolToChar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a char)",
                null,
                "insert into tab select cast(rnd_symbol('A', 'BC', 'K', null) as char) from long_sequence(10)",
                "a\n" +
                        "A\n" +
                        "K\n" +
                        "B\n" +
                        "\n" +
                        "B\n" +
                        "\n" +
                        "K\n" +
                        "\n" +
                        "K\n" +
                        "A\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolToDate() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a date)",
                null,
                "insert into tab select cast(rnd_symbol('2019-03-11T10:20:33.123Z', '2019-03-24T14:20:33.123Z', 'ABC', null) as date) from long_sequence(10)",
                "a\n" +
                        "2019-03-11T10:20:33.123Z\n" +
                        "\n" +
                        "2019-03-24T14:20:33.123Z\n" +
                        "\n" +
                        "2019-03-24T14:20:33.123Z\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "2019-03-11T10:20:33.123Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolToDouble() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a double)",
                null,
                "insert into tab select cast(rnd_symbol('1234.556', '988.223', 'abc', null) as double) from long_sequence(15)",
                "a\n" +
                        "1234.556\n" +
                        "null\n" +
                        "988.223\n" +
                        "null\n" +
                        "988.223\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "1234.556\n" +
                        "1234.556\n" +
                        "1234.556\n" +
                        "null\n" +
                        "null\n" +
                        "988.223\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolToFloat() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a float)",
                null,
                "insert into tab select cast(rnd_symbol('9.23', '4.15', 'xyz', null) as float) from long_sequence(15)",
                "a\n" +
                        "9.23\n" +
                        "null\n" +
                        "4.15\n" +
                        "null\n" +
                        "4.15\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "9.23\n" +
                        "9.23\n" +
                        "9.23\n" +
                        "null\n" +
                        "null\n" +
                        "4.15\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolToInt() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a int)",
                null,
                "insert into tab select cast(rnd_symbol('100', '200', 'abc', null) as int) from long_sequence(10)",
                "a\n" +
                        "100\n" +
                        "null\n" +
                        "200\n" +
                        "null\n" +
                        "200\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "100\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolToLong() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long)",
                null,
                "insert into tab select cast(rnd_symbol('100', '200', 'abc', null) as long) from long_sequence(10)",
                "a\n" +
                        "100\n" +
                        "null\n" +
                        "200\n" +
                        "null\n" +
                        "200\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "100\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolToLong256Sort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "0x123455\n" +
                        "0x123455\n" +
                        "0x123455\n" +
                        "0x123455\n" +
                        "0x123455\n" +
                        "0x123455\n" +
                        "0x8802ff90\n" +
                        "0x8802ff90\n" +
                        "0x8802ff90\n" +
                        "0x8802ff90\n" +
                        "0x8802ff90\n" +
                        "0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\n" +
                        "0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\n" +
                        "0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\n" +
                        "0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select rnd_symbol('0x00123455', '0x8802ff90', null, '0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926') a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testSymbolToShort() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a short)",
                null,
                "insert into tab select cast(rnd_symbol('23','56','100', null, 'y') as short) from long_sequence(10)",
                "a\n" +
                        "23\n" +
                        "0\n" +
                        "0\n" +
                        "56\n" +
                        "56\n" +
                        "0\n" +
                        "100\n" +
                        "56\n" +
                        "56\n" +
                        "0\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolToStr() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(rnd_symbol('abc', '135', null, 'xxp') as string) from long_sequence(10)",
                "a\n" +
                        "abc\n" +
                        "\n" +
                        "135\n" +
                        "xxp\n" +
                        "135\n" +
                        "xxp\n" +
                        "\n" +
                        "xxp\n" +
                        "\n" +
                        "abc\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolToStrConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(cast('334' as symbol) as string) from long_sequence(10)",
                "a\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolToStrSort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "200\n" +
                        "200\n" +
                        "200\n" +
                        "200\n" +
                        "200\n" +
                        "221\n" +
                        "221\n" +
                        "221\n" +
                        "221\n" +
                        "221\n",
                "select cast(a as string) x from tt order by x",
                "create table tt as (select rnd_symbol('1','200','221', null) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testSymbolToSymbol() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(rnd_symbol('23','56','100', null, 'y') as symbol) from long_sequence(10)",
                "a\n" +
                        "23\n" +
                        "\n" +
                        "\n" +
                        "56\n" +
                        "56\n" +
                        "y\n" +
                        "100\n" +
                        "56\n" +
                        "56\n" +
                        "\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolToTimestamp() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp)",
                null,
                "insert into tab select cast(rnd_symbol('2019-03-11T10:20:33.123897Z', '2019-03-24T14:20:33.123551Z', 'ABC', null) as timestamp) from long_sequence(10)",
                "a\n" +
                        "2019-03-11T10:20:33.123897Z\n" +
                        "\n" +
                        "2019-03-24T14:20:33.123551Z\n" +
                        "\n" +
                        "2019-03-24T14:20:33.123551Z\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "2019-03-11T10:20:33.123897Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolToTimestampNano() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp_ns)",
                null,
                "insert into tab select cast(rnd_symbol('2019-03-11T10:20:33.123897098Z', '2019-03-24T14:20:33.123551123Z', 'ABC', null) as timestamp_ns) from long_sequence(10)",
                "a\n" +
                        "2019-03-11T10:20:33.123897098Z\n" +
                        "\n" +
                        "2019-03-24T14:20:33.123551123Z\n" +
                        "\n" +
                        "2019-03-24T14:20:33.123551123Z\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "2019-03-11T10:20:33.123897098Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolToVarchar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(rnd_symbol('abc', '135', null, 'xxp') as varchar) from long_sequence(10)",
                "a\n" +
                        "abc\n" +
                        "\n" +
                        "135\n" +
                        "xxp\n" +
                        "135\n" +
                        "xxp\n" +
                        "\n" +
                        "xxp\n" +
                        "\n" +
                        "abc\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolToVarcharConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(cast('334' as symbol) as varchar) from long_sequence(10)",
                "a\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolToVarcharSort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "200\n" +
                        "200\n" +
                        "200\n" +
                        "200\n" +
                        "200\n" +
                        "221\n" +
                        "221\n" +
                        "221\n" +
                        "221\n" +
                        "221\n",
                "select cast(a as varchar) x from tt order by x",
                "create table tt as (select rnd_symbol('1','200','221', null) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testTimestampNanoToBoolean() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(rnd_timestamp(10000000000L, 100000000000L, 2)::timestamp_ns as boolean) from long_sequence(10)",
                "a\n" +
                        "true\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "false\n" +
                        "true\n",
                true,
                true,
                false
        );

        assertQueryNoLeakCheck(
                "cast\nfalse\n",
                "select cast(cast(0L as timestamp_ns) as boolean)",
                null,
                true,
                true,
                true
        );
        assertQueryNoLeakCheck(
                "cast\ntrue\n",
                "select cast(cast(5L as timestamp_ns) as boolean)",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testTimestampNanoToStrConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(cast(334l as timestamp_ns) as string) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.000000334Z\n" +
                        "1970-01-01T00:00:00.000000334Z\n" +
                        "1970-01-01T00:00:00.000000334Z\n" +
                        "1970-01-01T00:00:00.000000334Z\n" +
                        "1970-01-01T00:00:00.000000334Z\n" +
                        "1970-01-01T00:00:00.000000334Z\n" +
                        "1970-01-01T00:00:00.000000334Z\n" +
                        "1970-01-01T00:00:00.000000334Z\n" +
                        "1970-01-01T00:00:00.000000334Z\n" +
                        "1970-01-01T00:00:00.000000334Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampNsToBooleanConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(cast(1 as timestamp_ns) as boolean) from long_sequence(10)",
                "a\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampNsToByte() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a byte)",
                null,
                "insert into tab select cast(rnd_timestamp(96::timestamp_ns,100::timestamp_ns, 2) as byte) from long_sequence(10)",
                "a\n" +
                        "97\n" +
                        "0\n" +
                        "100\n" +
                        "99\n" +
                        "0\n" +
                        "97\n" +
                        "97\n" +
                        "98\n" +
                        "0\n" +
                        "96\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampNsToChar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a char)",
                null,
                "insert into tab select cast(rnd_timestamp(34::timestamp_ns,66::timestamp_ns,2) as char) from long_sequence(10)",
                "a\n" +
                        "7\n" +
                        "\n" +
                        "9\n" +
                        "0\n" +
                        "\n" +
                        "2\n" +
                        "+\n" +
                        "4\n" +
                        "\n" +
                        "-\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampNsToDate() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a date)",
                null,
                "insert into tab select cast(rnd_timestamp(1000000,10000000,2)::timestamp_ns as date) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:05.437Z\n" +
                        "\n" +
                        "1970-01-01T00:00:05.960Z\n" +
                        "1970-01-01T00:00:08.779Z\n" +
                        "\n" +
                        "1970-01-01T00:00:05.513Z\n" +
                        "1970-01-01T00:00:05.535Z\n" +
                        "1970-01-01T00:00:09.498Z\n" +
                        "\n" +
                        "1970-01-01T00:00:02.521Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampNsToDouble() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a double)",
                null,
                "insert into tab select cast(rnd_timestamp(1,150,2)::timestamp_ns as double) from long_sequence(10)",
                "a\n" +
                        "67000.0\n" +
                        "null\n" +
                        "30000.0\n" +
                        "99000.0\n" +
                        "null\n" +
                        "137000.0\n" +
                        "127000.0\n" +
                        "58000.0\n" +
                        "null\n" +
                        "111000.0\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampNsToFloat() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a float)",
                null,
                "insert into tab select cast(rnd_timestamp(1,150,2)::timestamp_ns as float) from long_sequence(10)",
                "a\n" +
                        "67000.0\n" +
                        "null\n" +
                        "30000.0\n" +
                        "99000.0\n" +
                        "null\n" +
                        "137000.0\n" +
                        "127000.0\n" +
                        "58000.0\n" +
                        "null\n" +
                        "111000.0\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampNsToInt() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a int)",
                null,
                "insert into tab select cast(rnd_timestamp(1000000L, 1000000000L, 2)::timestamp as int) from long_sequence(10)",
                "a\n" +
                        "985257636\n" +
                        "null\n" +
                        "968130026\n" +
                        "555619965\n" +
                        "null\n" +
                        "712286238\n" +
                        "215755333\n" +
                        "720037886\n" +
                        "null\n" +
                        "129724714\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampNsToLong() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long)",
                null,
                "insert into tab select cast(cast(rnd_long(1,15000000,100) as timestamp_ns) as long) from long_sequence(10)",
                "a\n" +
                        "13992367\n" +
                        "4501476\n" +
                        "2660374\n" +
                        "null\n" +
                        "5864007\n" +
                        "10281933\n" +
                        "6977935\n" +
                        "9100840\n" +
                        "8600061\n" +
                        "478012\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampNsToLong256Sort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "0x08\n" +
                        "0x11\n" +
                        "0x1e\n" +
                        "0x34\n" +
                        "0x3d\n" +
                        "0x4d\n" +
                        "0x57\n" +
                        "0x63\n" +
                        "0x80\n" +
                        "0x89\n" +
                        "0xa7\n" +
                        "0xc0\n" +
                        "0xc7\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select rnd_timestamp(1::timestamp_ns,200::timestamp_ns,1) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testTimestampNsToShort() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a short)",
                null,
                "insert into tab select cast(rnd_timestamp(23::timestamp_ns,56::timestamp_ns,2) as short) from long_sequence(10)",
                "a\n" +
                        "31\n" +
                        "0\n" +
                        "54\n" +
                        "23\n" +
                        "0\n" +
                        "29\n" +
                        "33\n" +
                        "24\n" +
                        "0\n" +
                        "51\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampNsToStr() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(rnd_timestamp(34,66,100)::timestamp_ns as string) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.000055000Z\n" +
                        "1970-01-01T00:00:00.000048000Z\n" +
                        "1970-01-01T00:00:00.000055000Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000045000Z\n" +
                        "1970-01-01T00:00:00.000036000Z\n" +
                        "1970-01-01T00:00:00.000034000Z\n" +
                        "1970-01-01T00:00:00.000058000Z\n" +
                        "1970-01-01T00:00:00.000045000Z\n" +
                        "1970-01-01T00:00:00.000061000Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampNsToStrSort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000008000Z\n" +
                        "1970-01-01T00:00:00.000017000Z\n" +
                        "1970-01-01T00:00:00.000030000Z\n" +
                        "1970-01-01T00:00:00.000052000Z\n" +
                        "1970-01-01T00:00:00.000061000Z\n" +
                        "1970-01-01T00:00:00.000077000Z\n" +
                        "1970-01-01T00:00:00.000087000Z\n" +
                        "1970-01-01T00:00:00.000099000Z\n" +
                        "1970-01-01T00:00:00.000128000Z\n" +
                        "1970-01-01T00:00:00.000137000Z\n" +
                        "1970-01-01T00:00:00.000167000Z\n" +
                        "1970-01-01T00:00:00.000192000Z\n" +
                        "1970-01-01T00:00:00.000199000Z\n",
                "select cast(a as string) x from tt order by x",
                "create table tt as (select rnd_timestamp(1,200,1)::timestamp_ns a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testTimestampNsToSymbol() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(rnd_timestamp(1::timestamp_ns,150::timestamp_ns,2) as symbol) from long_sequence(10)",
                "a\n" +
                        "67\n" +
                        "\n" +
                        "30\n" +
                        "99\n" +
                        "\n" +
                        "137\n" +
                        "127\n" +
                        "58\n" +
                        "\n" +
                        "111\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampNsToSymbolConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(cast(601l as timestamp_ns) as symbol) from long_sequence(10)",
                "a\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampNsToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "20\t1970-01-01T00:00:00.000000020Z\n" +
                        "\t\n" +
                        "11\t1970-01-01T00:00:00.000000011Z\n" +
                        "13\t1970-01-01T00:00:00.000000013Z\n" +
                        "\t\n" +
                        "15\t1970-01-01T00:00:00.000000015Z\n" +
                        "19\t1970-01-01T00:00:00.000000019Z\n" +
                        "17\t1970-01-01T00:00:00.000000017Z\n" +
                        "\t\n" +
                        "10\t1970-01-01T00:00:00.000000010Z\n" +
                        "\t\n" +
                        "17\t1970-01-01T00:00:00.000000017Z\n" +
                        "\t\n" +
                        "17\t1970-01-01T00:00:00.000000017Z\n" +
                        "18\t1970-01-01T00:00:00.000000018Z\n" +
                        "18\t1970-01-01T00:00:00.000000018Z\n" +
                        "\t\n" +
                        "12\t1970-01-01T00:00:00.000000012Z\n" +
                        "11\t1970-01-01T00:00:00.000000011Z\n" +
                        "15\t1970-01-01T00:00:00.000000015Z\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_timestamp(10::timestamp_ns, 20::timestamp_ns, 2) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testTimestampNsToVarchar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(rnd_timestamp(34::timestamp_ns,66::timestamp_ns,100) as varchar) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.000000055Z\n" +
                        "1970-01-01T00:00:00.000000048Z\n" +
                        "1970-01-01T00:00:00.000000055Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000000045Z\n" +
                        "1970-01-01T00:00:00.000000036Z\n" +
                        "1970-01-01T00:00:00.000000034Z\n" +
                        "1970-01-01T00:00:00.000000058Z\n" +
                        "1970-01-01T00:00:00.000000045Z\n" +
                        "1970-01-01T00:00:00.000000061Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampNsToVarcharConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(cast(334l as timestamp_ns) as varchar) from long_sequence(10)",
                "a\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampNsToVarcharSort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000008000Z\n" +
                        "1970-01-01T00:00:00.000017000Z\n" +
                        "1970-01-01T00:00:00.000030000Z\n" +
                        "1970-01-01T00:00:00.000052000Z\n" +
                        "1970-01-01T00:00:00.000061000Z\n" +
                        "1970-01-01T00:00:00.000077000Z\n" +
                        "1970-01-01T00:00:00.000087000Z\n" +
                        "1970-01-01T00:00:00.000099000Z\n" +
                        "1970-01-01T00:00:00.000128000Z\n" +
                        "1970-01-01T00:00:00.000137000Z\n" +
                        "1970-01-01T00:00:00.000167000Z\n" +
                        "1970-01-01T00:00:00.000192000Z\n" +
                        "1970-01-01T00:00:00.000199000Z\n",
                "select cast(a as varchar) x from tt order by x",
                "create table tt as (select rnd_timestamp(1,200,1)::timestamp_ns a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testTimestampToBoolean() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(rnd_timestamp(10000000000L, 100000000000L, 2) as boolean) from long_sequence(10)",
                "a\n" +
                        "true\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "false\n" +
                        "true\n",
                true,
                true,
                false
        );

        assertQueryNoLeakCheck(
                "cast\nfalse\n",
                "select cast(cast(0L as timestamp) as boolean)",
                null,
                true,
                true,
                true
        );
        assertQueryNoLeakCheck(
                "cast\ntrue\n",
                "select cast(cast(5L as timestamp) as boolean)",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testTimestampToBooleanConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(cast(1 as timestamp) as boolean) from long_sequence(10)",
                "a\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampToByte() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a byte)",
                null,
                "insert into tab select cast(rnd_timestamp(96,100, 2) as byte) from long_sequence(10)",
                "a\n" +
                        "97\n" +
                        "0\n" +
                        "100\n" +
                        "99\n" +
                        "0\n" +
                        "97\n" +
                        "97\n" +
                        "98\n" +
                        "0\n" +
                        "96\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampToChar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a char)",
                null,
                "insert into tab select cast(rnd_timestamp(34,66,2) as char) from long_sequence(10)",
                "a\n" +
                        "7\n" +
                        "\n" +
                        "9\n" +
                        "0\n" +
                        "\n" +
                        "2\n" +
                        "+\n" +
                        "4\n" +
                        "\n" +
                        "-\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampToDate() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a date)",
                null,
                "insert into tab select cast(rnd_timestamp(1000000,10000000,2) as date) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:05.437Z\n" +
                        "\n" +
                        "1970-01-01T00:00:05.960Z\n" +
                        "1970-01-01T00:00:08.779Z\n" +
                        "\n" +
                        "1970-01-01T00:00:05.513Z\n" +
                        "1970-01-01T00:00:05.535Z\n" +
                        "1970-01-01T00:00:09.498Z\n" +
                        "\n" +
                        "1970-01-01T00:00:02.521Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampToDouble() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a double)",
                null,
                "insert into tab select cast(rnd_timestamp(1,150,2) as double) from long_sequence(10)",
                "a\n" +
                        "67.0\n" +
                        "null\n" +
                        "30.0\n" +
                        "99.0\n" +
                        "null\n" +
                        "137.0\n" +
                        "127.0\n" +
                        "58.0\n" +
                        "null\n" +
                        "111.0\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampToFloat() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a float)",
                null,
                "insert into tab select cast(rnd_timestamp(1,150,2) as float) from long_sequence(10)",
                "a\n" +
                        "67.0\n" +
                        "null\n" +
                        "30.0\n" +
                        "99.0\n" +
                        "null\n" +
                        "137.0\n" +
                        "127.0\n" +
                        "58.0\n" +
                        "null\n" +
                        "111.0\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampToInt() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a int)",
                null,
                "insert into tab select cast(rnd_timestamp(1000000L, 1000000000L, 2) as int) from long_sequence(10)",
                "a\n" +
                        "985257636\n" +
                        "null\n" +
                        "968130026\n" +
                        "555619965\n" +
                        "null\n" +
                        "712286238\n" +
                        "215755333\n" +
                        "720037886\n" +
                        "null\n" +
                        "129724714\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampToLong() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long)",
                null,
                "insert into tab select cast(cast(rnd_long(1,15000000,100) as timestamp) as long) from long_sequence(10)",
                "a\n" +
                        "13992367\n" +
                        "4501476\n" +
                        "2660374\n" +
                        "null\n" +
                        "5864007\n" +
                        "10281933\n" +
                        "6977935\n" +
                        "9100840\n" +
                        "8600061\n" +
                        "478012\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampToLong256Sort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "0x08\n" +
                        "0x11\n" +
                        "0x1e\n" +
                        "0x34\n" +
                        "0x3d\n" +
                        "0x4d\n" +
                        "0x57\n" +
                        "0x63\n" +
                        "0x80\n" +
                        "0x89\n" +
                        "0xa7\n" +
                        "0xc0\n" +
                        "0xc7\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select rnd_timestamp(1,200,1) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testTimestampToShort() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a short)",
                null,
                "insert into tab select cast(rnd_timestamp(23,56,2) as short) from long_sequence(10)",
                "a\n" +
                        "31\n" +
                        "0\n" +
                        "54\n" +
                        "23\n" +
                        "0\n" +
                        "29\n" +
                        "33\n" +
                        "24\n" +
                        "0\n" +
                        "51\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampToStr() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(rnd_timestamp(34,66,100) as string) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.000055Z\n" +
                        "1970-01-01T00:00:00.000048Z\n" +
                        "1970-01-01T00:00:00.000055Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000045Z\n" +
                        "1970-01-01T00:00:00.000036Z\n" +
                        "1970-01-01T00:00:00.000034Z\n" +
                        "1970-01-01T00:00:00.000058Z\n" +
                        "1970-01-01T00:00:00.000045Z\n" +
                        "1970-01-01T00:00:00.000061Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampToStrConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(cast(334l as timestamp) as string) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.000334Z\n" +
                        "1970-01-01T00:00:00.000334Z\n" +
                        "1970-01-01T00:00:00.000334Z\n" +
                        "1970-01-01T00:00:00.000334Z\n" +
                        "1970-01-01T00:00:00.000334Z\n" +
                        "1970-01-01T00:00:00.000334Z\n" +
                        "1970-01-01T00:00:00.000334Z\n" +
                        "1970-01-01T00:00:00.000334Z\n" +
                        "1970-01-01T00:00:00.000334Z\n" +
                        "1970-01-01T00:00:00.000334Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampToStrSort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000008Z\n" +
                        "1970-01-01T00:00:00.000017Z\n" +
                        "1970-01-01T00:00:00.000030Z\n" +
                        "1970-01-01T00:00:00.000052Z\n" +
                        "1970-01-01T00:00:00.000061Z\n" +
                        "1970-01-01T00:00:00.000077Z\n" +
                        "1970-01-01T00:00:00.000087Z\n" +
                        "1970-01-01T00:00:00.000099Z\n" +
                        "1970-01-01T00:00:00.000128Z\n" +
                        "1970-01-01T00:00:00.000137Z\n" +
                        "1970-01-01T00:00:00.000167Z\n" +
                        "1970-01-01T00:00:00.000192Z\n" +
                        "1970-01-01T00:00:00.000199Z\n",
                "select cast(a as string) x from tt order by x",
                "create table tt as (select rnd_timestamp(1,200,1) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testTimestampToSymbol() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(rnd_timestamp(1,150,2) as symbol) from long_sequence(10)",
                "a\n" +
                        "67\n" +
                        "\n" +
                        "30\n" +
                        "99\n" +
                        "\n" +
                        "137\n" +
                        "127\n" +
                        "58\n" +
                        "\n" +
                        "111\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampToSymbolConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(cast(601l as timestamp) as symbol) from long_sequence(10)",
                "a\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "20\t1970-01-01T00:00:00.000020Z\n" +
                        "\t\n" +
                        "11\t1970-01-01T00:00:00.000011Z\n" +
                        "13\t1970-01-01T00:00:00.000013Z\n" +
                        "\t\n" +
                        "15\t1970-01-01T00:00:00.000015Z\n" +
                        "19\t1970-01-01T00:00:00.000019Z\n" +
                        "17\t1970-01-01T00:00:00.000017Z\n" +
                        "\t\n" +
                        "10\t1970-01-01T00:00:00.000010Z\n" +
                        "\t\n" +
                        "17\t1970-01-01T00:00:00.000017Z\n" +
                        "\t\n" +
                        "17\t1970-01-01T00:00:00.000017Z\n" +
                        "18\t1970-01-01T00:00:00.000018Z\n" +
                        "18\t1970-01-01T00:00:00.000018Z\n" +
                        "\t\n" +
                        "12\t1970-01-01T00:00:00.000012Z\n" +
                        "11\t1970-01-01T00:00:00.000011Z\n" +
                        "15\t1970-01-01T00:00:00.000015Z\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_timestamp(10, 20, 2) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testTimestampToVarchar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(rnd_timestamp(34,66,100) as varchar) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.000055Z\n" +
                        "1970-01-01T00:00:00.000048Z\n" +
                        "1970-01-01T00:00:00.000055Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000045Z\n" +
                        "1970-01-01T00:00:00.000036Z\n" +
                        "1970-01-01T00:00:00.000034Z\n" +
                        "1970-01-01T00:00:00.000058Z\n" +
                        "1970-01-01T00:00:00.000045Z\n" +
                        "1970-01-01T00:00:00.000061Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampToVarcharConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(cast(334l as timestamp) as varchar) from long_sequence(10)",
                "a\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampToVarcharSort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000008Z\n" +
                        "1970-01-01T00:00:00.000017Z\n" +
                        "1970-01-01T00:00:00.000030Z\n" +
                        "1970-01-01T00:00:00.000052Z\n" +
                        "1970-01-01T00:00:00.000061Z\n" +
                        "1970-01-01T00:00:00.000077Z\n" +
                        "1970-01-01T00:00:00.000087Z\n" +
                        "1970-01-01T00:00:00.000099Z\n" +
                        "1970-01-01T00:00:00.000128Z\n" +
                        "1970-01-01T00:00:00.000137Z\n" +
                        "1970-01-01T00:00:00.000167Z\n" +
                        "1970-01-01T00:00:00.000192Z\n" +
                        "1970-01-01T00:00:00.000199Z\n",
                "select cast(a as varchar) x from tt order by x",
                "create table tt as (select rnd_timestamp(1,200,1) a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testUuidToStr() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(rnd_uuid4() as string) from long_sequence(10)",
                "a\n" +
                        "0010cde8-12ce-40ee-8010-a928bb8b9650\n" +
                        "9f9b2131-d49f-4d1d-ab81-39815c50d341\n" +
                        "7bcd48d8-c77a-4655-b2a2-15ba0462ad15\n" +
                        "b5b2159a-2356-4217-965d-4c984f0ffa8a\n" +
                        "e8beef38-cd7b-43d8-9b2d-34586f6275fa\n" +
                        "322a2198-864b-4b14-b97f-a69eb8fec6cc\n" +
                        "980eca62-a219-40f1-a846-d7a3aa5aecce\n" +
                        "c1e63128-5c1a-4288-872b-fc5230158059\n" +
                        "716de3d2-5dcc-4d91-9fa2-397a5d8c84c4\n" +
                        "4b0f595f-143e-4d72-af1a-8266e7921e3b\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testUuidToVarchar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab select cast(rnd_uuid4() as varchar) from long_sequence(10)",
                "a\n" +
                        "0010cde8-12ce-40ee-8010-a928bb8b9650\n" +
                        "9f9b2131-d49f-4d1d-ab81-39815c50d341\n" +
                        "7bcd48d8-c77a-4655-b2a2-15ba0462ad15\n" +
                        "b5b2159a-2356-4217-965d-4c984f0ffa8a\n" +
                        "e8beef38-cd7b-43d8-9b2d-34586f6275fa\n" +
                        "322a2198-864b-4b14-b97f-a69eb8fec6cc\n" +
                        "980eca62-a219-40f1-a846-d7a3aa5aecce\n" +
                        "c1e63128-5c1a-4288-872b-fc5230158059\n" +
                        "716de3d2-5dcc-4d91-9fa2-397a5d8c84c4\n" +
                        "4b0f595f-143e-4d72-af1a-8266e7921e3b\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testVarcharToBoolean() throws Exception {
        assertQuery(
                "boolean\n" +
                        "false\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "false\n" +
                        "true\n" +
                        "false\n" +
                        "false\n",
                "select boolean from tab",
                "create table tab as (" +
                        "select cast(rnd_varchar('28', 'TRuE', '', null, 'false', 'true') as boolean) boolean from long_sequence(10)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testVarcharToByte() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a byte)",
                null,
                "insert into tab select cast(rnd_varchar('23','56','100', null) as byte) from long_sequence(10)",
                "a\n" +
                        "23\n" +
                        "100\n" +
                        "56\n" +
                        "0\n" +
                        "56\n" +
                        "0\n" +
                        "100\n" +
                        "0\n" +
                        "100\n" +
                        "23\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testVarcharToChar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a char)",
                null,
                "insert into tab select cast(rnd_varchar('A', 'BC', 'K', null) as char) from long_sequence(10)",
                "a\n" +
                        "A\n" +
                        "K\n" +
                        "B\n" +
                        "\n" +
                        "B\n" +
                        "\n" +
                        "K\n" +
                        "\n" +
                        "K\n" +
                        "A\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testVarcharToDate() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a date)",
                null,
                "insert into tab select cast(rnd_varchar('2019-03-11T10:20:33.123Z', '2019-03-24T14:20:33.123Z', 'ABC', null) as date) from long_sequence(10)",
                "a\n" +
                        "2019-03-11T10:20:33.123Z\n" +
                        "\n" +
                        "2019-03-24T14:20:33.123Z\n" +
                        "\n" +
                        "2019-03-24T14:20:33.123Z\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "2019-03-11T10:20:33.123Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testVarcharToDouble() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a double)",
                null,
                "insert into tab select cast(rnd_varchar('1234.556', '988.223', 'abc', null) as double) from long_sequence(15)",
                "a\n" +
                        "1234.556\n" +
                        "null\n" +
                        "988.223\n" +
                        "null\n" +
                        "988.223\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "1234.556\n" +
                        "1234.556\n" +
                        "1234.556\n" +
                        "null\n" +
                        "null\n" +
                        "988.223\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testVarcharToDouble_doubleParserTakingSlowPath() throws Exception {
        assertQuery(
                "a\n",
                "select cast(a as double) as a from tab",
                "create table tab (a varchar)",
                null,
                "insert into tab values ('4.9E-324')",
                "a\n" +
                        "4.9E-324\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testVarcharToFloat() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a float)",
                null,
                "insert into tab select cast(rnd_varchar('9.23', '4.15', 'xyz', null) as float) from long_sequence(15)",
                "a\n" +
                        "9.23\n" +
                        "null\n" +
                        "4.15\n" +
                        "null\n" +
                        "4.15\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "9.23\n" +
                        "9.23\n" +
                        "9.23\n" +
                        "null\n" +
                        "null\n" +
                        "4.15\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testVarcharToIPv4() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a IPv4)",
                null,
                "insert into tab select cast(rnd_varchar('171.30.189.77','111.221.228.130','201.100.238.229',null) as IPv4) from long_sequence(10)",
                "a\n" +
                        "171.30.189.77\n" +
                        "201.100.238.229\n" +
                        "111.221.228.130\n" +
                        "\n" +
                        "111.221.228.130\n" +
                        "\n" +
                        "201.100.238.229\n" +
                        "\n" +
                        "201.100.238.229\n" +
                        "171.30.189.77\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testVarcharToInt() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a int)",
                null,
                "insert into tab select cast(rnd_varchar('90092', '2203', null) as int) from long_sequence(10)",
                "a\n" +
                        "90092\n" +
                        "90092\n" +
                        "2203\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "2203\n" +
                        "90092\n" +
                        "2203\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testVarcharToLong() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long)",
                null,
                "insert into tab select cast(rnd_varchar('2334l', '99002', null) as long) from long_sequence(10)",
                "a\n" +
                        "2334\n" +
                        "2334\n" +
                        "99002\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "null\n" +
                        "99002\n" +
                        "2334\n" +
                        "99002\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testVarcharToLong256() throws Exception {
        assertQuery(
                "x\n" +
                        "0x123455\n" +
                        "\n" +
                        "0x8802ff90\n" +
                        "0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\n" +
                        "0x8802ff90\n" +
                        "0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\n" +
                        "\n" +
                        "0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\n" +
                        "\n" +
                        "0x123455\n" +
                        "0x123455\n" +
                        "0x123455\n" +
                        "\n" +
                        "0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\n" +
                        "0x8802ff90\n" +
                        "0x123455\n" +
                        "0x123455\n" +
                        "0x8802ff90\n" +
                        "0x8802ff90\n" +
                        "\n",
                "select cast(a as long256) x from tt",
                "create table tt as (select rnd_varchar('0x00123455', '0x8802ff90', null, '0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926') a from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testVarcharToShort() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a short)",
                null,
                "insert into tab select cast(rnd_varchar('23','56','100', null, 'y') as short) from long_sequence(10)",
                "a\n" +
                        "23\n" +
                        "0\n" +
                        "0\n" +
                        "56\n" +
                        "56\n" +
                        "0\n" +
                        "100\n" +
                        "56\n" +
                        "56\n" +
                        "0\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testVarcharToSymbol() throws Exception {
        assertQuery(
                "x\n" +
                        "a\n" +
                        "c\n" +
                        "b\n" +
                        "\n" +
                        "b\n" +
                        "\n" +
                        "c\n" +
                        "\n" +
                        "c\n" +
                        "a\n",
                "select cast(a as symbol) x from tt",
                "create table tt as (select rnd_varchar('a','b','c', null) a from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testVarcharToSymbolConst() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "abc\ta\n" +
                        "abc\tc\n" +
                        "abc\tb\n" +
                        "abc\t\n" +
                        "abc\tb\n" +
                        "abc\t\n" +
                        "abc\tc\n" +
                        "abc\t\n" +
                        "abc\tc\n" +
                        "abc\ta\n",
                "select cast('abc' as symbol) b, a from tab",
                "create table tab as (select rnd_varchar('a','b','c',null) a from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testVarcharToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "a\ta\n" +
                        "c\tc\n" +
                        "b\tb\n" +
                        "\t\n" +
                        "b\tb\n" +
                        "\t\n" +
                        "c\tc\n" +
                        "\t\n" +
                        "c\tc\n" +
                        "a\ta\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_varchar('a','b','c',null) a from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testVarcharToTimestamp() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp)",
                null,
                "insert into tab select cast(rnd_varchar('2019-03-11T10:20:33.123897Z', '2019-03-24T14:20:33.123551Z', 'ABC', null) as timestamp) from long_sequence(10)",
                "a\n" +
                        "2019-03-11T10:20:33.123897Z\n" +
                        "\n" +
                        "2019-03-24T14:20:33.123551Z\n" +
                        "\n" +
                        "2019-03-24T14:20:33.123551Z\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "2019-03-11T10:20:33.123897Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testVarcharToTimestampNs() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp_ns)",
                null,
                "insert into tab select cast(rnd_varchar('2019-03-11T10:20:33.123897123Z', '2019-03-24T14:20:33.123551098Z', 'ABC', null) as timestamp_ns) from long_sequence(10)",
                "a\n" +
                        "2019-03-11T10:20:33.123897123Z\n" +
                        "\n" +
                        "2019-03-24T14:20:33.123551098Z\n" +
                        "\n" +
                        "2019-03-24T14:20:33.123551098Z\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "2019-03-11T10:20:33.123897123Z\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testVarcharToUuid() throws Exception {
        assertQuery(
                "x\n" +
                        "0010cde8-12ce-40ee-8010-a928bb8b9650\n" +
                        "\n" +
                        "9f9b2131-d49f-4d1d-ab81-39815c50d341\n" +
                        "7bcd48d8-c77a-4655-b2a2-15ba0462ad15\n" +
                        "9f9b2131-d49f-4d1d-ab81-39815c50d341\n" +
                        "7bcd48d8-c77a-4655-b2a2-15ba0462ad15\n" +
                        "\n" +
                        "7bcd48d8-c77a-4655-b2a2-15ba0462ad15\n" +
                        "\n" +
                        "0010cde8-12ce-40ee-8010-a928bb8b9650\n",
                "select cast(a as uuid) x from tt",
                "create table tt as (select rnd_varchar('0010cde8-12ce-40ee-8010-a928bb8b9650', '9f9b2131-d49f-4d1d-ab81-39815c50d341', null, '7bcd48d8-c77a-4655-b2a2-15ba0462ad15') a from long_sequence(10))",
                null,
                true,
                true
        );
    }
}
