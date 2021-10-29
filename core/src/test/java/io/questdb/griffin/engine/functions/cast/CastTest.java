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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.griffin.AbstractGriffinTest;
import org.junit.Test;

public class CastTest extends AbstractGriffinTest {

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
                true
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
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "0\n" +
                        "1\n" +
                        "1\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "0\n",
                true,
                true,
                true
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
                true
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
                        "1970-01-01T00:00:00.001Z\n" +
                        "1970-01-01T00:00:00.001Z\n" +
                        "1970-01-01T00:00:00.001Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.001Z\n" +
                        "1970-01-01T00:00:00.001Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.000Z\n",
                true,
                true,
                true
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
                        "1.0\n" +
                        "1.0\n" +
                        "1.0\n" +
                        "0.0\n" +
                        "1.0\n" +
                        "1.0\n" +
                        "0.0\n" +
                        "0.0\n" +
                        "0.0\n" +
                        "0.0\n",
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
                        "1.0000\n" +
                        "1.0000\n" +
                        "1.0000\n" +
                        "0.0000\n" +
                        "1.0000\n" +
                        "1.0000\n" +
                        "0.0000\n" +
                        "0.0000\n" +
                        "0.0000\n" +
                        "0.0000\n",
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
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "0\n" +
                        "1\n" +
                        "1\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "0\n",
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
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "0\n" +
                        "1\n" +
                        "1\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "0\n",
                true,
                true,
                true
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
                        "0x01\n" +
                        "0x01\n" +
                        "0x01\n" +
                        "0x00\n" +
                        "0x01\n" +
                        "0x01\n" +
                        "0x00\n" +
                        "0x00\n" +
                        "0x00\n" +
                        "0x00\n",
                true,
                true,
                true
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
                        "0x01\n" +
                        "0x01\n" +
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
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "0\n" +
                        "1\n" +
                        "1\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "0\n",
                true,
                true,
                true
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
                true
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
                true
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
                false,
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
                true
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
                true
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
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000000Z\n",
                true,
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
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n",
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
                true
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
                true
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
                true
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
                true
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
                true
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
                        "76.0000\n" +
                        "102.0000\n" +
                        "27.0000\n" +
                        "87.0000\n" +
                        "79.0000\n" +
                        "79.0000\n" +
                        "122.0000\n" +
                        "83.0000\n" +
                        "90.0000\n" +
                        "76.0000\n",
                true,
                true,
                true
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
                true
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
                true
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
                true
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
                true
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
                true
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
                true
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
                true
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
                true
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
                true
        );
    }

    @Test
    public void testCharToBoolean() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast('f' as boolean) from long_sequence(10)",
                "a\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n",
                true,
                true,
                true
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
                true
        );
    }

    @Test
    public void testCharToByte() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a byte)",
                null,
                "insert into tab select cast(rnd_char() as byte) from long_sequence(10)",
                "a\n" +
                        "86\n" +
                        "84\n" +
                        "74\n" +
                        "87\n" +
                        "67\n" +
                        "80\n" +
                        "83\n" +
                        "87\n" +
                        "72\n" +
                        "89\n",
                true,
                true,
                true
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
                true
        );
    }

    @Test
    public void testCharToDate() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a date)",
                null,
                "insert into tab select cast(rnd_char() as date) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.086Z\n" +
                        "1970-01-01T00:00:00.084Z\n" +
                        "1970-01-01T00:00:00.074Z\n" +
                        "1970-01-01T00:00:00.087Z\n" +
                        "1970-01-01T00:00:00.067Z\n" +
                        "1970-01-01T00:00:00.080Z\n" +
                        "1970-01-01T00:00:00.083Z\n" +
                        "1970-01-01T00:00:00.087Z\n" +
                        "1970-01-01T00:00:00.072Z\n" +
                        "1970-01-01T00:00:00.089Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testCharToDouble() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a double)",
                null,
                "insert into tab select cast(rnd_char() as double) from long_sequence(10)",
                "a\n" +
                        "86.0\n" +
                        "84.0\n" +
                        "74.0\n" +
                        "87.0\n" +
                        "67.0\n" +
                        "80.0\n" +
                        "83.0\n" +
                        "87.0\n" +
                        "72.0\n" +
                        "89.0\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testCharToFloat() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a float)",
                null,
                "insert into tab select cast(rnd_char() as float) from long_sequence(10)",
                "a\n" +
                        "86.0000\n" +
                        "84.0000\n" +
                        "74.0000\n" +
                        "87.0000\n" +
                        "67.0000\n" +
                        "80.0000\n" +
                        "83.0000\n" +
                        "87.0000\n" +
                        "72.0000\n" +
                        "89.0000\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testCharToInt() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a int)",
                null,
                "insert into tab select cast(rnd_char() as int) from long_sequence(10)",
                "a\n" +
                        "86\n" +
                        "84\n" +
                        "74\n" +
                        "87\n" +
                        "67\n" +
                        "80\n" +
                        "83\n" +
                        "87\n" +
                        "72\n" +
                        "89\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testCharToLong() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long)",
                null,
                "insert into tab select cast(rnd_char() as long) from long_sequence(10)",
                "a\n" +
                        "86\n" +
                        "84\n" +
                        "74\n" +
                        "87\n" +
                        "67\n" +
                        "80\n" +
                        "83\n" +
                        "87\n" +
                        "72\n" +
                        "89\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testCharToLong256() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long256)",
                null,
                "insert into tab select cast(rnd_char() as long256) from long_sequence(10)",
                "a\n" +
                        "0x56\n" +
                        "0x54\n" +
                        "0x4a\n" +
                        "0x57\n" +
                        "0x43\n" +
                        "0x50\n" +
                        "0x53\n" +
                        "0x57\n" +
                        "0x48\n" +
                        "0x59\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testCharToLong256Sort() throws Exception {
        assertQuery(
                "x\n" +
                        "0x43\n" +
                        "0x45\n" +
                        "0x47\n" +
                        "0x48\n" +
                        "0x48\n" +
                        "0x4a\n" +
                        "0x4e\n" +
                        "0x50\n" +
                        "0x50\n" +
                        "0x52\n" +
                        "0x52\n" +
                        "0x53\n" +
                        "0x54\n" +
                        "0x56\n" +
                        "0x57\n" +
                        "0x57\n" +
                        "0x58\n" +
                        "0x58\n" +
                        "0x59\n" +
                        "0x5a\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select rnd_char() a from long_sequence(20))",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testCharToShort() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a short)",
                null,
                "insert into tab select cast(rnd_char() as short) from long_sequence(10)",
                "a\n" +
                        "86\n" +
                        "84\n" +
                        "74\n" +
                        "87\n" +
                        "67\n" +
                        "80\n" +
                        "83\n" +
                        "87\n" +
                        "72\n" +
                        "89\n",
                true,
                true,
                true
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
                true
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
                true
        );
    }

    @Test
    public void testCharToStrConstZero() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast('' as string) from long_sequence(10)",
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
                true
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
                true
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
                true
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
                "insert into tab select cast(rnd_char() as timestamp) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.000086Z\n" +
                        "1970-01-01T00:00:00.000084Z\n" +
                        "1970-01-01T00:00:00.000074Z\n" +
                        "1970-01-01T00:00:00.000087Z\n" +
                        "1970-01-01T00:00:00.000067Z\n" +
                        "1970-01-01T00:00:00.000080Z\n" +
                        "1970-01-01T00:00:00.000083Z\n" +
                        "1970-01-01T00:00:00.000087Z\n" +
                        "1970-01-01T00:00:00.000072Z\n" +
                        "1970-01-01T00:00:00.000089Z\n",
                true,
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
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n",
                true,
                true,
                true
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
                true
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
                true
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
                true
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
                true
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
                        "NaN\n" +
                        "30.0\n" +
                        "99.0\n" +
                        "NaN\n" +
                        "137.0\n" +
                        "127.0\n" +
                        "58.0\n" +
                        "NaN\n" +
                        "111.0\n",
                true,
                true,
                true
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
                        "67.0000\n" +
                        "NaN\n" +
                        "30.0000\n" +
                        "99.0000\n" +
                        "NaN\n" +
                        "137.0000\n" +
                        "127.0000\n" +
                        "58.0000\n" +
                        "NaN\n" +
                        "111.0000\n",
                true,
                true,
                true
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
                        "NaN\n" +
                        "968130026\n" +
                        "555619965\n" +
                        "NaN\n" +
                        "712286238\n" +
                        "215755333\n" +
                        "720037886\n" +
                        "NaN\n" +
                        "129724714\n",
                true,
                true,
                true
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
                        "NaN\n" +
                        "7587030\n" +
                        "11082999\n" +
                        "NaN\n" +
                        "602537\n" +
                        "5112277\n" +
                        "5361808\n" +
                        "NaN\n" +
                        "8600061\n",
                true,
                true,
                true
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
                true
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
                true
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
                true
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
                "create table tt as (select rnd_date(1,200,1) a from long_sequence(20))",
                null,
                true,
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
                true
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
                true
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
                true
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
                true
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
                true
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
                        "NaN\n",
                true,
                true,
                true
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
                        "0.8043\n" +
                        "0.0849\n" +
                        "0.0844\n" +
                        "0.6509\n" +
                        "0.7906\n" +
                        "0.2245\n" +
                        "0.3491\n" +
                        "0.7611\n" +
                        "0.4218\n" +
                        "NaN\n",
                true,
                true,
                true
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
                true
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
                        "NaN\n",
                true,
                true,
                true
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
                true
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
                true
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
                true
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
                true
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
                true
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
                true
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
                        "\tNaN\n" +
                        "0.7261136209823622\t0.7261136209823622\n" +
                        "0.4224356661645131\t0.4224356661645131\n" +
                        "0.7094360487171202\t0.7094360487171202\n" +
                        "0.38539947865244994\t0.38539947865244994\n" +
                        "0.0035983672154330515\t0.0035983672154330515\n" +
                        "0.3288176907679504\t0.3288176907679504\n" +
                        "\tNaN\n" +
                        "0.9771103146051203\t0.9771103146051203\n" +
                        "0.24808812376657652\t0.24808812376657652\n" +
                        "0.6381607531178513\t0.6381607531178513\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_double(2) a from long_sequence(20))",
                null,
                true,
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
                true
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
                true
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
                true
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
                        "NaN\n" +
                        "0.0848696231842041\n" +
                        "0.29919904470443726\n" +
                        "NaN\n" +
                        "0.934460461139679\n" +
                        "0.1312335729598999\n" +
                        "0.7905675172805786\n" +
                        "NaN\n" +
                        "0.2245233654975891\n",
                true,
                true,
                true
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
                        "0.8043\n" +
                        "NaN\n" +
                        "0.0849\n" +
                        "0.2992\n" +
                        "NaN\n" +
                        "0.9345\n" +
                        "0.1312\n" +
                        "0.7906\n" +
                        "NaN\n" +
                        "0.2245\n",
                true,
                true,
                true
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
                        "NaN\n" +
                        "848\n" +
                        "2991\n" +
                        "NaN\n" +
                        "9344\n" +
                        "1312\n" +
                        "7905\n" +
                        "NaN\n" +
                        "2245\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testFloatToLong() throws Exception {
        compiler.compile("create table rndfloat as (select rnd_float(2) fl from long_sequence(10))", sqlExecutionContext);
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
                        "NaN\tNaN\n" +
                        "0.0848696231842041\t848696231\n" +
                        "0.29919904470443726\t2991990447\n" +
                        "NaN\tNaN\n" +
                        "0.934460461139679\t9344604611\n" +
                        "0.1312335729598999\t1312335729\n" +
                        "0.7905675172805786\t7905675172\n" +
                        "NaN\tNaN\n" +
                        "0.2245233654975891\t2245233654\n",
                true,
                true,
                true
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
                true
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
                true
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
                        "0.6608\n" +
                        "0.8043\n" +
                        "0.2246\n" +
                        "0.1297\n" +
                        "0.0849\n" +
                        "0.2846\n" +
                        "0.2992\n" +
                        "0.0844\n" +
                        "0.2045\n" +
                        "0.9345\n",
                true,
                true,
                true
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
                        "1.3400\n" +
                        "1.3400\n" +
                        "1.3400\n" +
                        "1.3400\n" +
                        "1.3400\n" +
                        "1.3400\n" +
                        "1.3400\n" +
                        "1.3400\n" +
                        "1.3400\n" +
                        "1.3400\n",
                true,
                true,
                true
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
                        "0.0849\n" +
                        "0.1312\n" +
                        "0.2245\n" +
                        "0.2992\n" +
                        "0.3491\n" +
                        "0.5244\n" +
                        "0.5599\n" +
                        "0.6277\n" +
                        "0.6694\n" +
                        "0.7261\n" +
                        "0.7611\n" +
                        "0.7906\n" +
                        "0.8043\n" +
                        "0.9345\n",
                "select cast(a as string) x from tt order by x",
                "create table tt as (select rnd_float(2) a from long_sequence(20))",
                null,
                true,
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
                        "0.8043\n" +
                        "\n" +
                        "0.0849\n" +
                        "0.2992\n" +
                        "\n" +
                        "0.9345\n" +
                        "0.1312\n" +
                        "0.7906\n" +
                        "\n" +
                        "0.2245\n",
                true,
                true,
                true
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
                        "1.5000\n" +
                        "1.5000\n" +
                        "1.5000\n" +
                        "1.5000\n" +
                        "1.5000\n" +
                        "1.5000\n" +
                        "1.5000\n" +
                        "1.5000\n" +
                        "1.5000\n" +
                        "1.5000\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testFloatToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "0.8043\t0.8043\n" +
                        "\tNaN\n" +
                        "0.0849\t0.0849\n" +
                        "0.2992\t0.2992\n" +
                        "\tNaN\n" +
                        "0.9345\t0.9345\n" +
                        "0.1312\t0.1312\n" +
                        "0.7906\t0.7906\n" +
                        "\tNaN\n" +
                        "0.2245\t0.2245\n" +
                        "\tNaN\n" +
                        "0.3491\t0.3491\n" +
                        "\tNaN\n" +
                        "0.7611\t0.7611\n" +
                        "0.5244\t0.5244\n" +
                        "0.5599\t0.5599\n" +
                        "\tNaN\n" +
                        "0.7261\t0.7261\n" +
                        "0.6277\t0.6277\n" +
                        "0.6694\t0.6694\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_float(2) a from long_sequence(20))",
                null,
                true,
                true,
                true
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
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n",
                true,
                true,
                true
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
                true
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
                true
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
                true
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
                true
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
                        "NaN\n" +
                        "7.0\n" +
                        "17.0\n" +
                        "65.0\n" +
                        "32.0\n" +
                        "67.0\n" +
                        "106.0\n",
                true,
                true,
                true
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
                        "19.0000\n" +
                        "72.0000\n" +
                        "90.0000\n" +
                        "NaN\n" +
                        "7.0000\n" +
                        "17.0000\n" +
                        "65.0000\n" +
                        "32.0000\n" +
                        "67.0000\n" +
                        "106.0000\n",
                true,
                true,
                true
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
                        "NaN\n" +
                        "37\n" +
                        "38\n" +
                        "38\n" +
                        "56\n" +
                        "52\n" +
                        "49\n",
                true,
                true,
                true
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
                        "NaN\n" +
                        "7\n" +
                        "17\n" +
                        "65\n" +
                        "32\n" +
                        "67\n" +
                        "106\n",
                true,
                true,
                true
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
                true
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
                true
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
                true
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
                true
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
                true
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
                true
        );
    }

    @Test
    public void testIntToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "16\t16\n" +
                        "\tNaN\n" +
                        "11\t11\n" +
                        "20\t20\n" +
                        "\tNaN\n" +
                        "11\t11\n" +
                        "12\t12\n" +
                        "18\t18\n" +
                        "\tNaN\n" +
                        "17\t17\n" +
                        "\tNaN\n" +
                        "16\t16\n" +
                        "\tNaN\n" +
                        "19\t19\n" +
                        "15\t15\n" +
                        "15\t15\n" +
                        "\tNaN\n" +
                        "12\t12\n" +
                        "15\t15\n" +
                        "18\t18\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_int(10, 20, 2) a from long_sequence(20))",
                null,
                true,
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
                true
        );
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
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n",
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
                true
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
                true
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
                true
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
                        "false\n" +
                        "false\n" +
                        "true\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n",
                true,
                true,
                true
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
                true
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
                true
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
                true
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
                true
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
                        "NaN\n" +
                        "57.0\n" +
                        "33.0\n" +
                        "85.0\n" +
                        "40.0\n" +
                        "111.0\n" +
                        "112.0\n",
                true,
                true,
                true
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
                        "67.0000\n" +
                        "126.0000\n" +
                        "124.0000\n" +
                        "NaN\n" +
                        "57.0000\n" +
                        "33.0000\n" +
                        "85.0000\n" +
                        "40.0000\n" +
                        "111.0000\n" +
                        "112.0000\n",
                true,
                true,
                true
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
                        "NaN\n" +
                        "45\n" +
                        "36\n" +
                        "34\n" +
                        "58\n" +
                        "45\n" +
                        "61\n",
                true,
                true,
                true
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
                        "NaN\n" +
                        "57\n" +
                        "33\n" +
                        "85\n" +
                        "40\n" +
                        "111\n" +
                        "112\n",
                true,
                true,
                true
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
                true
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
                true
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
                true
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
                true
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
                true
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
                true
        );
    }

    @Test
    public void testLongToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "20\t20\n" +
                        "\tNaN\n" +
                        "11\t11\n" +
                        "13\t13\n" +
                        "\tNaN\n" +
                        "15\t15\n" +
                        "19\t19\n" +
                        "17\t17\n" +
                        "\tNaN\n" +
                        "10\t10\n" +
                        "\tNaN\n" +
                        "17\t17\n" +
                        "\tNaN\n" +
                        "17\t17\n" +
                        "18\t18\n" +
                        "18\t18\n" +
                        "\tNaN\n" +
                        "12\t12\n" +
                        "11\t11\n" +
                        "15\t15\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_long(10, 20, 2) a from long_sequence(20))",
                null,
                true,
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
                true
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
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n",
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
                true
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
                true
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
                true
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
                true
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
                true
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
                        "-27056.0000\n" +
                        "24814.0000\n" +
                        "-11455.0000\n" +
                        "-13027.0000\n" +
                        "-21227.0000\n" +
                        "-22955.0000\n" +
                        "-1398.0000\n" +
                        "21015.0000\n" +
                        "30202.0000\n" +
                        "-19496.0000\n",
                true,
                true,
                true
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
                true
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
                true
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
                true
        );
    }

    @Test
    public void testShortToLong256Sort() throws Exception {
        assertQuery(
                "x\n" +
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
                        "0xfffffffffffffa8a\n" +
                        "0x1e3b\n" +
                        "0x2d91\n" +
                        "0x5217\n" +
                        "0x5d72\n" +
                        "0x60ee\n" +
                        "0x75fa\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select rnd_short() a from long_sequence(20))",
                null,
                true,
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
                true
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
                true
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
                true
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
                true
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
                true
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
                true
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
                true
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
                true
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
                        "NaN\n" +
                        "988.2230000000001\n" +
                        "NaN\n" +
                        "988.2230000000001\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "1234.556\n" +
                        "1234.556\n" +
                        "1234.556\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "988.2230000000001\n",
                true,
                true,
                true
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
                        "9.2300\n" +
                        "NaN\n" +
                        "4.1500\n" +
                        "NaN\n" +
                        "4.1500\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "9.2300\n" +
                        "9.2300\n" +
                        "9.2300\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "4.1500\n",
                true,
                true,
                true
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
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "2203\n" +
                        "90092\n" +
                        "2203\n",
                true,
                true,
                true
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
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "99002\n" +
                        "2334\n" +
                        "99002\n",
                true,
                true,
                true
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
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\n" +
                        "0x123455\n" +
                        "0x123455\n" +
                        "0x123455\n" +
                        "0x123455\n" +
                        "0x8802ff90\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select rnd_str('0x00123455', '0x8802ff90', 'z', null, '0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926', '0x880z', '0xhello') a from long_sequence(20))",
                null,
                true,
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
                true
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
                false,
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
                true
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
                true
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
                true
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
                true
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
                        "NaN\n" +
                        "988.2230000000001\n" +
                        "NaN\n" +
                        "988.2230000000001\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "1234.556\n" +
                        "1234.556\n" +
                        "1234.556\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "988.2230000000001\n",
                true,
                true,
                true
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
                        "9.2300\n" +
                        "NaN\n" +
                        "4.1500\n" +
                        "NaN\n" +
                        "4.1500\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "9.2300\n" +
                        "9.2300\n" +
                        "9.2300\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "4.1500\n",
                true,
                true,
                true
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
                        "NaN\n" +
                        "200\n" +
                        "NaN\n" +
                        "200\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "100\n",
                true,
                true,
                true
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
                        "NaN\n" +
                        "200\n" +
                        "NaN\n" +
                        "200\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "NaN\n" +
                        "100\n",
                true,
                true,
                true
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
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\n" +
                        "0x123455\n" +
                        "0x123455\n" +
                        "0x123455\n" +
                        "0x123455\n" +
                        "0x8802ff90\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select rnd_symbol('0x00123455', '0x8802ff90', 'z', null, '0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926', '0x880z', '0xhello') a from long_sequence(20))",
                null,
                true,
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
                "insert into tt select rnd_symbol('0x00123455', '0x8802ff90', 'z', null, '0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926', '0x880z', '0xhello') a from long_sequence(20)",
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\n" +
                        "0x123455\n" +
                        "0x123455\n" +
                        "0x123455\n" +
                        "0x123455\n" +
                        "0x8802ff90\n",
                true,
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
                true
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
                true
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
                true
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
                false,
                true
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
                false,
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
                false,
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
                true
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
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n",
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
                true
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
                true
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
                true
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
                true
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
                        "NaN\n" +
                        "30.0\n" +
                        "99.0\n" +
                        "NaN\n" +
                        "137.0\n" +
                        "127.0\n" +
                        "58.0\n" +
                        "NaN\n" +
                        "111.0\n",
                true,
                true,
                true
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
                        "67.0000\n" +
                        "NaN\n" +
                        "30.0000\n" +
                        "99.0000\n" +
                        "NaN\n" +
                        "137.0000\n" +
                        "127.0000\n" +
                        "58.0000\n" +
                        "NaN\n" +
                        "111.0000\n",
                true,
                true,
                true
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
                        "NaN\n" +
                        "968130026\n" +
                        "555619965\n" +
                        "NaN\n" +
                        "712286238\n" +
                        "215755333\n" +
                        "720037886\n" +
                        "NaN\n" +
                        "129724714\n",
                true,
                true,
                true
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
                        "NaN\n" +
                        "5864007\n" +
                        "10281933\n" +
                        "6977935\n" +
                        "9100840\n" +
                        "8600061\n" +
                        "478012\n",
                true,
                true,
                true
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
                true
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
                true
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
                true
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
                "create table tt as (select rnd_timestamp(1,200,1) a from long_sequence(20))",
                null,
                true,
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
                true
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
                true
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
                true,
                true
        );
    }
}