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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
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
                """
                        a
                        false
                        false
                        false
                        true
                        false
                        false
                        true
                        true
                        true
                        true
                        """,
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
                """
                        a
                        0
                        0
                        0
                        1
                        0
                        0
                        1
                        1
                        1
                        1
                        """,
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
                """
                        a
                        F
                        F
                        F
                        T
                        F
                        F
                        T
                        T
                        T
                        T
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.000Z
                        1970-01-01T00:00:00.000Z
                        1970-01-01T00:00:00.000Z
                        1970-01-01T00:00:00.001Z
                        1970-01-01T00:00:00.000Z
                        1970-01-01T00:00:00.000Z
                        1970-01-01T00:00:00.001Z
                        1970-01-01T00:00:00.001Z
                        1970-01-01T00:00:00.001Z
                        1970-01-01T00:00:00.001Z
                        """,
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
                """
                        a
                        0.0
                        0.0
                        0.0
                        1.0
                        0.0
                        0.0
                        1.0
                        1.0
                        1.0
                        1.0
                        """,
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
                """
                        a
                        0.0
                        0.0
                        0.0
                        1.0
                        0.0
                        0.0
                        1.0
                        1.0
                        1.0
                        1.0
                        """,
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
                """
                        a
                        0
                        0
                        0
                        1
                        0
                        0
                        1
                        1
                        1
                        1
                        """,
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
                """
                        a
                        0
                        0
                        0
                        1
                        0
                        0
                        1
                        1
                        1
                        1
                        """,
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
                """
                        a
                        0x00
                        0x00
                        0x00
                        0x01
                        0x00
                        0x00
                        0x01
                        0x01
                        0x01
                        0x01
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testBooleanToLong256Sort() throws Exception {
        assertQuery(
                """
                        x
                        0x00
                        0x00
                        0x00
                        0x00
                        0x00
                        0x00
                        0x00
                        0x00
                        0x00
                        0x00
                        0x00
                        0x01
                        0x01
                        0x01
                        0x01
                        0x01
                        0x01
                        0x01
                        0x01
                        0x01
                        """,
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
                """
                        a
                        0
                        0
                        0
                        1
                        0
                        0
                        1
                        1
                        1
                        1
                        """,
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
                """
                        a
                        false
                        false
                        false
                        true
                        false
                        false
                        true
                        true
                        true
                        true
                        """,
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
                """
                        a
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testBooleanToStrSort() throws Exception {
        assertQuery(
                """
                        x
                        false
                        false
                        false
                        false
                        false
                        false
                        false
                        false
                        false
                        false
                        false
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        """,
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
                """
                        a
                        false
                        false
                        false
                        true
                        false
                        false
                        true
                        true
                        true
                        true
                        """,
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
                """
                        a
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testBooleanToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                """
                        b\ta
                        false\tfalse
                        false\tfalse
                        false\tfalse
                        true\ttrue
                        false\tfalse
                        false\tfalse
                        true\ttrue
                        true\ttrue
                        true\ttrue
                        true\ttrue
                        false\tfalse
                        false\tfalse
                        false\tfalse
                        true\ttrue
                        true\ttrue
                        true\ttrue
                        true\ttrue
                        false\tfalse
                        false\tfalse
                        false\tfalse
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.000000Z
                        1970-01-01T00:00:00.000000Z
                        1970-01-01T00:00:00.000000Z
                        1970-01-01T00:00:00.000001Z
                        1970-01-01T00:00:00.000000Z
                        1970-01-01T00:00:00.000000Z
                        1970-01-01T00:00:00.000001Z
                        1970-01-01T00:00:00.000001Z
                        1970-01-01T00:00:00.000001Z
                        1970-01-01T00:00:00.000001Z
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.000000000Z
                        1970-01-01T00:00:00.000000000Z
                        1970-01-01T00:00:00.000000000Z
                        1970-01-01T00:00:00.000000001Z
                        1970-01-01T00:00:00.000000000Z
                        1970-01-01T00:00:00.000000000Z
                        1970-01-01T00:00:00.000000001Z
                        1970-01-01T00:00:00.000000001Z
                        1970-01-01T00:00:00.000000001Z
                        1970-01-01T00:00:00.000000001Z
                        """,
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
                """
                        a
                        false
                        false
                        false
                        true
                        false
                        false
                        true
                        true
                        true
                        true
                        """,
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
                """
                        a
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testBooleanToVarcharSort() throws Exception {
        assertQuery(
                """
                        x
                        false
                        false
                        false
                        false
                        false
                        false
                        false
                        false
                        false
                        false
                        false
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        """,
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
                """
                        a
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        """,
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
                """
                        a
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        """,
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
                """
                        a
                        76
                        102
                        27
                        87
                        79
                        79
                        122
                        83
                        90
                        76
                        """,
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
                """
                        a
                        L
                        f
                        \u001B
                        W
                        O
                        O
                        z
                        S
                        Z
                        L
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.076Z
                        1970-01-01T00:00:00.102Z
                        1970-01-01T00:00:00.027Z
                        1970-01-01T00:00:00.087Z
                        1970-01-01T00:00:00.079Z
                        1970-01-01T00:00:00.079Z
                        1970-01-01T00:00:00.122Z
                        1970-01-01T00:00:00.083Z
                        1970-01-01T00:00:00.090Z
                        1970-01-01T00:00:00.076Z
                        """,
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
                """
                        a
                        76.0
                        102.0
                        27.0
                        87.0
                        79.0
                        79.0
                        122.0
                        83.0
                        90.0
                        76.0
                        """,
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
                """
                        a
                        76.0
                        102.0
                        27.0
                        87.0
                        79.0
                        79.0
                        122.0
                        83.0
                        90.0
                        76.0
                        """,
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
                """
                        a
                        76
                        102
                        27
                        87
                        79
                        79
                        122
                        83
                        90
                        76
                        """,
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
                """
                        a
                        76
                        102
                        27
                        87
                        79
                        79
                        122
                        83
                        90
                        76
                        """,
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
                """
                        a
                        0x4c
                        0x66
                        0x1b
                        0x57
                        0x4f
                        0x4f
                        0x7a
                        0x53
                        0x5a
                        0x4c
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testByteToLong256Sort() throws Exception {
        assertQuery(
                """
                        x
                        0x15
                        0x1b
                        0x20
                        0x37
                        0x4a
                        0x4a
                        0x4c
                        0x4c
                        0x4f
                        0x4f
                        0x53
                        0x53
                        0x54
                        0x54
                        0x57
                        0x58
                        0x5a
                        0x5b
                        0x66
                        0x7a
                        """,
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
                """
                        a
                        76
                        102
                        27
                        87
                        79
                        79
                        122
                        83
                        90
                        76
                        """,
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
                """
                        a
                        76
                        102
                        27
                        87
                        79
                        79
                        122
                        83
                        90
                        76
                        """,
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
                """
                        a
                        34
                        34
                        34
                        34
                        34
                        34
                        34
                        34
                        34
                        34
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testByteToStrSort() throws Exception {
        assertQuery(
                """
                        x
                        102
                        122
                        21
                        27
                        32
                        55
                        74
                        74
                        76
                        76
                        79
                        79
                        83
                        83
                        84
                        84
                        87
                        88
                        90
                        91
                        """,
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
                """
                        a
                        76
                        102
                        27
                        87
                        79
                        79
                        122
                        83
                        90
                        76
                        """,
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
                """
                        a
                        14
                        14
                        14
                        14
                        14
                        14
                        14
                        14
                        14
                        14
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testByteToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                """
                        b\ta
                        76\t76
                        102\t102
                        27\t27
                        87\t87
                        79\t79
                        79\t79
                        122\t122
                        83\t83
                        90\t90
                        76\t76
                        84\t84
                        84\t84
                        74\t74
                        55\t55
                        83\t83
                        88\t88
                        32\t32
                        21\t21
                        91\t91
                        74\t74
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.000076Z
                        1970-01-01T00:00:00.000102Z
                        1970-01-01T00:00:00.000027Z
                        1970-01-01T00:00:00.000087Z
                        1970-01-01T00:00:00.000079Z
                        1970-01-01T00:00:00.000079Z
                        1970-01-01T00:00:00.000122Z
                        1970-01-01T00:00:00.000083Z
                        1970-01-01T00:00:00.000090Z
                        1970-01-01T00:00:00.000076Z
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.000000076Z
                        1970-01-01T00:00:00.000000102Z
                        1970-01-01T00:00:00.000000027Z
                        1970-01-01T00:00:00.000000087Z
                        1970-01-01T00:00:00.000000079Z
                        1970-01-01T00:00:00.000000079Z
                        1970-01-01T00:00:00.000000122Z
                        1970-01-01T00:00:00.000000083Z
                        1970-01-01T00:00:00.000000090Z
                        1970-01-01T00:00:00.000000076Z
                        """,
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
                """
                        a
                        76
                        102
                        27
                        87
                        79
                        79
                        122
                        83
                        90
                        76
                        """,
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
                """
                        a
                        34
                        34
                        34
                        34
                        34
                        34
                        34
                        34
                        34
                        34
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testByteToVarcharSort() throws Exception {
        assertQuery(
                """
                        x
                        102
                        122
                        21
                        27
                        32
                        55
                        74
                        74
                        76
                        76
                        79
                        79
                        83
                        83
                        84
                        84
                        87
                        88
                        90
                        91
                        """,
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
                """
                        cast
                        false
                        true
                        true
                        true
                        false
                        false
                        """,
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
                """
                        a
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        """,
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
                """
                        a
                        7
                        0
                        8
                        0
                        7
                        1
                        2
                        1
                        6
                        3
                        """,
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
                """
                        a
                        V
                        T
                        J
                        W
                        C
                        P
                        S
                        W
                        H
                        Y
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.007Z
                        1970-01-01T00:00:00.000Z
                        1970-01-01T00:00:00.008Z
                        1970-01-01T00:00:00.000Z
                        1970-01-01T00:00:00.007Z
                        1970-01-01T00:00:00.001Z
                        1970-01-01T00:00:00.002Z
                        1970-01-01T00:00:00.001Z
                        1970-01-01T00:00:00.006Z
                        1970-01-01T00:00:00.003Z
                        """,
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
                """
                        a
                        7.0
                        0.0
                        8.0
                        0.0
                        7.0
                        1.0
                        2.0
                        1.0
                        6.0
                        3.0
                        """,
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
                """
                        a
                        7.0
                        0.0
                        8.0
                        0.0
                        7.0
                        1.0
                        2.0
                        1.0
                        6.0
                        3.0
                        """,
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
                """
                        a
                        7
                        0
                        8
                        0
                        7
                        1
                        2
                        1
                        6
                        3
                        """,
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
                """
                        a
                        7
                        0
                        8
                        0
                        7
                        1
                        2
                        1
                        6
                        3
                        """,
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
                """
                        a
                        0x07
                        0x00
                        0x08
                        0x00
                        0x07
                        0x01
                        0x02
                        0x01
                        0x06
                        0x03
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testCharToLong256Sort() throws Exception {
        assertQuery(
                """
                        x
                        0x30
                        0x31
                        0x31
                        0x31
                        0x32
                        0x32
                        0x33
                        0x34
                        0x34
                        0x37
                        0x37
                        0x37
                        0x38
                        0x38
                        0x38
                        0x38
                        0x38
                        0x39
                        0x39
                        0x39
                        """,
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
                """
                        a
                        7
                        0
                        8
                        0
                        7
                        1
                        2
                        1
                        6
                        3
                        """,
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
                """
                        a
                        V
                        T
                        J
                        W
                        C
                        P
                        S
                        W
                        H
                        Y
                        """,
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
                """
                        a
                        A
                        A
                        A
                        A
                        A
                        A
                        A
                        A
                        A
                        A
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testCharToStrSort() throws Exception {
        assertQuery(
                """
                        x
                        C
                        E
                        G
                        H
                        H
                        J
                        N
                        P
                        P
                        R
                        R
                        S
                        T
                        V
                        W
                        W
                        X
                        X
                        Y
                        Z
                        """,
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
                """
                        a
                        V
                        T
                        J
                        W
                        C
                        P
                        S
                        W
                        H
                        Y
                        """,
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
                """
                        a
                        X
                        X
                        X
                        X
                        X
                        X
                        X
                        X
                        X
                        X
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testCharToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                """
                        b\ta
                        V\tV
                        T\tT
                        J\tJ
                        W\tW
                        C\tC
                        P\tP
                        S\tS
                        W\tW
                        H\tH
                        Y\tY
                        R\tR
                        X\tX
                        P\tP
                        E\tE
                        H\tH
                        N\tN
                        R\tR
                        X\tX
                        G\tG
                        Z\tZ
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.000007Z
                        1970-01-01T00:00:00.000000Z
                        1970-01-01T00:00:00.000008Z
                        1970-01-01T00:00:00.000000Z
                        1970-01-01T00:00:00.000007Z
                        1970-01-01T00:00:00.000001Z
                        1970-01-01T00:00:00.000002Z
                        1970-01-01T00:00:00.000001Z
                        1970-01-01T00:00:00.000006Z
                        1970-01-01T00:00:00.000003Z
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.000000007Z
                        1970-01-01T00:00:00.000000000Z
                        1970-01-01T00:00:00.000000008Z
                        1970-01-01T00:00:00.000000000Z
                        1970-01-01T00:00:00.000000007Z
                        1970-01-01T00:00:00.000000001Z
                        1970-01-01T00:00:00.000000002Z
                        1970-01-01T00:00:00.000000001Z
                        1970-01-01T00:00:00.000000006Z
                        1970-01-01T00:00:00.000000003Z
                        """,
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
                """
                        a
                        V
                        T
                        J
                        W
                        C
                        P
                        S
                        W
                        H
                        Y
                        """,
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
                """
                        a
                        A
                        A
                        A
                        A
                        A
                        A
                        A
                        A
                        A
                        A
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testCharToVarcharSort() throws Exception {
        assertQuery(
                """
                        x
                        C
                        E
                        G
                        H
                        H
                        J
                        N
                        P
                        P
                        R
                        R
                        S
                        T
                        V
                        W
                        W
                        X
                        X
                        Y
                        Z
                        """,
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
                """
                        a
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        """,
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
                """
                        a
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        """,
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
                """
                        a
                        97
                        0
                        100
                        99
                        0
                        97
                        97
                        98
                        0
                        96
                        """,
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
                """
                        a
                        7
                        
                        9
                        0
                        
                        2
                        +
                        4
                        
                        -
                        """,
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
                """
                        a
                        1970-01-01T01:30:37.285Z
                        
                        1970-01-01T01:39:20.648Z
                        1970-01-01T02:26:19.401Z
                        
                        1970-01-01T01:31:53.880Z
                        1970-01-01T01:32:15.804Z
                        1970-01-01T02:38:18.131Z
                        
                        1970-01-01T00:42:01.595Z
                        """,
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
                """
                        a
                        67.0
                        null
                        30.0
                        99.0
                        null
                        137.0
                        127.0
                        58.0
                        null
                        111.0
                        """,
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
                """
                        a
                        67.0
                        null
                        30.0
                        99.0
                        null
                        137.0
                        127.0
                        58.0
                        null
                        111.0
                        """,
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
                """
                        a
                        985257636
                        null
                        968130026
                        555619965
                        null
                        712286238
                        215755333
                        720037886
                        null
                        129724714
                        """,
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
                """
                        a
                        13992367
                        null
                        7587030
                        11082999
                        null
                        602537
                        5112277
                        5361808
                        null
                        8600061
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testDateToLong256Sort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        
                        
                        
                        
                        
                        0x08
                        0x11
                        0x1e
                        0x34
                        0x3d
                        0x4d
                        0x57
                        0x63
                        0x80
                        0x89
                        0xa7
                        0xc0
                        0xc7
                        """,
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
                """
                        a
                        31
                        0
                        54
                        23
                        0
                        29
                        33
                        24
                        0
                        51
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.055Z
                        1970-01-01T00:00:00.048Z
                        1970-01-01T00:00:00.055Z
                        
                        1970-01-01T00:00:00.045Z
                        1970-01-01T00:00:00.036Z
                        1970-01-01T00:00:00.034Z
                        1970-01-01T00:00:00.058Z
                        1970-01-01T00:00:00.045Z
                        1970-01-01T00:00:00.061Z
                        """,
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
                """
                        a
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testDateToStrSort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        
                        
                        
                        
                        
                        1970-01-01T00:00:00.008Z
                        1970-01-01T00:00:00.017Z
                        1970-01-01T00:00:00.030Z
                        1970-01-01T00:00:00.052Z
                        1970-01-01T00:00:00.061Z
                        1970-01-01T00:00:00.077Z
                        1970-01-01T00:00:00.087Z
                        1970-01-01T00:00:00.099Z
                        1970-01-01T00:00:00.128Z
                        1970-01-01T00:00:00.137Z
                        1970-01-01T00:00:00.167Z
                        1970-01-01T00:00:00.192Z
                        1970-01-01T00:00:00.199Z
                        """,
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
                """
                        a
                        67
                        
                        30
                        99
                        
                        137
                        127
                        58
                        
                        111
                        """,
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
                """
                        a
                        601
                        601
                        601
                        601
                        601
                        601
                        601
                        601
                        601
                        601
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testDateToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                """
                        b\ta
                        20\t1970-01-01T00:00:00.020Z
                        \t
                        11\t1970-01-01T00:00:00.011Z
                        13\t1970-01-01T00:00:00.013Z
                        \t
                        15\t1970-01-01T00:00:00.015Z
                        19\t1970-01-01T00:00:00.019Z
                        17\t1970-01-01T00:00:00.017Z
                        \t
                        10\t1970-01-01T00:00:00.010Z
                        \t
                        17\t1970-01-01T00:00:00.017Z
                        \t
                        17\t1970-01-01T00:00:00.017Z
                        18\t1970-01-01T00:00:00.018Z
                        18\t1970-01-01T00:00:00.018Z
                        \t
                        12\t1970-01-01T00:00:00.012Z
                        11\t1970-01-01T00:00:00.011Z
                        15\t1970-01-01T00:00:00.015Z
                        """,
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
                """
                        a
                        1970-01-01T00:01:38.083000Z
                        
                        1970-01-01T00:00:06.240000Z
                        1970-01-01T00:00:53.076000Z
                        
                        1970-01-01T00:01:03.779000Z
                        1970-01-01T00:01:23.737000Z
                        1970-01-01T00:02:23.935000Z
                        
                        1970-01-01T00:01:15.474000Z
                        """,
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
                """
                        a
                        1970-01-01T00:01:38.083000000Z
                        
                        1970-01-01T00:00:06.240000000Z
                        1970-01-01T00:00:53.076000000Z
                        
                        1970-01-01T00:01:03.779000000Z
                        1970-01-01T00:01:23.737000000Z
                        1970-01-01T00:02:23.935000000Z
                        
                        1970-01-01T00:01:15.474000000Z
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.055Z
                        1970-01-01T00:00:00.048Z
                        1970-01-01T00:00:00.055Z
                        
                        1970-01-01T00:00:00.045Z
                        1970-01-01T00:00:00.036Z
                        1970-01-01T00:00:00.034Z
                        1970-01-01T00:00:00.058Z
                        1970-01-01T00:00:00.045Z
                        1970-01-01T00:00:00.061Z
                        """,
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
                """
                        a
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testDateToVarcharSort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        
                        
                        
                        
                        
                        1970-01-01T00:00:00.008Z
                        1970-01-01T00:00:00.017Z
                        1970-01-01T00:00:00.030Z
                        1970-01-01T00:00:00.052Z
                        1970-01-01T00:00:00.061Z
                        1970-01-01T00:00:00.077Z
                        1970-01-01T00:00:00.087Z
                        1970-01-01T00:00:00.099Z
                        1970-01-01T00:00:00.128Z
                        1970-01-01T00:00:00.137Z
                        1970-01-01T00:00:00.167Z
                        1970-01-01T00:00:00.192Z
                        1970-01-01T00:00:00.199Z
                        """,
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
                """
                        a
                        8
                        0
                        0
                        6
                        7
                        2
                        3
                        7
                        4
                        0
                        """,
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
                """
                        a
                        1
                        7
                        )
                        %
                        5
                        0
                        *
                        8
                        7
                        !
                        """,
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
                """
                        a
                        1970-01-01T00:00:08.043Z
                        1970-01-01T00:00:00.848Z
                        1970-01-01T00:00:00.843Z
                        1970-01-01T00:00:06.508Z
                        1970-01-01T00:00:07.905Z
                        1970-01-01T00:00:02.245Z
                        1970-01-01T00:00:03.491Z
                        1970-01-01T00:00:07.611Z
                        1970-01-01T00:00:04.217Z
                        
                        """,
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
                """
                        a
                        0.8043224099968393
                        0.08486964232560668
                        0.0843832076262595
                        0.6508594025855301
                        0.7905675319675964
                        0.22452340856088226
                        0.3491070363730514
                        0.7611029514995744
                        0.4217768841969397
                        null
                        """,
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
                """
                        a
                        0.8043224
                        0.084869646
                        0.084383205
                        0.6508594
                        0.7905675
                        0.22452341
                        0.34910703
                        0.761103
                        0.4217769
                        null
                        """,
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
                """
                        a
                        66
                        22
                        8
                        29
                        20
                        65
                        84
                        98
                        22
                        50
                        """,
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
                """
                        a
                        804
                        84
                        84
                        650
                        790
                        224
                        349
                        761
                        421
                        null
                        """,
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
                """
                        a
                        0x0c45e2
                        0x014b85
                        0x01499f
                        0x09ee6b
                        0x0c1027
                        0x036d0b
                        0x0553b3
                        0x0b9d0e
                        0x066f90
                        
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testDoubleToLong256Sort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        0x0f52
                        0x016749
                        0x01695b
                        0x03bbfa
                        0x042050
                        0x05780b
                        0x05ce6e
                        0x0668f5
                        0x0703d8
                        0x0706a6
                        0x0a9d2b
                        0x0ad33c
                        0x0bcca5
                        0x0c13a7
                        0x0ca8a2
                        0x0d2616
                        0x0d60a7
                        0x10405a
                        """,
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
                """
                        a
                        8
                        0
                        0
                        6
                        7
                        2
                        3
                        7
                        4
                        0
                        """,
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
                """
                        a
                        0.6607777894187332
                        0.2246301342497259
                        0.08486964232560668
                        0.299199045961845
                        0.20447441837877756
                        0.6508594025855301
                        0.8423410920883345
                        0.9856290845874263
                        0.22452340856088226
                        0.5093827001617407
                        """,
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
                """
                        a
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testDoubleToStrSort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        0.0035983672154330515
                        0.0843832076262595
                        0.08486964232560668
                        0.22452340856088226
                        0.24808812376657652
                        0.3288176907679504
                        0.3491070363730514
                        0.38539947865244994
                        0.4217768841969397
                        0.4224356661645131
                        0.6381607531178513
                        0.6508594025855301
                        0.7094360487171202
                        0.7261136209823622
                        0.7611029514995744
                        0.7905675319675964
                        0.8043224099968393
                        0.9771103146051203
                        """,
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
                """
                        a
                        0.8043224099968393
                        0.08486964232560668
                        0.0843832076262595
                        0.6508594025855301
                        0.7905675319675964
                        0.22452340856088226
                        0.3491070363730514
                        0.7611029514995744
                        0.4217768841969397
                        
                        """,
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
                """
                        a
                        1.5
                        1.5
                        1.5
                        1.5
                        1.5
                        1.5
                        1.5
                        1.5
                        1.5
                        1.5
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testDoubleToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                """
                        b\ta
                        0.8043224099968393\t0.8043224099968393
                        0.08486964232560668\t0.08486964232560668
                        0.0843832076262595\t0.0843832076262595
                        0.6508594025855301\t0.6508594025855301
                        0.7905675319675964\t0.7905675319675964
                        0.22452340856088226\t0.22452340856088226
                        0.3491070363730514\t0.3491070363730514
                        0.7611029514995744\t0.7611029514995744
                        0.4217768841969397\t0.4217768841969397
                        \tnull
                        0.7261136209823622\t0.7261136209823622
                        0.4224356661645131\t0.4224356661645131
                        0.7094360487171202\t0.7094360487171202
                        0.38539947865244994\t0.38539947865244994
                        0.0035983672154330515\t0.0035983672154330515
                        0.3288176907679504\t0.3288176907679504
                        \tnull
                        0.9771103146051203\t0.9771103146051203
                        0.24808812376657652\t0.24808812376657652
                        0.6381607531178513\t0.6381607531178513
                        """,
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
                """
                        a
                        1970-01-01T00:01:20.432240Z
                        1970-01-01T00:00:08.486964Z
                        1970-01-01T00:00:08.438320Z
                        1970-01-01T00:01:05.085940Z
                        1970-01-01T00:01:19.056753Z
                        1970-01-01T00:00:22.452340Z
                        1970-01-01T00:00:34.910703Z
                        1970-01-01T00:01:16.110295Z
                        1970-01-01T00:00:42.177688Z
                        
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.080432240Z
                        1970-01-01T00:00:00.008486964Z
                        1970-01-01T00:00:00.008438320Z
                        1970-01-01T00:00:00.065085940Z
                        1970-01-01T00:00:00.079056753Z
                        1970-01-01T00:00:00.022452340Z
                        1970-01-01T00:00:00.034910703Z
                        1970-01-01T00:00:00.076110295Z
                        1970-01-01T00:00:00.042177688Z
                        
                        """,
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
                """
                        a
                        0.6607777894187332
                        0.2246301342497259
                        0.08486964232560668
                        0.299199045961845
                        0.20447441837877756
                        0.6508594025855301
                        0.8423410920883345
                        0.9856290845874263
                        0.22452340856088226
                        0.5093827001617407
                        """,
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
                """
                        a
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testDoubleToVarcharSort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        0.0035983672154330515
                        0.0843832076262595
                        0.08486964232560668
                        0.22452340856088226
                        0.24808812376657652
                        0.3288176907679504
                        0.3491070363730514
                        0.38539947865244994
                        0.4217768841969397
                        0.4224356661645131
                        0.6381607531178513
                        0.6508594025855301
                        0.7094360487171202
                        0.7261136209823622
                        0.7611029514995744
                        0.7905675319675964
                        0.8043224099968393
                        0.9771103146051203
                        """,
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
                """
                        a
                        66
                        80
                        22
                        12
                        8
                        28
                        29
                        8
                        20
                        93
                        """,
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
                """
                        a
                        1
                        &
                        7
                        -
                        )
                        $
                        %
                        )
                        5
                        3
                        """,
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
                """
                        a
                        1970-01-01T00:13:24.322Z
                        
                        1970-01-01T00:01:24.869Z
                        1970-01-01T00:04:59.199Z
                        
                        1970-01-01T00:15:34.460Z
                        1970-01-01T00:02:11.233Z
                        1970-01-01T00:13:10.567Z
                        
                        1970-01-01T00:03:44.523Z
                        """,
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
                """
                        a
                        0.804322361946106
                        null
                        0.0848696231842041
                        0.29919904470443726
                        null
                        0.934460461139679
                        0.1312335729598999
                        0.7905675172805786
                        null
                        0.2245233654975891
                        """,
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
                """
                        a
                        0.80432236
                        null
                        0.08486962
                        0.29919904
                        null
                        0.93446046
                        0.13123357
                        0.7905675
                        null
                        0.22452337
                        """,
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
                """
                        a
                        8043
                        null
                        848
                        2991
                        null
                        9344
                        1312
                        7905
                        null
                        2245
                        """,
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
                """
                        fl\ta
                        0.804322361946106\t8043223619
                        null\tnull
                        0.0848696231842041\t848696231
                        0.29919904470443726\t2991990447
                        null\tnull
                        0.934460461139679\t9344604611
                        0.1312335729598999\t1312335729
                        0.7905675172805786\t7905675172
                        null\tnull
                        0.2245233654975891\t2245233654
                        """,
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
                """
                        a
                        0x0c45e2
                        
                        0x014b85
                        0x0490bf
                        
                        0x0e423c
                        0x0200a1
                        0x0c1027
                        
                        0x036d0b
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testFloatToLong256Sort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        
                        
                        
                        
                        0x01695b
                        0x022ec4
                        0x03bbfa
                        0x04f9ee
                        0x05ce6e
                        0x08b8ad
                        0x095004
                        0x0a709b
                        0x0b221c
                        0x0c13a7
                        0x0ca8a2
                        0x0d2616
                        0x0d60a7
                        0x0f8ac1
                        """,
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
                """
                        a
                        660
                        804
                        224
                        129
                        84
                        284
                        299
                        84
                        204
                        934
                        """,
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
                """
                        a
                        0.66077775
                        0.80432236
                        0.22463012
                        0.12966657
                        0.08486962
                        0.28455776
                        0.29919904
                        0.08438319
                        0.20447439
                        0.93446046
                        """,
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
                """
                        a
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testFloatToStrSort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        
                        
                        
                        
                        0.08486962
                        0.13123357
                        0.22452337
                        0.29919904
                        0.34910703
                        0.5243723
                        0.55991614
                        0.6276954
                        0.6693837
                        0.7261136
                        0.7611029
                        0.7905675
                        0.80432236
                        0.93446046
                        """,
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
                """
                        a
                        0.80432236
                        
                        0.08486962
                        0.29919904
                        
                        0.93446046
                        0.13123357
                        0.7905675
                        
                        0.22452337
                        """,
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
                """
                        a
                        1.5
                        1.5
                        1.5
                        1.5
                        1.5
                        1.5
                        1.5
                        1.5
                        1.5
                        1.5
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testFloatToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                """
                        b\ta
                        0.80432236\t0.80432236
                        \tnull
                        0.08486962\t0.08486962
                        0.29919904\t0.29919904
                        \tnull
                        0.93446046\t0.93446046
                        0.13123357\t0.13123357
                        0.7905675\t0.7905675
                        \tnull
                        0.22452337\t0.22452337
                        \tnull
                        0.34910703\t0.34910703
                        \tnull
                        0.7611029\t0.7611029
                        0.5243723\t0.5243723
                        0.55991614\t0.55991614
                        \tnull
                        0.7261136\t0.7261136
                        0.6276954\t0.6276954
                        0.6693837\t0.6693837
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.080432240Z
                        
                        1970-01-01T00:00:00.008486962Z
                        1970-01-01T00:00:00.029919904Z
                        
                        1970-01-01T00:00:00.093446048Z
                        1970-01-01T00:00:00.013123357Z
                        1970-01-01T00:00:00.079056752Z
                        
                        1970-01-01T00:00:00.022452336Z
                        """,
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
                """
                        a
                        1970-01-01T00:01:20.432240Z
                        
                        1970-01-01T00:00:08.486962Z
                        1970-01-01T00:00:29.919904Z
                        
                        1970-01-01T00:01:33.446048Z
                        1970-01-01T00:00:13.123357Z
                        1970-01-01T00:01:19.056752Z
                        
                        1970-01-01T00:00:22.452336Z
                        """,
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
                """
                        a
                        0.66077775
                        0.80432236
                        0.22463012
                        0.12966657
                        0.08486962
                        0.28455776
                        0.29919904
                        0.08438319
                        0.20447439
                        0.93446046
                        """,
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
                """
                        a
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        1.34
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testFloatToVarcharSort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        
                        
                        
                        
                        0.08486962
                        0.13123357
                        0.22452337
                        0.29919904
                        0.34910703
                        0.5243723
                        0.55991614
                        0.6276954
                        0.6693837
                        0.7261136
                        0.7611029
                        0.7905675
                        0.80432236
                        0.93446046
                        """,
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
                """
                        a
                        9v1s8hm7wpks
                        46swgj10r88k
                        jnw97u4yuquw
                        zfuqd3bf8hbu
                        hp4muv5tgg3q
                        wh4b6vntdq1c
                        s2z2fydsjq5n
                        1cjjwk6r9jfe
                        mmt89425bhff
                        71ftmpy5v1uy
                        """,
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
                """
                        a
                        9v1s8hm7wpks
                        46swgj10r88k
                        jnw97u4yuquw
                        zfuqd3bf8hbu
                        hp4muv5tgg3q
                        wh4b6vntdq1c
                        s2z2fydsjq5n
                        1cjjwk6r9jfe
                        mmt89425bhff
                        71ftmpy5v1uy
                        """,
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
                """
                        a
                        171.30.189.77
                        201.100.238.229
                        111.221.228.130
                        
                        111.221.228.130
                        
                        201.100.238.229
                        
                        201.100.238.229
                        171.30.189.77
                        """,
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
                """
                        a
                        192.168.0.1
                        192.168.0.1
                        192.168.0.1
                        192.168.0.1
                        192.168.0.1
                        192.168.0.1
                        192.168.0.1
                        192.168.0.1
                        192.168.0.1
                        192.168.0.1
                        """,
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
                """
                        a
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        """,
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
                """
                        column
                        true
                        """,
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
                """
                        a
                        true
                        true
                        true
                        false
                        true
                        true
                        true
                        true
                        true
                        true
                        """,
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
                """
                        a
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        """,
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
                """
                        a
                        41
                        28
                        0
                        100
                        5
                        72
                        72
                        24
                        53
                        50
                        """,
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
                """
                        a
                        (
                        <
                        9
                        
                        %
                        &
                        &
                        8
                        4
                        1
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.019Z
                        1970-01-01T00:00:00.072Z
                        1970-01-01T00:00:00.090Z
                        
                        1970-01-01T00:00:00.007Z
                        1970-01-01T00:00:00.017Z
                        1970-01-01T00:00:00.065Z
                        1970-01-01T00:00:00.032Z
                        1970-01-01T00:00:00.067Z
                        1970-01-01T00:00:00.106Z
                        """,
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
                """
                        a
                        19.0
                        72.0
                        90.0
                        null
                        7.0
                        17.0
                        65.0
                        32.0
                        67.0
                        106.0
                        """,
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
                """
                        a
                        19.0
                        72.0
                        90.0
                        null
                        7.0
                        17.0
                        65.0
                        32.0
                        67.0
                        106.0
                        """,
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
                """
                        a
                        40
                        60
                        57
                        null
                        37
                        38
                        38
                        56
                        52
                        49
                        """,
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
                """
                        a
                        19
                        72
                        90
                        null
                        7
                        17
                        65
                        32
                        67
                        106
                        """,
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
                """
                        a
                        0x28
                        0x3c
                        0x39
                        
                        0x25
                        0x26
                        0x26
                        0x38
                        0x34
                        0x31
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testIntToLong256Sort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        
                        
                        
                        
                        
                        0x2b
                        0x31
                        0x33
                        0x38
                        0x4b
                        0x66
                        0x68
                        0x69
                        0x6a
                        0x75
                        0x77
                        0xad
                        0xc6
                        """,
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
                """
                        a
                        37
                        48
                        30
                        0
                        55
                        51
                        53
                        54
                        23
                        34
                        """,
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
                """
                        a
                        40
                        60
                        57
                        
                        37
                        38
                        38
                        56
                        52
                        49
                        """,
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
                """
                        a
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testIntToStrSort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        
                        
                        
                        
                        
                        102
                        104
                        105
                        106
                        117
                        119
                        173
                        198
                        43
                        49
                        51
                        56
                        75
                        """,
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
                """
                        a
                        19
                        72
                        90
                        
                        7
                        17
                        65
                        32
                        67
                        106
                        """,
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
                """
                        a
                        601
                        601
                        601
                        601
                        601
                        601
                        601
                        601
                        601
                        601
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testIntToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                """
                        b\ta
                        16\t16
                        \tnull
                        11\t11
                        20\t20
                        \tnull
                        11\t11
                        12\t12
                        18\t18
                        \tnull
                        17\t17
                        \tnull
                        16\t16
                        \tnull
                        19\t19
                        15\t15
                        15\t15
                        \tnull
                        12\t12
                        15\t15
                        18\t18
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.000019Z
                        1970-01-01T00:00:00.000072Z
                        1970-01-01T00:00:00.000090Z
                        
                        1970-01-01T00:00:00.000007Z
                        1970-01-01T00:00:00.000017Z
                        1970-01-01T00:00:00.000065Z
                        1970-01-01T00:00:00.000032Z
                        1970-01-01T00:00:00.000067Z
                        1970-01-01T00:00:00.000106Z
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.000000019Z
                        1970-01-01T00:00:00.000000072Z
                        1970-01-01T00:00:00.000000090Z
                        
                        1970-01-01T00:00:00.000000007Z
                        1970-01-01T00:00:00.000000017Z
                        1970-01-01T00:00:00.000000065Z
                        1970-01-01T00:00:00.000000032Z
                        1970-01-01T00:00:00.000000067Z
                        1970-01-01T00:00:00.000000106Z
                        """,
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
                """
                        a
                        40
                        60
                        57
                        
                        37
                        38
                        38
                        56
                        52
                        49
                        """,
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
                """
                        a
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testIntToVarcharSort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        
                        
                        
                        
                        
                        102
                        104
                        105
                        106
                        117
                        119
                        173
                        198
                        43
                        49
                        51
                        56
                        75
                        """,
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
                """
                        cast
                        ('1991-11-08T09:53:57.643Z', '1995-12-22T06:21:36.636Z')
                        ('1987-02-10T05:53:36.329Z', '1992-01-23T11:30:00.830Z')
                        ('1976-02-09T03:40:32.587Z', '1979-08-18T11:00:20.247Z')
                        ('2028-07-16T17:59:01.082Z', '2029-03-05T12:49:29.174Z')
                        ('2014-08-18T15:50:20.864Z', '2016-02-17T19:58:31.466Z')
                        ('1998-08-22T09:31:10.281Z', '2003-08-09T14:59:20.394Z')
                        ('2031-04-13T21:30:51.977Z', '2033-04-24T06:22:27.339Z')
                        ('1998-08-25T13:53:59.100Z', '2000-06-21T03:19:31.403Z')
                        ('2031-02-04T05:18:53.600Z', '2033-08-21T03:18:57.217Z')
                        ('1975-05-26T13:57:40.478Z', '1977-07-09T21:00:52.129Z')
                        """,
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
                """
                        a
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        """,
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
                """
                        a
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        """,
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
                """
                        a
                        -1148479920
                        73575701
                        1868723706
                        -1436881714
                        1569490116
                        1530831067
                        1125579207
                        -85170055
                        -1101822104
                        -1125169127
                        """,
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
                """
                        a
                        4689592037643856
                        8260188555232587029
                        -2653407051020864006
                        7513930126251977934
                        -6943924477733600060
                        7953532976996720859
                        -3985256597569472057
                        -8671107786057422727
                        -4485747798769957016
                        375856366519011353
                        """,
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
                """
                        a
                        0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650
                        0xb5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa65572a215ba0462ad15
                        0x322a2198864beb14797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fa
                        0xc1e631285c1ab288c72bfc5230158059980eca62a219a0f16846d7a3aa5aecce
                        0x4b0f595f143e5d722f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4
                        0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db
                        0xa0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88c8b1863d4316f9c7
                        0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879
                        0x523eb59d99c647af9840ad8800156d26c718ab5cbb3fd261c1bf6c24be538768
                        0x5b9832d4b5522a9474ce62a98a4516952705e02c613acfc405374f5fbcef4819
                        """,
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
                """
                        a
                        0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650
                        0xb5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa65572a215ba0462ad15
                        0x322a2198864beb14797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fa
                        0xc1e631285c1ab288c72bfc5230158059980eca62a219a0f16846d7a3aa5aecce
                        0x4b0f595f143e5d722f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4
                        0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db
                        0xa0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88c8b1863d4316f9c7
                        0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879
                        0x523eb59d99c647af9840ad8800156d26c718ab5cbb3fd261c1bf6c24be538768
                        0x5b9832d4b5522a9474ce62a98a4516952705e02c613acfc405374f5fbcef4819
                        """,
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
                """
                        a
                        true
                        true
                        true
                        false
                        true
                        true
                        true
                        true
                        true
                        true
                        """,
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
                """
                        a
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        """,
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
                """
                        a
                        97
                        96
                        0
                        99
                        97
                        98
                        100
                        100
                        96
                        97
                        """,
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
                """
                        a
                        7
                        0
                        7
                        
                        -
                        $
                        "
                        :
                        -
                        =
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.067Z
                        1970-01-01T00:00:00.126Z
                        1970-01-01T00:00:00.124Z
                        
                        1970-01-01T00:00:00.057Z
                        1970-01-01T00:00:00.033Z
                        1970-01-01T00:00:00.085Z
                        1970-01-01T00:00:00.040Z
                        1970-01-01T00:00:00.111Z
                        1970-01-01T00:00:00.112Z
                        """,
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
                """
                        a
                        67.0
                        126.0
                        124.0
                        null
                        57.0
                        33.0
                        85.0
                        40.0
                        111.0
                        112.0
                        """,
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
                """
                        a
                        67.0
                        126.0
                        124.0
                        null
                        57.0
                        33.0
                        85.0
                        40.0
                        111.0
                        112.0
                        """,
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
                """
                        a
                        55
                        48
                        55
                        null
                        45
                        36
                        34
                        58
                        45
                        61
                        """,
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
                """
                        a
                        67
                        126
                        124
                        null
                        57
                        33
                        85
                        40
                        111
                        112
                        """,
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
                """
                        a
                        0x37
                        0x30
                        0x37
                        
                        0x2d
                        0x24
                        0x22
                        0x3a
                        0x2d
                        0x3d
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToLong256Sort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        
                        
                        
                        
                        
                        0x08
                        0x11
                        0x1e
                        0x34
                        0x3d
                        0x4d
                        0x57
                        0x63
                        0x80
                        0x89
                        0xa7
                        0xc0
                        0xc7
                        """,
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
                """
                        a
                        31
                        26
                        38
                        0
                        47
                        41
                        53
                        46
                        51
                        46
                        """,
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
                """
                        a
                        55
                        48
                        55
                        
                        45
                        36
                        34
                        58
                        45
                        61
                        """,
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
                """
                        a
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToStrSort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        
                        
                        
                        
                        
                        128
                        137
                        167
                        17
                        192
                        199
                        30
                        52
                        61
                        77
                        8
                        87
                        99
                        """,
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
                """
                        a
                        67
                        126
                        124
                        
                        57
                        33
                        85
                        40
                        111
                        112
                        """,
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
                """
                        a
                        601
                        601
                        601
                        601
                        601
                        601
                        601
                        601
                        601
                        601
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                """
                        b\ta
                        20\t20
                        \tnull
                        11\t11
                        13\t13
                        \tnull
                        15\t15
                        19\t19
                        17\t17
                        \tnull
                        10\t10
                        \tnull
                        17\t17
                        \tnull
                        17\t17
                        18\t18
                        18\t18
                        \tnull
                        12\t12
                        11\t11
                        15\t15
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.000067Z
                        1970-01-01T00:00:00.000126Z
                        1970-01-01T00:00:00.000124Z
                        
                        1970-01-01T00:00:00.000057Z
                        1970-01-01T00:00:00.000033Z
                        1970-01-01T00:00:00.000085Z
                        1970-01-01T00:00:00.000040Z
                        1970-01-01T00:00:00.000111Z
                        1970-01-01T00:00:00.000112Z
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.000000067Z
                        1970-01-01T00:00:00.000000126Z
                        1970-01-01T00:00:00.000000124Z
                        
                        1970-01-01T00:00:00.000000057Z
                        1970-01-01T00:00:00.000000033Z
                        1970-01-01T00:00:00.000000085Z
                        1970-01-01T00:00:00.000000040Z
                        1970-01-01T00:00:00.000000111Z
                        1970-01-01T00:00:00.000000112Z
                        """,
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
                """
                        a
                        55
                        48
                        55
                        
                        45
                        36
                        34
                        58
                        45
                        61
                        """,
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
                """
                        a
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testLongToVarcharSort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        
                        
                        
                        
                        
                        128
                        137
                        167
                        17
                        192
                        199
                        30
                        52
                        61
                        77
                        8
                        87
                        99
                        """,
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
                """
                        a
                        
                        
                        
                        
                        
                        
                        
                        
                        
                        
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testSelfCastInGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            CreateTableTestUtils.createAllTableWithNewTypes(engine, PartitionBy.DAY, ColumnType.TIMESTAMP);
            execute(
                    "insert into all2 select * from (" +
                            "select" +
                            " rnd_int()," +
                            " rnd_short()," +
                            " rnd_byte()," +
                            " rnd_double()," +
                            " rnd_float()," +
                            " rnd_long()," +
                            " rnd_str(2,3,0)," +
                            " rnd_symbol('A','D')," +
                            " rnd_boolean()," +
                            " rnd_bin()," +
                            " rnd_date()," +
                            " rnd_long256()," +
                            " rnd_char()," +
                            " rnd_uuid4()," +
                            " rnd_ipv4()," +
                            " rnd_varchar(2,3,0)," +
                            " timestamp_sequence(0L, 10L) ts " +
                            "from long_sequence(3)" +
                            ") timestamp(ts);"
            );

            assertQuery(
                    """
                            ts\ti\tsh\tb\td\tf\tl\tstr\tsym\tbool\tbin\tdat\tl256\tc\tu\tip\tv\tcount
                            1970-01-01T00:00:00.000000Z\t-1148479920\t24814\t27\t0.12966659791573354\t0.2845577597618103\t-7611843578141082998\tYR\tA\tfalse\t00000000 f1 59 88 c4 91 3b 72 db f3 04 1b c7 88 de a0 79
                            00000010 3c 77 15 68 61 26 af 19 c4 95 94 36 53 49 b4 59\t1970-01-01T00:47:07.518Z\t0x63eb3740c80f661e9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b\tD\tcec82869-edec-421b-8259-3f82b430328d\t235.156.195.158\tjF\t1
                            1970-01-01T00:00:00.000010Z\t2085282008\t-1379\t44\t0.12026122412833129\t0.6761934757232666\t8325936937764905778\tQU\tD\ttrue\t00000000 42 fc 31 79 5f 8b 81 2b 93 4d 1a 8e 78 b5 b9 11
                            00000010 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t1970-01-01T00:06:35.663Z\t0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI\ta5f80be4-b45b-4437-8929-90e1a29afcac\t184.92.27.200\tkV\t1
                            1970-01-01T00:00:00.000020Z\t532665695\t-4874\t54\t0.7588175403454873\t0.5406709313392639\t-8081265393416742311\tYCT\tA\tfalse\t00000000 7f d7 6f b8 c9 ae 28 c7 84 47 dc d2 85 7f a5 b8
                            00000010 7b 4a 9d 46 7c 8d dd 93 e6 d0 b3 2b 07 98 cc 76\t1970-01-01T00:19:39.064Z\t0xd25adf928386cdd2d992946a26184664ba453d761efcf9bb7ee6a03f4f930fa3\tS\taf44c40a-67ef-4e1c-9b3e-f21223ee8849\t130.40.224.242\t軦۽㒾\t1
                            """,
                    "select" +
                            " timestamp::timestamp ts," +
                            " int::int i," +
                            " short::short sh," +
                            " byte::byte b," +
                            " double::double d," +
                            " float::float f," +
                            " long::long l," +
                            " str::string str," +
                            " sym::symbol sym," +
                            " bool::boolean bool," +
                            " bin::binary bin," +
                            " date::date dat," +
                            " long256::long256 l256," +
                            " chr::char c," +
                            " uuid::uuid u," +
                            " ipv4::ipv4 ip," +
                            " varchar::varchar v," +
                            " count() " +
                            "from all2 " +
                            "order by ts",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testShortToBoolean() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(rnd_short() as boolean) from long_sequence(10)",
                """
                        a
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        """,
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
                """
                        a
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        """,
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
                """
                        a
                        80
                        -18
                        65
                        29
                        21
                        85
                        -118
                        23
                        -6
                        -40
                        """,
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
                """
                        a
                        ?
                        A
                        &
                        ;
                        *
                        6
                        .
                        =
                        )
                        <
                        """,
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
                """
                        a
                        1969-12-31T23:59:32.944Z
                        1970-01-01T00:00:24.814Z
                        1969-12-31T23:59:48.545Z
                        1969-12-31T23:59:46.973Z
                        1969-12-31T23:59:38.773Z
                        1969-12-31T23:59:37.045Z
                        1969-12-31T23:59:58.602Z
                        1970-01-01T00:00:21.015Z
                        1970-01-01T00:00:30.202Z
                        1969-12-31T23:59:40.504Z
                        """,
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
                """
                        a
                        -27056.0
                        24814.0
                        -11455.0
                        -13027.0
                        -21227.0
                        -22955.0
                        -1398.0
                        21015.0
                        30202.0
                        -19496.0
                        """,
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
                """
                        a
                        -27056.0
                        24814.0
                        -11455.0
                        -13027.0
                        -21227.0
                        -22955.0
                        -1398.0
                        21015.0
                        30202.0
                        -19496.0
                        """,
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
                """
                        a
                        -27056
                        24814
                        -11455
                        -13027
                        -21227
                        -22955
                        -1398
                        21015
                        30202
                        -19496
                        """,
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
                """
                        a
                        -27056
                        24814
                        -11455
                        -13027
                        -21227
                        -22955
                        -1398
                        21015
                        30202
                        -19496
                        """,
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
                """
                        a
                        0xffffffffffff9650
                        0x60ee
                        0xffffffffffffd341
                        0xffffffffffffcd1d
                        0xffffffffffffad15
                        0xffffffffffffa655
                        0xfffffffffffffa8a
                        0x5217
                        0x75fa
                        0xffffffffffffb3d8
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testShortToLong256Sort() throws Exception {
        assertQuery(
                """
                        x
                        0x1e3b
                        0x2d91
                        0x5217
                        0x5d72
                        0x60ee
                        0x75fa
                        0xffffffffffff8059
                        0xffffffffffff84c4
                        0xffffffffffff9650
                        0xffffffffffffa0f1
                        0xffffffffffffa655
                        0xffffffffffffad15
                        0xffffffffffffb288
                        0xffffffffffffb3d8
                        0xffffffffffffc6cc
                        0xffffffffffffcd1d
                        0xffffffffffffd341
                        0xffffffffffffeb14
                        0xffffffffffffecce
                        0xfffffffffffffa8a
                        """,
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
                """
                        a
                        -27056
                        24814
                        -11455
                        -13027
                        -21227
                        -22955
                        -1398
                        21015
                        30202
                        -19496
                        """,
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
                """
                        a
                        -27056
                        24814
                        -11455
                        -13027
                        -21227
                        -22955
                        -1398
                        21015
                        30202
                        -19496
                        """,
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
                """
                        a
                        10
                        10
                        10
                        10
                        10
                        10
                        10
                        10
                        10
                        10
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testShortToStrSort() throws Exception {
        assertQuery(
                """
                        x
                        -11455
                        -13027
                        -1398
                        -14644
                        -19496
                        -19832
                        -21227
                        -22955
                        -24335
                        -27056
                        -31548
                        -32679
                        -4914
                        -5356
                        11665
                        21015
                        23922
                        24814
                        30202
                        7739
                        """,
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
                """
                        a
                        -27056
                        24814
                        -11455
                        -13027
                        -21227
                        -22955
                        -1398
                        21015
                        30202
                        -19496
                        """,
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
                """
                        a
                        99
                        99
                        99
                        99
                        99
                        99
                        99
                        99
                        99
                        99
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testShortToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                """
                        b\ta
                        -27056\t-27056
                        24814\t24814
                        -11455\t-11455
                        -13027\t-13027
                        -21227\t-21227
                        -22955\t-22955
                        -1398\t-1398
                        21015\t21015
                        30202\t30202
                        -19496\t-19496
                        -14644\t-14644
                        -5356\t-5356
                        -4914\t-4914
                        -24335\t-24335
                        -32679\t-32679
                        -19832\t-19832
                        -31548\t-31548
                        11665\t11665
                        7739\t7739
                        23922\t23922
                        """,
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
                """
                        a
                        1969-12-31T23:59:59.972944Z
                        1970-01-01T00:00:00.024814Z
                        1969-12-31T23:59:59.988545Z
                        1969-12-31T23:59:59.986973Z
                        1969-12-31T23:59:59.978773Z
                        1969-12-31T23:59:59.977045Z
                        1969-12-31T23:59:59.998602Z
                        1970-01-01T00:00:00.021015Z
                        1970-01-01T00:00:00.030202Z
                        1969-12-31T23:59:59.980504Z
                        """,
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
                """
                        a
                        1969-12-31T23:59:59.999972944Z
                        1970-01-01T00:00:00.000024814Z
                        1969-12-31T23:59:59.999988545Z
                        1969-12-31T23:59:59.999986973Z
                        1969-12-31T23:59:59.999978773Z
                        1969-12-31T23:59:59.999977045Z
                        1969-12-31T23:59:59.999998602Z
                        1970-01-01T00:00:00.000021015Z
                        1970-01-01T00:00:00.000030202Z
                        1969-12-31T23:59:59.999980504Z
                        """,
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
                """
                        a
                        -27056
                        24814
                        -11455
                        -13027
                        -21227
                        -22955
                        -1398
                        21015
                        30202
                        -19496
                        """,
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
                """
                        a
                        10
                        10
                        10
                        10
                        10
                        10
                        10
                        10
                        10
                        10
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testShortToVarcharSort() throws Exception {
        assertQuery(
                """
                        x
                        -11455
                        -13027
                        -1398
                        -14644
                        -19496
                        -19832
                        -21227
                        -22955
                        -24335
                        -27056
                        -31548
                        -32679
                        -4914
                        -5356
                        11665
                        21015
                        23922
                        24814
                        30202
                        7739
                        """,
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
                """
                        a
                        
                        
                        
                        
                        
                        
                        
                        
                        
                        
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testStrToBoolean() throws Exception {
        assertQuery(
                """
                        boolean
                        false
                        false
                        true
                        true
                        true
                        true
                        false
                        true
                        false
                        false
                        """,
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
                """
                        a
                        23
                        100
                        56
                        0
                        56
                        0
                        100
                        0
                        100
                        23
                        """,
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
                """
                        a
                        A
                        K
                        B
                        
                        B
                        
                        K
                        
                        K
                        A
                        """,
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
                "insert into tab select cast(rnd_str('2019-03-11T10:20:33.123Z', '2019-03-24T14:20:33.001456Z', 'ABC', '2019-03-24T14:20:33.002456789Z', null) as date) from long_sequence(15)",
                """
                        a
                        2019-03-11T10:20:33.123Z
                        2019-03-24T14:20:33.002Z
                        2019-03-24T14:20:33.002Z
                        2019-03-24T14:20:33.001Z
                        2019-03-24T14:20:33.001Z
                        
                        
                        2019-03-24T14:20:33.001Z
                        2019-03-24T14:20:33.001Z
                        2019-03-24T14:20:33.002Z
                        2019-03-24T14:20:33.001Z
                        
                        
                        2019-03-24T14:20:33.002Z
                        2019-03-24T14:20:33.001Z
                        """,
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
                """
                        a
                        1234.556
                        null
                        988.223
                        null
                        988.223
                        null
                        null
                        null
                        null
                        1234.556
                        1234.556
                        1234.556
                        null
                        null
                        988.223
                        """,
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
                """
                        a
                        4.9E-324
                        """,
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
                """
                        a
                        9.23
                        null
                        4.15
                        null
                        4.15
                        null
                        null
                        null
                        null
                        9.23
                        9.23
                        9.23
                        null
                        null
                        4.15
                        """,
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
                """
                        a
                        90092
                        90092
                        2203
                        null
                        null
                        null
                        null
                        2203
                        90092
                        2203
                        """,
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
                """
                        a
                        2334
                        2334
                        99002
                        null
                        null
                        null
                        null
                        99002
                        2334
                        99002
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testStrToLong256Sort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        
                        
                        
                        0x123455
                        0x123455
                        0x123455
                        0x123455
                        0x123455
                        0x123455
                        0x8802ff90
                        0x8802ff90
                        0x8802ff90
                        0x8802ff90
                        0x8802ff90
                        0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926
                        0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926
                        0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926
                        0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926
                        """,
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
                """
                        a
                        23
                        0
                        0
                        56
                        56
                        0
                        100
                        56
                        56
                        0
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testStrToSymbolConst() throws Exception {
        assertQuery(
                """
                        b\ta
                        abc\tJWCP
                        abc\t
                        abc\tYRXP
                        abc\tNRXGZ
                        abc\tUXIBB
                        abc\tPGWFF
                        abc\tDEYYQ
                        abc\tBHFOW
                        abc\tDXYS
                        abc\tOUOJ
                        abc\tRUED
                        abc\tQULO
                        abc\tGETJ
                        abc\tZSRYR
                        abc\tVTMHG
                        abc\tZZVD
                        abc\tMYICC
                        abc\tOUIC
                        abc\tKGHV
                        abc\tSDOTS
                        """,
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
                """
                        b\ta
                        JWCP\tJWCP
                        \t
                        YRXP\tYRXP
                        NRXGZ\tNRXGZ
                        UXIBB\tUXIBB
                        PGWFF\tPGWFF
                        DEYYQ\tDEYYQ
                        BHFOW\tBHFOW
                        DXYS\tDXYS
                        OUOJ\tOUOJ
                        RUED\tRUED
                        QULO\tQULO
                        GETJ\tGETJ
                        ZSRYR\tZSRYR
                        VTMHG\tVTMHG
                        ZZVD\tZZVD
                        MYICC\tMYICC
                        OUIC\tOUIC
                        KGHV\tKGHV
                        SDOTS\tSDOTS
                        """,
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
                """
                        x
                        
                        
                        
                        
                        a
                        a
                        a
                        a
                        a
                        a
                        b
                        b
                        b
                        b
                        b
                        c
                        c
                        c
                        c
                        c
                        """,
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
                """
                        a
                        2019-03-11T10:20:33.123897Z
                        
                        2019-03-24T14:20:33.123551Z
                        
                        2019-03-24T14:20:33.123551Z
                        
                        
                        
                        
                        2019-03-11T10:20:33.123897Z
                        """,
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
                """
                        a
                        2019-03-11T10:20:33.123897123Z
                        
                        2019-03-24T14:20:33.123551098Z
                        
                        2019-03-24T14:20:33.123551098Z
                        
                        
                        
                        
                        2019-03-11T10:20:33.123897123Z
                        """,
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
                """
                        a
                        раз
                        три
                        два
                        
                        два
                        
                        три
                        
                        три
                        раз
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testStringRegClass() throws Exception {
        assertQuery(
                """
                        b\ta
                        2615\tpg_namespace
                        2615\tpg_namespace
                        1259\tpg_class
                        1259\tpg_class
                        1259\tpg_class
                        """,
                "select cast(a as string)::regclass b, a from tab",
                "create table tab as (select rnd_symbol('pg_namespace', 'pg_class') a from long_sequence(5))",
                null,
                true,
                true
        );
    }

    @Test
    public void testSymbolColumnToSymbolInGroupBy1() throws Exception {
        assertQuery(
                "timestamp\tID\tEvt1\n",
                "SELECT cast(timestamp as LONG) AS timestamp, symbol::SYMBOL AS \"ID\", round(avg(price))::LONG AS \"Evt1\" " +
                        "FROM 'trades'" +
                        "ORDER BY 1",
                "create table trades (timestamp timestamp, symbol symbol, price double) timestamp(timestamp)",
                null,
                "insert into trades select x::timestamp, x::string, x from long_sequence(10)",
                """
                        timestamp\tID\tEvt1
                        1\t1\t1
                        2\t2\t2
                        3\t3\t3
                        4\t4\t4
                        5\t5\t5
                        6\t6\t6
                        7\t7\t7
                        8\t8\t8
                        9\t9\t9
                        10\t10\t10
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolColumnToSymbolInGroupBy2() throws Exception {
        assertQuery(
                "cast\tavg\n",
                "SELECT cast(symbol as SYMBOL), avg(price) " +
                        "FROM 'trades' " +
                        "ORDER BY avg(price)",
                "create table trades (timestamp timestamp, symbol symbol, price double) timestamp(timestamp)",
                null,
                "insert into trades select x::timestamp, x::string, x from long_sequence(10)",
                """
                        cast\tavg
                        1\t1.0
                        2\t2.0
                        3\t3.0
                        4\t4.0
                        5\t5.0
                        6\t6.0
                        7\t7.0
                        8\t8.0
                        9\t9.0
                        10\t10.0
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolColumnToSymbolInGroupBy3() throws Exception {
        assertQuery(
                "coalesce\tavg\n",
                "SELECT coalesce(cast(symbol as SYMBOL), 'foobar'), avg(price)" +
                        "FROM 'trades' " +
                        "ORDER BY coalesce(cast(symbol as SYMBOL), 'foobar')",
                "create table trades (timestamp timestamp, symbol symbol, price double) timestamp(timestamp)",
                null,
                "insert into trades select x::timestamp, x::string, x from long_sequence(10)",
                """
                        coalesce\tavg
                        1\t1.0
                        10\t10.0
                        2\t2.0
                        3\t3.0
                        4\t4.0
                        5\t5.0
                        6\t6.0
                        7\t7.0
                        8\t8.0
                        9\t9.0
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolColumnToSymbolInGroupBy4() throws Exception {
        assertQuery(
                "i\ts\tmax\n",
                "SELECT i::int i, symbol::symbol s, max(price)" +
                        "FROM 'trades'" +
                        "ORDER BY 1",
                "create table trades (timestamp timestamp, i int, symbol symbol, price double) timestamp(timestamp)",
                null,
                "insert into trades select x::timestamp, x, x::string, x from long_sequence(10)",
                """
                        i\ts\tmax
                        1\t1\t1.0
                        2\t2\t2.0
                        3\t3\t3.0
                        4\t4\t4.0
                        5\t5\t5.0
                        6\t6\t6.0
                        7\t7\t7.0
                        8\t8\t8.0
                        9\t9\t9.0
                        10\t10\t10.0
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolColumnToSymbolInSampleBy() throws Exception {
        assertQuery(
                "timestamp\tID\tEvt1\n",
                "SELECT timestamp, symbol::SYMBOL AS \"ID\", round(avg(price))::LONG AS \"Evt1\" " +
                        "FROM 'trades'" +
                        "SAMPLE BY 1h",
                "create table trades (timestamp timestamp, symbol symbol, price double) timestamp(timestamp)",
                "timestamp",
                "insert into trades select x::timestamp, x::string, x from long_sequence(10)",
                """
                        timestamp\tID\tEvt1
                        1970-01-01T00:00:00.000000Z\t1\t1
                        1970-01-01T00:00:00.000000Z\t2\t2
                        1970-01-01T00:00:00.000000Z\t3\t3
                        1970-01-01T00:00:00.000000Z\t4\t4
                        1970-01-01T00:00:00.000000Z\t5\t5
                        1970-01-01T00:00:00.000000Z\t6\t6
                        1970-01-01T00:00:00.000000Z\t7\t7
                        1970-01-01T00:00:00.000000Z\t8\t8
                        1970-01-01T00:00:00.000000Z\t9\t9
                        1970-01-01T00:00:00.000000Z\t10\t10
                        """,
                true,
                true,
                false
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
                """
                        x
                        
                        
                        
                        
                        
                        
                        
                        0x123455
                        0x123455
                        0x8802ff90
                        0x8802ff90
                        0x8802ff90
                        0x8802ff90
                        0x8802ff90
                        0x8802ff90
                        0x8802ff90
                        0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926
                        0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926
                        0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926
                        0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926
                        """,
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
                """
                        x
                        
                        
                        
                        
                        1
                        1
                        1
                        1
                        1
                        1
                        200
                        200
                        200
                        200
                        200
                        221
                        221
                        221
                        221
                        221
                        """,
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
                """
                        a
                        23
                        100
                        56
                        0
                        56
                        0
                        100
                        0
                        100
                        23
                        """,
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
                """
                        a
                        A
                        K
                        B
                        
                        B
                        
                        K
                        
                        K
                        A
                        """,
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
                """
                        a
                        2019-03-11T10:20:33.123Z
                        
                        2019-03-24T14:20:33.123Z
                        
                        2019-03-24T14:20:33.123Z
                        
                        
                        
                        
                        2019-03-11T10:20:33.123Z
                        """,
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
                """
                        a
                        1234.556
                        null
                        988.223
                        null
                        988.223
                        null
                        null
                        null
                        null
                        1234.556
                        1234.556
                        1234.556
                        null
                        null
                        988.223
                        """,
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
                """
                        a
                        9.23
                        null
                        4.15
                        null
                        4.15
                        null
                        null
                        null
                        null
                        9.23
                        9.23
                        9.23
                        null
                        null
                        4.15
                        """,
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
                """
                        a
                        100
                        null
                        200
                        null
                        200
                        null
                        null
                        null
                        null
                        100
                        """,
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
                """
                        a
                        100
                        null
                        200
                        null
                        200
                        null
                        null
                        null
                        null
                        100
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolToLong256Sort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        
                        
                        
                        0x123455
                        0x123455
                        0x123455
                        0x123455
                        0x123455
                        0x123455
                        0x8802ff90
                        0x8802ff90
                        0x8802ff90
                        0x8802ff90
                        0x8802ff90
                        0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926
                        0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926
                        0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926
                        0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926
                        """,
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
                """
                        a
                        23
                        0
                        0
                        56
                        56
                        0
                        100
                        56
                        56
                        0
                        """,
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
                """
                        a
                        abc
                        
                        135
                        xxp
                        135
                        xxp
                        
                        xxp
                        
                        abc
                        """,
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
                """
                        a
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolToStrSort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        
                        
                        1
                        1
                        1
                        1
                        1
                        1
                        200
                        200
                        200
                        200
                        200
                        221
                        221
                        221
                        221
                        221
                        """,
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
                """
                        a
                        23
                        
                        
                        56
                        56
                        y
                        100
                        56
                        56
                        
                        """,
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
                """
                        a
                        2019-03-11T10:20:33.123897Z
                        
                        2019-03-24T14:20:33.123551Z
                        
                        2019-03-24T14:20:33.123551Z
                        
                        
                        
                        
                        2019-03-11T10:20:33.123897Z
                        """,
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
                """
                        a
                        2019-03-11T10:20:33.123897098Z
                        
                        2019-03-24T14:20:33.123551123Z
                        
                        2019-03-24T14:20:33.123551123Z
                        
                        
                        
                        
                        2019-03-11T10:20:33.123897098Z
                        """,
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
                """
                        a
                        abc
                        
                        135
                        xxp
                        135
                        xxp
                        
                        xxp
                        
                        abc
                        """,
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
                """
                        a
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testSymbolToVarcharSort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        
                        
                        1
                        1
                        1
                        1
                        1
                        1
                        200
                        200
                        200
                        200
                        200
                        221
                        221
                        221
                        221
                        221
                        """,
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
                """
                        a
                        true
                        false
                        true
                        true
                        false
                        true
                        true
                        true
                        false
                        true
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.000000334Z
                        1970-01-01T00:00:00.000000334Z
                        1970-01-01T00:00:00.000000334Z
                        1970-01-01T00:00:00.000000334Z
                        1970-01-01T00:00:00.000000334Z
                        1970-01-01T00:00:00.000000334Z
                        1970-01-01T00:00:00.000000334Z
                        1970-01-01T00:00:00.000000334Z
                        1970-01-01T00:00:00.000000334Z
                        1970-01-01T00:00:00.000000334Z
                        """,
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
                """
                        a
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        """,
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
                "insert into tab select cast(rnd_timestamp_ns(96, 100, 2) as byte) from long_sequence(10)",
                """
                        a
                        97
                        0
                        100
                        99
                        0
                        97
                        97
                        98
                        0
                        96
                        """,
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
                "insert into tab select cast(rnd_timestamp(34,66,2) as char) from long_sequence(10)",
                """
                        a
                        7
                        
                        9
                        0
                        
                        2
                        +
                        4
                        
                        -
                        """,
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
                """
                        a
                        1970-01-01T00:00:05.437Z
                        
                        1970-01-01T00:00:05.960Z
                        1970-01-01T00:00:08.779Z
                        
                        1970-01-01T00:00:05.513Z
                        1970-01-01T00:00:05.535Z
                        1970-01-01T00:00:09.498Z
                        
                        1970-01-01T00:00:02.521Z
                        """,
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
                """
                        a
                        67000.0
                        null
                        30000.0
                        99000.0
                        null
                        137000.0
                        127000.0
                        58000.0
                        null
                        111000.0
                        """,
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
                """
                        a
                        67000.0
                        null
                        30000.0
                        99000.0
                        null
                        137000.0
                        127000.0
                        58000.0
                        null
                        111000.0
                        """,
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
                """
                        a
                        985257636
                        null
                        968130026
                        555619965
                        null
                        712286238
                        215755333
                        720037886
                        null
                        129724714
                        """,
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
                """
                        a
                        13992367
                        4501476
                        2660374
                        null
                        5864007
                        10281933
                        6977935
                        9100840
                        8600061
                        478012
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampNsToLong256Sort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        
                        
                        
                        
                        
                        0x08
                        0x11
                        0x1e
                        0x34
                        0x3d
                        0x4d
                        0x57
                        0x63
                        0x80
                        0x89
                        0xa7
                        0xc0
                        0xc7
                        """,
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select rnd_timestamp_ns(1,200,1) a from long_sequence(20))",
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
                "insert into tab select cast(rnd_timestamp(23,56,2) as short) from long_sequence(10)",
                """
                        a
                        31
                        0
                        54
                        23
                        0
                        29
                        33
                        24
                        0
                        51
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.000055000Z
                        1970-01-01T00:00:00.000048000Z
                        1970-01-01T00:00:00.000055000Z
                        
                        1970-01-01T00:00:00.000045000Z
                        1970-01-01T00:00:00.000036000Z
                        1970-01-01T00:00:00.000034000Z
                        1970-01-01T00:00:00.000058000Z
                        1970-01-01T00:00:00.000045000Z
                        1970-01-01T00:00:00.000061000Z
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampNsToStrSort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        
                        
                        
                        
                        
                        1970-01-01T00:00:00.000008000Z
                        1970-01-01T00:00:00.000017000Z
                        1970-01-01T00:00:00.000030000Z
                        1970-01-01T00:00:00.000052000Z
                        1970-01-01T00:00:00.000061000Z
                        1970-01-01T00:00:00.000077000Z
                        1970-01-01T00:00:00.000087000Z
                        1970-01-01T00:00:00.000099000Z
                        1970-01-01T00:00:00.000128000Z
                        1970-01-01T00:00:00.000137000Z
                        1970-01-01T00:00:00.000167000Z
                        1970-01-01T00:00:00.000192000Z
                        1970-01-01T00:00:00.000199000Z
                        """,
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
                "insert into tab select cast(rnd_timestamp(1,150,2) as symbol) from long_sequence(10)",
                """
                        a
                        67
                        
                        30
                        99
                        
                        137
                        127
                        58
                        
                        111
                        """,
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
                """
                        a
                        601
                        601
                        601
                        601
                        601
                        601
                        601
                        601
                        601
                        601
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampNsToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                """
                        b\ta
                        20\t1970-01-01T00:00:00.000000020Z
                        \t
                        11\t1970-01-01T00:00:00.000000011Z
                        13\t1970-01-01T00:00:00.000000013Z
                        \t
                        15\t1970-01-01T00:00:00.000000015Z
                        19\t1970-01-01T00:00:00.000000019Z
                        17\t1970-01-01T00:00:00.000000017Z
                        \t
                        10\t1970-01-01T00:00:00.000000010Z
                        \t
                        17\t1970-01-01T00:00:00.000000017Z
                        \t
                        17\t1970-01-01T00:00:00.000000017Z
                        18\t1970-01-01T00:00:00.000000018Z
                        18\t1970-01-01T00:00:00.000000018Z
                        \t
                        12\t1970-01-01T00:00:00.000000012Z
                        11\t1970-01-01T00:00:00.000000011Z
                        15\t1970-01-01T00:00:00.000000015Z
                        """,
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_timestamp_ns(10, 20, 2) a from long_sequence(20))",
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
                "insert into tab select cast(rnd_timestamp_ns(34,66,100) as varchar) from long_sequence(10)",
                """
                        a
                        1970-01-01T00:00:00.000000055Z
                        1970-01-01T00:00:00.000000048Z
                        1970-01-01T00:00:00.000000055Z
                        
                        1970-01-01T00:00:00.000000045Z
                        1970-01-01T00:00:00.000000036Z
                        1970-01-01T00:00:00.000000034Z
                        1970-01-01T00:00:00.000000058Z
                        1970-01-01T00:00:00.000000045Z
                        1970-01-01T00:00:00.000000061Z
                        """,
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
                """
                        a
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampNsToVarcharSort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        
                        
                        
                        
                        
                        1970-01-01T00:00:00.000008000Z
                        1970-01-01T00:00:00.000017000Z
                        1970-01-01T00:00:00.000030000Z
                        1970-01-01T00:00:00.000052000Z
                        1970-01-01T00:00:00.000061000Z
                        1970-01-01T00:00:00.000077000Z
                        1970-01-01T00:00:00.000087000Z
                        1970-01-01T00:00:00.000099000Z
                        1970-01-01T00:00:00.000128000Z
                        1970-01-01T00:00:00.000137000Z
                        1970-01-01T00:00:00.000167000Z
                        1970-01-01T00:00:00.000192000Z
                        1970-01-01T00:00:00.000199000Z
                        """,
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
                """
                        a
                        true
                        false
                        true
                        true
                        false
                        true
                        true
                        true
                        false
                        true
                        """,
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
                """
                        a
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        true
                        """,
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
                """
                        a
                        97
                        0
                        100
                        99
                        0
                        97
                        97
                        98
                        0
                        96
                        """,
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
                """
                        a
                        7
                        
                        9
                        0
                        
                        2
                        +
                        4
                        
                        -
                        """,
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
                """
                        a
                        1970-01-01T00:00:05.437Z
                        
                        1970-01-01T00:00:05.960Z
                        1970-01-01T00:00:08.779Z
                        
                        1970-01-01T00:00:05.513Z
                        1970-01-01T00:00:05.535Z
                        1970-01-01T00:00:09.498Z
                        
                        1970-01-01T00:00:02.521Z
                        """,
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
                """
                        a
                        67.0
                        null
                        30.0
                        99.0
                        null
                        137.0
                        127.0
                        58.0
                        null
                        111.0
                        """,
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
                """
                        a
                        67.0
                        null
                        30.0
                        99.0
                        null
                        137.0
                        127.0
                        58.0
                        null
                        111.0
                        """,
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
                """
                        a
                        985257636
                        null
                        968130026
                        555619965
                        null
                        712286238
                        215755333
                        720037886
                        null
                        129724714
                        """,
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
                """
                        a
                        13992367
                        4501476
                        2660374
                        null
                        5864007
                        10281933
                        6977935
                        9100840
                        8600061
                        478012
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampToLong256Sort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        
                        
                        
                        
                        
                        0x08
                        0x11
                        0x1e
                        0x34
                        0x3d
                        0x4d
                        0x57
                        0x63
                        0x80
                        0x89
                        0xa7
                        0xc0
                        0xc7
                        """,
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
                """
                        a
                        31
                        0
                        54
                        23
                        0
                        29
                        33
                        24
                        0
                        51
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.000055Z
                        1970-01-01T00:00:00.000048Z
                        1970-01-01T00:00:00.000055Z
                        
                        1970-01-01T00:00:00.000045Z
                        1970-01-01T00:00:00.000036Z
                        1970-01-01T00:00:00.000034Z
                        1970-01-01T00:00:00.000058Z
                        1970-01-01T00:00:00.000045Z
                        1970-01-01T00:00:00.000061Z
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.000334Z
                        1970-01-01T00:00:00.000334Z
                        1970-01-01T00:00:00.000334Z
                        1970-01-01T00:00:00.000334Z
                        1970-01-01T00:00:00.000334Z
                        1970-01-01T00:00:00.000334Z
                        1970-01-01T00:00:00.000334Z
                        1970-01-01T00:00:00.000334Z
                        1970-01-01T00:00:00.000334Z
                        1970-01-01T00:00:00.000334Z
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampToStrSort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        
                        
                        
                        
                        
                        1970-01-01T00:00:00.000008Z
                        1970-01-01T00:00:00.000017Z
                        1970-01-01T00:00:00.000030Z
                        1970-01-01T00:00:00.000052Z
                        1970-01-01T00:00:00.000061Z
                        1970-01-01T00:00:00.000077Z
                        1970-01-01T00:00:00.000087Z
                        1970-01-01T00:00:00.000099Z
                        1970-01-01T00:00:00.000128Z
                        1970-01-01T00:00:00.000137Z
                        1970-01-01T00:00:00.000167Z
                        1970-01-01T00:00:00.000192Z
                        1970-01-01T00:00:00.000199Z
                        """,
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
                """
                        a
                        67
                        
                        30
                        99
                        
                        137
                        127
                        58
                        
                        111
                        """,
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
                """
                        a
                        601
                        601
                        601
                        601
                        601
                        601
                        601
                        601
                        601
                        601
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                """
                        b\ta
                        20\t1970-01-01T00:00:00.000020Z
                        \t
                        11\t1970-01-01T00:00:00.000011Z
                        13\t1970-01-01T00:00:00.000013Z
                        \t
                        15\t1970-01-01T00:00:00.000015Z
                        19\t1970-01-01T00:00:00.000019Z
                        17\t1970-01-01T00:00:00.000017Z
                        \t
                        10\t1970-01-01T00:00:00.000010Z
                        \t
                        17\t1970-01-01T00:00:00.000017Z
                        \t
                        17\t1970-01-01T00:00:00.000017Z
                        18\t1970-01-01T00:00:00.000018Z
                        18\t1970-01-01T00:00:00.000018Z
                        \t
                        12\t1970-01-01T00:00:00.000012Z
                        11\t1970-01-01T00:00:00.000011Z
                        15\t1970-01-01T00:00:00.000015Z
                        """,
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
                """
                        a
                        1970-01-01T00:00:00.000055Z
                        1970-01-01T00:00:00.000048Z
                        1970-01-01T00:00:00.000055Z
                        
                        1970-01-01T00:00:00.000045Z
                        1970-01-01T00:00:00.000036Z
                        1970-01-01T00:00:00.000034Z
                        1970-01-01T00:00:00.000058Z
                        1970-01-01T00:00:00.000045Z
                        1970-01-01T00:00:00.000061Z
                        """,
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
                """
                        a
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        334
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testTimestampToVarcharSort() throws Exception {
        assertQuery(
                """
                        x
                        
                        
                        
                        
                        
                        
                        
                        1970-01-01T00:00:00.000008Z
                        1970-01-01T00:00:00.000017Z
                        1970-01-01T00:00:00.000030Z
                        1970-01-01T00:00:00.000052Z
                        1970-01-01T00:00:00.000061Z
                        1970-01-01T00:00:00.000077Z
                        1970-01-01T00:00:00.000087Z
                        1970-01-01T00:00:00.000099Z
                        1970-01-01T00:00:00.000128Z
                        1970-01-01T00:00:00.000137Z
                        1970-01-01T00:00:00.000167Z
                        1970-01-01T00:00:00.000192Z
                        1970-01-01T00:00:00.000199Z
                        """,
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
                """
                        a
                        0010cde8-12ce-40ee-8010-a928bb8b9650
                        9f9b2131-d49f-4d1d-ab81-39815c50d341
                        7bcd48d8-c77a-4655-b2a2-15ba0462ad15
                        b5b2159a-2356-4217-965d-4c984f0ffa8a
                        e8beef38-cd7b-43d8-9b2d-34586f6275fa
                        322a2198-864b-4b14-b97f-a69eb8fec6cc
                        980eca62-a219-40f1-a846-d7a3aa5aecce
                        c1e63128-5c1a-4288-872b-fc5230158059
                        716de3d2-5dcc-4d91-9fa2-397a5d8c84c4
                        4b0f595f-143e-4d72-af1a-8266e7921e3b
                        """,
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
                """
                        a
                        0010cde8-12ce-40ee-8010-a928bb8b9650
                        9f9b2131-d49f-4d1d-ab81-39815c50d341
                        7bcd48d8-c77a-4655-b2a2-15ba0462ad15
                        b5b2159a-2356-4217-965d-4c984f0ffa8a
                        e8beef38-cd7b-43d8-9b2d-34586f6275fa
                        322a2198-864b-4b14-b97f-a69eb8fec6cc
                        980eca62-a219-40f1-a846-d7a3aa5aecce
                        c1e63128-5c1a-4288-872b-fc5230158059
                        716de3d2-5dcc-4d91-9fa2-397a5d8c84c4
                        4b0f595f-143e-4d72-af1a-8266e7921e3b
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testVarcharToBoolean() throws Exception {
        assertQuery(
                """
                        boolean
                        false
                        false
                        true
                        true
                        true
                        true
                        false
                        true
                        false
                        false
                        """,
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
                """
                        a
                        23
                        100
                        56
                        0
                        56
                        0
                        100
                        0
                        100
                        23
                        """,
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
                """
                        a
                        A
                        K
                        B
                        
                        B
                        
                        K
                        
                        K
                        A
                        """,
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
                "insert into tab select cast(rnd_varchar('2019-03-11T10:20:33.123Z', '2019-03-24T14:20:33.123Z', 'ABC', null, '2019-03-24T14:20:33.00123Z', '2019-03-24T14:20:33.00312334Z') as date) from long_sequence(15)",
                """
                        a
                        2019-03-11T10:20:33.123Z
                        2019-03-11T10:20:33.123Z
                        2019-03-24T14:20:33.123Z
                        2019-03-24T14:20:33.003Z
                        2019-03-24T14:20:33.003Z
                        2019-03-24T14:20:33.003Z
                        
                        2019-03-24T14:20:33.123Z
                        2019-03-11T10:20:33.123Z
                        2019-03-24T14:20:33.001Z
                        2019-03-24T14:20:33.001Z
                        
                        2019-03-24T14:20:33.001Z
                        2019-03-24T14:20:33.123Z
                        2019-03-24T14:20:33.123Z
                        """,
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
                """
                        a
                        1234.556
                        null
                        988.223
                        null
                        988.223
                        null
                        null
                        null
                        null
                        1234.556
                        1234.556
                        1234.556
                        null
                        null
                        988.223
                        """,
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
                """
                        a
                        4.9E-324
                        """,
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
                """
                        a
                        9.23
                        null
                        4.15
                        null
                        4.15
                        null
                        null
                        null
                        null
                        9.23
                        9.23
                        9.23
                        null
                        null
                        4.15
                        """,
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
                """
                        a
                        171.30.189.77
                        201.100.238.229
                        111.221.228.130
                        
                        111.221.228.130
                        
                        201.100.238.229
                        
                        201.100.238.229
                        171.30.189.77
                        """,
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
                """
                        a
                        90092
                        90092
                        2203
                        null
                        null
                        null
                        null
                        2203
                        90092
                        2203
                        """,
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
                """
                        a
                        2334
                        2334
                        99002
                        null
                        null
                        null
                        null
                        99002
                        2334
                        99002
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testVarcharToLong256() throws Exception {
        assertQuery(
                """
                        x
                        0x123455
                        
                        0x8802ff90
                        0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926
                        0x8802ff90
                        0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926
                        
                        0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926
                        
                        0x123455
                        0x123455
                        0x123455
                        
                        0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926
                        0x8802ff90
                        0x123455
                        0x123455
                        0x8802ff90
                        0x8802ff90
                        
                        """,
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
                """
                        a
                        23
                        0
                        0
                        56
                        56
                        0
                        100
                        56
                        56
                        0
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testVarcharToSymbol() throws Exception {
        assertQuery(
                """
                        x
                        a
                        c
                        b
                        
                        b
                        
                        c
                        
                        c
                        a
                        """,
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
                """
                        b\ta
                        abc\ta
                        abc\tc
                        abc\tb
                        abc\t
                        abc\tb
                        abc\t
                        abc\tc
                        abc\t
                        abc\tc
                        abc\ta
                        """,
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
                """
                        b\ta
                        a\ta
                        c\tc
                        b\tb
                        \t
                        b\tb
                        \t
                        c\tc
                        \t
                        c\tc
                        a\ta
                        """,
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
                """
                        a
                        2019-03-11T10:20:33.123897Z
                        
                        2019-03-24T14:20:33.123551Z
                        
                        2019-03-24T14:20:33.123551Z
                        
                        
                        
                        
                        2019-03-11T10:20:33.123897Z
                        """,
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
                """
                        a
                        2019-03-11T10:20:33.123897123Z
                        
                        2019-03-24T14:20:33.123551098Z
                        
                        2019-03-24T14:20:33.123551098Z
                        
                        
                        
                        
                        2019-03-11T10:20:33.123897123Z
                        """,
                true,
                true,
                false
        );
    }

    @Test
    public void testVarcharToUuid() throws Exception {
        assertQuery(
                """
                        x
                        0010cde8-12ce-40ee-8010-a928bb8b9650
                        
                        9f9b2131-d49f-4d1d-ab81-39815c50d341
                        7bcd48d8-c77a-4655-b2a2-15ba0462ad15
                        9f9b2131-d49f-4d1d-ab81-39815c50d341
                        7bcd48d8-c77a-4655-b2a2-15ba0462ad15
                        
                        7bcd48d8-c77a-4655-b2a2-15ba0462ad15
                        
                        0010cde8-12ce-40ee-8010-a928bb8b9650
                        """,
                "select cast(a as uuid) x from tt",
                "create table tt as (select rnd_varchar('0010cde8-12ce-40ee-8010-a928bb8b9650', '9f9b2131-d49f-4d1d-ab81-39815c50d341', null, '7bcd48d8-c77a-4655-b2a2-15ba0462ad15') a from long_sequence(10))",
                null,
                true,
                true
        );
    }
}
