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
public class ExplicitStringyCastTest extends AbstractCairoTest {

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
            " rnd_str(3,3,0) a_string," +
            " rnd_varchar(3,3,0) a_varchar," +
            " from long_sequence(1))";
    private final String castType;
    private final String expectedCastValue;

    public ExplicitStringyCastTest(String castType, String expectedCastValue) {
        this.castType = castType;
        this.expectedCastValue = expectedCastValue;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"boolean", "false"},
                {"byte", "0"},
                {"short", "1"},
                {"int", "1"},
                {"long", "0"},
                {"long256", "0x797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fab5b2159a23565217"},
                {"float", "0.13123357"},
                {"double", "0.8423410920883345"},
                {"date", "2024-11-26T02:19:54.316Z"},
                {"timestamp", "2024-12-04T22:56:43.359212Z"},
                {"char", "G"},
                {"symbol", "sym1"},
                {"uuid4", "83881d41-7184-4cf3-ae60-a01a5b3ea0db"},
                {"string", "XIB"},
                {"varchar", "鼷G\uD991\uDE7E"},
        });
    }

    @Test
    public void testCastToString() throws Exception {
        assertQuery("a\ttypeOf\n" +
                        expectedCastValue + "\tSTRING\n",
                String.format("select a, typeOf(a) typeOf from" +
                        " (select cast(a_%s as string) a from cast_table)", castType),
                castTableDdl, null, true, true
        );
    }

    @Test
    public void testCastToVarchar() throws Exception {
        assertQuery("a\ttypeOf\n" +
                        expectedCastValue + "\tVARCHAR\n",
                String.format("select a, typeOf(a) typeOf from" +
                        " (select cast(a_%s as varchar) a from cast_table)", castType),
                castTableDdl, null, true, true
        );
    }
}
