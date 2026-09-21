/*+*****************************************************************************
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
        assertQuery("tab_boolean where v is NOT NULL")
                .ddl("""
                        create table tab_boolean as (
                            select rnd_boolean() as v from long_sequence(9)
                        );""")
                .mutateWith("insert into tab_boolean values(null)")
                .expectSize()
                .returns("""
                        v
                        false
                        false
                        false
                        true
                        false
                        false
                        true
                        true
                        true
                        """, """
                        v
                        false
                        false
                        false
                        true
                        false
                        false
                        true
                        true
                        true
                        false
                        """);
    }

    @Test
    public void testBooleanIsNull() throws Exception {
        assertQuery("tab_boolean where v is NULL")
                .ddl("""
                        create table tab_boolean as (
                            select rnd_boolean() as v from long_sequence(9)
                        );""")
                .mutateWith("insert into tab_boolean values(NULL)")
                .expectSize()
                .returns("v\n", "v\n");
    }

    @Test
    public void testBooleanSelectCast() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select cast(0 AS BOOLEAN) IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\nfalse\n");
            assertQuery("select cast(0L AS BOOLEAN) IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\nfalse\n");
            assertQuery("select cast('' AS BOOLEAN) IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\nfalse\n");
            assertQuery("select cast(NULL AS BOOLEAN) IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\nfalse\n");
            assertQuery("select cast(false AS BOOLEAN) IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\nfalse\n");
            assertQuery("select cast(0 AS BOOLEAN) IS NOT NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\ntrue\n");
            assertQuery("select cast(0L AS BOOLEAN) IS NOT NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\ntrue\n");
            assertQuery("select cast('' AS BOOLEAN) IS NOT NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\ntrue\n");
            assertQuery("select cast(NULL AS BOOLEAN) IS NOT NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\ntrue\n");
            assertQuery("select cast(false AS BOOLEAN) IS NOT NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\ntrue\n");
        });
    }

    @Test
    public void testByteIsNotNull() throws Exception {
        assertQuery("tab_byte where v is NOT NULL")
                .ddl("""
                        create table tab_byte as (
                            select rnd_byte() as v from long_sequence(9)
                        );""")
                .mutateWith("insert into tab_byte values(null)")
                .expectSize()
                .returns("""
                        v
                        76
                        102
                        27
                        87
                        79
                        79
                        122
                        83
                        90
                        """, """
                        v
                        76
                        102
                        27
                        87
                        79
                        79
                        122
                        83
                        90
                        0
                        """);
    }

    @Test
    public void testByteIsNull() throws Exception {
        assertQuery("tab_byte where v is NULL")
                .ddl("""
                        create table tab_byte as (
                            select rnd_byte() as v from long_sequence(9)
                        );""")
                .mutateWith("insert into tab_byte values(NULL)")
                .expectSize()
                .returns("v\n", "v\n");
    }

    @Test
    public void testByteSelectCast() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select cast(0 AS BYTE) IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\nfalse\n");
            assertQuery("select cast(0L AS BYTE) IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\nfalse\n");
            assertQuery("select cast('' AS BYTE) IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\nfalse\n");
            assertQuery("select cast(NULL AS BYTE) IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\nfalse\n");
            assertQuery("select cast(false AS BYTE) IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\nfalse\n");
            assertQuery("select cast(0 AS BYTE) IS NOT NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\ntrue\n");
            assertQuery("select cast(0L AS BYTE) IS NOT NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\ntrue\n");
            assertQuery("select cast('' AS BYTE) IS NOT NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\ntrue\n");
            assertQuery("select cast(NULL AS BYTE) IS NOT NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\ntrue\n");
            assertQuery("select cast(false AS BYTE) IS NOT NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\ntrue\n");
        });
    }

    @Test
    public void testCharIsNotNull() throws Exception {
        assertQuery("tab_char where v is NOT NULL")
                .ddl("""
                        create table tab_char as (
                            select rnd_char() as v from long_sequence(9)
                        );""")
                .mutateWith("insert into tab_char values(null)")
                .returns("""
                        v
                        V
                        T
                        J
                        W
                        C
                        P
                        S
                        W
                        H
                        """, """
                        v
                        V
                        T
                        J
                        W
                        C
                        P
                        S
                        W
                        H
                        """);
    }

    @Test
    public void testCharIsNull() throws Exception {
        assertQuery("tab_char where v is NULL")
                .ddl("""
                        create table tab_char as (
                            select rnd_char() as v from long_sequence(9)
                        );""")
                .mutateWith("insert into tab_char values(NULL)")
                .returns("v\n", "v\n\n");
    }

    @Test
    public void testCharSelectCast() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select cast(0 AS CHAR) IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\ntrue\n");
            assertQuery("select cast(0L AS CHAR) IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\ntrue\n");
            assertQuery("select cast('' AS CHAR) IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\ntrue\n");
            assertQuery("select cast(NULL AS CHAR) IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\ntrue\n");
            assertQuery("select cast(false AS CHAR) IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\nfalse\n");
            assertQuery("select cast(0 AS CHAR) IS NOT NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\nfalse\n");
            assertQuery("select cast(0L AS CHAR) IS NOT NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\nfalse\n");
            assertQuery("select cast('' AS CHAR) IS NOT NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\nfalse\n");
            assertQuery("select cast(NULL AS CHAR) IS NOT NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\nfalse\n");
            assertQuery("select cast(false AS CHAR) IS NOT NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\ntrue\n");
        });
    }

    @Test
    public void testShortIsNotNull() throws Exception {
        assertQuery("tab_short where v is NOT NULL")
                .ddl("""
                        create table tab_short as (
                            select rnd_short() as v from long_sequence(9)
                        );""")
                .mutateWith("insert into tab_short values(null)")
                .expectSize()
                .returns("""
                        v
                        -27056
                        24814
                        -11455
                        -13027
                        -21227
                        -22955
                        -1398
                        21015
                        30202
                        """, """
                        v
                        -27056
                        24814
                        -11455
                        -13027
                        -21227
                        -22955
                        -1398
                        21015
                        30202
                        0
                        """);
    }

    @Test
    public void testShortIsNull() throws Exception {
        assertQuery("tab_short where v is NULL")
                .ddl("""
                        create table tab_short as (
                            select rnd_short() as v from long_sequence(9)
                        );""")
                .mutateWith("insert into tab_short values(NULL)")
                .expectSize()
                .returns("v\n", "v\n");
    }

    @Test
    public void testShortSelectCast() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select cast(0 AS SHORT) IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\nfalse\n");
            assertQuery("select cast(0L AS SHORT) IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\nfalse\n");
            assertQuery("select cast('' AS SHORT) IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\nfalse\n");
            assertQuery("select cast(NULL AS SHORT) IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\nfalse\n");
            assertQuery("select cast(false AS SHORT) IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\nfalse\n");
            assertQuery("select cast(0 AS SHORT) IS NOT NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\ntrue\n");
            assertQuery("select cast(0L AS SHORT) IS NOT NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\ntrue\n");
            assertQuery("select cast('' AS SHORT) IS NOT NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\ntrue\n");
            assertQuery("select cast(NULL AS SHORT) IS NOT NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\ntrue\n");
            assertQuery("select cast(false AS SHORT) IS NOT NULL")
                    .noLeakCheck()
                    .expectSize()
                    .returns("column\ntrue\n");
        });
    }
}
