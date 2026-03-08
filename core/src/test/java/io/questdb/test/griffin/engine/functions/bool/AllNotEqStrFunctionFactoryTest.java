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

package io.questdb.test.griffin.engine.functions.bool;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class AllNotEqStrFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstant() throws Exception {
        assertQuery(
                "x\n" +
                        "1\n",
                "select * from long_sequence(1) where 'aaa' <> all('{abc,xyz}'::text[])",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testEmptyArray() throws Exception {
        assertQuery(
                "a\n" +
                        "aaa\n" +
                        "aaa\n" +
                        "bbb\n" +
                        "ccc\n" +
                        "ccc\n",
                "select * from tab where a <> all('{}'::text[])",
                "create table tab as (select rnd_str('aaa', 'bbb', 'ccc') a from long_sequence(5));",
                null,
                true,
                true
        );
    }

    @Test
    public void testMatch() throws Exception {
        assertQuery(
                "a\n" +
                        "aaa\n" +
                        "aaa\n" +
                        "bbb\n" +
                        "ccc\n" +
                        "ccc\n",
                "select * from tab where a <> all('{abc,xyz}'::text[])",
                "create table tab as (select rnd_str('aaa', 'bbb', 'ccc') a from long_sequence(5));",
                null,
                true,
                false
        );
    }

    @Test
    public void testMatchVarcharColumn() throws Exception {
        assertQuery(
                "a\n" +
                        "ганьба\n" +
                        "слава\n" +
                        "слава\n",
                "select * from tab where a <> all('{добрий,вечір}'::text[])",
                "create table tab as (select rnd_varchar('ганьба','слава','добрий','вечір') a from long_sequence(5));",
                null,
                true,
                false
        );
    }

    @Test
    public void testNoMatch() throws Exception {
        assertQuery(
                "a\n",
                "select * from tab where a <> all('{aaa,bbb,ccc}'::text[])",
                "create table tab as (select rnd_str('aaa', 'bbb', 'ccc') a from long_sequence(5));",
                null,
                true,
                false
        );
    }

    @Test
    public void testNull() throws Exception {
        assertQuery(
                "a\n",
                "select * from tab where a <> all('{aaa,bbb,ccc}'::text[])",
                "create table tab as (select cast(null as string) a from long_sequence(5));",
                null,
                true,
                false
        );
    }

    @Test
    public void testNullConstant() throws Exception {
        assertQuery(
                "x\n",
                "select * from long_sequence(1) where null <> all('{abc,xyz}'::text[])",
                null,
                null,
                false,
                true
        );
    }

    @Test
    public void testNullVarchar() throws Exception {
        assertQuery(
                "a\n",
                "select * from tab where a <> all('{добрий,вечір}'::text[])",
                "create table tab as (select cast(null as varchar) a from long_sequence(15));",
                null,
                true,
                false
        );
    }
}
