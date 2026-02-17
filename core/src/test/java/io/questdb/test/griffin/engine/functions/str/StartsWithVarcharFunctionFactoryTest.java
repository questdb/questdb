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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.str.StartsWithVarcharFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class StartsWithVarcharFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testNullOrEmptyString() throws Exception {
        call(utf8(null), utf8(null)).andAssert(false);
        call(utf8("test"), utf8(null)).andAssert(false);
        call(utf8(null), utf8("test")).andAssert(false);
        call(utf8(""), utf8("test")).andAssert(false);
        call(utf8("test"), utf8("")).andAssert(true);
        call(utf8(""), utf8("")).andAssert(true);
    }

    @Test
    public void testStartsWith() throws Exception {
        assertQuery("col\ntrue\n", "select starts_with('ABC'::varchar, 'ABC'::varchar) col");
        assertQuery("col\ntrue\n", "select starts_with('ABCX'::varchar, 'ABC'::varchar) col");
        assertQuery("col\nfalse\n", "select starts_with('ABC'::varchar, 'ABCX'::varchar) col");
        assertQuery("col\ntrue\n", "select starts_with('ABCDEFGH'::varchar, 'ABC'::varchar) col");
        assertQuery("col\nfalse\n", "select starts_with('ABCDEFGH'::varchar, 'XYZ'::varchar) col");
        assertQuery("col\nfalse\n", "select starts_with('ABCDEFGH'::varchar, 'ABCDEFGHIJK'::varchar) col");
        assertQuery("col\ntrue\n", "select starts_with('cAsEsEnsItIvE'::varchar, 'cAsE'::varchar) col");
        assertQuery("col\nfalse\n", "select starts_with('smallcase'::varchar, 'SMALL'::varchar) col");
        assertQuery("col\nfalse\n", "select starts_with('smallcase'::varchar, 'smaLL'::varchar) col");
    }

    @Test
    public void testStartsWithNonASCII() throws Exception {
        assertQuery("col\nfalse\n", "select starts_with('hőm','hőmé'::varchar) col");
        assertQuery("col\ntrue\n", "select starts_with('hőmé','hőmé'::varchar) col");
        assertQuery("col\ntrue\n", "select starts_with('hőmé','hőm'::varchar) col");
        assertQuery("col\nfalse\n", "select starts_with('hőmé','hőmo'::varchar) col");
        assertQuery("col\ntrue\n", "select starts_with('hőmérséklet','hőmé'::varchar) col");
        assertQuery("col\nfalse\n", "select starts_with('hőmérséklet','hőmérséklee'::varchar) col");
    }

    @Test
    public void testStartsWithSpecialCharacters() throws Exception {
        assertQuery("col\ntrue\n", "select starts_with('~!@#$%^&*()_-:<>?,./'::varchar, '~!@#'::varchar) col");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new StartsWithVarcharFunctionFactory();
    }
}
