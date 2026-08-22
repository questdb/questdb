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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.str.StartsWithStrFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class StartsWithStrFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testNullOrEmptyString() throws Exception {
        call(null, null).andAssert(false);
        call("test", null).andAssert(false);
        call(null, "test").andAssert(false);
        call("", "test").andAssert(false);
        call("test", "").andAssert(true);
        call("", "").andAssert(true);
    }

    @Test
    public void testStartsWith() throws Exception {
        assertQuery("select starts_with('ABCDEFGH', 'ABC') col").expectSize().returns("col\ntrue\n");
        assertQuery("select starts_with('ABCDEFGH', 'XYZ') col").expectSize().returns("col\nfalse\n");
        assertQuery("select starts_with('ABCDEFGH', 'ABCDEFGHIJK') col").expectSize().returns("col\nfalse\n");
        assertQuery("select starts_with('cAsEsEnsItIvE', 'cAsE') col").expectSize().returns("col\ntrue\n");
        assertQuery("select starts_with('smallcase', 'SMALL') col").expectSize().returns("col\nfalse\n");
        assertQuery("select starts_with('smallcase', 'smaLL') col").expectSize().returns("col\nfalse\n");
    }

    @Test
    public void testStartsWithNonASCII() throws Exception {
        assertQuery("select starts_with('hőmérséklet','hőmé') col").expectSize().returns("col\ntrue\n");
    }

    @Test
    public void testStartsWithSpecialCharacters() throws Exception {
        assertQuery("select starts_with('~!@#$%^&*()_-:<>?,./', '~!@#') col").expectSize().returns("col\ntrue\n");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new StartsWithStrFunctionFactory();
    }
}
