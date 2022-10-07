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

package io.questdb.griffin.engine.functions.str;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class StartsWithStrFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Override
    protected FunctionFactory getFunctionFactory() {
        return new StartsWithStrFunctionFactory();
    }

    @Test
    public void testStartsWith() throws Exception {
        assertQuery("k\tstr\tstartswith\n"+
                        "JWCPSWHYRXPEHNRXGZSXUXIB\tJWCPS\ttrue\n" +
                        "GPGWFFYUDE\tGPGWF\ttrue\n",
                "select k, substring(k,0,6) str, starts_with(k, substring(k,0,6)) startswith from x",
                "create table x as (select rnd_str(10,25,1) k from long_sequence(2))",
                null,true,true, true);
    }

    @Test
    public void testNullOrEmptyString() throws Exception {
        call(null, null).andAssert(false);
        call("test",null).andAssert(false);
        call(null, "test").andAssert(false);
        call("", "test").andAssert(false);
        call("test", "").andAssert(false);
    }

    @Test
    public void testForEmptyString() throws Exception {
        call("","").andAssert(true);
    }
}
