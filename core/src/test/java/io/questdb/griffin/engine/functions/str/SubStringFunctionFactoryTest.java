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
import io.questdb.std.Numbers;
import org.junit.Test;

public class SubStringFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new SubStringFunctionFactory();
    }

    @Test
    public void testSimple() throws Exception {
        assertQuery(
                "k\tsubstring\tlength\n" +
                        "JWCPSWHYRXPEHNRX\tNRX\t3\n" +
                        "SXUXIBBTGPGWFFYU\tFYU\t3\n" +
                        "YYQEHBHFOWLPDXYSBEO\tXYSBEO\t6\n" +
                        "JSHRUEDRQQUL\t\t0\n" +
                        "\t\t-1\n" +  // null
                        "GETJRSZSRYRFBVTMHGOO\tVTMHGO\t6\n" +
                        "VDZJMYICCXZOUIC\tIC\t2\n" +
                        "KGHVUVSDOTSEDYYCTGQO\tYYCTGQ\t6\n" +
                        "\t\t-1\n" +  // null
                        "WCKYLSUWDSWUGSH\tSH\t2\n",
                "select k, substring(k,14,6), length(substring(k,14,6)) from x",
                "create table x as (select rnd_str(10,20,1) k from long_sequence(10))",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testNullOrEmptyStr() throws Exception {
        call(null, 2, 4).andAssert(null);
        call("", 2, 4).andAssert("");
    }

    @Test
    public void testInvalidLength() throws Exception {
        call("foo", 3, -1).andAssert(null);
        call("foo", 3, Numbers.INT_NaN).andAssert(null);
    }

    @Test
    public void testZeroLength() throws Exception {
        call("foo", 3, 0).andAssert("");
        call(null, 3, 0).andAssert(null);
    }

    @Test
    public void testNonPositiveStart() throws Exception {
        call("foo", -3, 1).andAssert("f");
        call("foo", -3, 0).andAssert("");
        call(null, -3, 0).andAssert(null);
    }

    @Test
    public void testStartOrLenOutOfRange() throws Exception {
        call("foo", 10, 1).andAssert("");
        call("foo", 10, 10).andAssert("");
        call("foo", 1, 10).andAssert("foo");
    }

}
