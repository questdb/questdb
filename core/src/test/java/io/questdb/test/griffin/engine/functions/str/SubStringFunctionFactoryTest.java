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

import io.questdb.cairo.CairoException;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.str.SubStringFunctionFactory;
import io.questdb.std.Numbers;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class SubStringFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testNonPositiveStart() throws Exception {
        call("foo", -3, 4).andAssert("");
        call("foo", -3, 5).andAssert("f");
        call(null, -3, 0).andAssert(null);
    }

    @Test
    public void testNullOrEmptyStr() throws Exception {
        call(null, 2, 4).andAssert(null);
        call("", 2, 4).andAssert("");
    }

    @Test
    public void testSimple() throws Exception {
        assertQuery(
                "k\tsubstring\tlength\n" +
                        "JWCPSWHYRXPEHNRXGZSXUXIB\tNRXGZS\t6\n" +
                        "GPGWFFYUDE\t\t0\n" +
                        "QEHBHFOWLPDXYSBEOUOJSH\tSBEOUO\t6\n" +
                        "EDRQQULOFJGETJRSZSRY\tJRSZSR\t6\n" +
                        "BVTMHGOOZZVDZJMYICCXZ\tJMYICC\t6\n" +
                        "ICWEKGHVUVSDOTSEDYYCTGQ\tTSEDYY\t6\n" +
                        "YXWCKYLSUWDSWUGSH\tUGSH\t4\n" +
                        "NVTIQBZXIOVIKJSMSSUQSR\tJSMSSU\t6\n" +
                        "\t\t-1\n" +  // null
                        "VVSJOJIPHZEPIHVLTO\tHVLTO\t5\n" +
                        "JUMLGLHMLLEOY\t\t0\n" +
                        "\t\t-1\n" +  // null
                        "IPZIMNZZRMFMBEZG\tEZG\t3\n" +
                        "VDKFLOPJOXPKRGIIHY\tGIIHY\t5\n" +
                        "OQMYSSMPGLUOHNZHZS\tNZHZS\t5\n",
                "select k, substring(k,14,6), length(substring(k,14,6)) from x",
                "create table x as (select rnd_str(10,25,1) k from long_sequence(15))",
                null,
                true,
                true
        );
    }

    @Test
    public void testStartOrLenOutOfRange() throws Exception {
        call("foo", 10, 1).andAssert("");
        call("foo", 10, 10).andAssert("");
        call("foo", 1, 10).andAssert("foo");
        call(null, 2, 4).andAssert(null);
    }

    @Test
    public void testZeroOrInvalidLength() throws Exception {
        call("foo", 3, 0).andAssert("");
        call(null, 3, 0).andAssert(null);
        call("foo", 3, Numbers.INT_NULL).andAssert(null);

        try {
            call("foo", 3, -1).andAssert(null);
            assertExceptionNoLeakCheck("non-const negative len is not allowed");
        } catch (CairoException e) {
            // negative substring length is not allowed
        }

        try {
            assertQueryNoLeakCheck(
                    null,
                    "select substring('foo',1,-6)",
                    null,
                    true
            );
            assertExceptionNoLeakCheck("const negative len is not allowed");
        } catch (SqlException e) {
            // negative substring length is not allowed
        }
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new SubStringFunctionFactory();
    }
}
