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

package io.questdb.test.griffin.engine.functions.bin;

import io.questdb.PropertyKey;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.bin.Base64FunctionFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class Base64FunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testInvalidLength() {
        try {
            assertQueryNoLeakCheck("", "select base64(rnd_bin(6,6,0), 0)", null);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "maxLength has to be greater than 0");
        }
    }

    @Test
    public void testLengthHardLimit() throws Exception {
        node1.setProperty(PropertyKey.BINARYDATA_ENCODING_MAXLENGTH, 9); // update limit temporarily up to 9 bytes
        AbstractCairoTest.assertQuery("x\ty\n" +
                        "00000000 ee 41 1d 15 55 8a 17 fa d8 cc 14 ce f1 59 88 c4\t7kEdFVWKF/rY\n" +
                        "00000000 3b 72 db f3 04 1b c7 88 de a0 79 3c 77 15 68 61\tO3Lb8wQbx4je\n" +
                        "00000000 af 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e 38\trxnElZQ2U0m0\n" +
                        "00000000 1b 9e f4 c8 39 09 fe d8 9d 30 78 36 6a 32 de e4\tG570yDkJ/tid\n" +
                        "00000000 d2 35 07 42 fc 31 79 5f 8b 81 2b 93 4d 1a 8e 78\t0jUHQvwxeV+L\n",
                "select x, base64(x, 100) y from t",
                "create table t as (select rnd_bin(16,16,0) x from long_sequence(5))",
                null,
                true,
                true
        );
    }

    @Test
    public void testRandomBinSeq() throws Exception {
        AbstractCairoTest.assertQuery("x\ty\n" +
                        "00000000 ee 41 1d 15 55 8a\t7kEdFVWK\n" +
                        "\t\n" +
                        "00000000 d8 cc 14 ce f1 59\t2MwUzvFZ\n" +
                        "00000000 c4 91 3b 72 db f3\txJE7ctvz\n" +
                        "00000000 1b c7 88 de a0 79\tG8eI3qB5\n" +
                        "00000000 77 15 68 61 26 af\tdxVoYSav\n" +
                        "\t\n" +
                        "00000000 95 94 36 53 49 b4\tlZQ2U0m0\n" +
                        "\t\n" +
                        "00000000 3b 08 a1 1e 38 8d\tOwihHjiN\n",
                "select x, base64(x, 100) y from t",
                "create table t as (select rnd_bin(6,6,1) x from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new Base64FunctionFactory();
    }
}
