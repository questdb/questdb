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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.str.ToCharBinFunctionFactory;
import io.questdb.std.Rnd;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class ToCharBinVFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testDanglingLastLine() throws SqlException {
        Rnd rnd = new Rnd();
        call(rnd.nextBytes(150))
                .andAssert("00000000 56 54 4a 57 43 50 53 57 48 59 52 58 50 45 48 4e\n" +
                        "00000010 52 58 47 5a 53 58 55 58 49 42 42 54 47 50 47 57\n" +
                        "00000020 46 46 59 55 44 45 59 59 51 45 48 42 48 46 4f 57\n" +
                        "00000030 4c 50 44 58 59 53 42 45 4f 55 4f 4a 53 48 52 55\n" +
                        "00000040 45 44 52 51 51 55 4c 4f 46 4a 47 45 54 4a 52 53\n" +
                        "00000050 5a 53 52 59 52 46 42 56 54 4d 48 47 4f 4f 5a 5a\n" +
                        "00000060 56 44 5a 4a 4d 59 49 43 43 58 5a 4f 55 49 43 57\n" +
                        "00000070 45 4b 47 48 56 55 56 53 44 4f 54 53 45 44 59 59\n" +
                        "00000080 43 54 47 51 4f 4c 59 58 57 43 4b 59 4c 53 55 57\n" +
                        "00000090 44 53 57 55 47 53");
    }

    @Test
    public void testExactLines() throws SqlException {
        Rnd rnd = new Rnd();
        call(rnd.nextBytes(256))
                .andAssert("00000000 56 54 4a 57 43 50 53 57 48 59 52 58 50 45 48 4e\n" +
                        "00000010 52 58 47 5a 53 58 55 58 49 42 42 54 47 50 47 57\n" +
                        "00000020 46 46 59 55 44 45 59 59 51 45 48 42 48 46 4f 57\n" +
                        "00000030 4c 50 44 58 59 53 42 45 4f 55 4f 4a 53 48 52 55\n" +
                        "00000040 45 44 52 51 51 55 4c 4f 46 4a 47 45 54 4a 52 53\n" +
                        "00000050 5a 53 52 59 52 46 42 56 54 4d 48 47 4f 4f 5a 5a\n" +
                        "00000060 56 44 5a 4a 4d 59 49 43 43 58 5a 4f 55 49 43 57\n" +
                        "00000070 45 4b 47 48 56 55 56 53 44 4f 54 53 45 44 59 59\n" +
                        "00000080 43 54 47 51 4f 4c 59 58 57 43 4b 59 4c 53 55 57\n" +
                        "00000090 44 53 57 55 47 53 48 4f 4c 4e 56 54 49 51 42 5a\n" +
                        "000000a0 58 49 4f 56 49 4b 4a 53 4d 53 53 55 51 53 52 4c\n" +
                        "000000b0 54 4b 56 56 53 4a 4f 4a 49 50 48 5a 45 50 49 48\n" +
                        "000000c0 56 4c 54 4f 56 4c 4a 55 4d 4c 47 4c 48 4d 4c 4c\n" +
                        "000000d0 45 4f 59 50 48 52 49 50 5a 49 4d 4e 5a 5a 52 4d\n" +
                        "000000e0 46 4d 42 45 5a 47 48 57 56 44 4b 46 4c 4f 50 4a\n" +
                        "000000f0 4f 58 50 4b 52 47 49 49 48 59 48 42 4f 51 4d 59");
    }

    @Test
    public void testNull() throws SqlException {
        call((Object) null).andAssert(null);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new ToCharBinFunctionFactory();
    }
}