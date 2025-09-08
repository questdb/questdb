/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.griffin.engine.functions.date.ToStrTimestampFunctionFactory;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class ToCharTimestampVCFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testNaN() throws SqlException {
        call(Numbers.LONG_NULL, "dd/MM/yyyy hh:mm:ss").andAssert(null);
    }

    @Test
    public void testNullFormat() {
        assertFailure(10, "format must not be null", 0L, null);
    }

    @Test
    public void testSimple() throws SqlException, NumericException {
        call(MicrosFormatUtils.parseTimestamp("2018-03-10T11:03:33.123Z"),
                "dd/MM/yyyy hh:mm:ss").andAssert("10/03/2018 11:03:33");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new ToStrTimestampFunctionFactory();
    }
}