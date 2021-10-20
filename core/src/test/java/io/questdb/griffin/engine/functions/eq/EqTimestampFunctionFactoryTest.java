/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.std.NumericException;
import org.junit.Test;

import static io.questdb.std.datetime.microtime.TimestampFormatUtils.parseUTCTimestamp;

public class EqTimestampFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testEquals() throws SqlException, NumericException {
        long t1 = parseUTCTimestamp("2020-12-31T23:59:59.000000Z");
        long t2 = parseUTCTimestamp("2020-12-31T23:59:59.000001Z");
        callBySignature("=(NN)", t1, t1).andAssert(true);
        callBySignature("=(NN)", t1, t2).andAssert(false);
    }

    @Test
    public void testNotEquals() throws SqlException, NumericException {
        long t1 = parseUTCTimestamp("2020-12-31T23:59:59.000000Z");
        long t2 = parseUTCTimestamp("2020-12-31T23:59:59.000001Z");
        callBySignature("<>(NN)", t1, t1).andAssert(false);
        callBySignature("<>(NN)", t1, t2).andAssert(true);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new EqTimestampFunctionFactory();
    }
}
