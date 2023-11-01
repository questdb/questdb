/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.griffin.engine.functions.lt;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.lt.LtDateFunctionFactory;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class LtDateFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testGreaterOrEqThanNull() throws SqlException, NumericException {
        long t1 = DateFormatUtils.parseDate("2020-12-31T23:59:59.000Z");
        long t2 = Numbers.LONG_NaN;
        callBySignature(">=(MM)", t1, t1).andAssert(true);
        callBySignature(">=(MM)", t1, t2).andAssert(false);
        callBySignature(">=(MM)", t2, t1).andAssert(false);
        callBySignature(">=(MM)", t2, t2).andAssert(false);
    }

    @Test
    public void testGreaterThan() throws SqlException, NumericException {
        long t1 = DateFormatUtils.parseDate("2020-12-31T23:59:59.000Z");
        long t2 = DateFormatUtils.parseDate("2020-12-31T23:59:59.001Z");
        callBySignature(">(MM)", t1, t1).andAssert(false);
        callBySignature(">(MM)", t1, t2).andAssert(false);
        callBySignature(">(MM)", t2, t1).andAssert(true);
    }

    @Test
    public void testGreaterThanNull() throws SqlException, NumericException {
        long t1 = DateFormatUtils.parseDate("2020-12-31T23:59:59.000Z");
        long t2 = Numbers.LONG_NaN;
        callBySignature(">(MM)", t1, t1).andAssert(false);
        callBySignature(">(MM)", t1, t2).andAssert(false);
        callBySignature(">(MM)", t2, t1).andAssert(false);
        callBySignature(">(MM)", t2, t2).andAssert(false);
    }

    @Test
    public void testGreaterThanOrEqualTo() throws SqlException, NumericException {
        long t1 = DateFormatUtils.parseDate("2020-12-31T23:59:59.000Z");
        long t2 = DateFormatUtils.parseDate("2020-12-31T23:59:59.001Z");
        callBySignature(">=(MM)", t1, t1).andAssert(true);
        callBySignature(">=(MM)", t1, t2).andAssert(false);
        callBySignature(">=(MM)", t2, t1).andAssert(true);
    }

    @Test
    public void testLessOrEqThanNull() throws SqlException, NumericException {
        long t1 = DateFormatUtils.parseDate("2020-12-31T23:59:59.000Z");
        long t2 = Numbers.LONG_NaN;
        callBySignature("<=(MM)", t1, t1).andAssert(true);
        callBySignature("<=(MM)", t1, t2).andAssert(false);
        callBySignature("<=(MM)", t2, t1).andAssert(false);
        callBySignature("<=(MM)", t2, t2).andAssert(false);
    }

    @Test
    public void testLessThan() throws SqlException, NumericException {
        long t1 = DateFormatUtils.parseDate("2020-12-31T23:59:59.000Z");
        long t2 = DateFormatUtils.parseDate("2020-12-31T23:59:59.001Z");
        callBySignature("<(MM)", t1, t1).andAssert(false);
        callBySignature("<(MM)", t1, t2).andAssert(true);
        callBySignature("<(MM)", t2, t1).andAssert(false);
    }

    @Test
    public void testLessThanOrEqualTo() throws SqlException, NumericException {
        long t1 = DateFormatUtils.parseDate("2020-12-31T23:59:59.000Z");
        long t2 = DateFormatUtils.parseDate("2020-12-31T23:59:59.001Z");
        callBySignature("<=(MM)", t1, t1).andAssert(true);
        callBySignature("<=(MM)", t1, t2).andAssert(true);
        callBySignature("<=(MM)", t2, t1).andAssert(false);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new LtDateFunctionFactory();
    }
}
