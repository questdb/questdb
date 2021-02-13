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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.griffin.engine.functions.columns.StrColumn;
import io.questdb.griffin.engine.functions.columns.TimestampColumn;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.std.datetime.microtime.TimestampFormatUtils.parseUTCTimestamp;

public class EqTimestampStrFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testTimestampEqualsString() throws SqlException, NumericException {
        testTimestampAsString("=(NS)");
    }

    @Test
    public void testTimestampEqualsStringWithPeriod() throws SqlException, NumericException {
        testTimestampAsStringWithPeriod("=(NS)");
    }

    @Test
    public void testTimestampEqualsStringWithPeriodAndCount() throws SqlException, NumericException {
        testTimestampAsStringWithPeriodAndCount("=(NS)");
    }

    @Test
    public void testStringEqualsTimestamp() throws SqlException, NumericException {
        testTimestampAsString("=(SN)");
    }

    @Test
    public void testStringWithPeriodEqualsTimestamp() throws SqlException, NumericException {
        testTimestampAsStringWithPeriod("=(SN)");
    }

    @Test
    public void testStringWithPeriodAndCountEqualsTimestamp() throws SqlException, NumericException {
        testTimestampAsStringWithPeriodAndCount("=(SN)");
    }

    @Test
    public void testTimestampNotEqualsString() throws SqlException, NumericException {
        testTimestampAsString("<>(NS)");
    }

    @Test
    public void testTimestampNotEqualsStringWithPeriod() throws SqlException, NumericException {
        testTimestampAsStringWithPeriod("<>(NS)");
    }

    @Test
    public void testTimestampNotEqualsStringWithPeriodAndCount() throws SqlException, NumericException {
        testTimestampAsStringWithPeriodAndCount("<>(NS)");
    }

    @Test
    public void testStringNotEqualsTimestamp() throws SqlException, NumericException {
        testTimestampAsString("<>(SN)");
    }

    @Test
    public void testStringWithPeriodNotEqualsTimestamp() throws SqlException, NumericException {
        testTimestampAsStringWithPeriod("<>(SN)");
    }

    @Test
    public void testStringWithPeriodAndCountNotEqualsTimestamp() throws SqlException, NumericException {
        testTimestampAsStringWithPeriodAndCount("<>(SN)");
    }

    @Test
    public void testFailureWhenConstantStringIsNotValidTimestamp() throws NumericException {
        assertFailure(true, 38, "Not a date", parseUTCTimestamp("2020-12-31T23:59:59.000000Z"), "abc");
    }

    @Test
    public void testFalseWhenVariableStringIsNotValidTimestamp() throws SqlException, NumericException {
        long timestamp = parseUTCTimestamp("2020-12-31T23:59:59.000000Z");
        CharSequence invalidTimestamp = "abc";
        FunctionFactory factory = getFunctionFactory();
        ObjList<Function> args = new ObjList<>();
        args.add(new TimestampColumn(0, 0));
        args.add(new StrColumn(1, 5));
        Function function = factory.newInstance(args, 3, configuration, sqlExecutionContext);
        Assert.assertFalse(function.getBool(new Record() {
            @Override
            public CharSequence getStr(int col) {
                return invalidTimestamp;
            }

            @Override
            public long getTimestamp(int col) {
                return timestamp;
            }
        }));
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new EqTimestampStrFunctionFactory();
    }

    private void testTimestampAsString(String signature) throws NumericException, SqlException {
        String t1 = "2020-12-31T23:59:59.000000Z";
        String t2 = "2020-12-31T23:59:59.000001Z";
        callAndAssert(signature, parseUTCTimestamp(t1), t1, true);
        callAndAssert(signature, parseUTCTimestamp(t1), t2, false);
        callAndAssert(signature, parseUTCTimestamp(t1), "2020", true);
        callAndAssert(signature, parseUTCTimestamp(t1), "2019", false);
    }

    private void testTimestampAsStringWithPeriod(String signature) throws NumericException, SqlException {
        String t1 = "2020-12-31T23:59:59.000000Z";
        callAndAssert(signature, parseUTCTimestamp(t1), "2020-12-31T23:59:58.000000Z;1s", true);
        callAndAssert(signature, parseUTCTimestamp(t1), "2020-12-31T23:59:59.000000Z;1s", true);
        callAndAssert(signature, parseUTCTimestamp(t1), "2020-12-31T23:59:59.000000Z;-1s", false);
        callAndAssert(signature, parseUTCTimestamp(t1), "2019;1y", true);
        callAndAssert(signature, parseUTCTimestamp(t1), "2020;-1s", false);
    }

    private void testTimestampAsStringWithPeriodAndCount(String signature) throws NumericException, SqlException {
        String t1 = "2020-12-31T23:59:59.000000Z";
        String t2 = "2020-12-31T23:59:58.000000Z";
        callAndAssert(signature, parseUTCTimestamp(t1), "2020-12-23T23:30:00.000000Z;30m;2d;5", true);
        callAndAssert(signature, parseUTCTimestamp(t1), "2020-12-24T23:30:00.000000Z;30m;2d;5", false);
        callAndAssert(signature, parseUTCTimestamp(t1), "2020-12-21T23:30:00.000000Z;30m;2d;5", false);
        callAndAssert(signature, parseUTCTimestamp(t1), "2020-12-23T23:20:00.000000Z;30m;2d;5", false);
        callAndAssert(signature, parseUTCTimestamp(t1), "2020-12-31T23:59:59.000000Z;30m;2d;5", true);
        callAndAssert(signature, parseUTCTimestamp(t1), "2020-12-31T23:29:59.000000Z;30m;2d;5", true);
        callAndAssert(signature, parseUTCTimestamp(t1), "2020-12-31T23:59:59.000001Z;30m;2d;5", false);
        callAndAssert(signature, parseUTCTimestamp(t1), "2020-12-23;-1s;2d;5", false);
        callAndAssert(signature, parseUTCTimestamp(t2), "2020-12-23;-1s;2d;5", true);
        callAndAssert(signature, parseUTCTimestamp(t1), "2020-12-24T00:00:00.000000Z;-1s;2d;5", false);
    }

    private void callAndAssert(String signature, long arg1, String arg2, boolean expectedIfEquals) throws SqlException {
        boolean rightArgIsString = signature.contains("(NS)");
        boolean equalsOperator = signature.startsWith("=");
        if (rightArgIsString) {
            callBySignature(signature, arg1, arg2).andAssert(equalsOperator == expectedIfEquals);
        } else {
            callBySignature(signature, arg2, arg1).andAssert(equalsOperator == expectedIfEquals);
        }
    }
}
