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

package io.questdb.griffin.engine.functions.lt;

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

public class LtStrTimestampFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testFullTimestampLessThan() throws SqlException, NumericException {
        long t1 = parseUTCTimestamp("2020-12-31T23:59:59.999999Z");
        callBySignature("<(SN)", "2020-12-31T23:59:59.999999Z", t1).andAssert(false);
        callBySignature("<(SN)", "2020-12-31T23:59:58.999998Z", t1).andAssert(true);
        callBySignature("<(SN)", "2021-01-01T00:00:00.000001Z", t1).andAssert(false);
    }

    @Test
    public void testPartialTimestampLessThan() throws SqlException, NumericException {
        long t1 = parseUTCTimestamp("2020-01-01T00:00:00.000000Z");
        long t2 = parseUTCTimestamp("2020-06-30T23:59:59.999999Z");
        long t3 = parseUTCTimestamp("2020-12-31T23:59:59.999999Z");
        callBySignature("<(SN)", "2020", t1).andAssert(false);
        callBySignature("<(SN)", "2020", t2).andAssert(false);
        callBySignature("<(SN)", "2020", t3).andAssert(false);
        callBySignature("<(SN)", "2019", t1).andAssert(true);
        callBySignature("<(SN)", "2019", t2).andAssert(true);
        callBySignature("<(SN)", "2019", t3).andAssert(true);
        callBySignature("<(SN)", "2021", t1).andAssert(false);
        callBySignature("<(SN)", "2021", t2).andAssert(false);
        callBySignature("<(SN)", "2021", t3).andAssert(false);
    }

    @Test
    public void testLessThanOrEqualToFullTimestamp() throws SqlException, NumericException {
        long t1 = parseUTCTimestamp("2020-12-31T23:59:59.999999Z");
        callBySignature("<=(NS)", t1, "2020-12-31T23:59:59.999999Z").andAssert(true);
        callBySignature("<=(NS)", t1, "2020-12-31T23:59:58.999998Z").andAssert(false);
        callBySignature("<=(NS)", t1, "2021-01-01T00:00:00.000001Z").andAssert(true);
    }

    @Test
    public void testLessThanOrEqualToPartialTimestamp() throws SqlException, NumericException {
        long t1 = parseUTCTimestamp("2020-01-01T00:00:00.000000Z");
        long t2 = parseUTCTimestamp("2020-06-30T23:59:59.999999Z");
        long t3 = parseUTCTimestamp("2020-12-31T23:59:59.999999Z");
        callBySignature("<=(NS)", t1, "2020").andAssert(true);
        callBySignature("<=(NS)", t2, "2020").andAssert(true);
        callBySignature("<=(NS)", t3, "2020").andAssert(true);
        callBySignature("<=(NS)", t1, "2019").andAssert(false);
        callBySignature("<=(NS)", t2, "2019").andAssert(false);
        callBySignature("<=(NS)", t3, "2019").andAssert(false);
        callBySignature("<=(NS)", t1, "2021").andAssert(true);
        callBySignature("<=(NS)", t2, "2021").andAssert(true);
        callBySignature("<=(NS)", t3, "2021").andAssert(true);
    }

    @Test
    public void testGreaterThanFullTimestamp() throws SqlException, NumericException {
        long t1 = parseUTCTimestamp("2020-12-31T23:59:59.999999Z");
        callBySignature(">(NS)", t1, "2020-12-31T23:59:59.999999Z").andAssert(false);
        callBySignature(">(NS)", t1, "2020-12-31T23:59:58.999998Z").andAssert(true);
        callBySignature(">(NS)", t1, "2021-01-01T00:00:00.000001Z").andAssert(false);
    }

    @Test
    public void testGreaterThanPartialTimestamp() throws SqlException, NumericException {
        long t1 = parseUTCTimestamp("2020-01-01T00:00:00.000000Z");
        long t2 = parseUTCTimestamp("2020-06-30T23:59:59.999999Z");
        long t3 = parseUTCTimestamp("2020-12-31T23:59:59.999999Z");
        callBySignature(">(NS)", t1, "2020").andAssert(false);
        callBySignature(">(NS)", t2, "2020").andAssert(false);
        callBySignature(">(NS)", t3, "2020").andAssert(false);
        callBySignature(">(NS)", t1, "2019").andAssert(true);
        callBySignature(">(NS)", t2, "2019").andAssert(true);
        callBySignature(">(NS)", t3, "2019").andAssert(true);
        callBySignature(">(NS)", t1, "2021").andAssert(false);
        callBySignature(">(NS)", t2, "2021").andAssert(false);
        callBySignature(">(NS)", t3, "2021").andAssert(false);
    }

    @Test
    public void testFullTimestampGreaterThanOrEqualTo() throws SqlException, NumericException {
        long t1 = parseUTCTimestamp("2020-12-31T23:59:59.999999Z");
        callBySignature(">=(SN)", "2020-12-31T23:59:59.999999Z", t1).andAssert(true);
        callBySignature(">=(SN)", "2020-12-31T23:59:58.999998Z", t1).andAssert(false);
        callBySignature(">=(SN)", "2021-01-01T00:00:00.000001Z", t1).andAssert(true);
    }

    @Test
    public void testPartialTimestampGreaterThanOrEqualTo() throws SqlException, NumericException {
        long t1 = parseUTCTimestamp("2020-01-01T00:00:00.000000Z");
        long t2 = parseUTCTimestamp("2020-06-30T23:59:59.999999Z");
        long t3 = parseUTCTimestamp("2020-12-31T23:59:59.999999Z");
        callBySignature(">=(SN)", "2020", t1).andAssert(true);
        callBySignature(">=(SN)", "2020", t2).andAssert(true);
        callBySignature(">=(SN)", "2020", t3).andAssert(true);
        callBySignature(">=(SN)", "2019", t1).andAssert(false);
        callBySignature(">=(SN)", "2019", t2).andAssert(false);
        callBySignature(">=(SN)", "2019", t3).andAssert(false);
        callBySignature(">=(SN)", "2021", t1).andAssert(true);
        callBySignature(">=(SN)", "2021", t2).andAssert(true);
        callBySignature(">=(SN)", "2021", t3).andAssert(true);
    }

    @Test
    public void testFailureWhenConstantStringIsNotValidTimestamp() throws NumericException {
        assertFailure(true, 0, "Invalid date", "abc", parseUTCTimestamp("2020-12-31T23:59:59.000000Z"));
    }

    @Test
    public void testFalseWhenVariableStringIsNotValidTimestamp() throws SqlException, NumericException {
        long timestamp = parseUTCTimestamp("2020-12-31T23:59:59.000000Z");
        CharSequence invalidTimestamp = "abc";
        FunctionFactory factory = getFunctionFactory();
        ObjList<Function> args = new ObjList<>();
        args.add(new StrColumn(0, 0));
        args.add(new TimestampColumn(1, 5));
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
        return new LtStrTimestampFunctionFactory();
    }
}
