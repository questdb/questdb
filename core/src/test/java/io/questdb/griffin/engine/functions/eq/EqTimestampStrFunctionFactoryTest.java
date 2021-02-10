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
    public void testTimestampStringEquals() throws SqlException, NumericException {
        String t1 = "2020-12-31T23:59:59.000000Z";
        String t2 = "2020-12-31T23:59:59.000001Z";
        callBySignature("=(NS)", parseUTCTimestamp(t1), t1).andAssert(true);
        callBySignature("=(NS)", parseUTCTimestamp(t1), t2).andAssert(false);
        callBySignature("=(NS)", parseUTCTimestamp(t1), "2020").andAssert(true);
        callBySignature("=(NS)", parseUTCTimestamp(t1), "2019").andAssert(false);
    }

    @Test
    public void testStringTimestampEquals() throws SqlException, NumericException {
        String t1 = "2020-12-31T23:59:59.000000Z";
        String t2 = "2020-12-31T23:59:59.000001Z";
        callBySignature("=(SN)", t1, parseUTCTimestamp(t1)).andAssert(true);
        callBySignature("=(SN)", t1, parseUTCTimestamp(t2)).andAssert(false);
        callBySignature("=(SN)", "2020", parseUTCTimestamp(t1)).andAssert(true);
        callBySignature("=(SN)", "2019", parseUTCTimestamp(t1)).andAssert(false);
    }

    @Test
    public void testTimestampStringNotEquals() throws SqlException, NumericException {
        String t1 = "2020-12-31T23:59:59.000000Z";
        String t2 = "2020-12-31T23:59:59.000001Z";
        callBySignature("<>(NS)", parseUTCTimestamp(t1), t1).andAssert(false);
        callBySignature("<>(NS)", parseUTCTimestamp(t1), t2).andAssert(true);
        callBySignature("<>(NS)", parseUTCTimestamp(t1), "2020").andAssert(false);
        callBySignature("<>(NS)", parseUTCTimestamp(t1), "2019").andAssert(true);
    }

    @Test
    public void testStringTimestampNotEquals() throws SqlException, NumericException {
        String t1 = "2020-12-31T23:59:59.000000Z";
        String t2 = "2020-12-31T23:59:59.000001Z";
        callBySignature("<>(SN)", t1, parseUTCTimestamp(t1)).andAssert(false);
        callBySignature("<>(SN)", t1, parseUTCTimestamp(t2)).andAssert(true);
        callBySignature("<>(SN)", "2020", parseUTCTimestamp(t1)).andAssert(false);
        callBySignature("<>(SN)", "2019", parseUTCTimestamp(t1)).andAssert(true);
    }

    @Test
    public void testFailureWhenConstantStringIsNotValidTimestamp() throws NumericException {
        assertFailure(true, 36, "could not parse timestamp [value='abc']", parseUTCTimestamp("2020-12-31T23:59:59.000000Z"), "abc");
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
}
