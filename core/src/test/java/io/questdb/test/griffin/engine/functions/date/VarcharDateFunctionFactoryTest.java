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

package io.questdb.test.griffin.engine.functions.date;

import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.columns.VarcharColumn;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.date.VarcharToDateFunctionFactory;
import io.questdb.griffin.engine.functions.date.VarcharToPgDateFunctionFactory;
import io.questdb.griffin.engine.functions.date.VarcharToTimestampVCFunctionFactory;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Assert;
import org.junit.Test;

public class VarcharDateFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testUtf8FunctionsAreThreadSafe() throws SqlException {
        assertThreadSafe(new VarcharToDateFunctionFactory(), new VarcharColumn(0), new StrConstant("yyyy年MM月dd日"));
        assertThreadSafe(new VarcharToPgDateFunctionFactory(), new VarcharColumn(0));
        assertThreadSafe(new VarcharToTimestampVCFunctionFactory(), new VarcharColumn(0), new StrConstant("yyyy年MM月dd日"));
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new VarcharToDateFunctionFactory();
    }

    private void assertThreadSafe(FunctionFactory factory, Function... functions) throws SqlException {
        ObjList<Function> args = new ObjList<>();
        IntList argPositions = new IntList();
        for (Function function : functions) {
            args.add(function);
            argPositions.add(0);
        }

        try (Function function = factory.newInstance(0, args, argPositions, configuration, sqlExecutionContext)) {
            Assert.assertTrue(function.isThreadSafe());
        }
    }
}
