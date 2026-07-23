/*+*****************************************************************************
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
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Assert;
import org.junit.Test;

public class VarcharDateFunctionUtilsTest extends AbstractFunctionFactoryTest {

    @Test
    public void testIsAsciiOnlyPattern() throws SqlException {
        assertAsciiOnly("G", false);
        assertAsciiOnly("M", true);
        assertAsciiOnly("MM", true);
        assertAsciiOnly("MMM", false);
        assertAsciiOnly("MMMM", false);
        assertAsciiOnly("E", false);
        assertAsciiOnly("EE", false);
        assertAsciiOnly("a", false);
        assertAsciiOnly("z", false);
        assertAsciiOnly("zz", false);
        assertAsciiOnly("zzz", false);
        assertAsciiOnly("Z", false);
        assertAsciiOnly("x", false);
        assertAsciiOnly("xx", false);
        assertAsciiOnly("xxx", false);
        assertAsciiOnly("yyyy年MM月dd日", false);
        assertAsciiOnly("yyyy-MM-dd HH:mm:ss.SSS", true);
        assertAsciiOnly("MM-dd-MM", true);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new VarcharToDateFunctionFactory();
    }

    private void assertAsciiOnly(String pattern, boolean expected) throws SqlException {
        final ObjList<Function> args = new ObjList<>();
        args.add(new VarcharColumn(0));
        args.add(new StrConstant(pattern));

        final IntList argPositions = new IntList();
        argPositions.add(0);
        argPositions.add(0);

        try (Function function = getFunctionFactory().newInstance(
                0,
                args,
                argPositions,
                configuration,
                sqlExecutionContext
        )) {
            final boolean isAsciiOnly = function.getClass().getName().endsWith("$ToAsciiDateFunction");
            Assert.assertEquals(pattern, expected, isAsciiOnly);
        }
    }
}
