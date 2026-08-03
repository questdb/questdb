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

package io.questdb.test.griffin.engine.functions.array;

import io.questdb.cairo.ColumnType;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class ArrayCreateFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllDecimalElements() throws Exception {
        assertException("SELECT ARRAY[1.5m, 2.5m]", 13, "unsupported array element type [type=DECIMAL(2,1)]");
    }

    @Test
    public void testDecimalBindVariableElement() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setDecimal(0, 0, 0, 0, 125, ColumnType.getDecimalType(10, 2));
            assertExceptionNoLeakCheck("SELECT ARRAY[$1]", 13, "unsupported array element type [type=DECIMAL(10,2)]");
            assertExceptionNoLeakCheck("SELECT ARRAY[$1, 1.0]", 13, "unsupported array element type [type=DECIMAL(10,2)]");
            assertExceptionNoLeakCheck("SELECT ARRAY[1.0, $1]", 18, "unsupported array element type [type=DECIMAL(10,2)]");
        });
    }

    @Test
    public void testDecimalColumnElement() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (d DECIMAL(10, 2))");
            execute("INSERT INTO tango VALUES (1.25m)");
            assertExceptionNoLeakCheck(
                    "SELECT ARRAY[d] FROM tango",
                    13,
                    "unsupported array element type [type=DECIMAL(10,2)]"
            );
        });
    }

    @Test
    public void testDoubleBindVariableElement() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setDouble(0, 2.5);
            assertQuery("SELECT ARRAY[$1, 1.0] arr")
                    .noLeakCheck()
                    .expectSize()
                    .returns("arr\n[2.5,1.0]\n");
        });
    }

    @Test
    public void testDoubleElementsStillWork() throws Exception {
        assertMemoryLeak(() -> assertSqlWithTypes(
                "ARRAY\n[[1.0,2.0],[3.0,4.5]]:DOUBLE[][]\n",
                "SELECT ARRAY[[1.0, 2], [3, 4.5]]"
        ));
    }

    @Test
    public void testMixedDecimalAndDoubleElements() throws Exception {
        assertException("SELECT ARRAY[1.5, 2.5m]", 18, "unsupported array element type [type=DECIMAL(2,1)]");
        assertException("SELECT ARRAY[2.5m, 1.5]", 13, "unsupported array element type [type=DECIMAL(2,1)]");
    }

    @Test
    public void testNestedDecimalElements() throws Exception {
        assertException("SELECT ARRAY[[1.5m, 2.5m], [3.5m, 4.5m]]", 28, "unsupported array element type [type=DECIMAL(2,1)]");
    }

    @Test
    public void testNonNumericElements() throws Exception {
        assertException("SELECT ARRAY[true, false]", 13, "unsupported array element type [type=BOOLEAN]");
        assertException("SELECT ARRAY[rnd_uuid4()]", 13, "unsupported array element type [type=UUID]");
    }

    @Test
    public void testNullBindVariableElement() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.define(0, ColumnType.DOUBLE, 0);
            assertQuery("SELECT ARRAY[$1, 1.0] arr")
                    .noLeakCheck()
                    .expectSize()
                    .returns("arr\n[null,1.0]\n");

            // the decimal bind variable is rejected on its type, whether or not it carries a value
            bindVariableService.clear();
            bindVariableService.define(0, ColumnType.getDecimalType(10, 2), 0);
            assertExceptionNoLeakCheck("SELECT ARRAY[$1, 1.0]", 13, "unsupported array element type [type=DECIMAL(10,2)]");
        });
    }
}
