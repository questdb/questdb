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

package io.questdb.test.griffin.engine.functions.constants;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.Constants;
import org.junit.Assert;
import org.junit.Test;

public class ConstantsTest {
    @Test
    public void testNullConstants() {
        for (int i = 0; i < ColumnType.NULL; i++) { // NULL is the last type

            // Skip known non-nullable types
            if (i == ColumnType.BYTE || i == ColumnType.SHORT || i == ColumnType.BOOLEAN || i == ColumnType.CHAR || i == ColumnType.ARRAY) {
                continue;
            }

            ConstantFunction nullConstant = null;
            try {
                nullConstant = Constants.getNullConstant(i);
            } catch (AssertionError ae) {
                // expected for non-nullable and special types
            }

            if (nullConstant != null) {
                Assert.assertTrue("Null constant for type '" + ColumnType.nameOf(i)
                        + "', id = " + i + " is not null, function: " + nullConstant, nullConstant.isNullConstant());
            }
        }
    }
}
