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

package io.questdb.test.griffin.engine.functions.decimal;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.DecimalFunction;
import io.questdb.griffin.engine.functions.decimal.Decimal128Function;
import io.questdb.std.Decimal128;
import org.junit.Test;

public class Decimal128FunctionTest {
    private static final DecimalFunction function = new Decimal128Function(ColumnType.getDecimalType(38, 0)) {
        @Override
        public void getDecimal128(Record record, Decimal128 sink) {
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }
    };

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal16() {
        function.getDecimal16(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal256() {
        function.getDecimal256(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal32() {
        function.getDecimal32(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal64() {
        function.getDecimal64(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal8() {
        function.getDecimal8(null);
    }
}
