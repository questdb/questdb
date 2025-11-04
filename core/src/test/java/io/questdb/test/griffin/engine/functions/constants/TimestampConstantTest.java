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

package io.questdb.test.griffin.engine.functions.constants;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import org.junit.Assert;
import org.junit.Test;

public class TimestampConstantTest {
    @Test
    public void testTimestampConstant() {
        TimestampConstant constant = new TimestampConstant(909120909900L, ColumnType.TIMESTAMP_MICRO);
        Assert.assertTrue(constant.isConstant());
        Assert.assertEquals(909120909900L, constant.getTimestamp(null));
    }

    @Test
    public void testTimestampNSConstant() {
        TimestampConstant constant = new TimestampConstant(909120909900000L, ColumnType.TIMESTAMP_NANO);
        Assert.assertTrue(constant.isConstant());
        Assert.assertEquals(909120909900000L, constant.getTimestamp(null));
    }
}