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

package io.questdb.test.griffin;

import io.questdb.griffin.Plannable;
import io.questdb.griffin.TextPlanSink;
import io.questdb.std.Decimals;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

public class TextPlanSinkTest {

    @Test
    public void testSink() {
        TextPlanSink sink = new TextPlanSink();

        sink.val(0.0f);
        sink.val(" ");
        sink.val((Plannable) null);
        sink.val(" ");
        ObjList<Object> list = new ObjList<>();
        list.add(null);
        sink.val(list, 0, 1);

        Assert.assertEquals("0.0 null [null]", sink.getSink().toString());
    }

    @Test
    public void testSinkDecimal() {
        TextPlanSink sink = new TextPlanSink();

        sink.valDecimal(12300, 5, 2);
        sink.val(" ");
        sink.valDecimal(Decimals.DECIMAL64_NULL, 5, 2);
        sink.val(" ");
        sink.valDecimal(123, 456, 38, 15);
        sink.val(" ");
        sink.valDecimal(
                Decimals.DECIMAL128_HI_NULL,
                Decimals.DECIMAL128_LO_NULL,
                38,
                10
        );
        sink.val(" ");
        sink.valDecimal(123, 456, 789, 12, 75, 15);
        sink.val(" ");
        sink.valDecimal(
                Decimals.DECIMAL256_HH_NULL,
                Decimals.DECIMAL256_HL_NULL,
                Decimals.DECIMAL256_LH_NULL,
                Decimals.DECIMAL256_LL_NULL,
                75,
                15
        );

        Assert.assertEquals("123.00 null 2268949.521066274849224 null 772083513452561734106970858370490908534443021.732119385735180 null", sink.getSink().toString());
    }
}
