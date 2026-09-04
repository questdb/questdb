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

package io.questdb.test.cutlass.qwp;

import io.questdb.cutlass.qwp.server.egress.QwpEgressUpgradeProcessor;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Test;

public class QwpEgressMaxBatchRowsTest {

    @Test
    public void testBrowserUrlParameterIsAccepted() {
        Assert.assertEquals(
                512,
                QwpEgressUpgradeProcessor.negotiateMaxBatchRows(null, new Utf8String("512"))
        );
    }

    @Test
    public void testHeaderTakesPrecedence() {
        Assert.assertEquals(
                256,
                QwpEgressUpgradeProcessor.negotiateMaxBatchRows(
                        new Utf8String("256"),
                        new Utf8String("512")
                )
        );
    }

    @Test
    public void testInvalidAndOversizedValuesUseServerBounds() {
        Assert.assertEquals(
                QwpEgressUpgradeProcessor.MAX_ROWS_PER_BATCH,
                QwpEgressUpgradeProcessor.negotiateMaxBatchRows(null, null)
        );
        Assert.assertEquals(
                QwpEgressUpgradeProcessor.MAX_ROWS_PER_BATCH,
                QwpEgressUpgradeProcessor.negotiateMaxBatchRows(null, new Utf8String("invalid"))
        );
        Assert.assertEquals(
                QwpEgressUpgradeProcessor.MAX_ROWS_PER_BATCH,
                QwpEgressUpgradeProcessor.negotiateMaxBatchRows(null, new Utf8String("0"))
        );
        Assert.assertEquals(
                QwpEgressUpgradeProcessor.MAX_ROWS_PER_BATCH,
                QwpEgressUpgradeProcessor.negotiateMaxBatchRows(null, new Utf8String("1048576"))
        );
    }
}
