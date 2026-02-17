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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.cutlass.line.tcp.LineTcpReceiverConfigurationHelper;
import org.junit.Assert;
import org.junit.Test;

public class LineTcpReceiverConfigurationHelperTest {
    @Test
    public void testILPCommitInterval() {
        Assert.assertEquals(1000, LineTcpReceiverConfigurationHelper.calcCommitInterval(1_000_000, 0.0, 1000));
        Assert.assertEquals(500, LineTcpReceiverConfigurationHelper.calcCommitInterval(1_000_000, 0.5, 1000));
        Assert.assertEquals(3000, LineTcpReceiverConfigurationHelper.calcCommitInterval(1_000_000, -0.5, 3000));
    }

}