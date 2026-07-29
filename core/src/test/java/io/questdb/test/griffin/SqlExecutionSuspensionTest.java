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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoException;
import io.questdb.griffin.SqlExecutionSuspension;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlExecutionSuspensionTest {

    @Test
    public void testBlockingModeReturnsNull() {
        final SuspensionScope.Mode previousMode = SuspensionScope.enter(SuspensionScope.Mode.BLOCKING);
        try {
            Assert.assertNull(SqlExecutionSuspension.currentFiber());
        } finally {
            SuspensionScope.restore(previousMode);
        }
    }

    @Test
    public void testFiberModeWithoutMountedFiberThrows() {
        final SuspensionScope.Mode previousMode = SuspensionScope.enter(SuspensionScope.Mode.FIBER);
        try {
            SqlExecutionSuspension.currentFiber();
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "SQL suspension requires a mounted fiber");
        } finally {
            SuspensionScope.restore(previousMode);
        }
    }

    @Test
    public void testForbiddenModeThrows() {
        final SuspensionScope.Mode previousMode = SuspensionScope.enter(SuspensionScope.Mode.FORBIDDEN);
        try {
            SqlExecutionSuspension.currentFiber();
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "SQL suspension is forbidden");
        } finally {
            SuspensionScope.restore(previousMode);
        }
    }

    @Test
    public void testNullModeReturnsNull() {
        final SuspensionScope.Mode previousMode = SuspensionScope.enter(null);
        try {
            Assert.assertNull(SqlExecutionSuspension.currentFiber());
        } finally {
            SuspensionScope.restore(previousMode);
        }
    }
}
