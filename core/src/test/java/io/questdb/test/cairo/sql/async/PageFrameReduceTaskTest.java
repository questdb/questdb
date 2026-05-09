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

package io.questdb.test.cairo.sql.async;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class PageFrameReduceTaskTest extends AbstractTest {

    @Test
    public void testBuildErrorPreservesInterruptionForTimeout() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            PageFrameReduceTask task = new PageFrameReduceTask(configuration, MemoryTag.NATIVE_DEFAULT);
            try {
                task.setErrorMsg(CairoException.queryTimedOut());
                RuntimeException re = task.buildError();
                Assert.assertTrue(re instanceof CairoException);
                CairoException ce = (CairoException) re;
                Assert.assertTrue("timeout should set isInterruption", ce.isInterruption());
                Assert.assertFalse("timeout should not set isCancellation", ce.isCancellation());
                Assert.assertFalse("timeout should not set isOutOfMemory", ce.isOutOfMemory());
            } finally {
                Misc.free(task);
            }
        });
    }

    @Test
    public void testBuildErrorPreservesCancellation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            PageFrameReduceTask task = new PageFrameReduceTask(configuration, MemoryTag.NATIVE_DEFAULT);
            try {
                task.setErrorMsg(CairoException.queryCancelled());
                RuntimeException re = task.buildError();
                Assert.assertTrue(re instanceof CairoException);
                CairoException ce = (CairoException) re;
                Assert.assertTrue("cancellation should set isInterruption", ce.isInterruption());
                Assert.assertTrue("cancellation should set isCancellation", ce.isCancellation());
            } finally {
                Misc.free(task);
            }
        });
    }
}
