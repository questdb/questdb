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
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
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

    @Test
    public void testBuildErrorPreservesImplicitCastException() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            PageFrameReduceTask task = new PageFrameReduceTask(configuration, MemoryTag.NATIVE_DEFAULT);
            try {
                task.setErrorMsg(ImplicitCastException.instance().position(42).put("inconvertible value"));
                RuntimeException re = task.buildError();
                Assert.assertTrue("ImplicitCastException must round-trip with KIND_IMPLICIT_CAST", re instanceof ImplicitCastException);
                ImplicitCastException ice = (ImplicitCastException) re;
                Assert.assertEquals(42, ice.getPosition());
                TestUtils.assertContains(ice.getFlyweightMessage(), "inconvertible value");
            } finally {
                Misc.free(task);
            }
        });
    }

    @Test
    public void testBuildErrorPreservesNumericException() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            PageFrameReduceTask task = new PageFrameReduceTask(configuration, MemoryTag.NATIVE_DEFAULT);
            try {
                task.setErrorMsg(NumericException.instance().position(17).put("integer constant expected"));
                RuntimeException re = task.buildError();
                Assert.assertTrue("NumericException must round-trip with KIND_NUMERIC", re instanceof NumericException);
                NumericException ne = (NumericException) re;
                Assert.assertEquals(17, ne.getPosition());
                TestUtils.assertContains(ne.getFlyweightMessage(), "integer constant expected");
            } finally {
                Misc.free(task);
            }
        });
    }
}
