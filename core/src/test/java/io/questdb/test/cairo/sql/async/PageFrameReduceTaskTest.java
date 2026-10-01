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
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.sql.async.AsyncQueryErrorState;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.network.NetworkError;
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
    public void testAsyncQueryErrorStateCairoExceptionRoundTrip() {
        final AsyncQueryErrorState error = new AsyncQueryErrorState();
        error.setError(
                CairoException.critical(42)
                        .position(7)
                        .put("disk on fire")
                        .setCancellation(true)
                        .setInterruption(true)
                        .setOutOfMemory(true)
        );

        final RuntimeException exception = error.buildException();
        Assert.assertTrue(exception instanceof CairoException);
        final CairoException cairoException = (CairoException) exception;
        Assert.assertEquals(42, cairoException.getErrno());
        Assert.assertEquals(7, cairoException.getPosition());
        Assert.assertTrue(cairoException.isCancellation());
        Assert.assertTrue(cairoException.isCritical());
        Assert.assertTrue(cairoException.isInterruption());
        Assert.assertTrue(cairoException.isOutOfMemory());
        TestUtils.assertContains(cairoException.getFlyweightMessage(), "disk on fire");
    }

    @Test
    public void testAsyncQueryErrorStateFirstErrorWinsAndClearAllowsReuse() {
        final AsyncQueryErrorState error = new AsyncQueryErrorState();
        error.setError(CairoException.nonCritical().put("first"));
        error.setError(CairoException.nonCritical().put("ignored"));

        CairoException exception = (CairoException) error.buildException();
        TestUtils.assertContains(exception.getFlyweightMessage(), "first");
        TestUtils.assertNotContains(exception.getFlyweightMessage(), "ignored");

        error.clear();
        Assert.assertFalse(error.hasError());
        error.setError(CairoException.nonCritical().put("second"));
        exception = (CairoException) error.buildException();
        TestUtils.assertContains(exception.getFlyweightMessage(), "second");
    }

    @Test
    public void testAsyncQueryErrorStateFlyweightExceptionsRoundTrip() {
        final AsyncQueryErrorState error = new AsyncQueryErrorState();
        error.setError(ImplicitCastException.instance().position(11).put("bad cast"));

        RuntimeException exception = error.buildException();
        Assert.assertTrue(exception instanceof ImplicitCastException);
        Assert.assertEquals(11, ((ImplicitCastException) exception).getPosition());
        TestUtils.assertContains(((ImplicitCastException) exception).getFlyweightMessage(), "bad cast");

        error.clear();
        error.setError(NumericException.instance().position(13).put("bad number"));
        exception = error.buildException();
        Assert.assertTrue(exception instanceof NumericException);
        Assert.assertEquals(13, ((NumericException) exception).getPosition());
        TestUtils.assertContains(((NumericException) exception).getFlyweightMessage(), "bad number");
    }

    @Test
    public void testAsyncQueryErrorStatePreservesBrokenConnection() {
        final AsyncQueryErrorState error = new AsyncQueryErrorState();
        error.setError(CairoException.queryDisconnected(42));

        final CairoException exception = (CairoException) error.buildException();
        Assert.assertEquals(
                SqlExecutionCircuitBreaker.STATE_BROKEN_CONNECTION,
                exception.getInterruptionReason()
        );
        Assert.assertFalse(exception.isCancellation());
        Assert.assertTrue(exception.isInterruption());
    }

    @Test
    public void testAsyncQueryErrorStateThrowErrorPreservesCairoExceptionAsFlyweight() {
        final AsyncQueryErrorState error = new AsyncQueryErrorState();
        error.setError(CairoException.nonCritical().put("reducer failed"));

        try {
            error.throwError();
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "reducer failed");
        }
    }

    @Test
    public void testAsyncQueryErrorStateThrowErrorPreservesCairoErrorType() {
        final AsyncQueryErrorState error = new AsyncQueryErrorState();
        final CairoError cause = new CairoError("fatal reducer failure");
        error.setError(cause);

        try {
            error.throwError();
            Assert.fail();
        } catch (CairoError e) {
            Assert.assertSame(cause, e);
        }
    }

    @Test
    public void testAsyncQueryErrorStateThrowErrorPreservesErrorType() {
        final AsyncQueryErrorState error = new AsyncQueryErrorState();
        final OutOfMemoryError oom = new OutOfMemoryError("Java heap space");
        error.setError(oom);

        try {
            error.throwError();
            Assert.fail();
        } catch (OutOfMemoryError e) {
            Assert.assertSame(oom, e);
        }
    }

    @Test
    public void testAsyncQueryErrorStateThrowErrorPreservesNetworkErrorSnapshot() {
        final AsyncQueryErrorState error = new AsyncQueryErrorState();
        final NetworkError source = NetworkError.instance(42, "network failure");
        error.setError(source);
        NetworkError.instance(99, "reused carrier error");

        try {
            error.throwError();
            Assert.fail();
        } catch (NetworkError e) {
            Assert.assertNotSame(source, e);
            Assert.assertEquals(42, e.getErrno());
            TestUtils.assertContains(e.getFlyweightMessage(), "network failure");
        }
    }

    @Test
    public void testAsyncQueryErrorStateThrowErrorPreservesRuntimeExceptionType() {
        final AsyncQueryErrorState error = new AsyncQueryErrorState();
        final IllegalStateException cause = new IllegalStateException("broken reducer");
        error.setError(cause);

        try {
            error.throwError();
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertSame(cause, e);
        }

        error.clear();
        error.setError(CairoException.nonCritical().put("flyweight"));
        try {
            error.throwError();
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "flyweight");
        }
    }

    @Test
    public void testAsyncQueryErrorStateThrowErrorPreservesTableReferenceOutOfDateException() {
        final AsyncQueryErrorState error = new AsyncQueryErrorState();
        final TableReferenceOutOfDateException cause = TableReferenceOutOfDateException.of("tab");
        error.setError(cause);

        Assert.assertSame(cause, error.buildException());
        try {
            error.throwError();
            Assert.fail();
        } catch (TableReferenceOutOfDateException e) {
            Assert.assertSame(cause, e);
            TestUtils.assertContains(e.getFlyweightMessage(), "tab");
        }
    }

    @Test
    public void testAsyncQueryErrorStateUnexpectedExceptionBecomesCairoException() {
        final AsyncQueryErrorState error = new AsyncQueryErrorState();
        error.setError(new IllegalStateException("broken reducer"));

        final RuntimeException exception = error.buildException();
        Assert.assertTrue(exception instanceof CairoException);
        final CairoException cairoException = (CairoException) exception;
        Assert.assertFalse(cairoException.isCritical());
        TestUtils.assertContains(cairoException.getFlyweightMessage(), "unexpected async query error: broken reducer");
    }

    @Test
    public void testBuildErrorPreservesBrokenConnection() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            PageFrameReduceTask task = new PageFrameReduceTask(configuration, MemoryTag.NATIVE_DEFAULT);
            try {
                task.setErrorMsg(CairoException.queryDisconnected(42));
                final CairoException exception = (CairoException) task.buildError();
                Assert.assertEquals(
                        SqlExecutionCircuitBreaker.STATE_BROKEN_CONNECTION,
                        exception.getInterruptionReason()
                );
                Assert.assertFalse(exception.isCancellation());
                Assert.assertTrue(exception.isInterruption());
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
    public void testBuildErrorPreservesCriticalErrno() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            PageFrameReduceTask task = new PageFrameReduceTask(configuration, MemoryTag.NATIVE_DEFAULT);
            try {
                task.setErrorMsg(CairoException.critical(42).position(7).put("disk on fire"));
                RuntimeException re = task.buildError();
                Assert.assertTrue(re instanceof CairoException);
                CairoException ce = (CairoException) re;
                Assert.assertEquals("errno must round-trip", 42, ce.getErrno());
                Assert.assertTrue("critical worker error must stay critical", ce.isCritical());
                Assert.assertEquals(7, ce.getPosition());
                TestUtils.assertContains(ce.getFlyweightMessage(), "disk on fire");
            } finally {
                Misc.free(task);
            }
        });
    }

    @Test
    public void testBuildErrorPreservesNonCriticalErrno() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            PageFrameReduceTask task = new PageFrameReduceTask(configuration, MemoryTag.NATIVE_DEFAULT);
            try {
                task.setErrorMsg(CairoException.nonCritical().put("bad value"));
                RuntimeException re = task.buildError();
                Assert.assertTrue(re instanceof CairoException);
                CairoException ce = (CairoException) re;
                Assert.assertEquals(CairoException.NON_CRITICAL, ce.getErrno());
                Assert.assertFalse("non-critical worker error must stay non-critical", ce.isCritical());
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
