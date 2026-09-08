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

package io.questdb.test.std;

import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ex.FatalError;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;

public class MiscTest {

    @Test
    public void testClearBestEffortFoldsClearFailures() {
        // a null object leaves the chain untouched
        Assert.assertNull(Misc.clearBestEffort(null, null));
        final RuntimeException existingPrimary = new RuntimeException("existing");
        Assert.assertSame(existingPrimary, Misc.clearBestEffort(existingPrimary, null));

        // a successful clear() leaves the chain untouched
        final TestMutable successful = new TestMutable(null);
        Assert.assertNull(Misc.clearBestEffort(null, successful));
        Assert.assertEquals(1, successful.clearCount);
        Assert.assertSame(existingPrimary, Misc.clearBestEffort(existingPrimary, successful));
        Assert.assertEquals(2, successful.clearCount);
        Assert.assertEquals(0, existingPrimary.getSuppressed().length);

        // a clear() failure folds into the chain instead of propagating and becomes the primary
        final RuntimeException runtimeFailure = new RuntimeException("runtime");
        final TestMutable runtimeThrowing = new TestMutable(runtimeFailure);
        Assert.assertSame(runtimeFailure, Misc.clearBestEffort(null, runtimeThrowing));
        Assert.assertEquals(1, runtimeThrowing.clearCount);
        Assert.assertEquals(0, runtimeFailure.getSuppressed().length);

        // a later failure attaches to the existing primary as suppressed; Errors count too
        final Error errorFailure = new AssertionError("error");
        final TestMutable errorThrowing = new TestMutable(errorFailure);
        Assert.assertSame(existingPrimary, Misc.clearBestEffort(existingPrimary, errorThrowing));
        Assert.assertEquals(1, errorThrowing.clearCount);
        Assert.assertArrayEquals(new Throwable[]{errorFailure}, existingPrimary.getSuppressed());

        // clear() rethrowing the primary itself must not self-suppress
        final RuntimeException selfPrimary = new RuntimeException("self");
        final TestMutable selfThrowing = new TestMutable(selfPrimary);
        Assert.assertSame(selfPrimary, Misc.clearBestEffort(selfPrimary, selfThrowing));
        Assert.assertEquals(1, selfThrowing.clearCount);
        Assert.assertEquals(0, selfPrimary.getSuppressed().length);
    }

    @Test
    public void testClearObjListBestEffortContinuesAfterFailures() {
        Assert.assertNull(Misc.clearObjListBestEffort(null, null));
        final RuntimeException existingPrimary = new RuntimeException("existing");
        Assert.assertSame(existingPrimary, Misc.clearObjListBestEffort(existingPrimary, null));

        final ObjList<Mutable> mutables = new ObjList<>();
        final RuntimeException firstFailure = new RuntimeException("first");
        final Error secondFailure = new AssertionError("second");
        final RuntimeException thirdFailure = new RuntimeException("third");
        final TestMutable first = new TestMutable(firstFailure);
        final TestMutable second = new TestMutable(secondFailure);
        final TestMutable third = new TestMutable(thirdFailure);
        final TestMutable last = new TestMutable(null);
        mutables.add(null);
        mutables.add(first);
        mutables.add(second);
        mutables.add(third);
        mutables.add(last);

        final Throwable result = Misc.clearObjListBestEffort(null, mutables);

        // the loop clears every remaining entry after the first failure, throwing or not
        Assert.assertEquals(1, first.clearCount);
        Assert.assertEquals(1, second.clearCount);
        Assert.assertEquals(1, third.clearCount);
        Assert.assertEquals(1, last.clearCount);
        Assert.assertSame(firstFailure, result);
        Assert.assertArrayEquals(new Throwable[]{secondFailure, thirdFailure}, firstFailure.getSuppressed());

        // the list retains its entries, so a second pass clears them again and folds into the same
        // chain; the entry rethrowing the primary does not self-suppress
        Assert.assertSame(result, Misc.clearObjListBestEffort(result, mutables));
        Assert.assertEquals(2, first.clearCount);
        Assert.assertEquals(2, second.clearCount);
        Assert.assertEquals(2, third.clearCount);
        Assert.assertEquals(2, last.clearCount);
        Assert.assertArrayEquals(
                new Throwable[]{secondFailure, thirdFailure, secondFailure, thirdFailure},
                firstFailure.getSuppressed()
        );
    }

    @Test
    public void testFoldCleanupFailureChainsAndRejectsSelfSuppression() {
        // with no primary yet the failure becomes the primary
        final RuntimeException failure = new RuntimeException("failure");
        Assert.assertSame(failure, Misc.foldCleanupFailure(null, failure));
        Assert.assertEquals(0, failure.getSuppressed().length);

        // later failures attach to the primary in arrival order
        final RuntimeException primary = new RuntimeException("primary");
        final Error errorFailure = new AssertionError("error");
        Assert.assertSame(primary, Misc.foldCleanupFailure(primary, failure));
        Assert.assertSame(primary, Misc.foldCleanupFailure(primary, errorFailure));
        Assert.assertArrayEquals(new Throwable[]{failure, errorFailure}, primary.getSuppressed());

        // folding the primary into itself is a no-op: Throwable.addSuppressed(this) throws
        // IllegalArgumentException("Self-suppression not permitted")
        Assert.assertSame(primary, Misc.foldCleanupFailure(primary, primary));
        Assert.assertArrayEquals(new Throwable[]{failure, errorFailure}, primary.getSuppressed());
    }

    @Test
    public void testFreeBestEffortHandlesNullAndExistingPrimary() {
        Assert.assertNull(Misc.freeBestEffort(null, null));

        final RuntimeException primary = new RuntimeException("primary");
        Assert.assertSame(primary, Misc.freeBestEffort(primary, null));

        final RuntimeException closeFailure = new RuntimeException("close");
        final TestCloseable closeable = new TestCloseable(null, closeFailure);
        Assert.assertSame(primary, Misc.freeBestEffort(primary, closeable));
        Assert.assertEquals(1, closeable.closeCount);
        Assert.assertArrayEquals(new Throwable[]{closeFailure}, primary.getSuppressed());

        final RuntimeException selfPrimary = new RuntimeException("self");
        final TestCloseable selfThrowing = new TestCloseable(null, selfPrimary);
        Assert.assertSame(selfPrimary, Misc.freeBestEffort(selfPrimary, selfThrowing));
        Assert.assertEquals(1, selfThrowing.closeCount);
        Assert.assertEquals(0, selfPrimary.getSuppressed().length);
    }

    @Test
    public void testFreeObjListBestEffortNullsSlotsAndContinuesAfterFailures() {
        Assert.assertNull(Misc.freeObjListBestEffort(null, null));
        final RuntimeException existingPrimary = new RuntimeException("existing");
        Assert.assertSame(existingPrimary, Misc.freeObjListBestEffort(existingPrimary, null));

        final ObjList<Closeable> closeables = new ObjList<>();
        final RuntimeException firstFailure = new RuntimeException("first");
        final Error secondFailure = new AssertionError("second");
        final RuntimeException thirdFailure = new RuntimeException("third");
        final TestCloseable first = new TestCloseable(() -> Assert.assertNull(closeables.getQuick(1)), firstFailure);
        final TestCloseable second = new TestCloseable(() -> Assert.assertNull(closeables.getQuick(2)), secondFailure);
        final TestCloseable third = new TestCloseable(() -> Assert.assertNull(closeables.getQuick(3)), thirdFailure);
        final TestCloseable last = new TestCloseable(() -> Assert.assertNull(closeables.getQuick(4)), null);
        closeables.add(null);
        closeables.add(first);
        closeables.add(second);
        closeables.add(third);
        closeables.add(last);

        final Throwable result = Misc.freeObjListBestEffort(null, closeables);
        Assert.assertSame(firstFailure, result);
        Assert.assertArrayEquals(new Throwable[]{secondFailure, thirdFailure}, firstFailure.getSuppressed());
        Assert.assertEquals(1, first.closeCount);
        Assert.assertEquals(1, second.closeCount);
        Assert.assertEquals(1, third.closeCount);
        Assert.assertEquals(1, last.closeCount);
        for (int i = 0, n = closeables.size(); i < n; i++) {
            Assert.assertNull(closeables.getQuick(i));
        }

        Assert.assertSame(result, Misc.freeObjListBestEffort(result, closeables));
        Assert.assertArrayEquals(new Throwable[]{secondFailure, thirdFailure}, firstFailure.getSuppressed());
        Assert.assertEquals(1, first.closeCount);
        Assert.assertEquals(1, second.closeCount);
        Assert.assertEquals(1, third.closeCount);
        Assert.assertEquals(1, last.closeCount);
    }

    @Test
    public void testFreeObjListWithPrimaryNullsSlotsAndContinuesAfterFailures() {
        final RuntimeException primary = new RuntimeException("primary");
        Misc.freeObjList(null, primary);

        final ObjList<Closeable> closeables = new ObjList<>();
        final RuntimeException firstFailure = new RuntimeException("first");
        final Error secondFailure = new AssertionError("second");
        final TestCloseable first = new TestCloseable(() -> Assert.assertNull(closeables.getQuick(1)), firstFailure);
        final TestCloseable second = new TestCloseable(() -> Assert.assertNull(closeables.getQuick(2)), secondFailure);
        final TestCloseable last = new TestCloseable(() -> Assert.assertNull(closeables.getQuick(3)), null);
        closeables.add(null);
        closeables.add(first);
        closeables.add(second);
        closeables.add(last);

        Misc.freeObjList(closeables, primary);

        Assert.assertArrayEquals(new Throwable[]{firstFailure, secondFailure}, primary.getSuppressed());
        Assert.assertEquals(1, first.closeCount);
        Assert.assertEquals(1, second.closeCount);
        Assert.assertEquals(1, last.closeCount);
        for (int i = 0, n = closeables.size(); i < n; i++) {
            Assert.assertNull(closeables.getQuick(i));
        }
    }

    @Test
    public void testFreeWithPrimarySuppressesCloseFailure() {
        final RuntimeException primary = new RuntimeException("primary");
        Misc.free(null, primary);

        final TestCloseable successful = new TestCloseable(null, null);
        Misc.free(successful, primary);
        Assert.assertEquals(1, successful.closeCount);
        Assert.assertEquals(0, primary.getSuppressed().length);

        final RuntimeException runtimeFailure = new RuntimeException("runtime");
        final TestCloseable runtimeThrowing = new TestCloseable(null, runtimeFailure);
        Misc.free(runtimeThrowing, primary);

        final Error errorFailure = new AssertionError("error");
        final TestCloseable errorThrowing = new TestCloseable(null, errorFailure);
        Misc.free(errorThrowing, primary);

        final IOException ioFailure = new IOException("io");
        Misc.free(() -> {
            throw ioFailure;
        }, primary);

        final Throwable[] suppressed = primary.getSuppressed();
        Assert.assertEquals(3, suppressed.length);
        Assert.assertSame(runtimeFailure, suppressed[0]);
        Assert.assertSame(errorFailure, suppressed[1]);
        Assert.assertTrue(suppressed[2] instanceof FatalError);
        Assert.assertSame(ioFailure, suppressed[2].getCause());
        Assert.assertEquals(1, runtimeThrowing.closeCount);
        Assert.assertEquals(1, errorThrowing.closeCount);

        final RuntimeException selfPrimary = new RuntimeException("self");
        final TestCloseable selfThrowing = new TestCloseable(null, selfPrimary);
        Misc.free(selfThrowing, selfPrimary);
        Assert.assertEquals(1, selfThrowing.closeCount);
        Assert.assertEquals(0, selfPrimary.getSuppressed().length);
    }

    private static class TestCloseable implements Closeable {
        private final Runnable closeAction;
        private int closeCount;
        private final Throwable failure;

        private TestCloseable(Runnable closeAction, Throwable failure) {
            this.closeAction = closeAction;
            this.failure = failure;
        }

        @Override
        public void close() {
            closeCount++;
            if (closeAction != null) {
                closeAction.run();
            }
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (failure instanceof Error error) {
                throw error;
            }
        }
    }

    private static class TestMutable implements Mutable {
        private final Throwable failure;
        private int clearCount;

        private TestMutable(Throwable failure) {
            this.failure = failure;
        }

        @Override
        public void clear() {
            clearCount++;
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (failure instanceof Error error) {
                throw error;
            }
        }
    }
}
