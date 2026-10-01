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

import io.questdb.griffin.ResourceScope;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;

public class ResourceScopeTest {
    @Test
    public void testCleanupClearsBeforeCloseAndContinuesAfterFailures() {
        final ResourceScope scope = new ResourceScope();
        final int firstSlot = scope.reserve();
        final int secondSlot = scope.reserve();
        final RuntimeException firstFailure = new RuntimeException("first close");
        final RuntimeException secondFailure = new RuntimeException("second close");
        final TestResource first = new TestResource(secondFailure);
        final TestResource second = new TestResource(firstFailure) {
            @Override
            public void close() {
                Assert.assertThrows(IllegalStateException.class, () -> scope.detach(secondSlot));
                super.close();
            }
        };
        scope.own(firstSlot, first);
        scope.own(secondSlot, second);

        Assert.assertSame(firstFailure, scope.closeOwned(-1, null));
        Assert.assertArrayEquals(new Throwable[]{secondFailure}, firstFailure.getSuppressed());
        Assert.assertEquals(1, first.closeCount);
        Assert.assertEquals(1, second.closeCount);
        Assert.assertNull(scope.closeOwned(-1, null));
        Assert.assertEquals(1, first.closeCount);
        Assert.assertEquals(1, second.closeCount);
        Assert.assertThrows(IllegalStateException.class, () -> scope.detach(firstSlot));
        Assert.assertEquals(2, scope.reserve());
        scope.clear();
    }

    @Test
    public void testCleanupPreservesPrimaryAndAvoidsSelfSuppression() {
        final ResourceScope scope = new ResourceScope();
        final RuntimeException primary = new RuntimeException("construction");
        final RuntimeException closeFailure = new RuntimeException("close");
        final TestResource first = new TestResource(primary);
        final TestResource second = new TestResource(closeFailure);
        scope.own(scope.reserve(), first);
        scope.own(scope.reserve(), second);

        Assert.assertSame(primary, scope.closeOwned(-1, primary));
        Assert.assertArrayEquals(new Throwable[]{closeFailure}, primary.getSuppressed());
        Assert.assertEquals(1, first.closeCount);
        Assert.assertEquals(1, second.closeCount);
        scope.close();
    }

    @Test
    public void testClearRethrowsCleanupFailureAndAllowsReuse() {
        final ResourceScope scope = new ResourceScope();
        final RuntimeException failure = new RuntimeException("close");
        final TestResource resource = new TestResource(failure);
        scope.own(scope.reserve(), resource);

        Assert.assertSame(failure, Assert.assertThrows(RuntimeException.class, scope::clear));
        Assert.assertEquals(1, resource.closeCount);
        final int slot = scope.reserve();
        Assert.assertEquals(0, slot);
        Assert.assertThrows(IllegalStateException.class, () -> scope.detach(slot));
        final TestResource next = new TestResource(null);
        scope.own(slot, next);
        scope.close();
        Assert.assertEquals(1, next.closeCount);
        Assert.assertEquals(1, resource.closeCount);
    }

    @Test
    public void testCompositeAdoptionDoesNotCloseChildTwice() {
        final ResourceScope scope = new ResourceScope();
        final int childSlot = scope.reserve();
        final TestResource child = new TestResource(null);
        scope.own(childSlot, child);
        final int compositeSlot = scope.reserve();
        final Closeable adopted = scope.detach(childSlot);
        scope.own(compositeSlot, adopted::close);

        scope.close();
        Assert.assertEquals(1, child.closeCount);
    }

    @Test
    public void testDependentsCloseBeforeProviders() {
        final ResourceScope scope = new ResourceScope();
        final TestResource provider = new TestResource(null);
        scope.own(scope.reserve(), provider);
        final TestResource dependent = new TestResource(null) {
            @Override
            public void close() {
                Assert.assertEquals(0, provider.closeCount);
                super.close();
            }
        };
        scope.own(scope.reserve(), dependent);

        scope.close();
        Assert.assertEquals(1, dependent.closeCount);
        Assert.assertEquals(1, provider.closeCount);
    }

    @Test
    public void testRetainsCompletedRootUntilTemporaryCleanupSucceeds() {
        final ResourceScope scope = new ResourceScope();
        final int rootSlot = scope.reserve();
        final TestResource root = new TestResource(null);
        scope.own(rootSlot, root);
        final RuntimeException failure = new RuntimeException("temporary close");
        final TestResource temporary = new TestResource(failure);
        scope.own(scope.reserve(), temporary);

        final Throwable cleanupFailure = scope.closeOwned(rootSlot, null);
        Assert.assertSame(failure, cleanupFailure);
        Assert.assertEquals(0, root.closeCount);
        Assert.assertSame(failure, scope.closeOwned(-1, cleanupFailure));
        Assert.assertEquals(1, root.closeCount);
        Assert.assertEquals(1, temporary.closeCount);
        scope.clear();
    }

    @Test
    public void testTransferKeepsSlotsStableAndRejectsSecondClaim() {
        final ResourceScope source = new ResourceScope();
        final ResourceScope target = new ResourceScope();
        final int sourceSlot = source.reserve();
        final int unusedSlot = source.reserve();
        final TestResource resource = new TestResource(null);
        source.own(sourceSlot, resource);
        Assert.assertThrows(IllegalStateException.class, () -> source.detach(unusedSlot));
        Assert.assertThrows(IllegalStateException.class, () -> source.own(sourceSlot, resource));
        Assert.assertThrows(NullPointerException.class, () -> source.own(unusedSlot, null));

        final int targetSlot = target.reserve();
        target.own(targetSlot, source.detach(sourceSlot));
        Assert.assertThrows(IllegalStateException.class, () -> source.detach(sourceSlot));
        Assert.assertEquals(2, source.reserve());
        source.close();
        Assert.assertEquals(0, resource.closeCount);
        Assert.assertSame(resource, target.detach(targetSlot));
        target.close();
        Assert.assertEquals(0, resource.closeCount);
        resource.close();
        Assert.assertEquals(1, resource.closeCount);
    }

    private static class TestResource implements Closeable {
        private final RuntimeException failure;
        private int closeCount;

        private TestResource(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public void close() {
            closeCount++;
            if (failure != null) {
                throw failure;
            }
        }
    }
}
