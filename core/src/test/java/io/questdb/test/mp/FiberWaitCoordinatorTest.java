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

package io.questdb.test.mp;

import io.questdb.mp.continuation.FiberEventWaitQueue;
import io.questdb.mp.continuation.FiberWaitCoordinator;
import org.junit.Assert;
import org.junit.Test;

public class FiberWaitCoordinatorTest {

    @Test
    public void testAbortInvalidatesArm() {
        TestTarget target = new TestTarget();
        FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
        long token = coordinator.beginBuild(2);

        armSources(coordinator, token, 1);
        Assert.assertTrue(coordinator.abort(token));
        Assert.assertEquals(1, target.abortCount);
        Assert.assertFalse(coordinator.fire(token, FiberWaitCoordinator.REASON_WAL));
        Assert.assertEquals(FiberWaitCoordinator.REASON_NONE, coordinator.consume(token));
        Assert.assertEquals(1, target.abortCount);
    }

    @Test
    public void testEarlyFireWaitsForSeal() {
        TestTarget target = new TestTarget();
        FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
        long token = coordinator.beginBuild(2);

        Assert.assertTrue(coordinator.fire(token, FiberWaitCoordinator.REASON_TIMER));
        Assert.assertEquals(0, target.fireCount);
        armSources(coordinator, token, 2);
        Assert.assertTrue(coordinator.seal(token));

        Assert.assertEquals(1, target.fireCount);
        Assert.assertEquals(FiberWaitCoordinator.REASON_TIMER, target.reason);
        Assert.assertTrue(coordinator.isFired(token));
        Assert.assertEquals(FiberWaitCoordinator.REASON_TIMER, coordinator.consume(token));
        Assert.assertEquals(1, target.abortCount);
    }

    @Test
    public void testFiringCanBeHelped() {
        TestTarget target = new TestTarget();
        target.refusedFireCount = 1;
        FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
        long token = coordinator.beginBuild(1);
        armSources(coordinator, token, 1);
        Assert.assertTrue(coordinator.seal(token));

        Assert.assertTrue(coordinator.fire(token, FiberWaitCoordinator.REASON_WAL));
        Assert.assertFalse(coordinator.isFired(token));

        Assert.assertEquals(FiberWaitCoordinator.REASON_WAL, coordinator.consume(token));
        Assert.assertEquals(2, target.fireCount);
    }

    @Test
    public void testIncompleteRegistrationCannotSeal() {
        FiberWaitCoordinator coordinator = new FiberWaitCoordinator(new TestTarget());
        long token = coordinator.beginBuild(2);
        armSources(coordinator, token, 1);
        try {
            coordinator.seal(token);
            Assert.fail();
        } catch (IllegalStateException ignored) {
        }
    }

    @Test
    public void testNormalFireChoosesOneReason() {
        TestTarget target = new TestTarget();
        FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
        long token = coordinator.beginBuild(1);
        armSources(coordinator, token, 1);
        Assert.assertTrue(coordinator.seal(token));
        Assert.assertTrue(coordinator.isArmed(token));

        Assert.assertTrue(coordinator.fire(token, FiberWaitCoordinator.REASON_WAL));
        Assert.assertFalse(coordinator.fire(token, FiberWaitCoordinator.REASON_TIMER));

        Assert.assertEquals(1, target.fireCount);
        Assert.assertEquals(FiberWaitCoordinator.REASON_WAL, coordinator.consume(token));
    }

    @Test
    public void testPreferPendingCancelResolvesEarlyReturn() {
        TestTarget target = new TestTarget();
        FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
        long token = coordinator.beginBuild(2);
        armSources(coordinator, token, 1);

        // no pending reason: the early-return reason stands
        Assert.assertEquals(
                FiberWaitCoordinator.REASON_CAPACITY,
                coordinator.preferPendingCancel(token, FiberWaitCoordinator.REASON_CAPACITY)
        );

        // a cancellation recorded while the wait is still building wins over the early return
        Assert.assertTrue(coordinator.fire(token, FiberWaitCoordinator.REASON_CANCEL));
        Assert.assertEquals(
                FiberWaitCoordinator.REASON_CANCEL,
                coordinator.preferPendingCancel(token, FiberWaitCoordinator.REASON_CAPACITY)
        );
        Assert.assertEquals(
                FiberWaitCoordinator.REASON_CAPACITY,
                coordinator.preferPendingCancel(token + 1, FiberWaitCoordinator.REASON_CAPACITY)
        );
        Assert.assertTrue(coordinator.abort(token));
        Assert.assertEquals(FiberWaitCoordinator.REASON_NONE, coordinator.consume(token));

        // a non-cancel pending reason does not override the early return
        token = coordinator.beginBuild(2);
        armSources(coordinator, token, 1);
        Assert.assertTrue(coordinator.fire(token, FiberWaitCoordinator.REASON_PROGRESS));
        Assert.assertEquals(
                FiberWaitCoordinator.REASON_CAPACITY,
                coordinator.preferPendingCancel(token, FiberWaitCoordinator.REASON_CAPACITY)
        );
        Assert.assertTrue(coordinator.abort(token));
        Assert.assertEquals(FiberWaitCoordinator.REASON_NONE, coordinator.consume(token));
    }

    @Test
    public void testShutdownDuringBuildFiresAtSeal() {
        TestTarget target = new TestTarget();
        FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
        long token = coordinator.beginBuild(1);
        Assert.assertEquals(token, coordinator.currentToken());

        coordinator.shutdown();

        Assert.assertEquals(0, target.fireCount);
        armSources(coordinator, token, 1);
        Assert.assertTrue(coordinator.seal(token));
        Assert.assertEquals(1, target.fireCount);
        Assert.assertEquals(FiberWaitCoordinator.REASON_SHUTDOWN, coordinator.consume(token));
        Assert.assertEquals(0, coordinator.currentToken());
        coordinator.shutdown();
        Assert.assertEquals(1, target.fireCount);
    }

    @Test
    public void testStaleTokenCannotFireReusedCoordinator() {
        TestTarget target = new TestTarget();
        FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
        long oldToken = coordinator.beginBuild(1);
        armSources(coordinator, oldToken, 1);
        Assert.assertTrue(coordinator.seal(oldToken));
        Assert.assertTrue(coordinator.fire(oldToken, FiberWaitCoordinator.REASON_TIMER));
        Assert.assertEquals(FiberWaitCoordinator.REASON_TIMER, coordinator.consume(oldToken));

        long token = coordinator.beginBuild(1);
        Assert.assertNotEquals(oldToken, token);
        armSources(coordinator, token, 1);
        Assert.assertTrue(coordinator.seal(token));
        Assert.assertFalse(coordinator.fire(oldToken, FiberWaitCoordinator.REASON_SHUTDOWN));
        Assert.assertTrue(coordinator.isArmed(token));
        Assert.assertEquals(1, target.fireCount);
    }

    @Test
    public void testTokenExhaustionDoesNotWrap() {
        final FiberWaitCoordinator coordinator = new FiberWaitCoordinator(new TestTarget());
        coordinator.setTokenForTesting(Long.MAX_VALUE);

        try {
            coordinator.beginBuild(1);
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertEquals("wait coordinator token exhausted", e.getMessage());
        }
        Assert.assertEquals(0, coordinator.currentToken());
    }

    private static void armSources(FiberWaitCoordinator coordinator, long token, int count) {
        for (int i = 0; i < count; i++) {
            Assert.assertTrue(
                    coordinator.armEvent(
                            token,
                            new FiberEventWaitQueue(FiberWaitCoordinator.REASON_PROGRESS)
                    )
            );
        }
    }

    private static final class TestTarget implements FiberWaitCoordinator.Target {
        private int abortCount;
        private int fireCount;
        private int reason;
        private int refusedFireCount;

        @Override
        public void abortWait(long token) {
            abortCount++;
        }

        @Override
        public boolean fireWait(long token, int reason) {
            fireCount++;
            this.reason = reason;
            if (refusedFireCount > 0) {
                refusedFireCount--;
                return false;
            }
            return true;
        }
    }
}
