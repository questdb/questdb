package io.questdb.test.lifecycle;

import io.questdb.lifecycle.LifecycleOrchestrator;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@code LifecycleOrchestrator.close()} promises {@code executor.shutdown()} +
 * {@code awaitInFlightWork()} BEFORE the pre-join cancel hook runs, so a task already running on
 * the lifecycle executor is guaranteed to have completed by the time the hook observes state.
 * Both existing hook tests ({@code LifecycleOrchestratorPreJoinCancelHookTest},
 * {@code LifecycleOrchestratorHookThrowsTest}) leave the executor idle, so a revert that reorders
 * the hook ahead of the drain stays green under them. This test parks a task on the executor and
 * asserts the hook sees it as completed, going RED under that reorder.
 * <p>
 * The discriminator is a {@link CountDownLatch} counted down from inside an override of
 * {@code awaitInFlightWork()}, not a sleep: the closer thread can only reach that override after
 * {@code close()} has called {@code executor.shutdown()}, so the latch opening is proof the closer
 * reached the drain boundary, independent of how the JVM schedules the closer thread relative to
 * the test thread. The moment the latch opens, the correctly-ordered tree cannot yet have run the
 * hook (it is still blocked awaiting the parked task inside {@code super.awaitInFlightWork()}), so
 * a hook-before-drain mutant is caught deterministically: on that mutant the hook runs (on the same
 * closer thread, in program order) before the drain override's countDown, so it is already visible
 * the instant the test thread's await returns.
 */
public class LifecycleOrchestratorExecutorDrainOrderTest {

    @Rule
    public Timeout timeout = Timeout.builder().withTimeout(30, TimeUnit.SECONDS).withLookingForStuckThread(true).build();

    @Test
    public void closeDrainsExecutorBeforePreJoinCancelHookObservesState() throws Exception {
        final CountDownLatch closerAtDrain = new CountDownLatch(1);
        final CountDownLatch releaseTask = new CountDownLatch(1);
        final CountDownLatch taskStarted = new CountDownLatch(1);
        final AtomicBoolean taskCompleted = new AtomicBoolean();
        final AtomicBoolean hookCalled = new AtomicBoolean();
        final AtomicBoolean hookSawTaskCompleted = new AtomicBoolean();
        final ExecutorAccessOrchestrator orch = new ExecutorAccessOrchestrator(closerAtDrain);

        orch.submitToExecutor(() -> {
            taskStarted.countDown();
            try {
                releaseTask.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            taskCompleted.set(true);
        });
        orch.setPreJoinCancelHook(() -> {
            hookCalled.set(true);
            hookSawTaskCompleted.set(taskCompleted.get());
        });

        Assert.assertTrue("the executor task must be running before close()",
                taskStarted.await(10, TimeUnit.SECONDS));

        final Thread closer = new Thread(orch::close, "orch-closer");
        try {
            closer.start();
            // closerAtDrain can only open after close() has called executor.shutdown() and entered
            // awaitInFlightWork(), so this proves the closer reached the drain boundary -- no sleep
            // margin needed, and no dependency on how promptly the closer thread gets scheduled.
            Assert.assertTrue("the closer thread must reach the executor drain boundary",
                    closerAtDrain.await(30, TimeUnit.SECONDS));
            // On the correctly-ordered tree the hook cannot have run yet: awaitInFlightWork() is
            // still blocked on the parked task. On a hook-before-drain mutant the hook already ran,
            // on the closer thread, strictly before this countDown -- so it is visible right here.
            Assert.assertFalse(
                    "the pre-join cancel hook must not run before the executor drain boundary is reached",
                    hookCalled.get());

            releaseTask.countDown();
            closer.join(TimeUnit.SECONDS.toMillis(10));
        } finally {
            releaseTask.countDown();
            closer.join(TimeUnit.SECONDS.toMillis(10));
        }

        Assert.assertFalse("the closer thread must terminate", closer.isAlive());
        Assert.assertTrue("the executor task must have completed", taskCompleted.get());
        Assert.assertTrue("the pre-join cancel hook must run", hookCalled.get());
        Assert.assertTrue(
                "the pre-join cancel hook must observe the executor task as already completed -- "
                        + "close() must drain the executor before running the hook",
                hookSawTaskCompleted.get());
    }

    // Test seam: the OSS executor field is protected, so a same-package subclass could reach it
    // directly, but this test lives in io.questdb.test.lifecycle (a different package from the
    // production io.questdb.lifecycle). Mirrors EntLifecycleOrchestrator.executeSwitchTask, which
    // exists for the same reason.
    private static final class ExecutorAccessOrchestrator extends LifecycleOrchestrator {
        private final CountDownLatch closerAtDrain;

        ExecutorAccessOrchestrator(CountDownLatch closerAtDrain) {
            super(null, null, null);
            this.closerAtDrain = closerAtDrain;
        }

        void submitToExecutor(Runnable task) {
            executor.execute(task);
        }

        @Override
        protected boolean awaitInFlightWork() {
            closerAtDrain.countDown();
            return super.awaitInFlightWork();
        }
    }
}
