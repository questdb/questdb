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
 */
public class LifecycleOrchestratorExecutorDrainOrderTest {

    @Rule
    public Timeout timeout = Timeout.builder().withTimeout(30, TimeUnit.SECONDS).withLookingForStuckThread(true).build();

    @Test
    public void closeDrainsExecutorBeforePreJoinCancelHookObservesState() throws Exception {
        final ExecutorAccessOrchestrator orch = new ExecutorAccessOrchestrator();
        final CountDownLatch taskStarted = new CountDownLatch(1);
        final CountDownLatch releaseTask = new CountDownLatch(1);
        final AtomicBoolean taskCompleted = new AtomicBoolean();
        final AtomicBoolean hookSawTaskCompleted = new AtomicBoolean();

        orch.submitToExecutor(() -> {
            taskStarted.countDown();
            try {
                releaseTask.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            taskCompleted.set(true);
        });
        orch.setPreJoinCancelHook(() -> hookSawTaskCompleted.set(taskCompleted.get()));

        Assert.assertTrue("the executor task must be running before close()",
                taskStarted.await(10, TimeUnit.SECONDS));

        final Thread closer = new Thread(orch::close, "orch-closer");
        try {
            closer.start();
            // Give close() a wide berth to reach the executor drain before releasing the task: on
            // the correctly-ordered tree the hook cannot fire until awaitInFlightWork() returns, so
            // this margin only matters for discriminating a reordered mutant (whose hook runs
            // immediately, well before this sleep elapses).
            Thread.sleep(300);
            releaseTask.countDown();
            closer.join(TimeUnit.SECONDS.toMillis(10));
        } finally {
            releaseTask.countDown();
            closer.join(TimeUnit.SECONDS.toMillis(10));
        }

        Assert.assertTrue("the executor task must have completed", taskCompleted.get());
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
        ExecutorAccessOrchestrator() {
            super(null, null, null);
        }

        void submitToExecutor(Runnable task) {
            executor.execute(task);
        }
    }
}
