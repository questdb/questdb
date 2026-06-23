package io.questdb.test.lifecycle.fakes;

import io.questdb.lifecycle.Component;
import io.questdb.lifecycle.LifecycleContext;
import io.questdb.std.ObjList;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Test fake: start() blocks on a CountDownLatch the test releases via releaseBarrier().
 * <p>
 * WR-07: awaitEntered() lets the test wait for start() to actually begin its blocking await
 * without a flaky Thread.sleep. The entered latch is flipped BEFORE the blocking await,
 * so a successful awaitEntered() guarantees that the runner thread has reached the barrier.
 */
public final class BarrierComponent implements Component {

    private final CountDownLatch entered = new CountDownLatch(1);
    private final ObjList<String> hardDeps;
    private final CountDownLatch latch = new CountDownLatch(1);
    private final String name;
    private final ObjList<String> softDeps;
    private volatile boolean started;
    private volatile boolean stopped;

    public BarrierComponent(String name) {
        this(name, new ObjList<>(), new ObjList<>());
    }

    public BarrierComponent(String name, ObjList<String> hardDeps, ObjList<String> softDeps) {
        this.name = name;
        this.hardDeps = hardDeps;
        this.softDeps = softDeps;
    }

    /**
     * Blocks the caller until {@link #start(LifecycleContext)} has actually been invoked
     * and is about to enter its blocking await. Returns true if entered within the given
     * timeout, false otherwise.
     */
    public boolean awaitEntered(long timeoutMillis) throws InterruptedException {
        return entered.await(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public ObjList<String> hardRequiredDependencies() {
        return hardDeps;
    }

    public boolean isStarted() {
        return started;
    }

    public boolean isStopped() {
        return stopped;
    }

    @Override
    public String name() {
        return name;
    }

    public void releaseBarrier() {
        latch.countDown();
    }

    @Override
    public ObjList<String> softDependencies() {
        return softDeps;
    }

    @Override
    public void start(LifecycleContext ctx) {
        entered.countDown();
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        started = true;
    }

    @Override
    public void stop() {
        stopped = true;
    }
}
