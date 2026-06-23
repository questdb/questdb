package io.questdb.test.lifecycle.fakes;

import io.questdb.lifecycle.Component;
import io.questdb.lifecycle.LifecycleContext;
import io.questdb.lifecycle.State;
import io.questdb.std.ObjList;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test fake: records every {@link #onDependencyState} callback in a
 * thread-safe queue and records start/stop invocations with monotonic seq.
 * <p>
 * NON-FINAL (W8 fix) so tests may extend it for ad-hoc behavior overrides
 * (e.g., a start() that publishes DEGRADED + onStableBelow callback).
 */
public class ProbeComponent implements Component {

    public static final class Event {
        public final State current;
        public final String depName;
        public final State previous;
        public final long seq;

        Event(long seq, String depName, State previous, State current) {
            this.seq = seq;
            this.depName = depName;
            this.previous = previous;
            this.current = current;
        }
    }

    private static final AtomicLong SEQ = new AtomicLong();
    public final LinkedBlockingQueue<Event> events = new LinkedBlockingQueue<>();
    private volatile LifecycleContext capturedCtx;
    private final ObjList<String> hardDeps;
    private final AtomicBoolean holdDegraded = new AtomicBoolean(false);
    private final String name;
    private final AtomicReference<State> pendingAdvance = new AtomicReference<>(null);
    private final ObjList<String> softDeps;
    private volatile long startSeq = -1;
    private volatile long stopSeq = -1;

    public ProbeComponent(String name) {
        this(name, new ObjList<>(), new ObjList<>());
    }

    public ProbeComponent(String name, ObjList<String> hardDeps, ObjList<String> softDeps) {
        this.name = name;
        this.hardDeps = hardDeps;
        this.softDeps = softDeps;
    }

    public long getStartSeq() {
        return startSeq;
    }

    public long getStopSeq() {
        return stopSeq;
    }

    @Override
    public ObjList<String> hardRequiredDependencies() {
        return hardDeps;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void onDependencyState(String depName, State previous, State current) {
        events.add(new Event(SEQ.incrementAndGet(), depName, previous, current));
    }

    @Override
    public ObjList<String> softDependencies() {
        return softDeps;
    }

    /**
     * Advance the probe to the given state. Must be called after the probe has started
     * and published DEGRADED via holdInDegraded(). Publishes the state on the captured
     * context, which triggers onDependencyState callbacks on listeners.
     */
    public void advanceTo(State next) {
        State pending = pendingAdvance.getAndSet(null);
        if (pending == null) {
            // Component did not hold in degraded; publish directly if context is available.
            if (capturedCtx != null) {
                capturedCtx.publish(next);
            }
        } else {
            if (capturedCtx != null) {
                capturedCtx.publish(next);
            }
        }
    }

    /**
     * Put the probe into hold-in-DEGRADED mode before registering it.
     * When the orchestrator calls start(ctx), the probe will publish STARTING
     * then DEGRADED, then return -- leaving the component in DEGRADED state
     * until advanceTo(State.READY) is called.
     */
    public void holdInDegraded() {
        holdDegraded.set(true);
    }

    @Override
    public void start(LifecycleContext ctx) {
        startSeq = SEQ.incrementAndGet();
        this.capturedCtx = ctx;
        if (holdDegraded.get()) {
            // Publish DEGRADED so the orchestrator does not auto-promote to READY.
            // The component stays DEGRADED until advanceTo(State.READY) is called.
            ctx.publish(State.DEGRADED);
            pendingAdvance.set(State.READY);
        }
    }

    @Override
    public void stop() {
        stopSeq = SEQ.incrementAndGet();
    }
}
