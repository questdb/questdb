package io.questdb.lifecycle;

import io.questdb.WorkerPoolManager;
import io.questdb.cairo.O3PartitionJob;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Owns the component DAG: validates acyclicity, runs parallel start in
 * topological order, and reverse-topological shutdown. Implements
 * {@link QuietCloseable} so it slots into FreeOnExit's LIFO close stack via
 * {@code freeOnExit(orchestrator)} in {@link io.questdb.ServerMain#start}.
 * <p>
 * Threading: dedicated bounded {@link ThreadPoolExecutor} sized off
 * {@link Runtime#availableProcessors()} capped at 8, with
 * {@code allowCoreThreadTimeOut(true)} and a short keep-alive so steady-state
 * thread count is zero. Threads are named {@code lifecycle-N} so jstack
 * output stays operator-friendly.
 * <p>
 * Single emission point: every state transition flows through
 * {@link #publishInternal(String, State, CharSequence)}, which emits one
 * ASCII log line per transition.
 * <p>
 * Single-shot lifecycle: {@link #run()} may be invoked at most once per
 * instance. A validation failure (cycle / unknown dep / duplicate) leaves the
 * orchestrator in a closed-but-never-started state; create a new instance to
 * retry.
 * <p>
 * Non-final so enterprise overlays can subclass to add role-aware surface,
 * and so {@code LifecycleOrchestratorTest} can subclass and override
 * {@link #closeLogAndExit(int)} to observe the exit-on-uncaught path without
 * actually closing the shared log subsystem or terminating the JVM.
 */
public class LifecycleOrchestrator implements QuietCloseable {

    private static final Log LOG = LogFactory.getLog(LifecycleOrchestrator.class);
    private static final int MAX_THREADS = 8;
    protected final ExecutorService executor;
    @Nullable
    protected ObjList<Component> reverseTopoOrder;
    @Nullable
    protected ObjList<Component> topoOrder;
    // The thread that called run() and is walking the boot topo order (each component's start()
    // runs serially on this thread, including a long-running PITR restore that holds it for the
    // whole boot). close() bounded-joins it before the reverse-topo stop loop so a SIGTERM that
    // arrives mid-boot cannot free engine resources from under an in-flight start(). null before
    // run() starts and after run() returns. volatile gives the close thread the happens-before
    // edge to observe the captured (or cleared) reference.
    @Nullable
    private volatile Thread bootThread;
    // Bound on how long close() waits for the boot thread to observe shutdown and unwind. The boot
    // walk checks closed.get() between components and the in-flight start() (the restore) polls its
    // own cancel flag, so a clean unwind is prompt; the bound only caps a wedged start() so close()
    // can never block forever. A survivor past this bound is rendered safe by the engine's own
    // isClosing() guards (a woken start() refuses to build native state on a closing engine), not by
    // racing the free -- so this is a bounded join with an ownership backstop, never a bounded-join
    // that then frees under a live writer.
    private static final long BOOT_JOIN_BUDGET_MS = 30_000L;
    private final AtomicBoolean closed = new AtomicBoolean();
    // When a component's start() throws, the orchestrator retains the first such throwable and
    // the component name so run() can chain them into the LifecycleStartupException. Retaining
    // only the first failure keeps the cause stable across a cascade where several dependents are
    // also marked FAILED. Both fields are written once under the startAllInTopologicalOrder loop
    // (single-threaded at that point) and read once in run() after the loop returns.
    private String firstFailedComponentName;
    private Throwable firstFailedComponentThrowable;
    private final Log injectedLog;
    // latestProgress and lastTransitionMicros are read by the /lifecycle HTTP handler
    // thread via snapshot() while concurrently written by the lifecycle/main thread (publishInternal
    // and ContextImpl.progress). Use ConcurrentHashMap so reads see a consistent point-in-time view
    // and writers never race the hashmap's rehash boundary.
    private final ConcurrentHashMap<String, Long> lastTransitionMicros = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ProgressEvent> latestProgress = new ConcurrentHashMap<>();
    // Runs after the executor drain but before the reverse-topo stop loop. ServerMain installs
    // a worker-pool halt here: the stop loop frees component resources (e.g. the http dispatcher's
    // native FDSet) dependents-first, and on a boot-failure rollback shared pool workers are still
    // running at that point -- they must stop touching those resources before the loop frees them.
    @Nullable
    private volatile Runnable preStopHook;
    private final AtomicBoolean ran = new AtomicBoolean();
    private final ObjList<Component> registry = new ObjList<>();
    private final CharSequenceObjHashMap<Component> registryByName = new CharSequenceObjHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicLong stableWatchIdSeq = new AtomicLong();
    private final ConcurrentHashMap<Long, StableWatch> stableWatchers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicReference<State>> states = new ConcurrentHashMap<>();
    @Nullable
    private final Object tokioRuntime;
    @Nullable
    private final WorkerPoolManager workerPoolManager;

    public LifecycleOrchestrator(
            @Nullable Log injectedLog,
            @Nullable WorkerPoolManager workerPoolManager,
            @Nullable Object tokioRuntime
    ) {
        this.injectedLog = injectedLog != null ? injectedLog : LOG;
        this.workerPoolManager = workerPoolManager;
        this.tokioRuntime = tokioRuntime;
        // PathClearingThreadPoolExecutor.afterExecute clears the per-thread Path thread-locals after
        // every task. The lifecycle-N threads are plain JDK threads, not WorkerPool workers, so they
        // never get the WorkerPool.setupPathCleaner path cleanup that runs Path.THREAD_LOCAL_CLEANER +
        // O3PartitionJob.THREAD_LOCAL_CLEANER on worker halt. Any task that touches a thread-local Path
        // (a role-switch cascade, the mat-view writer-pool drain, or a stable-below callback) would
        // otherwise leave a 256-byte native NATIVE_PATH_THREAD_LOCAL malloc behind on the lifecycle
        // thread; when the thread idle-times-out the JVM drops the ThreadLocalMap but never close()s the
        // native Path, leaking it for the life of the server across repeated switches.
        this.executor = new PathClearingThreadPoolExecutor(
                Math.min(Runtime.getRuntime().availableProcessors(), MAX_THREADS),
                Math.min(Runtime.getRuntime().availableProcessors(), MAX_THREADS),
                10L, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                lifecycleThreadFactory()
        );
        ((ThreadPoolExecutor) executor).allowCoreThreadTimeOut(true);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        // Shut down + await the executor BEFORE the reverse-topo stop loop. Previously the
        // stop loop ran first and executor.shutdown ran after -- which left a window where an
        // in-flight switchRole on the lifecycle executor thread could touch a component that
        // the stop loop had just freed (a native use-after-free / NPE). Draining the executor here
        // means every in-flight task either completes before the stop loop runs, or the new
        // closed.get() short-circuit at the top of startAllInTopologicalOrder fires first and
        // the task exits without progressing further.
        executor.shutdown();
        // Rendezvous with any in-flight executor work (an enterprise role-switch cascade) before
        // the stop loop frees component resources. The base implementation just awaits the executor
        // for the default budget; the enterprise overlay overrides awaitInFlightWork to extend the
        // await to the ACTIVE switch budget and to nudge the cascade (interrupt its drain sleep) so
        // it observes shutdown promptly and exits at its next isClosing() boundary check. The bound
        // is the switch budget, never an unbounded join -- a wedged cascade that exceeds even the
        // budget falls through to the stop loop logged, not blocked forever.
        if (!awaitInFlightWork()) {
            injectedLog.error()
                    .$("lifecycle executor did not drain within the close budget; proceeding to the "
                            + "reverse-topo stop loop (in-flight task, if any, must self-terminate at its "
                            + "next closed boundary check)").I$();
        }
        // Rendezvous with an in-flight BOOT walk before the stop loop frees engine resources. The
        // boot thread runs each component's start() serially (a PITR restore can hold it for the
        // whole boot); a SIGTERM hook firing mid-boot must not let freeOnExit free the engine while
        // an engine.load()/native restore is still running on the boot thread. closed is already
        // true above, so the boot walk's between-components check exits and the in-flight start()
        // observes shutdown (the restore polls its cancel flag, the SWITCHING/STARTING stop predicate
        // routes a mid-restore component through stop()->signalRestoreCancel). The join is bounded:
        // a wedged start() that ignores shutdown only delays close() by the budget, after which the
        // stop loop proceeds and the engine's own isClosing() guards keep a late-woken start() from
        // building native state on the closing engine -- ownership, not the race, is the backstop.
        // Never self-join: run() calls close() on its own thread on a boot-essential failure.
        final Thread boot = bootThread;
        if (boot != null && boot != Thread.currentThread() && boot.isAlive()) {
            try {
                boot.join(BOOT_JOIN_BUDGET_MS);
                if (boot.isAlive()) {
                    injectedLog.error()
                            .$("boot thread did not unwind within the close budget; proceeding to the "
                                    + "reverse-topo stop loop (a late-woken start() is refused by the "
                                    + "engine's closing guards, not freed under)").I$();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                injectedLog.error()
                        .$("interrupted while joining the boot thread on close; proceeding to the "
                                + "reverse-topo stop loop").I$();
            }
        }
        // reverseTopoOrder is null if validateAndComputeOrder() never ran (or threw before
        // assignment). In that case there are no started components -- skip the per-component stop
        // loop and just shut down the executor. This makes close() safe to call after a validation
        // failure in run() and on an orchestrator that was never run() at all.
        if (reverseTopoOrder != null) {
            final Runnable hook = preStopHook;
            if (hook != null) {
                try {
                    hook.run();
                } catch (Throwable t) {
                    injectedLog.error().$("pre-stop hook failed ").$(t).$();
                }
            }
            for (int i = 0, n = reverseTopoOrder.size(); i < n; i++) {
                Component c = reverseTopoOrder.getQuick(i);
                State current = stateOf(c.name());
                // STARTING is included so a SIGTERM that arrives while a long-running start
                // is in flight (e.g. BackupRestoreEnvelope mid-PITR-restore, which can run for
                // minutes) still routes through the component's stop() method. The component's
                // stop() is the only path that invokes signalRestoreCancel + awaitRestoreCancel,
                // and without this the 30s SIGTERM window hangs until SIGKILL. The transition table
                // permits STARTING -> STOPPING for this case.
                //
                // SWITCHING is included for the same reason: a SIGTERM that arrives while an
                // enterprise role-switch cascade has a component mid-switch leaves that component in
                // SWITCHING. Skipping it here would let the reverse-topo stop loop free the
                // component's dependents (and ultimately the engine) without ever calling the
                // mid-switch component's stop() -- so a woken cascade step could touch a half-freed
                // component. Stopping a SWITCHING component routes it through stop() like any other
                // started component; the transition table permits SWITCHING -> STOPPING.
                if (current == State.READY || current == State.DEGRADED || current == State.STARTING
                        || current == State.SWITCHING) {
                    publishInternal(c.name(), State.STOPPING, null);
                    try {
                        c.stop();
                        publishInternal(c.name(), State.STOPPED, null);
                    } catch (Throwable t) {
                        publishInternal(c.name(), State.FAILED, "stop failed: " + t.getMessage());
                    }
                }
            }
        }
    }

    /**
     * Production seam: returns the per-component LifecycleContext that the orchestrator dispatches
     * to {@link Component#start(LifecycleContext)}. Enterprise overlays override this to wrap the
     * base context with role-aware accessors. The silent no-op inside publishInternal for unknown
     * component names means a typo passes without warning; the orchestrator now logs at INFO
     * level when publishInternal is invoked against an unregistered name so accidental misuse
     * surfaces in the log stream.
     */
    public LifecycleContext contextFor(String componentName) {
        return new ContextImpl(componentName);
    }

    @Nullable
    public Component getComponent(String name) {
        return registryByName.get(name);
    }

    public void register(Component component) {
        if (running.get() || closed.get()) {
            throw new IllegalStateException("can only register components before run");
        }
        String n = component.name();
        validateComponentName(n);
        if (registryByName.contains(n)) {
            throw new LifecycleStartupException("duplicate component name: " + n);
        }
        registry.add(component);
        registryByName.put(n, component);
        states.put(n, new AtomicReference<>(State.INIT));
    }

    public void run() {
        // Validate FIRST, then single-shot CAS, then flip running. A validation failure
        // (unknown dep / cycle / duplicate name) is retryable -- the caller can fix the registry
        // via subsequent register() calls and re-invoke run(). Previously the ran CAS happened
        // BEFORE validateAndComputeOrder, so a transient validation failure locked the orchestrator
        // permanently with an unhelpful "may only be called once" error on retry.
        //
        // register() guards based on `running`, which stays false until the CAS
        // succeeds AND validation has already passed. close() defensively handles a null
        // reverseTopoOrder for the "constructed but never run" case.
        validateAndComputeOrder();   // may throw LifecycleStartupException; ran + running still false
        if (!ran.compareAndSet(false, true)) {
            throw new IllegalStateException("LifecycleOrchestrator.run may only be called once");
        }
        running.set(true);
        // Publish the boot thread so a concurrent close() (the SIGTERM shutdown hook runs on a
        // different thread) can bounded-join the in-flight boot walk before freeing engine state.
        // Cleared in the finally so a completed boot leaves no stale handle for a later close().
        bootThread = Thread.currentThread();
        try {
            startAllInTopologicalOrder();
        } finally {
            bootThread = null;
        }
        if (anyFailed()) {
            close();
            // Chain the first failed component's throwable AND fold its message into the wrapper
            // text, so the real cause (the specific error message or error-code URL the component
            // threw) is visible directly in the top-level boot error an operator sees in the log,
            // not only via getCause(). The throwable stays chained for callers that unwrap it.
            if (firstFailedComponentThrowable != null) {
                String causeMessage = firstFailedComponentThrowable.getMessage();
                throw new LifecycleStartupException(
                        "boot-essential component(s) failed [component=" + firstFailedComponentName + "]"
                                + (causeMessage != null ? ": " + causeMessage : ""),
                        firstFailedComponentThrowable
                );
            }
            throw new LifecycleStartupException("boot-essential component(s) failed");
        }
    }

    /**
     * Installs a hook that {@link #close()} runs after the executor drain and before the
     * reverse-topo stop loop. ServerMain supplies a bounded worker-pool halt so no pool worker
     * still touches component resources (native fd sets, queues) while the stop loop frees them
     * on a boot-failure rollback. The hook must be idempotent with the normal shutdown path.
     */
    public void setPreStopHook(@Nullable Runnable hook) {
        this.preStopHook = hook;
    }

    public LifecycleSnapshot snapshot() {
        long capturedAt = MicrosecondClockImpl.INSTANCE.getTicks();
        ObjList<LifecycleSnapshot.ComponentSnapshot> snaps = new ObjList<>();
        for (int i = 0, n = registry.size(); i < n; i++) {
            Component c = registry.getQuick(i);
            String name = c.name();
            Long lastTs = lastTransitionMicros.get(name);
            snaps.add(new LifecycleSnapshot.ComponentSnapshot(
                    name,
                    stateOf(name),
                    lastTs != null ? lastTs : 0L,
                    latestProgress.get(name),
                    c.hardRequiredDependencies(),
                    c.softDependencies()
            ));
        }
        return new LifecycleSnapshot(capturedAt, snaps);
    }

    public State stateOf(String componentName) {
        AtomicReference<State> ref = states.get(componentName);
        return ref != null ? ref.get() : State.INIT;
    }

    /**
     * Rendezvous hook invoked from {@link #close()} after {@code executor.shutdown()} and before
     * the reverse-topo stop loop. The base implementation simply awaits the executor for the
     * default 30s close budget so an in-flight task completes (or self-terminates at its next
     * {@code closed.get()} boundary check) before any component is stopped.
     * <p>
     * Enterprise overlays override this to extend the await to the ACTIVE role-switch budget (a
     * switch cascade can legitimately run longer than 30s while draining busy writers) and to nudge
     * the cascade -- interrupt its drain sleep -- so it observes shutdown promptly. The bound is
     * always finite (the switch budget, itself capped), never an unbounded join.
     *
     * @return {@code true} if the executor terminated within the budget; {@code false} if it
     * timed out, in which case {@link #close()} logs and proceeds to the stop loop.
     */
    protected boolean awaitInFlightWork() {
        try {
            return executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    protected void cascadeFailedThroughHardDeps(String failedName) {
        // BFS through reverse hard-dep adjacency. Soft deps do not propagate failure, so skip them.
        for (int i = 0, n = registry.size(); i < n; i++) {
            Component c = registry.getQuick(i);
            ObjList<String> hard = c.hardRequiredDependencies();
            for (int j = 0, m = hard.size(); j < m; j++) {
                if (failedName.contentEquals(hard.getQuick(j))) {
                    AtomicReference<State> ref = states.get(c.name());
                    if (ref != null && ref.get() != State.FAILED) {
                        publishInternal(c.name(), State.FAILED, "hard dep " + failedName + " failed");
                    }
                }
            }
        }
    }

    /**
     * Test seam (exit-on-uncaught path): production closes the log ring buffer via
     * {@link LogFactory#closeInstance()} and terminates the JVM with {@code System.exit(55)}.
     * Tests override to record the exit code and skip both the log close and the actual JVM
     * termination so the suite can run without losing logging for subsequent tests.
     * Protected so {@code LifecycleOrchestratorTest} (different package) can subclass and override it.
     */
    protected void closeLogAndExit(int code) {
        LogFactory.closeInstance();
        System.exit(code);
    }

    /**
     * Closed-state accessor for subclass consumption. {@code EntLifecycleOrchestrator.submitSwitch}
     * consults this to reject post-close switch requests without elevating the
     * visibility of the {@code private final AtomicBoolean closed} field.
     */
    protected boolean isClosed() {
        return closed.get();
    }

    protected void publishInternal(String name, State next, @Nullable CharSequence reason) {
        AtomicReference<State> ref = states.get(name);
        if (ref == null) {
            // Surface accidental misuse via contextFor() with an unregistered component name.
            // The early return preserves the silent-no-op contract that LifecycleStateTest relies on
            // (minting a context for "a" before run()), but the log line ensures typos do not
            // disappear silently.
            injectedLog.info()
                    .$("publish for unregistered component name=").$(name)
                    .$(" attempted to=").$(next)
                    .I$();
            return;
        }
        State prev;
        do {
            prev = ref.get();
            if (prev == next) {
                // Idempotent same-state republish. A clean role switch re-fires the
                // "engine reached READY" dependency callbacks on every network service
                // (pg-wire, web-http, ilp-tcp), each of which republishes itself READY
                // while it is already READY; the boot path likewise re-publishes STARTING.
                // The transition table has no self-loop, so treating these as transitions
                // routed every clean switch through the error-level invalid-transition log.
                // A republish to the current state changes nothing: skip it quietly (debug,
                // not error) without firing the transition log or the state-change dispatch.
                try {
                    injectedLog.debug()
                            .$("ignoring same-state republish component=").$(name)
                            .$(" state=").$(next)
                            .I$();
                } catch (Throwable ignore) {
                    // Diagnostics only; the log subsystem can already be halted when a
                    // JVM-shutdown close publishes late (see emitTransitionLog).
                }
                return;
            }
            if (prev == State.FAILED && next != State.FAILED) {
                // FAILED is a terminal state; ignore any late publish that tries to leave it.
                try {
                    injectedLog.error()
                            .$("rejecting transition from FAILED component=").$(name)
                            .$(" attempted to=").$(next)
                            .I$();
                } catch (Throwable ignore) {
                    // Same shutdown-race guard as emitTransitionLog.
                }
                return;
            }
            if (!isValidTransition(prev, next)) {
                try {
                    injectedLog.error()
                            .$("invalid transition component=").$(name)
                            .$(" from=").$(prev)
                            .$(" to=").$(next)
                            .I$();
                } catch (Throwable ignore) {
                    // Same shutdown-race guard as emitTransitionLog.
                }
                return;
            }
        } while (!ref.compareAndSet(prev, next));
        long ts = MicrosecondClockImpl.INSTANCE.getTicks();
        Long lastTs = lastTransitionMicros.get(name);
        long since = lastTs != null ? ts - lastTs : 0L;
        lastTransitionMicros.put(name, ts);
        emitTransitionLog(name, prev, next, ts, since, reason);
        if (next == State.FAILED) {
            cascadeFailedThroughHardDeps(name);
        }
        dispatchStateChange(name, prev, next);
    }

    private static boolean contains(ObjList<Component> list, Component c) {
        for (int i = 0, n = list.size(); i < n; i++) {
            if (list.getQuick(i) == c) {
                return true;
            }
        }
        return false;
    }

    private static boolean isValidTransition(State from, State to) {
        if (to == State.FAILED) {
            return from != State.FAILED && from != State.STOPPED;
        }
        return switch (from) {
            case INIT -> to == State.STARTING;
            // STARTING -> STOPPING is permitted so close() can request cancellation of an
            // in-flight long-running start (e.g. PITR restore inside BackupRestoreEnvelope.start()).
            // BackupRestoreEnvelope.stop() invokes signalRestoreCancel + awaitRestoreCancel(25_000L)
            // to unwind a running restore inside the 30s SIGTERM window; without this transition
            // close() would silently skip the stop() call and the JVM hangs until SIGKILL.
            case STARTING -> to == State.DEGRADED || to == State.READY || to == State.STOPPING;
            case DEGRADED -> to == State.READY || to == State.SWITCHING || to == State.STOPPING;
            case READY -> to == State.DEGRADED || to == State.SWITCHING || to == State.STOPPING;
            case SWITCHING -> to == State.DEGRADED || to == State.READY || to == State.STOPPING;
            case STOPPING -> to == State.STOPPED;
            case STOPPED, FAILED -> false;
        };
    }

    /**
     * Enforce the JSON-safety invariant that {@link LifecycleProcessor} relies on when
     * emitting component names via {@code putAsciiQuoted} (no escaping). All names must match
     * {@code [a-z0-9-]+} -- lowercase ASCII letters, digits, and hyphens only. Production names
     * already comply ("min-http", "factory-provider", "ent-engine-init", ...); enforcing it at
     * registration prevents a future typo or accidental control character from emitting
     * malformed JSON over the wire.
     */
    private static void validateComponentName(String name) {
        if (name == null || name.isEmpty()) {
            throw new LifecycleStartupException("component name must be non-empty");
        }
        for (int i = 0, n = name.length(); i < n; i++) {
            char c = name.charAt(i);
            boolean ok = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-';
            if (!ok) {
                throw new LifecycleStartupException(
                        "component name must match [a-z0-9-]+ for JSON-safe serialization (LifecycleProcessor); got: " + name);
            }
        }
    }

    private boolean allHardDependentsStable(String name) {
        for (int i = 0, n = registry.size(); i < n; i++) {
            Component c = registry.getQuick(i);
            ObjList<String> hard = c.hardRequiredDependencies();
            for (int j = 0, m = hard.size(); j < m; j++) {
                if (name.contentEquals(hard.getQuick(j))) {
                    if (!stateOf(c.name()).isStable()) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    // Scans the entire registry for any component in FAILED state -- a flat scan
    // over all registered components, not a reachability walk.
    private boolean anyFailed() {
        for (int i = 0, n = registry.size(); i < n; i++) {
            if (stateOf(registry.getQuick(i).name()) == State.FAILED) {
                return true;
            }
        }
        return false;
    }

    private void dispatchStateChange(String componentName, State previous, State current) {
        // Fire onDependencyState for every component that lists componentName as soft or hard dep.
        for (int i = 0, n = registry.size(); i < n; i++) {
            Component listener = registry.getQuick(i);
            ObjList<String> hard = listener.hardRequiredDependencies();
            ObjList<String> soft = listener.softDependencies();
            boolean isDep = false;
            for (int j = 0, m = hard.size(); j < m; j++) {
                if (componentName.contentEquals(hard.getQuick(j))) {
                    isDep = true;
                    break;
                }
            }
            if (!isDep) {
                for (int j = 0, m = soft.size(); j < m; j++) {
                    if (componentName.contentEquals(soft.getQuick(j))) {
                        isDep = true;
                        break;
                    }
                }
            }
            if (isDep) {
                listener.onDependencyState(componentName, previous, current);
            }
        }
        // Fire stable-below callbacks if all hard-required dependents of componentName are now stable.
        fireStableBelowCallbacks();
    }

    /**
     * Emits the advisory transition record, tolerating a halted log subsystem. During
     * JVM shutdown the log factory's own shutdown hook can free the log rings while
     * component close/stop paths are still publishing their final transitions (shutdown
     * hook order is unspecified, and a SIGTERM'd test JVM tears the servers and the
     * logger down concurrently). The record is diagnostics; the CAS'd state transition
     * and the dispatch that follow in {@code publishInternal} are load-bearing and must
     * survive a dead logger, so an emit failure is swallowed rather than allowed to
     * abort the FAILED cascade and the state-change dispatch.
     */
    private void emitTransitionLog(
            String name,
            State prev,
            State next,
            long ts,
            long since,
            @Nullable CharSequence reason
    ) {
        try {
            emitTransitionLog0(name, prev, next, ts, since, reason);
        } catch (Throwable ignore) {
            // Log subsystem already halted (JVM shutdown race); the transition itself
            // must still complete.
        }
    }

    private void emitTransitionLog0(
            String name,
            State prev,
            State next,
            long ts,
            long since,
            @Nullable CharSequence reason
    ) {
        if (next == State.FAILED) {
            io.questdb.log.LogRecord rec = injectedLog.error()
                    .$("component=").$(name)
                    .$(" from=").$(prev)
                    .$(" to=").$(next)
                    .$(" ts=").$(ts)
                    .$(" since=").$(since);
            if (reason != null) {
                rec.$(" reason=\"").$(reason).$('"').I$();
            } else {
                rec.I$();
            }
        } else {
            injectedLog.info()
                    .$("component=").$(name)
                    .$(" from=").$(prev)
                    .$(" to=").$(next)
                    .$(" ts=").$(ts)
                    .$(" since=").$(since)
                    .I$();
        }
    }

    private void fireStableBelowCallbacks() {
        // Every state-change round re-evaluates every watcher; dispatch is global rather than
        // gated on a specific dep. Iterate the entry set so a watch that fires its single-shot
        // callback can be removed from the map by its key (safe to remove during a ConcurrentHashMap
        // iteration); otherwise a fired single-shot watch would linger as dead state and the map
        // would accumulate one entry per round-evaluated watch over the orchestrator's lifetime.
        for (java.util.Map.Entry<Long, StableWatch> e : stableWatchers.entrySet()) {
            StableWatch w = e.getValue();
            if (allHardDependentsStable(w.componentName)) {
                // getAndSet(null) is a single CAS so two threads observing READY at the same time
                // can no longer both null-and-invoke. Previously the three independent volatile ops
                // (read cb, null callback, run cb) allowed a race where both threads saw a non-null
                // callback and fired it twice. The callback contract is single-shot: only the thread
                // that wins the CAS sees a non-null callback, fires it, and removes the watch.
                Runnable cb = w.callback.getAndSet(null);
                if (cb != null) {
                    // The CAS winner owns this single-shot watch -- drop it from the map now that it
                    // has fired, preserving the at-most-once contract while not leaking the entry.
                    stableWatchers.remove(e.getKey());
                    try {
                        cb.run();
                    } catch (Throwable t) {
                        // A stage-2 callback failure must surface as a boot failure, not be
                        // silently swallowed. If the callback throws (e.g. the worker pool
                        // manager fails to create native threads), the server has no worker
                        // threads and cannot serve requests. Publishing FAILED here ensures
                        // anyFailed() trips in run(), so boot throws instead of returning a
                        // DEGRADED zombie that passes readiness probes but cannot do any work.
                        injectedLog.error()
                                .$("stable-below callback failed for ").$(w.componentName)
                                .$(": ").$(t.getMessage())
                                .I$();
                        publishInternal(w.componentName, State.FAILED, "stage-2 callback failed: " + t.getMessage());
                        // Retain the failure so run() can chain it into LifecycleStartupException.
                        if (firstFailedComponentThrowable == null) {
                            firstFailedComponentThrowable = t;
                            firstFailedComponentName = w.componentName;
                        }
                    }
                }
            }
        }
    }

    private ThreadFactory lifecycleThreadFactory() {
        AtomicLong counter = new AtomicLong();
        return r -> {
            Thread t = new Thread(r, "lifecycle-" + counter.incrementAndGet());
            t.setDaemon(true);
            // Exit-55 path: an uncaught throwable from any orchestrator-executed task
            // (LifecycleStartupException or any other Throwable) lands here. Run reverse-topo stop +
            // freeOnExit release via close(), then close the log subsystem, then exit. The native
            // handles release invariant requires close() to run the reverse-topo stop loop that
            // honors this. closeLogAndExit() is a test seam so unit tests can observe the exit
            // code without actually terminating the JVM.
            t.setUncaughtExceptionHandler((thread, ex) -> {
                injectedLog.error()
                        .$("lifecycle thread uncaught exception thread=").$(thread.getName())
                        .$(" failure=").$(ex)
                        .I$();
                try {
                    close();
                } catch (Throwable closeFailure) {
                    injectedLog.error()
                            .$("orchestrator close failed after switch exception failure=").$(closeFailure)
                            .I$();
                }
                closeLogAndExit(55);
            });
            return t;
        };
    }

    private void startAllInTopologicalOrder() {
        // Walk topo order serially. The hard-required chain degenerates
        // to serial-but-via-executor execution; the executor is constructed and
        // available for future parallel exploitation.
        assert topoOrder != null;
        for (int i = 0, n = topoOrder.size(); i < n; i++) {
            // Close-race short-circuit: if close() raced startup, exit the loop now so the
            // remaining components stay in INIT and the reverse-topo stop loop has nothing to
            // tear down for them. The in-flight component finishes start() naturally; close()'s
            // executor.shutdown + awaitTermination (run BEFORE the stop loop now) ensures that
            // completion drains before any stop() touches shared engine state.
            if (closed.get()) {
                return;
            }
            Component c = topoOrder.getQuick(i);
            // Check for failed hard deps before starting.
            ObjList<String> hard = c.hardRequiredDependencies();
            boolean depFailed = false;
            for (int j = 0, m = hard.size(); j < m; j++) {
                if (stateOf(hard.getQuick(j).toString()) == State.FAILED) {
                    depFailed = true;
                    break;
                }
            }
            if (depFailed) {
                if (stateOf(c.name()) != State.FAILED) {
                    publishInternal(c.name(), State.FAILED, "hard dep failed before start");
                }
                continue;
            }
            publishInternal(c.name(), State.STARTING, null);
            try {
                c.start(contextFor(c.name()));
                State after = stateOf(c.name());
                if (after == State.STARTING) {
                    // Component returned normally without explicitly publishing a terminal state;
                    // auto-promote to READY per convention.
                    publishInternal(c.name(), State.READY, null);
                }
            } catch (Throwable t) {
                publishInternal(c.name(), State.FAILED, t.toString());
                // Retain the first failure so run() can chain it into LifecycleStartupException.
                if (firstFailedComponentThrowable == null) {
                    firstFailedComponentThrowable = t;
                    firstFailedComponentName = c.name();
                }
            }
        }
    }

    private void validateAndComputeOrder() {
        // Validate all deps are registered.
        for (int i = 0, n = registry.size(); i < n; i++) {
            Component c = registry.getQuick(i);
            ObjList<String> hard = c.hardRequiredDependencies();
            for (int j = 0, m = hard.size(); j < m; j++) {
                String depName = hard.getQuick(j).toString();
                if (!registryByName.contains(depName)) {
                    throw new LifecycleStartupException("unknown dependency: " + depName + " required by " + c.name());
                }
            }
            ObjList<String> soft = c.softDependencies();
            for (int j = 0, m = soft.size(); j < m; j++) {
                String depName = soft.getQuick(j).toString();
                if (!registryByName.contains(depName)) {
                    throw new LifecycleStartupException("unknown dependency: " + depName + " required by " + c.name());
                }
            }
        }
        // Kahn's algorithm for topological sort.
        IntList inDegree = new IntList();
        for (int i = 0, n = registry.size(); i < n; i++) {
            inDegree.add(registry.getQuick(i).hardRequiredDependencies().size());
        }
        ObjList<Component> order = new ObjList<>();
        boolean progress = true;
        while (progress && order.size() < registry.size()) {
            progress = false;
            for (int i = 0, n = registry.size(); i < n; i++) {
                Component c = registry.getQuick(i);
                if (inDegree.getQuick(i) == 0 && !contains(order, c)) {
                    order.add(c);
                    progress = true;
                    String cName = c.name();
                    for (int k = 0, kn = registry.size(); k < kn; k++) {
                        Component other = registry.getQuick(k);
                        ObjList<String> hard = other.hardRequiredDependencies();
                        for (int j = 0, m = hard.size(); j < m; j++) {
                            if (cName.contentEquals(hard.getQuick(j))) {
                                inDegree.setQuick(k, inDegree.getQuick(k) - 1);
                            }
                        }
                    }
                }
            }
        }
        if (order.size() < registry.size()) {
            throw new LifecycleStartupException("dependency cycle detected in lifecycle DAG");
        }
        topoOrder = order;
        ObjList<Component> reverse = new ObjList<>();
        for (int i = order.size() - 1; i >= 0; i--) {
            reverse.add(order.getQuick(i));
        }
        reverseTopoOrder = reverse;
    }

    /**
     * ThreadPoolExecutor that clears the per-thread Path thread-locals after every task. Mirrors
     * {@link io.questdb.mp.WorkerPool}'s path cleanup (Path.THREAD_LOCAL_CLEANER +
     * O3PartitionJob.THREAD_LOCAL_CLEANER, run on worker halt), but does it after each task instead of
     * once at thread end. afterExecute runs on the worker thread that just ran the task, so clearing
     * here is deterministic and survives the executor's idle-timeout thread reaping -- a plain
     * ThreadFactory wrapper would only clear at the worker loop's top-level runnable, which never
     * returns until the thread idle-times-out, by which point the leak is already in place.
     */
    private static final class PathClearingThreadPoolExecutor extends ThreadPoolExecutor {
        PathClearingThreadPoolExecutor(
                int corePoolSize,
                int maximumPoolSize,
                long keepAliveTime,
                TimeUnit unit,
                BlockingQueue<Runnable> workQueue,
                ThreadFactory threadFactory
        ) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            super.afterExecute(r, t);
            Path.clearThreadLocals();
            Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
        }
    }

    private static final class StableWatch {
        final AtomicReference<Runnable> callback;
        final String componentName;

        StableWatch(String componentName, Runnable callback) {
            this.componentName = componentName;
            this.callback = new AtomicReference<>(callback);
        }
    }

    private final class ContextImpl implements LifecycleContext {
        private final String selfName;

        ContextImpl(String selfName) {
            this.selfName = selfName;
        }

        @Override
        public long onStableBelow(String componentName, Runnable callback) {
            long id = stableWatchIdSeq.incrementAndGet();
            stableWatchers.put(id, new StableWatch(componentName, callback));
            // Fire immediately if already stable.
            if (allHardDependentsStable(componentName)) {
                fireStableBelowCallbacks();
            }
            return id;
        }

        @Override
        public void progress(ProgressEvent event) {
            latestProgress.put(selfName, event);
        }

        @Override
        public void publish(State next) {
            publishInternal(selfName, next, null);
        }

        @Override
        public void publish(State next, CharSequence reason) {
            publishInternal(selfName, next, reason);
        }

        @Override
        public State state(String componentName) {
            return stateOf(componentName);
        }

        @Override
        @Nullable
        public Object tokioRuntime() {
            return tokioRuntime;
        }

        @Override
        public void unwatchStable(long watchId) {
            stableWatchers.remove(watchId);
        }

        @Override
        @Nullable
        public WorkerPoolManager workerPoolManager() {
            return workerPoolManager;
        }
    }
}
