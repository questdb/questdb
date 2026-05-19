package io.questdb.lifecycle;

import io.questdb.WorkerPoolManager;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Owns the component DAG: validates acyclicity, runs parallel start in
 * topological order, reverse-topological shutdown, and switchRole dispatch
 * (LIFE-02). Implements {@link QuietCloseable} so it slots into FreeOnExit's
 * LIFO close stack via {@code freeOnExit(orchestrator)} in
 * {@link io.questdb.ServerMain#start}.
 * <p>
 * Threading: dedicated bounded {@link ThreadPoolExecutor} sized off
 * {@link Runtime#availableProcessors()} capped at 8, with
 * {@code allowCoreThreadTimeOut(true)} and a short keep-alive so steady-state
 * thread count is zero. Threads are named {@code lifecycle-N} so jstack
 * output stays operator-friendly.
 * <p>
 * OBS-01 / LIFE-08 single emission point: every state transition flows
 * through {@link #publishInternal(String, State, CharSequence)}, which
 * emits one ASCII log line per transition in the format documented in
 * CONTEXT.md (Claude's Discretion section).
 * <p>
 * Single-shot lifecycle: {@link #run()} may be invoked at most once per
 * instance. A validation failure (cycle / unknown dep / duplicate) leaves the
 * orchestrator in a closed-but-never-started state; create a new instance to
 * retry.
 * <p>
 * Non-final solely so {@code LifecycleOrchestratorTest} can subclass and override
 * {@link #closeLogAndExit(int)} to observe the D6-06 exit-55 path without actually
 * closing the shared log subsystem or terminating the JVM. Production code must
 * not subclass this class.
 */
public class LifecycleOrchestrator implements QuietCloseable {

    private static final Log LOG = LogFactory.getLog(LifecycleOrchestrator.class);
    private static final int MAX_THREADS = 8;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ThreadPoolExecutor executor;
    private final Log injectedLog;
    // CR-02: latestProgress and lastTransitionMicros are read by the /lifecycle HTTP handler
    // thread via snapshot() while concurrently written by the lifecycle/main thread (publishInternal
    // and ContextImpl.progress). Use ConcurrentHashMap so reads see a consistent point-in-time view
    // and writers never race the hashmap's rehash boundary. WR-05: lastTransitionMicros sorts before
    // latestProgress.
    private final ConcurrentHashMap<String, Long> lastTransitionMicros = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ProgressEvent> latestProgress = new ConcurrentHashMap<>();
    private final AtomicBoolean ran = new AtomicBoolean();
    private final ObjList<Component> registry = new ObjList<>();
    private final CharSequenceObjHashMap<Component> registryByName = new CharSequenceObjHashMap<>();
    private final AtomicReference<Role> role;
    // D6-04 / Plan B Task 2: SwitchProcessor.onRequestComplete forwards parsed `timeout_ms` here BEFORE
    // submitSwitch. Enterprise registers a consumer that hands the value to
    // EntDynamicPropServerConfiguration.setRoleSwitchTimeoutMs(long); EntEngineInitEnvelope.switchRole
    // reads getRoleSwitchTimeoutMs() at switch time, replacing the previously hardcoded
    // DEFAULT_DRAIN_TIMEOUT_MS literal. Default = no-op for OSS standalone path.
    private volatile java.util.function.LongConsumer roleSwitchTimeoutConsumer = ms -> {
    };
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicLong stableWatchIdSeq = new AtomicLong();
    private final ConcurrentHashMap<Long, StableWatch> stableWatchers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicReference<State>> states = new ConcurrentHashMap<>();
    private final AtomicBoolean switchInFlight = new AtomicBoolean(false);
    @Nullable
    private final Object tokioRuntime;
    @Nullable
    private final WorkerPoolManager workerPoolManager;
    @Nullable
    private ObjList<Component> reverseTopoOrder;
    @Nullable
    private ObjList<Component> topoOrder;

    public LifecycleOrchestrator(
            Role initialRole,
            @Nullable Log injectedLog,
            @Nullable WorkerPoolManager workerPoolManager,
            @Nullable Object tokioRuntime
    ) {
        this.role = new AtomicReference<>(initialRole);
        this.injectedLog = injectedLog != null ? injectedLog : LOG;
        this.workerPoolManager = workerPoolManager;
        this.tokioRuntime = tokioRuntime;
        this.executor = new ThreadPoolExecutor(
                Math.min(Runtime.getRuntime().availableProcessors(), MAX_THREADS),
                Math.min(Runtime.getRuntime().availableProcessors(), MAX_THREADS),
                10L, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                lifecycleThreadFactory()
        );
        executor.allowCoreThreadTimeOut(true);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        // B3 fix: reverseTopoOrder is null if validateAndComputeOrder() never ran (or threw before
        // assignment). In that case there are no started components -- skip the per-component stop
        // loop and just shut down the executor. This makes close() safe to call after a validation
        // failure in run() and on an orchestrator that was never run() at all.
        if (reverseTopoOrder != null) {
            for (int i = 0, n = reverseTopoOrder.size(); i < n; i++) {
                Component c = reverseTopoOrder.getQuick(i);
                State current = stateOf(c.name());
                // CR-03: STARTING is included so a SIGTERM that arrives while a long-running start
                // is in flight (e.g. BackupRestoreEnvelope mid-PITR-restore, which can run for
                // minutes) still routes through the component's stop() method. The component's
                // stop() is the only path that invokes signalRestoreCancel + awaitRestoreCancel,
                // and without this the 30s SIGTERM window hangs until SIGKILL. The transition table
                // permits STARTING -> STOPPING for this case.
                if (current == State.READY || current == State.DEGRADED || current == State.STARTING) {
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
        executor.shutdown();
        try {
            executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * WR-11: contextFor is a TEST-ONLY hook. Production callers MUST NOT mint contexts here --
     * each component receives its own LifecycleContext via {@link Component#start(LifecycleContext)},
     * dispatched by the orchestrator. The silent no-op inside publishInternal for unknown component
     * names means a typo in test code passes without warning; the orchestrator now logs at INFO
     * level when publishInternal is invoked against an unregistered name so accidental misuse
     * surfaces in the log stream.
     */
    @TestOnly
    public LifecycleContext contextFor(String componentName) {
        return new ContextImpl(componentName);
    }

    public Role currentRole() {
        return role.get();
    }

    @Nullable
    public Component getComponent(String name) {
        return registryByName.get(name);
    }

    public boolean isSwitchInFlight() {
        return switchInFlight.get();
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

    /**
     * WR-01: enforce the JSON-safety invariant that {@link LifecycleProcessor} relies on when
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

    /**
     * Register a consumer that receives the parsed {@code timeout_ms} from the
     * {@code POST /lifecycle/switch} body BEFORE {@link #submitSwitch(Role)}. The enterprise side
     * registers a consumer that forwards to
     * {@code EntDynamicPropServerConfiguration.setRoleSwitchTimeoutMs(long)}, which
     * {@code EntEngineInitEnvelope.switchRole} reads when invoking
     * {@code engine.switchRole(Role, long)}. Default: no-op for the OSS standalone path
     * (the engine's compiled-in default applies).
     * <p>
     * Passing {@code null} resets the consumer to a no-op.
     */
    public void registerRoleSwitchTimeoutConsumer(@Nullable java.util.function.LongConsumer consumer) {
        this.roleSwitchTimeoutConsumer = consumer != null ? consumer : ms -> {
        };
    }

    public void run() {
        // WR-05: validate FIRST, then single-shot CAS, then flip running. A validation failure
        // (unknown dep / cycle / duplicate name) is retryable -- the caller can fix the registry
        // via subsequent register() calls and re-invoke run(). Previously the ran CAS happened
        // BEFORE validateAndComputeOrder, so a transient validation failure locked the orchestrator
        // permanently with an unhelpful "may only be called once" error on retry.
        //
        // B3 fix preserved: register() guards based on `running`, which stays false until the CAS
        // succeeds AND validation has already passed. close() defensively handles a null
        // reverseTopoOrder for the "constructed but never run" case.
        validateAndComputeOrder();   // may throw LifecycleStartupException; ran + running still false
        if (!ran.compareAndSet(false, true)) {
            throw new IllegalStateException("LifecycleOrchestrator.run may only be called once");
        }
        running.set(true);
        startAllInTopologicalOrder();
        if (anyFailedReachable()) {
            close();
            throw new LifecycleStartupException("boot-essential component(s) failed");
        }
    }

    /**
     * Forward the parsed {@code timeout_ms} (from {@code POST /lifecycle/switch}) to the
     * consumer registered via {@link #registerRoleSwitchTimeoutConsumer}. Called by
     * {@code SwitchProcessor} BEFORE {@link #submitSwitch(Role)} on the request-handling
     * thread. If no consumer was registered, this is a no-op (OSS standalone path).
     */
    public void setRoleSwitchTimeoutMs(long timeoutMs) {
        roleSwitchTimeoutConsumer.accept(timeoutMs);
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
        return new LifecycleSnapshot(capturedAt, role.get(), switchInFlight.get(), snaps);
    }

    public State stateOf(String componentName) {
        AtomicReference<State> ref = states.get(componentName);
        return ref != null ? ref.get() : State.INIT;
    }

    /**
     * D6-04: serialize switch requests via CAS on switchInFlight; on success submit the switch task
     * to the lifecycle-N executor. Uses {@code executor.execute(Runnable)} (NOT {@code submit}) so an
     * uncaught throwable from {@link #switchRole(Role)} propagates to the thread's
     * UncaughtExceptionHandler (D6-06 exit-55 path). The wrapping try/finally clears the in-flight
     * flag in all cases; the exception then escapes the lambda back into the executor's task wrapper
     * which dispatches to the thread's UEH.
     */
    public void submitSwitch(Role newRole) {
        if (!switchInFlight.compareAndSet(false, true)) {
            throw new SwitchInFlightException(role.get(), newRole);
        }
        executor.execute(() -> {
            try {
                switchRole(newRole);
            } finally {
                switchInFlight.set(false);
            }
        });
    }

    public void switchRole(Role newRole) {
        if (topoOrder == null) {
            // No components -- still publish the new role so /lifecycle reflects the switch.
            role.set(newRole);
            return;
        }
        for (int i = 0, n = topoOrder.size(); i < n; i++) {
            Component c = topoOrder.getQuick(i);
            String name = c.name();
            publishInternal(name, State.SWITCHING, null);
            try {
                c.switchRole(contextFor(name), newRole);
                // B5 fix: mirror the auto-publish-after-start pattern. If the component's switchRole
                // already published a terminal state (READY / DEGRADED / FAILED), stateOf returns that
                // state and we leave it. If it's still SWITCHING, the orchestrator publishes READY on
                // its behalf. The transition table allows SWITCHING -> READY.
                if (stateOf(name) == State.SWITCHING) {
                    publishInternal(name, State.READY, null);
                }
            } catch (Throwable t) {
                // D6-06 fail-fast: publish FAILED on the offending component, cascade FAILED through
                // hard-dependents, then propagate LifecycleStartupException. The orchestrator stops
                // invoking subsequent components in topoOrder; the executor's thread UEH runs
                // close() + LogFactory.closeInstance() + System.exit(55) so K8s restarts the pod.
                publishInternal(name, State.FAILED, "switchRole failed: " + t);
                cascadeFailedThroughHardDeps(name);
                throw new LifecycleStartupException("switch failed at component " + name, t);
            }
        }
        // WR-03: publish the new role ONLY after the topo-order cascade completes successfully.
        // Previously role.set(newRole) ran BEFORE iteration, so /lifecycle reported
        // currentRole=NEW_ROLE with switchInFlight=true throughout the multi-second drain --
        // misleading coordinators that watch currentRole to decide "the pod's role has flipped".
        // On failure (Throwable caught above) we never reach this line, so currentRole stays at
        // the OLD role and the cascade's FAILED publishes reflect that no component has switched.
        role.set(newRole);
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

    private boolean anyFailedReachable() {
        for (int i = 0, n = registry.size(); i < n; i++) {
            if (stateOf(registry.getQuick(i).name()) == State.FAILED) {
                return true;
            }
        }
        return false;
    }

    private void cascadeFailedThroughHardDeps(String failedName) {
        // BFS through reverse hard-dep adjacency. Skip soft deps per D-07.
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

    private static boolean contains(ObjList<Component> list, Component c) {
        for (int i = 0, n = list.size(); i < n; i++) {
            if (list.getQuick(i) == c) {
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

    private void emitTransitionLog(
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

    /**
     * Test seam (D6-06 exit-55 path): production closes the log ring buffer via
     * {@link LogFactory#closeInstance()} and terminates the JVM with {@code System.exit(55)}.
     * Tests override to record the exit code and skip both the log close and the actual JVM
     * termination so the suite can run without losing logging for subsequent tests.
     * Protected so {@code LifecycleOrchestratorTest} (different package) can subclass and override it.
     */
    @TestOnly
    protected void closeLogAndExit(int code) {
        LogFactory.closeInstance();
        System.exit(code);
    }

    private void fireStableBelowCallbacks() {
        // WR-01: the parameter previously named `changedComponent` was unused -- every state-change
        // round re-evaluates every watcher. Dropping the parameter clarifies that the dispatch is
        // global rather than gated on a specific dep.
        for (StableWatch w : stableWatchers.values()) {
            if (allHardDependentsStable(w.componentName)) {
                Runnable cb = w.callback;
                w.callback = null; // single-shot
                if (cb != null) {
                    try {
                        cb.run();
                    } catch (Throwable t) {
                        injectedLog.error()
                                .$("stable-below callback failed for ").$(w.componentName)
                                .$(": ").$(t.getMessage())
                                .I$();
                    }
                }
            }
        }
    }

    private static boolean isValidTransition(State from, State to) {
        if (to == State.FAILED) {
            return from != State.FAILED && from != State.STOPPED;
        }
        return switch (from) {
            case INIT -> to == State.STARTING;
            // CR-03: STARTING -> STOPPING is permitted so close() can request cancellation of an
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

    private ThreadFactory lifecycleThreadFactory() {
        AtomicLong counter = new AtomicLong();
        return r -> {
            Thread t = new Thread(r, "lifecycle-" + counter.incrementAndGet());
            t.setDaemon(true);
            // D6-06 exit-55 path: an uncaught throwable from the switch task (LifecycleStartupException
            // or any other Throwable) lands here. Run reverse-topo stop + freeOnExit release via
            // close(), then close the log subsystem, then exit. The Phase 2 D-08 invariant requires
            // native handles to release before exit; close() runs the reverse-topo stop loop that
            // honors this. exitOnSwitchFailure() is a package-private test seam so unit tests can
            // observe the exit code without actually terminating the JVM.
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

    private void publishInternal(String name, State next, @Nullable CharSequence reason) {
        AtomicReference<State> ref = states.get(name);
        if (ref == null) {
            // WR-11: surface accidental misuse via contextFor() with an unregistered component name.
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
            if (prev == State.FAILED && next != State.FAILED) {
                // FAILED is terminal -- ignore late publishes per D-11.
                injectedLog.error()
                        .$("rejecting transition from FAILED component=").$(name)
                        .$(" attempted to=").$(next)
                        .I$();
                return;
            }
            if (!isValidTransition(prev, next)) {
                injectedLog.error()
                        .$("invalid transition component=").$(name)
                        .$(" from=").$(prev)
                        .$(" to=").$(next)
                        .I$();
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

    private void startAllInTopologicalOrder() {
        // Walk topo order serially. Phase 2's hard-required chain degenerates
        // to serial-but-via-executor execution; the executor is constructed and
        // available for Phase 4+ parallel exploitation.
        assert topoOrder != null;
        for (int i = 0, n = topoOrder.size(); i < n; i++) {
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
        // Kahn's algorithm for topological sort. V <= 6 in Phase 2.
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

    private static final class StableWatch {
        final String componentName;
        volatile Runnable callback;

        StableWatch(String componentName, Runnable callback) {
            this.componentName = componentName;
            this.callback = callback;
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
        public Role role() {
            return role.get();
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
