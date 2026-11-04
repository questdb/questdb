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


package io.questdb.network;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.metrics.ConnectionSteeringMetrics;
import io.questdb.std.CpuTopology;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Steers each new connection on one port to the listener of the worker group on the socket whose
 * CPU processes the connection's SYN, through a classic BPF program attached to the port's
 * {@code SO_REUSEPORT} group ({@link SteeringProgram}).
 * <p>
 * The dispatchers of the port's groups open and close their listeners through this class, which
 * tracks the kernel's listener order under one lock per port: a listen appends a listener, and a
 * close moves the last listener into the freed slot. Every change rebuilds the program from the
 * port's full state and attaches it, and the kernel swaps programs atomically. A group whose
 * listener has stayed paused at its connection limit for the redirect delay, or has closed, sends
 * its CPUs' connections to the groups that accept.
 * <p>
 * A periodic check, on a thread of its own, predicts each group's share of new connections from
 * the receive layout ({@link ReceiveLayout}) and steers only while no group's predicted share
 * exceeds 1.2 times the fair share. A runtime guard falls back to the kernel's hash when the
 * busiest group holds more than 1.2·n/G + 2·√n of the n open connections accepted since steering
 * turned on, and the fallback lasts until the predicted shares change.
 */
public final class ConnectionSteering implements QuietCloseable {
    public static final long CHECK_INTERVAL_MS = 10_000;
    // the value listen() returns once the port's servers have started closing
    public static final int LISTEN_CLOSING = -1;
    public static final int REASON_ATTACH_FAILING = 1;
    public static final int REASON_COUNT = 11;
    public static final int REASON_GUARD_TRIPPED = 2;
    public static final int REASON_LAYOUT_CHECK_FAILED = 3;
    public static final int REASON_LAYOUT_UNREADABLE = 4;
    public static final int REASON_NO_CPU_SETS = 6;
    public static final int REASON_NONE = 0;
    public static final int REASON_NOT_CHECKED = 5;
    public static final int REASON_PROGRAM_TOO_LARGE = 7;
    public static final int REASON_QUEUE_TIMEOUT_OFF = 8;
    public static final int REASON_SHUT_DOWN = 9;
    public static final int REASON_UNSUPPORTED = 10;
    private static final long DEFAULT_OPTMEM_MAX = 20_480;
    private static final double GUARD_MARGIN_SIGMAS = 2;
    private static final double LIMIT_TIMES_FAIR_SHARE = 1.2;
    private static final Log LOG = LogFactory.getLog(ConnectionSteering.class);
    private static final long MAX_REDIRECT_DELAY_MS = 1_000;
    private static final long NOT_PAUSED = Long.MIN_VALUE;
    private static final String[] REASON_NAMES = {
            "attached",
            "attach_failing",
            "guard_tripped",
            "layout_check_failed",
            "layout_unreadable",
            "not_checked",
            "no_cpu_sets",
            "program_too_large",
            "queue_timeout_off",
            "shut_down",
            "unsupported"
    };
    private final long[] acceptingSinceMs;
    private final int bindIPv4;
    private final Object checkMonitor = new Object();
    private final int groupCount;
    private final int[][] groupCpus;
    // per group: the steering generation in the high 32 bits, and the open connections accepted
    // during it in the low 32 bits
    private final AtomicLongArray guardCounts;
    private final boolean isDetachSupported;
    private final ReceiveLayout layout;
    private final long[] listenerFds;
    private final ReentrantLock lock = new ReentrantLock();
    private final @Nullable ConnectionSteeringMetrics metrics;
    private final String name;
    private final NetworkFacade nf;
    private final AtomicIntegerArray openCounts;
    // the group of each listener index, in the kernel's order
    private final IntList order = new IntList();
    private final long[] pausedSinceMs;
    private final LongList program = new LongList();
    private final long redirectDelayMs;
    private final boolean[] redirected;
    private final int[] redirects;
    private final CpuTopology topology;
    private volatile long attachCount;
    private int[] cpuTable = new int[0];
    private volatile int generation;
    private boolean isAttachFailing;
    private volatile boolean isClosing;
    private boolean isGuardTripped;
    private boolean isProgramTooLarge;
    private @Nullable CpuTopology.OnlineCpus online;
    private long optmemMax = DEFAULT_OPTMEM_MAX;
    // a reason steering can't run at all, found at construction or at the first attach
    private volatile int permanentReason;
    private volatile int policyReason;
    private @Nullable String predictionSignature;
    private int redirectCount;
    private int[] targets = new int[0];
    private volatile @Nullable Thread thread;
    private @Nullable String tripSignature;

    /**
     * @param name           the port's name in logs and metrics, such as {@code http}
     * @param groupCpus      each group's CPU set, or null when the pool has none
     * @param kernelRelease  the running kernel's release, such as {@code 6.8.0-1012-aws}, or null
     *                       where the OS isn't Linux
     */
    public ConnectionSteering(
            String name,
            int groupCount,
            int @Nullable [][] groupCpus,
            NetworkFacade nf,
            CpuTopology topology,
            ReceiveLayout layout,
            int bindIPv4,
            long queueTimeoutMs,
            @Nullable String kernelRelease,
            @Nullable ConnectionSteeringMetrics metrics
    ) {
        this.name = name;
        this.groupCount = groupCount;
        this.groupCpus = groupCpus != null ? groupCpus : new int[0][];
        this.nf = nf;
        this.topology = topology;
        this.layout = layout;
        this.bindIPv4 = bindIPv4;
        this.metrics = metrics;
        this.acceptingSinceMs = new long[groupCount];
        this.pausedSinceMs = new long[groupCount];
        Arrays.fill(pausedSinceMs, NOT_PAUSED);
        this.listenerFds = new long[groupCount];
        Arrays.fill(listenerFds, -1);
        this.redirected = new boolean[groupCount];
        this.redirects = new int[groupCount];
        this.guardCounts = new AtomicLongArray(groupCount);
        this.openCounts = new AtomicIntegerArray(groupCount);
        this.redirectDelayMs = Math.min(MAX_REDIRECT_DELAY_MS, queueTimeoutMs / 2);
        this.isDetachSupported = isKernelAtLeast(kernelRelease, 5, 3);

        int reason = REASON_NONE;
        String detail = null;
        if (kernelRelease == null) {
            reason = REASON_UNSUPPORTED;
            detail = "it needs Linux";
        } else if (!isKernelAtLeast(kernelRelease, 4, 6)) {
            reason = REASON_UNSUPPORTED;
            detail = "it needs Linux 4.6 or later, and the kernel is " + kernelRelease;
        } else if (!hasCpuSets(groupCpus, groupCount)) {
            reason = REASON_NO_CPU_SETS;
            detail = "the worker groups have no CPU sets";
        } else if (queueTimeoutMs <= 0) {
            reason = REASON_QUEUE_TIMEOUT_OFF;
            detail = "the connection queue timeout is off, so every pause at the connection limit closes a listener";
        }
        this.permanentReason = reason;
        this.policyReason = reason != REASON_NONE ? reason : REASON_NOT_CHECKED;
        if (detail != null) {
            LOG.advisory().$("connection steering is off [who=").$(name).$(", reason=").$(detail).I$();
        }
        if (metrics != null) {
            metrics.register(this);
        }
    }

    /**
     * Creates the steering of a server's port, or returns null when the server has a single
     * listener or the setting is off. Steering that can't run, for example on an old kernel, logs
     * why and stays off.
     */
    public static @Nullable ConnectionSteering create(
            IODispatcherConfiguration configuration,
            int groupCount,
            int @Nullable [][] groupCpus
    ) {
        if (!configuration.isConnectionSteeringEnabled()) {
            return null;
        }
        if (groupCount < 2) {
            LOG.advisory().$("connection steering does nothing, the server has a single listener [who=")
                    .$(configuration.getDispatcherLogName()).I$();
            return null;
        }
        final CpuTopology topology = configuration.getConnectionSteeringTopology();
        return new ConnectionSteering(
                configuration.getDispatcherLogName(),
                groupCount,
                groupCpus,
                configuration.getNetworkFacade(),
                topology,
                new ReceiveLayout(topology.getProcRoot(), topology.getSysRoot()),
                configuration.getBindIPv4Address(),
                configuration.getQueueTimeout(),
                Os.isLinux() ? System.getProperty("os.version") : null,
                configuration.getConnectionSteeringMetrics()
        );
    }

    /**
     * True when the busiest group, holding {@code busiest} of the {@code total} connections the
     * guard counts, holds more than 1.2·n/G + 2·√n of them.
     */
    public static boolean exceedsGuard(long busiest, long total, int groupCount) {
        return busiest > getShareLimit(groupCount) * total + GUARD_MARGIN_SIGMAS * Math.sqrt(total);
    }

    /**
     * The limit of a group's share: 1.2 times the fair share.
     */
    public static double getShareLimit(int groupCount) {
        return LIMIT_TIMES_FAIR_SHARE / groupCount;
    }

    /**
     * True when the kernel release, such as {@code 6.8.0-1012-aws}, is at least major.minor.
     */
    public static boolean isKernelAtLeast(@Nullable String release, int major, int minor) {
        if (release == null) {
            return false;
        }
        final String[] parts = release.split("[.-]", 3);
        try {
            final int releaseMajor = Integer.parseInt(parts[0]);
            final int releaseMinor = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
            return releaseMajor > major || releaseMajor == major && releaseMinor >= minor;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static String reasonName(int reason) {
        return REASON_NAMES[reason];
    }

    /**
     * Runs a check of the receive layout and the guard. The periodic thread runs it every
     * {@link #CHECK_INTERVAL_MS}; {@link #start()} runs the first one.
     */
    public void check() {
        if (isClosing || permanentReason != REASON_NONE) {
            return;
        }
        CpuTopology.OnlineCpus readOnline = null;
        String onlineProblem = null;
        try {
            readOnline = topology.readOnlineCpus();
        } catch (IOException | RuntimeException e) {
            onlineProblem = "could not read the online CPUs: " + e.getMessage();
        }
        final ReceiveLayout.Snapshot snapshot = layout.read(bindIPv4);
        final long readOptmemMax = readOptmemMax();

        lock.lock();
        try {
            if (isClosing || permanentReason != REASON_NONE) {
                return;
            }
            optmemMax = readOptmemMax;
            boolean isRebuildNeeded = isAttachFailing;
            if (readOnline != null && (online == null || !readOnline.isSameAs(online))) {
                online = readOnline;
                cpuTable = SteeringCpuTable.build(groupCpus, readOnline);
                isRebuildNeeded = true;
            }
            final ReceiveLayout.Prediction prediction = onlineProblem != null
                    ? null
                    : ReceiveLayout.predict(snapshot, cpuTable, groupCount);
            final String signature = prediction != null ? prediction.getSignature() : "unreadable: " + onlineProblem;
            final boolean isLayoutChanged = !signature.equals(predictionSignature);
            predictionSignature = signature;

            if (getReason() == REASON_NONE) {
                checkGuard(signature);
            }
            if (isGuardTripped && !signature.equals(tripSignature)) {
                isGuardTripped = false;
                tripSignature = null;
            }

            final int reason;
            final String detail;
            if (prediction == null || prediction.getUnreadable() != null) {
                reason = REASON_LAYOUT_UNREADABLE;
                detail = prediction == null ? onlineProblem : prediction.getUnreadable();
            } else if (prediction.getMaxShare() > getShareLimit(groupCount) + 1e-9) {
                reason = REASON_LAYOUT_CHECK_FAILED;
                detail = "a group's predicted share of new connections exceeds " + formatPercent(getShareLimit(groupCount))
                        + " [shares=" + signature + ']';
            } else if (isGuardTripped) {
                reason = REASON_GUARD_TRIPPED;
                detail = null;
            } else {
                reason = REASON_NONE;
                detail = "predicted shares " + signature;
            }
            if (setPolicyReason(reason, detail) || isRebuildNeeded) {
                rebuild();
            } else if (isLayoutChanged && reason == REASON_NONE) {
                LOG.info().$("receive layout changed, connection steering stays on [who=").$(name)
                        .$(", shares=").$(signature).I$();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Stops the periodic check and stops listens and rebuilds; the first dispatcher of the port to
     * close calls it. Calling it again does nothing.
     */
    @Override
    public void close() {
        lock.lock();
        try {
            if (isClosing) {
                return;
            }
            isClosing = true;
        } finally {
            lock.unlock();
        }
        synchronized (checkMonitor) {
            checkMonitor.notifyAll();
        }
        final Thread t = thread;
        if (t != null && t != Thread.currentThread()) {
            try {
                t.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (metrics != null) {
            metrics.unregister(this);
        }
    }

    /**
     * Closes a group's listener under the port lock, updates the tracked order and rebuilds the
     * program, unless the servers are closing.
     */
    public void closeListener(int group, long fd) {
        lock.lock();
        try {
            nf.close(fd, LOG);
            if (listenerFds[group] == fd) {
                listenerFds[group] = -1;
                final int index = order.indexOf(group, 0, order.size());
                final int last = order.size() - 1;
                order.setQuick(index, order.getQuick(last));
                order.removeIndex(last);
                if (!isClosing) {
                    rebuild();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public long getAttachCount() {
        return attachCount;
    }

    public int getGroupCount() {
        return groupCount;
    }

    /**
     * The open connections of group {@code group} that the guard counts: those accepted since
     * steering last turned on.
     */
    public int getGuardCount(int group) {
        return (int) guardCounts.get(group);
    }

    public String getName() {
        return name;
    }

    @TestOnly
    public IntList getOrder() {
        return order;
    }

    public int getOpenCount(int group) {
        return openCounts.get(group);
    }

    /**
     * {@link #REASON_NONE} while the program steers, or why it doesn't.
     */
    public int getReason() {
        final int reason = policyReason;
        if (reason != REASON_NONE) {
            return reason;
        }
        if (isClosing) {
            return REASON_SHUT_DOWN;
        }
        if (isProgramTooLarge) {
            return REASON_PROGRAM_TOO_LARGE;
        }
        return isAttachFailing ? REASON_ATTACH_FAILING : REASON_NONE;
    }

    @TestOnly
    public boolean isRedirected(int group) {
        return redirected[group];
    }

    /**
     * Listens on a group's bound socket under the port lock, appends the listener to the tracked
     * order and rebuilds the program.
     *
     * @return 0, the errno of the failed {@code listen()}, or {@link #LISTEN_CLOSING} once the
     * port's servers have started closing
     */
    public int listen(int group, long fd, int backlog) {
        lock.lock();
        try {
            if (isClosing) {
                return LISTEN_CLOSING;
            }
            final int rc = nf.listen(fd, backlog);
            if (rc == 0) {
                listenerFds[group] = fd;
                order.add(group);
                rebuild();
            }
            return rc;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Counts a connection that group {@code group} accepted, and returns the stamp the connection
     * carries until {@link #onDisconnected(int, int)}.
     */
    public int onAccepted(int group) {
        openCounts.incrementAndGet(group);
        final int gen = generation;
        final long base = (long) gen << 32;
        while (true) {
            final long value = guardCounts.get(group);
            if ((value & 0xFFFF_FFFF_0000_0000L) != base) {
                return -1;
            }
            if (guardCounts.compareAndSet(group, value, value + 1)) {
                return gen;
            }
        }
    }

    public void onDisconnected(int group, int stamp) {
        openCounts.decrementAndGet(group);
        if (stamp < 0) {
            return;
        }
        final long base = (long) stamp << 32;
        while (true) {
            final long value = guardCounts.get(group);
            if ((value & 0xFFFF_FFFF_0000_0000L) != base || (value & 0xFFFF_FFFFL) == 0) {
                return;
            }
            if (guardCounts.compareAndSet(group, value, value - 1)) {
                return;
            }
        }
    }

    /**
     * Group {@code group}'s listener stopped accepting at the connection limit; its dispatcher calls
     * this.
     */
    public void onPause(int group, long nowMs) {
        pausedSinceMs[group] = nowMs;
    }

    /**
     * Group {@code group}'s listener accepts again; its dispatcher calls this.
     */
    public void onResume(int group, long nowMs) {
        pausedSinceMs[group] = NOT_PAUSED;
        acceptingSinceMs[group] = nowMs;
    }

    /**
     * Runs the first check and starts the periodic one, once every group's listener listens.
     */
    public void start() {
        if (permanentReason != REASON_NONE) {
            return;
        }
        check();
        final Thread t = new Thread(this::runChecks, "questdb-steering-" + name);
        t.setDaemon(true);
        thread = t;
        t.start();
    }

    /**
     * Starts or ends group {@code group}'s redirect once it has stayed paused, or accepting, for the
     * redirect delay. The group's dispatcher calls it on every iteration; it takes the port lock only
     * to change the redirect.
     */
    public void tick(int group, long nowMs) {
        final long pausedSince = pausedSinceMs[group];
        if (!redirected[group]) {
            if (pausedSince != NOT_PAUSED && nowMs - pausedSince >= redirectDelayMs) {
                setRedirected(group, true);
            }
        } else if (pausedSince == NOT_PAUSED && nowMs - acceptingSinceMs[group] >= redirectDelayMs) {
            setRedirected(group, false);
        }
    }

    private static String formatPercent(double share) {
        final long permille = Math.round(share * 1000);
        return permille / 10 + "." + permille % 10 + '%';
    }

    private static boolean hasCpuSets(int @Nullable [][] groupCpus, int groupCount) {
        if (groupCpus == null || groupCpus.length != groupCount) {
            return false;
        }
        for (int[] cpus : groupCpus) {
            if (cpus == null || cpus.length == 0) {
                return false;
            }
        }
        return true;
    }

    // attaches the program to the port's group through any of its listeners, or returns the errno
    private int attach(LongList prog) {
        final int rc = nf.attachReuseportProgram(listenerFds[order.getQuick(0)], prog);
        if (rc == 0) {
            attachCount++;
        }
        return rc;
    }

    private void buildOutOfRange() {
        program.clear();
        program.add(SteeringProgram.instruction(SteeringProgram.CODE_RET_K, 0, 0, SteeringProgram.OUT_OF_RANGE_INDEX));
    }

    // trips the guard when the busiest group holds clearly more than its share of the connections
    // accepted since steering turned on
    private void checkGuard(String signature) {
        long total = 0;
        long busiest = 0;
        int busiestGroup = 0;
        for (int g = 0; g < groupCount; g++) {
            final long count = getGuardCount(g);
            total += count;
            if (count > busiest) {
                busiest = count;
                busiestGroup = g;
            }
        }
        if (exceedsGuard(busiest, total, groupCount)) {
            isGuardTripped = true;
            tripSignature = signature;
            LOG.advisory().$("connection steering guard tripped, falling back to the kernel's hash until the receive layout changes [who=")
                    .$(name)
                    .$(", group=").$(busiestGroup)
                    .$(", groupConnections=").$(busiest)
                    .$(", connections=").$(total)
                    .I$();
        }
    }

    private void computeTargets(boolean isSteering) {
        if (targets.length != cpuTable.length) {
            targets = new int[cpuTable.length];
        }
        if (!isSteering) {
            Arrays.fill(targets, SteeringProgram.TARGET_OUT_OF_RANGE);
            return;
        }
        redirectCount = 0;
        for (int i = 0, n = order.size(); i < n; i++) {
            if (!redirected[order.getQuick(i)]) {
                redirects[redirectCount++] = i;
            }
        }
        final int[] groupTargets = new int[groupCount];
        for (int g = 0; g < groupCount; g++) {
            final int index = order.indexOf(g, 0, order.size());
            if (index >= 0 && !redirected[g]) {
                groupTargets[g] = index;
            } else if (redirectCount == 0) {
                groupTargets[g] = index >= 0 ? index : SteeringProgram.TARGET_OUT_OF_RANGE;
            } else if (redirectCount == 1) {
                groupTargets[g] = redirects[0];
            } else {
                groupTargets[g] = SteeringProgram.TARGET_REDIRECT;
            }
        }
        for (int cpu = 0; cpu < cpuTable.length; cpu++) {
            final int group = cpuTable[cpu];
            targets[cpu] = switch (group) {
                case SteeringCpuTable.OFFLINE -> SteeringProgram.TARGET_OFFLINE;
                case SteeringCpuTable.NO_GROUP -> SteeringProgram.TARGET_OUT_OF_RANGE;
                default -> groupTargets[group];
            };
        }
    }

    private long readOptmemMax() {
        try {
            return Long.parseLong(Files.readString(topology.getProcRoot().resolve("sys/net/core/optmem_max")).trim());
        } catch (IOException | RuntimeException e) {
            return DEFAULT_OPTMEM_MAX;
        }
    }

    // rebuilds the program from the port's full state and attaches it; the caller holds the lock
    private void rebuild() {
        if (isClosing || permanentReason != REASON_NONE || order.size() == 0) {
            return;
        }
        final boolean isSteering = policyReason == REASON_NONE;
        computeTargets(isSteering);
        SteeringProgram.build(targets, groupCount, redirects, redirectCount, program);
        final boolean isTooLarge = isSteering && SteeringProgram.estimateKernelBytes(program.size()) > optmemMax;
        if (isTooLarge != isProgramTooLarge) {
            isProgramTooLarge = isTooLarge;
            if (isTooLarge) {
                LOG.advisory().$("connection steering is off, its program is too large for net.core.optmem_max [who=").$(name)
                        .$(", instructions=").$(program.size())
                        .$(", optmemMax=").$(optmemMax)
                        .I$();
            }
        }
        final boolean isOutOfRange = !isSteering || isTooLarge;
        if (isTooLarge) {
            buildOutOfRange();
        }
        final int rc = attach(program);
        if (rc == 0) {
            if (isAttachFailing) {
                isAttachFailing = false;
                LOG.info().$("connection steering program attached after earlier failures [who=").$(name).I$();
            }
            return;
        }
        if (attachCount == 0 && rc == SocketCalls.ENOPROTOOPT) {
            // the kernel doesn't know the option; no program is attached, so the hash applies
            permanentReason = REASON_UNSUPPORTED;
            policyReason = REASON_UNSUPPORTED;
            LOG.advisory().$("connection steering is off, the kernel refused SO_ATTACH_REUSEPORT_CBPF [who=").$(name)
                    .$(", errno=").$(rc)
                    .I$();
            return;
        }
        isAttachFailing = true;
        LOG.error().$("could not attach the connection steering program, retrying on the next change or check [who=").$(name)
                .$(", errno=").$(rc)
                .$(", instructions=").$(program.size())
                .I$();
        if (!isOutOfRange) {
            buildOutOfRange();
            if (attach(program) == 0) {
                return;
            }
        }
        if (isDetachSupported) {
            final int detachRc = nf.detachReuseportProgram(listenerFds[order.getQuick(0)]);
            if (detachRc != 0) {
                LOG.error().$("could not detach the connection steering program [who=").$(name)
                        .$(", errno=").$(detachRc)
                        .I$();
            }
        } else {
            LOG.error().$("the previous connection steering program stays attached, the kernel can't detach it before Linux 5.3 [who=")
                    .$(name).I$();
        }
    }

    private void runChecks() {
        while (true) {
            synchronized (checkMonitor) {
                if (isClosing) {
                    return;
                }
                try {
                    checkMonitor.wait(CHECK_INTERVAL_MS);
                } catch (InterruptedException e) {
                    return;
                }
            }
            if (isClosing) {
                return;
            }
            try {
                check();
            } catch (Throwable th) {
                LOG.error().$("connection steering check failed [who=").$(name).$(", e=").$(th).I$();
            }
        }
    }

    // returns true when the reason changed; the caller holds the lock
    private boolean setPolicyReason(int reason, @Nullable String detail) {
        final int previous = policyReason;
        if (previous == reason) {
            return false;
        }
        policyReason = reason;
        if (reason == REASON_NONE) {
            // a new generation: the guard counts only connections accepted from now on
            final int gen = generation + 1;
            generation = gen;
            for (int g = 0; g < groupCount; g++) {
                guardCounts.set(g, (long) gen << 32);
            }
            LOG.info().$("connection steering is on [who=").$(name).$(", ").$(detail).I$();
        } else if (reason != REASON_GUARD_TRIPPED) {
            LOG.advisory().$("connection steering is off [who=").$(name)
                    .$(", reason=").$(reasonName(reason))
                    .$(", detail=").$(detail)
                    .I$();
        }
        return true;
    }

    private void setRedirected(int group, boolean isRedirected) {
        lock.lock();
        try {
            if (isClosing) {
                return;
            }
            redirected[group] = isRedirected;
            if (policyReason == REASON_NONE) {
                rebuild();
            }
        } finally {
            lock.unlock();
        }
    }
}
