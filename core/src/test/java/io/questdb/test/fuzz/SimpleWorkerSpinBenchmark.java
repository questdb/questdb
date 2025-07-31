/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.fuzz;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleWorkerSpinBenchmark {
    private static final Log LOG = LogFactory.getLog(SimpleWorkerSpinBenchmark.class);

    // Constants for consistent work distribution

    @Test
    public void testWorkerCpuSpinning() throws Exception {
        int targetWorkPerSecond = 200000;
        int testDurationSeconds = 15;
        int totalTargetWork = targetWorkPerSecond * testDurationSeconds;
        LOG.info().$("=== Worker CPU Spinning Benchmark ===").$();
        LOG.info().$("Target work per test: ").$(totalTargetWork).$(" in ").$(testDurationSeconds).$(" seconds").$();

        // Test with different worker counts to demonstrate the issue

        double t00In15By2 = runBenchmark(2, "2 workers", 15, 20000);
        double t00In15By4 = runBenchmark(4, "4 workers", 15, 20000);

        double tIn15By2 = runBenchmark(2, "2 workers", 15, 200000);
        double tIn15By4 = runBenchmark(4, "4 workers", 15, 200000);

        double t0In15By2 = runBenchmark(2, "2 workers", 15, 200000 * 10);
        double t0In15By4 = runBenchmark(4, "4 workers", 15, 200000 * 10);

        LOG.info().$("=== Benchmark Results ===").$();
        LOG.info().$("2 workers, 20k work/sec: ").$(String.format("%.2f", t00In15By2)).$(" work/cpu-second").$();
        LOG.info().$("4 workers, 20k work/sec: ").$(String.format("%.2f", t00In15By4)).$(" work/cpu-second").$();
        LOG.info().$("2 workers, 200k work/sec: ").$(String.format("%.2f", tIn15By2)).$(" work/cpu-second").$();
        LOG.info().$("4 workers, 200k work/sec: ").$(String.format("%.2f", tIn15By4)).$(" work/cpu-second").$();
        LOG.info().$("2 workers, 2M work/sec: ").$(String.format("%.2f", t0In15By2)).$(" work/cpu-second").$();
        LOG.info().$("4 workers, 2M work/sec: ").$(String.format("%.2f", t0In15By4)).$(" work/cpu-second").$();
        LOG.info().$("=== Benchmark Complete ===").$();
    }

    private double runBenchmark(int workerCount, String testName, int testDurationSeconds, int totalTargetWork) throws Exception {
        LOG.info().$("Running benchmark with ").$(testName).$();

        // Create worker pool configuration
        WorkerPoolConfiguration config = new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "spin-benchmark";
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }
        };

        WorkerPool workerPool = new WorkerPool(config);

        // Create work coordinator to ensure consistent total work
        WorkCoordinator workCoordinator = new WorkCoordinator(totalTargetWork, totalTargetWork / testDurationSeconds);

        // Create multiple jobs to increase contention and spinning
        // Mix of spinning jobs and idle jobs to better reproduce CPU spinning
        int totalJobs = workerCount * 10; // 10x more jobs than workers
        SpinningJob[] spinningJobs = new SpinningJob[workerCount];
        IdleJob[] idleJobs = new IdleJob[totalJobs - workerCount];

        // Create spinning jobs (active work generators)
        for (int i = 0; i < workerCount; i++) {
            spinningJobs[i] = new SpinningJob(workCoordinator);
            workerPool.assign(spinningJobs[i]);
        }

        // Create idle jobs (mostly return false, causing workers to spin)
        for (int i = 0; i < idleJobs.length; i++) {
            idleJobs[i] = new IdleJob(i + workerCount, workCoordinator);
            workerPool.assign(idleJobs[i]);
        }

        // Start CPU monitoring
        CpuTracker cpuTracker = new CpuTracker();
        cpuTracker.start();

        // Start worker pool
        workerPool.start(LOG);

        try {
            LOG.info().$("Running ").$(testName).$(" for 15 seconds...").$();

            // Phase 1: High contention (5 seconds) - jobs return true frequently
            setIdleJobMode(idleJobs, IdleJobMode.OCCASIONAL_WORK);
            Thread.sleep(5000);

            // Get final stats
            long actualWork = workCoordinator.getWorkCompleted();
            CpuStats stats = cpuTracker.getStats();
            double actualWorkRate = workCoordinator.getWorkRate();

            // Log results
            LOG.info().$("Peak CPU usage: ").$(String.format("%.1f", stats.peakCpuUsage)).$("%").$();
            LOG.info().$("CPU efficiency: ").$(String.format("%.2f", stats.getEfficiency(actualWork))).$(" work/cpu-second").$();

            return stats.getEfficiency(actualWork);

        } finally {
            // Clean up
            for (SpinningJob job : spinningJobs) {
                job.stop();
            }
            for (IdleJob job : idleJobs) {
                job.stop();
            }
            cpuTracker.stop();
            workerPool.halt();
        }
    }

    private void setIdleJobMode(IdleJob[] jobs, IdleJobMode mode) {
        for (IdleJob job : jobs) {
            job.setMode(mode);
        }
    }

    private enum IdleJobMode {
        OCCASIONAL_WORK,  // Sometimes return true to add to contention
        MOSTLY_IDLE,     // Almost always return false, maximizing spinning
        RARE_WORK        // Very rarely return true
    }

    private static class CpuStats {
        final double peakCpuUsage;
        final long threadsTracked;
        final double totalCpuUsage;
        final long wallTimeMs;

        public CpuStats(double totalCpuUsage, double peakCpuUsage, long wallTimeMs, long threadsTracked) {
            this.totalCpuUsage = totalCpuUsage;
            this.peakCpuUsage = peakCpuUsage;
            this.wallTimeMs = wallTimeMs;
            this.threadsTracked = threadsTracked;
        }

        public double getEfficiency(long workCompleted) {
            if (wallTimeMs == 0) return 0;
            return (double) workCompleted * 1000.0 / totalCpuUsage; // work per CPU second
        }
    }

    // Simple CPU usage tracker
    private static class CpuTracker {
        private final AtomicLong peakCpuUsage = new AtomicLong();
        private final AtomicLong samples = new AtomicLong();
        private final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        private final AtomicLong totalCpuUsage = new AtomicLong(); // Store as percentage * 100
        private final AtomicLong totalThreadsTracked = new AtomicLong();
        private final AtomicBoolean tracking = new AtomicBoolean(false);
        private long startWallTime;
        private Thread trackerThread;

        public CpuStats getStats() {
            long sampleCount = samples.get();
            if (sampleCount == 0) {
                return new CpuStats(0, 0, 0, 0);
            }

            // Use latest CPU usage values
            double totalCpu = totalCpuUsage.get(); // Convert back from percentage * 100
            double peakCpu = peakCpuUsage.get() / 100.0; // Convert back from percentage * 100
            long threadsTracked = totalThreadsTracked.get();

            // Calculate total CPU time from current usage
            long wallTimeMs = (System.nanoTime() - startWallTime) / 1_000_000L;

            return new CpuStats(totalCpu, peakCpu, wallTimeMs, threadsTracked);
        }

        public void start() {
            if (!threadBean.isCurrentThreadCpuTimeSupported()) {
                LOG.info().$("CPU time monitoring not supported on this platform").$();
                return;
            }

            threadBean.setThreadCpuTimeEnabled(true);
            tracking.set(true);
            startWallTime = System.nanoTime();

            LOG.info().$("Starting CPU tracking - looking for worker threads...").$();

            trackerThread = new Thread(this::trackCpuUsage);
            trackerThread.setName("CPU-Tracker");
            trackerThread.setDaemon(true);
            trackerThread.start();
        }

        public void stop() {
            tracking.set(false);
            if (trackerThread != null) {
                try {
                    trackerThread.join(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        private void trackCpuUsage() {
            while (tracking.get()) {
                try {
                    Thread.sleep(200); // Sample every 200ms

                    long currentTime = System.nanoTime();
                    long wallTimeDelta = currentTime - startWallTime;

                    // Find all threads
                    Thread[] allThreads = new Thread[Thread.activeCount() + 50];
                    int threadCount = Thread.enumerate(allThreads);

                    long totalCpuTime = 0;
                    int trackedThreads = 0;

                    for (int i = 0; i < threadCount; i++) {
                        Thread thread = allThreads[i];
                        if (thread != null) {
                            String threadName = thread.getName().toLowerCase();
                            // Track worker threads and any spinning threads
                            if (threadName.contains("worker") ||
                                    threadName.contains("spin") ||
                                    threadName.contains("questdb") ||
                                    threadName.contains("benchmark")) {

                                long cpuTime = threadBean.getThreadCpuTime(thread.getId());
                                if (cpuTime > 0) {
                                    totalCpuTime += cpuTime;
                                    trackedThreads++;
                                }
                            }
                        }
                    }

                    if (trackedThreads > 0 && wallTimeDelta > 0) {
                        // Calculate CPU percentage using latest values: (total cpu time / wall time) * 100
                        double cpuPercent = (double) totalCpuTime / wallTimeDelta * 100.0;

                        long cpuPercentInt = (long) (cpuPercent * 100); // Store as percentage * 100
                        totalCpuUsage.set(totalCpuTime / 1_000_000); // Use latest value, not cumulative
                        totalThreadsTracked.set(trackedThreads);
                        samples.incrementAndGet();

                        // Update peak
                        long currentPeak = peakCpuUsage.get();
                        while (cpuPercentInt > currentPeak && !peakCpuUsage.compareAndSet(currentPeak, cpuPercentInt)) {
                            currentPeak = peakCpuUsage.get();
                        }
                    }

                } catch (Exception e) {
                    LOG.error().$("Error tracking CPU: ").$(e).$();
                }
            }
        }
    }

    // Idle job that mostly returns false to maximize CPU spinning
    private static class IdleJob implements Job {
        private final int jobId;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final WorkCoordinator workCoordinator;
        double fakeTotal = 0;
        private int callCounter = 0;
        private volatile IdleJobMode mode = IdleJobMode.MOSTLY_IDLE;

        public IdleJob(int jobId, WorkCoordinator workCoordinator) {
            this.jobId = jobId;
            this.workCoordinator = workCoordinator;
        }

        @Override
        public boolean run(int workerId, RunStatus runStatus) {
            if (!running.get()) {
                return false;
            }

            callCounter++;

            switch (mode) {
                case OCCASIONAL_WORK:
                    // Return true occasionally to add to job queue contention
                    if (callCounter % 50 == 0 && workCoordinator.canDoWork()) {
                        if (doMinimalWork()) {
                            return true;
                        }
                    }
                    return callCounter % 20 == 0 && workCoordinator.canDoWork(); // 5% true rate when work available

                case MOSTLY_IDLE:
                    // Almost always return false - maximum spinning
                    if (callCounter % 5000 == 0 && workCoordinator.canDoWork()) {
                        if (doMinimalWork()) {
                            return true;
                        }
                    }
                    return false; // 99.98% false rate - causes maximum spinning

                case RARE_WORK:
                    // Very rarely return true
                    if (callCounter % 1000 == 0 && workCoordinator.canDoWork()) {
                        if (doMinimalWork()) {
                            return true;
                        }
                    }
                    return callCounter % 500 == 0 && workCoordinator.canDoWork(); // 0.2% true rate when work available

                default:
                    return false;
            }
        }

        public void setMode(IdleJobMode mode) {
            this.mode = mode;
        }

        public void stop() {
            running.set(false);
        }

        private boolean doMinimalWork() {
            // Only do work if coordinator allows it
            fakeTotal += Math.sqrt(jobId + callCounter);
            return true;
        }
    }

    // Job that simulates different workload patterns to trigger CPU spinning
    private static class SpinningJob implements Job {
        private static double sum = 0; // Static to avoid per-instance overhead
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final WorkCoordinator workCoordinator;

        public SpinningJob(WorkCoordinator workCoordinator) {
            this.workCoordinator = workCoordinator;
        }

        @Override
        public boolean run(int workerId, RunStatus runStatus) {
            if (!running.get()) {
                return false;
            }

            if (workCoordinator.canDoWork()) {
                doSomeWork();
                return true; // Tell worker more work is available
            }
            return false;
        }

        public void stop() {
            running.set(false);
        }

        private static double doSomeWork(double sum) {
            for (int i = 0; i < 1000; i++) {
                sum += Math.sqrt(i);
            }
            return sum;
        }

        private void doSomeWork() {
            // Add tiny CPU work to simulate real processing
            sum = doSomeWork(sum);
            workCoordinator.incrementWorkDone();
        }
    }

    // Coordinates work distribution to ensure consistent total work across tests
    private static class WorkCoordinator {
        private final long startTime;
        private final long totalWork;
        private final long targetWorkPerSecond;
        private final AtomicLong workCompleted = new AtomicLong();
        private volatile long lastElapsed = 0;

        public WorkCoordinator(long totalWork, long targetWorkPerSecond) {
            this.totalWork = totalWork;
            this.targetWorkPerSecond = targetWorkPerSecond;
            this.startTime = System.currentTimeMillis();
        }

        public boolean canDoWork() {
            return workCompleted.get() < totalWork && !shouldThrottleWork();
        }

        public long getWorkCompleted() {
            return workCompleted.get();
        }

        public double getWorkRate() {
            return getWorkRate(lastElapsed);
        }

        public double getWorkRate(long elapsed) {
            if (elapsed == 0) return 0;
            return (double) workCompleted.get() * 1000.0 / elapsed; // work per second
        }

        public void incrementWorkDone() {
            workCompleted.incrementAndGet();
        }

        public boolean shouldThrottleWork() {
            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed < 100) {
                lastElapsed = elapsed;
                return false; // Don't throttle in first 100ms
            }

            // Throttle if we're going too fast
            boolean shouldThrottle = workCompleted.get() > (elapsed * 1000) * targetWorkPerSecond * 1.1;
            if (!shouldThrottle) {
                lastElapsed = elapsed;
            }
            return shouldThrottle;
        }
    }
}