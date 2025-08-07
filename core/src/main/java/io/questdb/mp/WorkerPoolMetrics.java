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

package io.questdb.mp;

import io.questdb.std.Os;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * High-performance metrics collection for worker pools that avoids false sharing.
 * Each worker gets a dedicated cache-line padded slot to record utilization and parking state.
 * <p>
 * The structure provides:
 * - Utilization percentage recording per worker (double precision)
 * - Parking flags indicating which workers should park on the monitor
 * - Shared monitor object for worker coordination
 * <p>
 * False sharing is prevented by ensuring each worker's data occupies a full cache line (64 bytes).
 */
public final class WorkerPoolMetrics {

    // Cache line size for most modern CPUs - used for padding to avoid false sharing
    private static final int CACHE_LINE_SIZE = 64;
    // VarHandle for volatile access to array elements
    private static final VarHandle PARKING_FLAGS_HANDLE = MethodHandles.arrayElementVarHandle(byte[].class);
    
    private final Object[] parkingMonitors;
    private final int workerCount;
    private final WorkerStats[] workerStats;
    // Byte array for parking flags - each worker gets CACHE_LINE_SIZE bytes to prevent false sharing
    // Array elements are accessed/modified atomically to ensure thread safety
    private final byte[] parkingFlags;

    public WorkerPoolMetrics(int workerCount) {
        this.workerCount = workerCount;

        // Create individual monitor objects for each worker
        this.parkingMonitors = new Object[workerCount];
        for (int i = 0; i < workerCount; i++) {
            parkingMonitors[i] = new Object();
        }

        // Initialize worker stats
        workerStats = new WorkerStats[workerCount];
        for (int i = 0; i < workerCount; i++) {
            workerStats[i] = new WorkerStats();
        }

        // Allocate parking flags array with cache line padding to prevent false sharing
        // Each worker gets CACHE_LINE_SIZE bytes, but only the first byte is used for the flag
        this.parkingFlags = new byte[workerCount * CACHE_LINE_SIZE];
    }

    /**
     * Gets the count of active (non-parked) workers.
     *
     * @return number of workers that are not parked
     */
    public int getActiveWorkerCount() {
        return workerCount - getParkedWorkerCount();
    }

    public int getBlockedWorkerCount(long nowMicros, long blockedThresholdMicros) {
        int count = 0;
        for (int i = 0; i < workerCount; i++) {
            if (!isParked(i)) {
                if (workerStats[i].isBlocked(nowMicros, blockedThresholdMicros)) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Calculates the overall pool utilization as the average of all workers.
     *
     * @return average utilization percentage across all workers
     */
    public double getOverallUtilization() {
        if (workerCount == 0) {
            return 0.0;
        }

        int activeWorkerCount = 0;
        double sum = 0.0;
        for (int i = 0; i < workerCount; i++) {
            if (isParked(i)) {
                continue; // Skip parked workers
            }
            sum += workerStats[i].getUtilizationPercentage();
            activeWorkerCount++;
        }
        return sum / activeWorkerCount;
    }

    /**
     * Gets the count of currently parked workers.
     *
     * @return number of workers that have their parking flag set
     */
    public int getParkedWorkerCount() {
        int count = 0;
        for (int i = 0; i < workerCount; i++) {
            if (isParked(i)) {
                count++;
            }
        }
        return count;
    }

    /**
     * Returns the individual monitor object for a specific worker to park on.
     * Each worker has its own monitor to enable selective parking/unparking.
     *
     * @param workerId the worker ID (0-based index)
     * @return monitor object for this specific worker
     */
    public Object getParkingMonitor(int workerId) {
        assert workerId >= 0 && workerId < workerCount;
        return parkingMonitors[workerId];
    }

    /**
     * Gets the total number of workers this metrics structure supports.
     *
     * @return worker count
     */
    public int getWorkerCount() {
        return workerCount;
    }

    /**
     * Gets the parking flag for a specific worker.
     *
     * @param workerId the worker ID (0-based index)
     * @return true if worker should park, false otherwise
     */
    public boolean isParked(int workerId) {
        assert workerId >= 0 && workerId < workerCount;
        // Access the first byte of this worker's cache line slot with volatile semantics
        int flagIndex = workerId * CACHE_LINE_SIZE;
        return ((byte) PARKING_FLAGS_HANDLE.getVolatile(parkingFlags, flagIndex)) != 0;
    }

    /**
     * Parks a specific worker by setting its parking flag and notifying any management thread.
     * The worker should check its parking flag and wait on its individual monitor.
     *
     * @param workerId the worker ID to park
     */
    public void parkWorker(int workerId) {
        setParkingFlag(workerId, true);
        // Note: Worker will check flag and park itself on its monitor
    }

    public void recordIteration(int workerId, boolean useful) {
        workerStats[workerId].recordIteration(useful);
    }

    /**
     * Sets the parking flag for a specific worker, indicating it should park on the monitor.
     *
     * @param workerId   the worker ID (0-based index)
     * @param shouldPark true if worker should park, false otherwise
     */
    public void setParkingFlag(int workerId, boolean shouldPark) {
        assert workerId >= 0 && workerId < workerCount;
        // Set the first byte of this worker's cache line slot with volatile semantics
        int flagIndex = workerId * CACHE_LINE_SIZE;
        PARKING_FLAGS_HANDLE.setVolatile(parkingFlags, flagIndex, shouldPark ? (byte) 1 : (byte) 0);
    }

    public void start() {
        // No-op since parking flags array is already initialized in constructor
        // All array elements are initialized to 0 by default (false for parking flags)
    }

    /**
     * Unparks a specific worker by clearing its parking flag and notifying the worker.
     *
     * @param workerId the worker ID to unpark
     */
    public void unparkWorker(int workerId) {
        setParkingFlag(workerId, false);
        synchronized (parkingMonitors[workerId]) {
            parkingMonitors[workerId].notifyAll();
        }
    }


    private static class WorkerStats {
        private static final int SLIDING_WINDOW_SIZE = 1000; // Number of worker iterations (not time-based)
        private final boolean[] slidingWindow = new boolean[SLIDING_WINDOW_SIZE]; // Circular buffer tracking last N iterations
        private long lastUpdatedMiroTs;
        private long totalIterations;
        private long usefulIterations;
        private boolean windowFull = false;
        private int windowIndex = 0;

        private double getUtilizationPercentage() {
            long total = totalIterations;
            if (total == 0) {
                return 0.0;
            }

            if (!windowFull && windowIndex > 0) {
                int useful = 0;
                for (int i = 0; i < windowIndex; i++) {
                    if (slidingWindow[i]) useful++;
                }
                return (double) useful / windowIndex * 100.0;
            } else if (windowFull) {
                int useful = 0;
                for (boolean wasUseful : slidingWindow) {
                    if (wasUseful) useful++;
                }
                return (double) useful / SLIDING_WINDOW_SIZE * 100.0;
            }

            return (double) usefulIterations / total * 100.0;
        }

        private boolean isBlocked(long nowMicro, long blockedThreshold) {
            return nowMicro - lastUpdatedMiroTs > blockedThreshold;
        }

        private void recordIteration(boolean wasUseful) {
            totalIterations++;
            if (wasUseful) {
                usefulIterations++;
            }

            slidingWindow[windowIndex] = wasUseful;
            windowIndex = (windowIndex + 1) % SLIDING_WINDOW_SIZE;
            if (!windowFull && windowIndex == 0) {
                windowFull = true;
            }
            lastUpdatedMiroTs = Os.currentTimeMicros();
        }
    }
}