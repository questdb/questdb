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

import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

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
public final class WorkerPoolMetrics implements QuietCloseable {

    // Cache line size for most modern CPUs
    private static final int CACHE_LINE_SIZE = 64;
    private static final int PARKING_FLAG_OFFSET = 8; // boolean (1 byte)
    // Offsets within each worker slot
    // Size of each worker's metrics slot (padded to cache line)
    private static final int WORKER_SLOT_SIZE = CACHE_LINE_SIZE;
    private final Object[] parkingMonitors;
    private final long poolSleepMicro;
    private final int workerCount;
    private final WorkerStats[] workerStats;
    private long baseAddress;

    public WorkerPoolMetrics(int workerCount, long poolSleepMs) {
        this.workerCount = workerCount;

        // Create individual monitor objects for each worker
        this.parkingMonitors = new Object[workerCount];
        this.poolSleepMicro = poolSleepMs * 1000;
        for (int i = 0; i < workerCount; i++) {
            parkingMonitors[i] = new Object();
        }
        workerStats = new WorkerStats[workerCount];
        for (int i = 0; i < workerCount; i++) {
            workerStats[i] = new WorkerStats();
        }
    }

    /**
     * Free the allocated native memory. Must be called to prevent memory leaks.
     */
    @Override
    public void close() {
        if (baseAddress != 0) {
            Unsafe.free(baseAddress, (long) workerCount * WORKER_SLOT_SIZE, MemoryTag.NATIVE_DEFAULT);
            baseAddress = 0;
        }
    }

    /**
     * Gets the count of active (non-parked) workers.
     *
     * @return number of workers that are not parked
     */
    public int getActiveWorkerCount() {
        return workerCount - getParkedWorkerCount();
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

        long nowMicro = Os.currentTimeMicros();
        double sum = 0.0;
        for (int i = 0; i < workerCount; i++) {
            if (isParked(i)) {
                continue; // Skip parked workers
            }
            sum += workerStats[i].getUtilizationPercentage(nowMicro, poolSleepMicro); // Assuming poolSleepMs is 1000ms
        }
        return sum / workerCount;
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

//    /**
//     * Records the utilization percentage for a specific worker.
//     * This method is designed to be called frequently from worker threads with minimal overhead.
//     *
//     * @param workerId              the worker ID (0-based index)
//     * @param utilizationPercentage utilization value (0.0 to 100.0)
//     */
//    public void recordUtilization(int workerId, double utilizationPercentage) {
//        assert workerId >= 0 && workerId < workerCount;
//        long slotAddress = baseAddress + ((long) workerId * WORKER_SLOT_SIZE);
//        Unsafe.getUnsafe().putDouble(slotAddress + UTILIZATION_OFFSET, utilizationPercentage);
//    }

    /**
     * Sets the parking flag for a specific worker, indicating it should park on the monitor.
     *
     * @param workerId   the worker ID (0-based index)
     * @param shouldPark true if worker should park, false otherwise
     */
    public void setParkingFlag(int workerId, boolean shouldPark) {
        assert workerId >= 0 && workerId < workerCount;
        long slotAddress = baseAddress + ((long) workerId * WORKER_SLOT_SIZE);
        Unsafe.getUnsafe().putByte(slotAddress + PARKING_FLAG_OFFSET, shouldPark ? (byte) 1 : (byte) 0);
    }

    /**
     * Gets the parking flag for a specific worker.
     *
     * @param workerId the worker ID (0-based index)
     * @return true if worker should park, false otherwise
     */
    public boolean isParked(int workerId) {
        assert workerId >= 0 && workerId < workerCount;
        long slotAddress = baseAddress + ((long) workerId * WORKER_SLOT_SIZE);
        return Unsafe.getUnsafe().getByte(slotAddress + PARKING_FLAG_OFFSET) != 0;
    }

    public void start() {
        if (baseAddress != 0) {
            // Already started
            return;
        }

        // Allocate memory: workerCount * WORKER_SLOT_SIZE
        long totalSize = (long) workerCount * WORKER_SLOT_SIZE;
        this.baseAddress = Unsafe.malloc(totalSize, MemoryTag.NATIVE_DEFAULT);

        // Initialize all values to zero
        Unsafe.getUnsafe().setMemory(baseAddress, totalSize, (byte) 0);
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

        private double getUtilizationPercentage(long nowMicro, long poolSleepMicro) {
            if (nowMicro - lastUpdatedMiroTs > 2 * poolSleepMicro) {
                // If no updates in the last poolSleepMs, assume 100% utilization
                return 100.0;
            }

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