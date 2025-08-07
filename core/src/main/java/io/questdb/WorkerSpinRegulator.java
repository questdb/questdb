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

package io.questdb;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolMetrics;
import io.questdb.std.ObjList;
import io.questdb.std.Os;

import java.io.Closeable;

public class WorkerSpinRegulator implements Closeable {
    private static final Log LOG = LogFactory.getLog(WorkerSpinRegulator.class);
    private static final int throttleLoggingThresholdMicros = 5_000_000;
    private final ObjList<WorkerPool> workerPoolObjList = new ObjList<>();
    private long blockedWorkerTimeoutMicros = 100 * 1000; // 100 ms
    private long evaluateTimeout = Long.MAX_VALUE;
    private Thread regulatorThread;
    private volatile boolean started;

    public void addWorkerPool(WorkerPool workerPool) {
        if (started) {
            throw new IllegalStateException("Cannot add worker pool after start");
        }
        if (workerPool.getWorkerCount() > workerPool.getMinActiveWorkers()) {
            workerPoolObjList.add(workerPool);
            evaluateTimeout = Math.min(evaluateTimeout, workerPool.getEvaluateInterval());
        }
    }

    @Override
    public void close() {
        started = false;
        try {
            if (regulatorThread != null) {
                regulatorThread.join();
            }
        } catch (InterruptedException e) {
            LOG.error().$("Error while stopping WorkerSpinRegulator: ").$(e.getMessage()).I$();
        }
        regulatorThread = null;
    }

    public void halt() {
        started = false;
    }

    public void start() {
        started = true;
        if (regulatorThread != null) {
            close();
        }

        blockedWorkerTimeoutMicros = evaluateTimeout * 1000;
        if (workerPoolObjList.size() == 0) {
            LOG.info().$("No worker pools to regulate, skipping start").$();
            return;
        }

        regulatorThread = new Thread(() -> {
            while (started) {
                for (int i = 0, n = workerPoolObjList.size(); i < n; i++) {
                    resize(workerPoolObjList.getQuick(i));
                }
                Os.sleep(evaluateTimeout);
            }
        });
        regulatorThread.start();
    }

    /**
     * Parks a worker thread
     */
    private void parkWorker(WorkerPoolMetrics poolMetrics, String poolName, int activeWorkers, double currentUtilization, int parkCount) {
        boolean parked = false;

        for (int i = poolMetrics.getWorkerCount() - 1; i > -1 && parkCount > 0; i--) {
            if (!poolMetrics.isParked(i)) { // Only consider active workers
                poolMetrics.parkWorker(i);
                parkCount--;
                activeWorkers--;
                parked = true;
            }
        }

        if (parked) {
            if (poolMetrics.logParkingThrottled(Os.currentTimeMicros(), throttleLoggingThresholdMicros)) {
                LOG.info().$("parked workers [pool=").$(poolName)
                        .$(", parkedCount=").$(parkCount)
                        .$(", utilization=").$(currentUtilization)
                        .$(", activeWorkers=").$(activeWorkers)
                        .I$();
            }
        }
    }

    private void resize(WorkerPool workerPool) {
        var poolMetrics = workerPool.getPoolMetrics();
        var poolName = workerPool.getPoolName();
        double targetUtilization = workerPool.getTargetUtilization();
        double utilizationTolerance = workerPool.getUtilizationTolerance();
        int minActiveWorkers = workerPool.getMinActiveWorkers();

        // Get current pool utilization
        double currentUtilization = poolMetrics.getOverallUtilization();
        int activeWorkers = poolMetrics.getActiveWorkerCount();
        int totalWorkers = poolMetrics.getWorkerCount();

        // Calculate utilization deviation from target
        double utilizationDelta = currentUtilization - targetUtilization;

        // Decision logic with hysteresis
        int blockedWorkerCount = poolMetrics.getBlockedWorkerCount(Os.currentTimeMicros(), blockedWorkerTimeoutMicros);
        if (activeWorkers < totalWorkers && (utilizationDelta > utilizationTolerance || activeWorkers == blockedWorkerCount)) {
            // Utilization too high or all the running workers are blocked in long tasks.
            // Unpark a worker.
            unparkWorker(poolMetrics, poolName, activeWorkers, currentUtilization);
        } else if (utilizationDelta < -utilizationTolerance && activeWorkers > minActiveWorkers) {
            int minWorkers = Math.max(blockedWorkerCount + 1, minActiveWorkers);
            // Utilization too low - park a worker to increase load on remaining workers
            int parkWorkers = currentUtilization < 1.0 ? activeWorkers - minWorkers : 1;
            parkWorker(poolMetrics, poolName, activeWorkers, currentUtilization, parkWorkers);
        }
    }

    /**
     * Unparks a randomly selected parked worker to distribute load.
     */
    private void unparkWorker(WorkerPoolMetrics poolMetrics, String poolName, int workerCount, double currentUtilization) {
        // Find first parked worker (simple strategy)
        for (int i = 0, n = poolMetrics.getWorkerCount(); i < n; i++) {
            if (poolMetrics.isParked(i)) {
                poolMetrics.unparkWorker(i);

                if (poolMetrics.logParkingThrottled(Os.currentTimeMicros(), throttleLoggingThresholdMicros)) {
                    LOG.info().$("unparked worker [pool=").$(poolName)
                            .$(", workerId=").$(i)
                            .$(", utilization=").$(currentUtilization)
                            .$(", workerCount=").$(workerCount + 1)
                            .I$();
                }
                return;
            }
        }
    }

}
