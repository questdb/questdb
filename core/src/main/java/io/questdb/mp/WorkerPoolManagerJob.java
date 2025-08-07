///*******************************************************************************
// *     ___                  _   ____  ____
// *    / _ \ _   _  ___  ___| |_|  _ \| __ )
// *   | | | | | | |/ _ \/ __| __| | | |  _ \
// *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
// *    \__\_\\__,_|\___||___/\__|____/|____/
// *
// *  Copyright (c) 2014-2019 Appsicle
// *  Copyright (c) 2019-2024 QuestDB
// *
// *  Licensed under the Apache License, Version 2.0 (the "License");
// *  you may not use this file except in compliance with the License.
// *  You may obtain a copy of the License at
// *
// *  http://www.apache.org/licenses/LICENSE-2.0
// *
// *  Unless required by applicable law or agreed to in writing, software
// *  distributed under the License is distributed on an "AS IS" BASIS,
// *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *  See the License for the specific language governing permissions and
// *  limitations under the License.
// *
// ******************************************************************************/
//
//package io.questdb.mp;
//
//import io.questdb.log.Log;
//import io.questdb.log.LogFactory;
//
///**
// * Adaptive worker pool manager that dynamically parks and unparks workers based on utilization.
// * Aims to maintain optimal utilization by adjusting the active worker count.
// * <p>
// * The manager uses a feedback control approach:
// * - If overall utilization is below target, park workers to increase load on remaining workers
// * - If overall utilization is above target, unpark workers to distribute the load
// * - Includes hysteresis to prevent oscillation between parking/unparking decisions
// * <p>
// * Extends SynchronizedJob to ensure only one worker thread can execute management logic at a time,
// * preventing race conditions when making parking/unparking decisions.
// */
//public class WorkerPoolManagerJob extends SynchronizedJob {
//    // Default configuration values
//    private static final Log LOG = LogFactory.getLog(WorkerPoolManagerJob.class);
//    private final long evaluationInterval;
//    private final int minActiveWorkers;
//    private final WorkerPoolMetrics poolMetrics;
//    private final String poolName;
//    private final double targetUtilization;
//    private final double utilizationTolerance;
//    private long iterationCount = 0;
//    private long lastEvaluationIteration = 0;
//    private double lastUtilization = 0.0;
//
//
//    public WorkerPoolManagerJob(
//            WorkerPoolMetrics poolMetrics,
//            String poolName,
//            double targetUtilization,
//            double utilizationTolerance,
//            long evaluationInterval,
//            int minActiveWorkers
//    ) {
//        this.poolMetrics = poolMetrics;
//        this.poolName = poolName;
//        this.targetUtilization = targetUtilization;
//        this.utilizationTolerance = utilizationTolerance;
//        this.evaluationInterval = evaluationInterval;
//        this.minActiveWorkers = Math.max(1, minActiveWorkers);
//    }
//
//
//    /**
//     * Gets statistics about the manager's operation.
//     *
//     * @return formatted statistics string
//     */
//    public String getStatistics() {
//        return String.format("WorkerPoolManager[pool=%s, target=%.1f%%, current=%.1f%%, active=%d/%d, iterations=%d]",
//                poolName, targetUtilization, lastUtilization,
//                poolMetrics.getActiveWorkerCount(), poolMetrics.getWorkerCount(), iterationCount);
//    }
//
//    /**
//     * Parks the worker with the lowest utilization to encourage load concentration.
//     *
//     * @return true if a worker was parked, false otherwise
//     */
//    private boolean parkLeastUtilizedWorker() {
//        int leastUtilizedWorker = -1;
//        double lowestUtilization = Double.MAX_VALUE;
//
//        for (int i = 0; i < poolMetrics.getWorkerCount(); i++) {
//            if (!poolMetrics.shouldPark(i)) { // Only consider active workers
//                double utilization = poolMetrics.getUtilization(i);
//                if (utilization < lowestUtilization) {
//                    lowestUtilization = utilization;
//                    leastUtilizedWorker = i;
//                }
//            }
//        }
//
//        if (leastUtilizedWorker >= 0) {
//            poolMetrics.parkWorker(leastUtilizedWorker);
//            LOG.info().$("Parked worker [pool=").$(poolName)
//                    .$(", workerId=").$(leastUtilizedWorker)
//                    .$(", utilization=").$(lowestUtilization).I$();
//            return true;
//        }
//
//        return false;
//    }
//
//    /**
//     * Unparks a randomly selected parked worker to distribute load.
//     *
//     * @return true if a worker was unparked, false otherwise
//     */
//    private boolean unparkRandomWorker() {
//        // Find first parked worker (simple strategy)
//        for (int i = 0; i < poolMetrics.getWorkerCount(); i++) {
//            if (poolMetrics.shouldPark(i)) {
//                poolMetrics.unparkWorker(i);
//                LOG.info().$("Unparked worker [pool=").$(poolName)
//                        .$(", workerId=").$(i).I$();
//                return true;
//            }
//        }
//        return false;
//    }
//
//    @Override
//    protected boolean runSerially() {
//        iterationCount++;
//
//        // Only evaluate and potentially adjust workers at specified intervals
//        if (iterationCount - lastEvaluationIteration < evaluationInterval) {
//            return false; // No urgent work to do
//        }
//
//        lastEvaluationIteration = iterationCount;
//
//        // Get current pool utilization
//        double currentUtilization = poolMetrics.getOverallUtilization();
//        int activeWorkers = poolMetrics.getActiveWorkerCount();
//        int totalWorkers = poolMetrics.getWorkerCount();
//
//        // Calculate utilization deviation from target
//        double utilizationDelta = currentUtilization - targetUtilization;
//
//        LOG.debug().$("WorkerPool evaluation [pool=").$(poolName)
//                .$(", utilization=").$(currentUtilization)
//                .$(", target=").$(targetUtilization)
//                .$(", delta=").$(utilizationDelta)
//                .$(", active=").$(activeWorkers)
//                .$(", total=").$(totalWorkers).I$();
//
//        boolean madeAdjustment = false;
//
//        // Decision logic with hysteresis
//        if (utilizationDelta < -utilizationTolerance && activeWorkers > minActiveWorkers) {
//            // Utilization too low - park a worker to increase load on remaining workers
//            madeAdjustment = parkLeastUtilizedWorker();
//        } else if (utilizationDelta > utilizationTolerance && activeWorkers < totalWorkers) {
//            // Utilization too high - unpark a worker to distribute load
//            madeAdjustment = unparkRandomWorker();
//        }
//
//        if (madeAdjustment) {
//            LOG.info().$("WorkerPool adjusted [pool=").$(poolName)
//                    .$(", utilization=").$(currentUtilization)
//                    .$(", active=").$(poolMetrics.getActiveWorkerCount()).I$();
//        }
//
//        lastUtilization = currentUtilization;
//
//        // Return false since this is a periodic management task, not urgent
//        return false;
//    }
//}