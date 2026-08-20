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

package io.questdb.test.mp;

import io.questdb.Metrics;
import io.questdb.cairo.sql.async.PageFrameReduceJob;
import io.questdb.mp.Job;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;

public class TestWorkerPool extends WorkerPool {
    private final ObjList<PageFrameReduceJob> pageFrameReduceJobs = new ObjList<>();

    public static TestWorkerPool createWithRandomMode(Rnd rnd, WorkerPoolConfiguration configuration) {
        return new TestWorkerPool(withRandomMode(rnd, configuration));
    }

    public static WorkerPoolConfiguration withRandomMode(Rnd rnd, WorkerPoolConfiguration configuration) {
        return new ModeOverrideWorkerPoolConfiguration(configuration, TestUtils.getWorkerPoolMode(rnd));
    }

    public TestWorkerPool(int workerCount) {
        this("testing", workerCount, Metrics.DISABLED, WorkerPoolMode.LEGACY);
    }

    public TestWorkerPool(int workerCount, Metrics metrics) {
        this("testing", workerCount, metrics, WorkerPoolMode.LEGACY);
    }

    public TestWorkerPool(String poolName, int workerCount, Metrics metrics) {
        this(poolName, workerCount, metrics, WorkerPoolMode.LEGACY);
    }

    public TestWorkerPool(int workerCount, WorkerPoolMode workerPoolMode) {
        this("testing", workerCount, Metrics.DISABLED, workerPoolMode);
    }

    public TestWorkerPool(String poolName, int workerCount, Metrics metrics, WorkerPoolMode workerPoolMode) {
        this(new WorkerPoolConfiguration() {
            @Override
            public Metrics getMetrics() {
                return metrics;
            }

            @Override
            public String getPoolName() {
                return poolName;
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }

            @Override
            public WorkerPoolMode getWorkerPoolMode() {
                return workerPoolMode;
            }
        });
        assert workerCount > 0;
    }

    public TestWorkerPool(WorkerPoolConfiguration configuration) {
        super(configuration);
    }

    public TestWorkerPool(WorkerPoolConfiguration configuration, WorkerPoolMode workerPoolMode) {
        super(new ModeOverrideWorkerPoolConfiguration(configuration, workerPoolMode));
    }

    @Override
    public void assign(int worker, Job job) {
        if (job instanceof PageFrameReduceJob pageFrameReduceJob) {
            pageFrameReduceJobs.add(pageFrameReduceJob);
        }
        super.assign(worker, job);
    }

    public ObjList<PageFrameReduceJob> getPageFrameReduceJobs() {
        return pageFrameReduceJobs;
    }

    @Override
    public void halt() {
        haltAndAssertCleanForTest(DEFAULT_HALT_TIMEOUT_NANOS);
    }

    private static final class ModeOverrideWorkerPoolConfiguration implements WorkerPoolConfiguration {
        private final WorkerPoolConfiguration configuration;
        private final WorkerPoolMode workerPoolMode;

        private ModeOverrideWorkerPoolConfiguration(
                WorkerPoolConfiguration configuration,
                WorkerPoolMode workerPoolMode
        ) {
            this.configuration = configuration;
            this.workerPoolMode = workerPoolMode;
        }

        @Override
        public int getFiberMaxLiveCount() {
            return configuration.getFiberMaxLiveCount();
        }

        @Override
        public int getFiberMountBudget() {
            return configuration.getFiberMountBudget();
        }

        @Override
        public int getFiberRetainedCount() {
            return configuration.getFiberRetainedCount();
        }

        @Override
        public Metrics getMetrics() {
            return configuration.getMetrics();
        }

        @Override
        public long getNapThreshold() {
            return configuration.getNapThreshold();
        }

        @Override
        public String getPoolName() {
            return configuration.getPoolName();
        }

        @Override
        public long getSleepThreshold() {
            return configuration.getSleepThreshold();
        }

        @Override
        public long getSleepTimeout() {
            return configuration.getSleepTimeout();
        }

        @Override
        public int[] getWorkerAffinity() {
            return configuration.getWorkerAffinity();
        }

        @Override
        public int getWorkerCount() {
            return configuration.getWorkerCount();
        }

        @Override
        public WorkerPoolMode getWorkerPoolMode() {
            return workerPoolMode;
        }

        @Override
        public long getYieldThreshold() {
            return configuration.getYieldThreshold();
        }

        @Override
        public boolean haltOnError() {
            return configuration.haltOnError();
        }

        @Override
        public boolean isDaemonPool() {
            return configuration.isDaemonPool();
        }

        @Override
        public boolean isEnabled() {
            return configuration.isEnabled();
        }

        @Override
        public int workerPoolPriority() {
            return configuration.workerPoolPriority();
        }
    }
}
