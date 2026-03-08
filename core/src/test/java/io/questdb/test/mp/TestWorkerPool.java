/*******************************************************************************
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
import io.questdb.std.ObjList;

public class TestWorkerPool extends WorkerPool {
    private final ObjList<PageFrameReduceJob> pageFrameReduceJobs = new ObjList<>();

    public TestWorkerPool(int workerCount) {
        this("testing", workerCount, Metrics.DISABLED);
    }

    public TestWorkerPool(String poolName, int workerCount) {
        this(poolName, workerCount, Metrics.DISABLED);
    }

    public TestWorkerPool(int workerCount, Metrics metrics) {
        this("testing", workerCount, metrics);
    }

    public TestWorkerPool(String poolName, int workerCount, Metrics metrics) {
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
        });
        assert workerCount > 0;
    }

    public TestWorkerPool(WorkerPoolConfiguration configuration) {
        super(configuration);
    }

    @Override
    public void assign(int worker, Job job) {
        if (job instanceof PageFrameReduceJob) {
            pageFrameReduceJobs.add((PageFrameReduceJob) job);
        }
        super.assign(worker, job);
    }

    public ObjList<PageFrameReduceJob> getPageFrameReduceJobs() {
        return pageFrameReduceJobs;
    }
}
