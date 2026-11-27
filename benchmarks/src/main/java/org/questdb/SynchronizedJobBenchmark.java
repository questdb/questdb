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

package org.questdb;

import io.questdb.mp.Job;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Threads call {@code run()} on one job in a tight loop, the way the workers of a pool poll an IO
 * dispatcher. The {@code runs} counter is how many times per microsecond {@code runSerially()}
 * executes, which bounds a dispatcher's throughput; the primary score counts every attempt.
 * <ul>
 * <li>{@code readFirst}: {@link SynchronizedJob}, which attempts the CAS only when it reads the
 * lock as free.</li>
 * <li>{@code casFirst}: the same job with a lock attempt that always tries the CAS.</li>
 * </ul>
 * {@code work} is the CPU work inside the serialized section, in {@link Blackhole#consumeCPU}
 * tokens. Run with {@code -t} for the thread count; {@link #main} runs 2, 4 and 8 threads.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class SynchronizedJobBenchmark {
    @Param({"casFirst", "readFirst"})
    public String lockAttempt;
    @Param({"20", "200"})
    public int work;
    private Job job;

    public static void main(String[] args) throws RunnerException {
        for (int threads : new int[]{2, 4, 8}) {
            new Runner(new OptionsBuilder()
                    .include(SynchronizedJobBenchmark.class.getSimpleName())
                    .threads(threads)
                    .build()
            ).run();
        }
    }

    @Benchmark
    public void poll(Runs runs) {
        if (job.run(Job.RUNNING_STATUS)) {
            runs.runs++;
        }
    }

    @Setup(Level.Trial)
    public void setUp() {
        job = "casFirst".equals(lockAttempt) ? new CasFirstJob(work) : new ReadFirstJob(work);
    }

    @AuxCounters(AuxCounters.Type.OPERATIONS)
    @State(Scope.Thread)
    public static class Runs {
        public long runs;

        @Setup(Level.Iteration)
        public void reset() {
            runs = 0;
        }
    }

    // SynchronizedJob's structure with a lock attempt that always tries the CAS
    private static final class CasFirstJob implements Job {
        private static final long LOCKED_OFFSET = Unsafe.getFieldOffset(CasFirstJob.class, "locked");
        private final int work;
        // shared state the serialized section writes, as a dispatcher writes its own
        private long counter;
        @SuppressWarnings("FieldCanBeLocal")
        private volatile int locked = 0;

        private CasFirstJob(int work) {
            this.work = work;
        }

        @Override
        public boolean run(@NotNull Job.WorkerContext workerContext) {
            if (Unsafe.cas(this, LOCKED_OFFSET, 0, 1)) {
                try {
                    counter++;
                    Blackhole.consumeCPU(work);
                    return true;
                } finally {
                    locked = 0;
                }
            }
            return false;
        }
    }

    private static final class ReadFirstJob extends SynchronizedJob {
        private final int work;
        // shared state the serialized section writes, as a dispatcher writes its own
        private long counter;

        private ReadFirstJob(int work) {
            this.work = work;
        }

        @Override
        protected boolean runSerially() {
            counter++;
            Blackhole.consumeCPU(work);
            return true;
        }
    }
}
