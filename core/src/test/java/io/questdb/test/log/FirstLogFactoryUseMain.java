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

package io.questdb.test.log;

import io.questdb.Metrics;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecordUtf8Sink;
import io.questdb.log.LogWriter;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.ObjHashSet;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Runs the first LogFactory use of a fresh JVM the way Bootstrap does: configureRootDir(), then
 * getLog(). WorkerPool is not initialized at that point, so constructing the factory runs the
 * static initializer of WorkerPool, which asks LogFactory for a logger. A test JVM initializes
 * WorkerPool long before any test runs, so this path needs a JVM of its own.
 * <p>
 * The process exits with 0 when every check passes and with 1 after printing the failed check.
 */
public final class FirstLogFactoryUseMain {
    // the failing writer throws more often than its queue has slots, then idles
    private static final int FAILING_WRITER_FAILURE_COUNT = 64;
    private static final int FAILING_WRITER_QUEUE_DEPTH = 16;
    private static final long WAIT_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(10);

    private FirstLogFactoryUseMain() {
    }

    public static void main(String[] args) throws Exception {
        // CI exports QDB_LOG_* variables for its own log file, which would override the configuration below
        LogFactory.disableEnv();
        final File root = new File(args[1]);
        switch (args[0]) {
            case "rolling-writer" -> runRollingWriter(root);
            case "failing-writer" -> runFailingWriter(root);
            default -> check(false, "unknown scenario: " + args[0]);
        }
    }

    private static void check(boolean condition, String failure) {
        if (!condition) {
            System.err.println("CHECK FAILED: " + failure);
            System.exit(1);
        }
    }

    private static int countThreads(String namePrefix) {
        int count = 0;
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.getName().startsWith(namePrefix)) {
                count++;
            }
        }
        return count;
    }

    private static FailingLogWriter findFailingWriter() {
        final ObjHashSet<LogWriter> jobs = LogFactory.getInstance().getJobs();
        for (int i = 0, n = jobs.size(); i < n; i++) {
            if (jobs.get(i) instanceof FailingLogWriter failingWriter) {
                return failingWriter;
            }
        }
        check(false, "the factory has no failing writer");
        return null;
    }

    // A logging worker that reports the failures of its writers through the queues it drains
    // blocks once such a queue is full: critical() waits for a free slot, and no other thread
    // frees one.
    private static void runFailingWriter(File root) throws IOException {
        writeLogConf(root, """
                queueDepth=%d
                writers=fail
                w.fail.class=%s
                w.fail.level=INFO
                """.formatted(FAILING_WRITER_QUEUE_DEPTH, FailingLogWriter.class.getName()));
        LogFactory.configureRootDir(root.getAbsolutePath());
        LogFactory.getLog(FirstLogFactoryUseMain.class);

        final FailingLogWriter writer = findFailingWriter();
        final long runTarget = FAILING_WRITER_FAILURE_COUNT + 100;
        final long deadline = System.nanoTime() + WAIT_TIMEOUT_NANOS;
        while (writer.getRunCount() < runTarget) {
            check(
                    System.nanoTime() - deadline < 0,
                    "logging worker stopped running its writers [runs=" + writer.getRunCount() + ']'
            );
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        LogFactory.closeInstanceWithin(WAIT_TIMEOUT_NANOS);
    }

    private static void runRollingWriter(File root) throws IOException {
        writeLogConf(root, """
                writers=file
                w.file.class=io.questdb.log.LogRollingFileWriter
                w.file.location=${log.dir}/first-use.log.${date:yyyyMMdd}
                w.file.level=INFO,ERROR
                w.file.rollEvery=day
                """);
        LogFactory.configureRootDir(root.getAbsolutePath());
        final Log log = LogFactory.getLog(FirstLogFactoryUseMain.class);

        // every factory runs a logging worker of its own
        final int loggingWorkerCount = countThreads("logging_");
        check(loggingWorkerCount == 1, "expected one logging worker, found " + loggingWorkerCount);

        // the logger of WorkerPool writes through the same factory as every other logger
        final WorkerPool pool = new WorkerPool(new ProbePoolConfiguration());
        pool.start();
        pool.halt();
        log.info().$("first-use probe record").$();
        LogFactory.closeInstance();

        final File[] logFiles = new File(root, "log").listFiles();
        check(logFiles != null && logFiles.length == 1, "expected one log file, found " + Arrays.toString(logFiles));
        final String content = Files.readString(logFiles[0].toPath(), StandardCharsets.UTF_8);
        check(content.contains("first-use probe record"), "probe record missing from the log file:\n" + content);
        check(
                content.contains("worker pool configured [pool=first-use-probe"),
                "WorkerPool record missing from the log file:\n" + content
        );
    }

    private static void writeLogConf(File root, String content) throws IOException {
        final File confDir = new File(root, "conf");
        check(confDir.mkdirs() || confDir.isDirectory(), "cannot create " + confDir);
        Files.writeString(new File(confDir, LogFactory.DEFAULT_CONFIG_NAME).toPath(), content, StandardCharsets.UTF_8);
    }

    public static final class FailingLogWriter implements LogWriter {
        private final AtomicLong runCount = new AtomicLong();

        // LogFactory instantiates configured writers through this constructor
        @SuppressWarnings("unused")
        public FailingLogWriter(RingQueue<LogRecordUtf8Sink> ring, SCSequence subSeq, int level) {
        }

        @Override
        public void bindProperties(LogFactory factory) {
        }

        public long getRunCount() {
            return runCount.get();
        }

        // never consumes, so every record published to this writer stays in its queue
        @Override
        public boolean run(@NotNull WorkerContext workerContext) {
            if (runCount.incrementAndGet() <= FAILING_WRITER_FAILURE_COUNT) {
                throw new WriterFailure();
            }
            return false;
        }
    }

    private static final class ProbePoolConfiguration implements WorkerPoolConfiguration {
        @Override
        public Metrics getMetrics() {
            return Metrics.DISABLED;
        }

        @Override
        public String getPoolName() {
            return "first-use-probe";
        }

        @Override
        public int getWorkerCount() {
            return 1;
        }

        @Override
        public boolean isDaemonPool() {
            return true;
        }
    }

    private static final class WriterFailure extends RuntimeException {
        private WriterFailure() {
            super("simulated log writer failure", null, false, false);
        }
    }
}
