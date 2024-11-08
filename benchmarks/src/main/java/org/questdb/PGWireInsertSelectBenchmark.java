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

package org.questdb;

import io.questdb.client.Sender;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;

public class PGWireInsertSelectBenchmark {
    public static final int COOLDOWN_PERIOD_SECONDS = 30;
    private static final int INSERT_BATCH_SIZE = 500;
    private static final long RUNTIME_SECONDS = 10;

    private static int nInserters;
    private static int nSelectors;
    private static boolean useIlp;

    public static void main(String[] args) throws Exception {
        int[][] inserterSelectorCounts = {
                {1, 1}, {1, 2}, {2, 1}, {2, 2},
                {3, 1}, {3, 2}, {1, 3}, {2, 3}, {3, 3},
                {4, 1}, {4, 2}, {4, 3}, {1, 4}, {2, 4}, {3, 4}, {4, 4}
        };
        for (int[] inserterSelectorCount : inserterSelectorCounts) {
            nInserters = inserterSelectorCount[0];
            nSelectors = inserterSelectorCount[1];
            useIlp = false;
            runBenchmark();
            Thread.sleep(SECONDS.toMillis(COOLDOWN_PERIOD_SECONDS));
            useIlp = true;
            runBenchmark();
            Thread.sleep(SECONDS.toMillis(COOLDOWN_PERIOD_SECONDS));
        }
    }

    private static Connection createConnection() throws Exception {
        Class.forName("org.postgresql.Driver");
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        properties.setProperty("sslmode", "disable");
        properties.setProperty("binaryTransfer", Boolean.toString(true));
        // choices: "simple", "extended", "extendedForPrepared", "extendedCacheEverything"
        properties.setProperty("preferQueryMode", "extended");
//        properties.setProperty("prepareThreshold", String.valueOf(-1));

        TimeZone.setDefault(TimeZone.getTimeZone("EDT"));
        final String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", 8812);
        return DriverManager.getConnection(url, properties);
    }

    private static long makeTimestampMicros(long taskId, long insertId) {
        return TimeUnit.MILLISECONDS.toMicros(10 * insertId) + taskId;
    }

    private static void reportProgress(AtomicLongArray inserterProgress, AtomicLongArray selectorProgress) {
        long doneInserts = IntStream.range(0, inserterProgress.length()).mapToLong(inserterProgress::get).sum();
        long doneSelects = IntStream.range(0, selectorProgress.length()).mapToLong(selectorProgress::get).sum();
        System.out.printf("%,9d inserts %,9d selects\n", doneInserts, doneSelects);
    }

    private static void reportResult(long tookNs, AtomicLongArray inserterTotals, AtomicLongArray selectorTotals) {
        long nsPerSecond = SECONDS.toNanos(1);
        long doneInserts = IntStream.range(0, inserterTotals.length()).mapToLong(inserterTotals::get).sum();
        long doneSelects = IntStream.range(0, selectorTotals.length()).mapToLong(selectorTotals::get).sum();
        long insertsPerSecond = doneInserts * nsPerSecond / tookNs;
        long selectsPerSecond = doneSelects * nsPerSecond / tookNs;

        System.out.printf("\n%d %s inserters, %d selectors combined performance:\n" +
                        "%,d inserts per second, %,d selects per second\n" +
                        "Per-thread performance:\n" +
                        "%,d inserts per second, %,d selects per second\n\n",
                nInserters, useIlp ? "ILP" : "JDBC", nSelectors, insertsPerSecond, selectsPerSecond,
                nInserters != 0 ? insertsPerSecond / nInserters : 0,
                nSelectors != 0 ? selectsPerSecond / nSelectors : 0);
    }

    static void runBenchmark() {
        AtomicLongArray inserterProgress = new AtomicLongArray(nInserters);
        AtomicLongArray selectorProgress = new AtomicLongArray(nSelectors);
        ExecutorService pool = Executors.newFixedThreadPool(nInserters + nSelectors);
        try {
            try (Connection connection = createConnection();
                 Statement ddlStatement = connection.createStatement()
            ) {
                ddlStatement.execute("drop table if exists tango");
                ddlStatement.execute("create table tango (ts timestamp, n long) timestamp(ts) partition by hour");
            }
            long start = System.nanoTime();
            long deadline = start + SECONDS.toNanos(RUNTIME_SECONDS);
            for (int taskid = 0; taskid < nInserters; taskid++) {
                final int taskId = taskid;
                pool.submit(() -> {
                    if (useIlp) {
                        try (Sender sender = Sender.fromConfig("http::addr=localhost:9000;auto_flush=off;")) {
                            for (long i = 1; ; i++) {
                                sender.table("tango")
                                        .longColumn("n", 0)
                                        .at(makeTimestampMicros(taskId, i), ChronoUnit.MICROS);
                                if (i % INSERT_BATCH_SIZE == 0) {
                                    sender.flush();
                                    inserterProgress.lazySet(taskId, i);
                                }
                                if (System.nanoTime() > deadline) {
                                    inserterProgress.set(taskId, i);
                                    break;
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace(System.err);
                        }
                    } else {
                        try (Connection connection = createConnection();
                             PreparedStatement st = connection.prepareStatement("insert into tango values (?, ?)")
                        ) {
                            for (long i = 1; ; i++) {
                                st.setLong(1, makeTimestampMicros(taskId, i));
                                st.setLong(2, 0);
                                st.addBatch();
                                boolean batchComplete = i % INSERT_BATCH_SIZE == 0;
                                if (batchComplete) {
                                    st.executeBatch();
                                    inserterProgress.lazySet(taskId, i);
                                }
                                if (System.nanoTime() > deadline) {
                                    if (!batchComplete) {
                                        st.executeBatch();
                                    }
                                    inserterProgress.set(taskId, i);
                                    break;
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace(System.err);
                        }
                    }
                });
            }
            for (int taskid = 0; taskid < nSelectors; taskid++) {
                final int taskId = taskid;
                pool.submit(() -> {
                    try (Connection connection = createConnection();
                         PreparedStatement st = connection.prepareStatement("select ts, n from tango limit -1")
                    ) {
                        for (long i = 1; ; i++) {
                            ResultSet rs = st.executeQuery();
                            while (rs.next()) {
                                rs.getLong(2);
                            }
                            if (i % INSERT_BATCH_SIZE == 0) {
                                selectorProgress.lazySet(taskId, i);
                                if (System.nanoTime() > deadline) {
                                    break;
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace(System.err);
                    }
                });
            }
            pool.shutdown();
            while (true) {
                boolean done = pool.awaitTermination(1, SECONDS);
                reportProgress(inserterProgress, selectorProgress);
                if (done) break;
            }
            long finish = System.nanoTime();
            reportResult(finish - start, inserterProgress, selectorProgress);
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }
}
