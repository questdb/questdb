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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.IntStream;

public class PGWireInsertSelectBenchmark {
    public static void main(String[] args) throws Exception {
        int nInserters = 4;
        int nSelectors = 8;
        int insertCountPerThread = 10_000;
        int selectCountPerThread = 10_000;
        AtomicLongArray inserterProgress = new AtomicLongArray(nInserters);
        AtomicLongArray selectorProgress = new AtomicLongArray(nSelectors);
        ExecutorService pool = Executors.newFixedThreadPool(nInserters + nSelectors);
        try (Connection connection = createConnection();
             Statement ddlStatement = connection.createStatement()
        ) {
            ddlStatement.execute("drop table if exists tango");
            ddlStatement.execute("create table tango (" +
                    "ts timestamp, " +
                    "n long" +
                    ") timestamp(ts) partition by hour");
            for (int taskid = 0; taskid < nInserters; taskid++) {
                final int taskId = taskid;
                pool.submit(() -> {
                    try (PreparedStatement st = connection.prepareStatement("insert into tango values (?, ?)")) {
                        for (int i = 0; i < insertCountPerThread; i++) {
                            st.setLong(1, TimeUnit.MILLISECONDS.toMicros(10 * i));
                            st.setLong(2, 0);
                            st.executeUpdate();
                            inserterProgress.lazySet(taskId, i + 1);
                        }
                    } catch (Exception e) {
                        e.printStackTrace(System.err);
                    }
                });
            }
            for (int taskid = 0; taskid < nSelectors; taskid++) {
                final int taskId = taskid;
                pool.submit(() -> {
                    try (Statement st = connection.createStatement()) {
                        for (int i = 0; i < selectCountPerThread; i++) {
                            ResultSet rs = st.executeQuery("select ts, n from tango limit -1");
                            while (rs.next()) {
                                rs.getLong(2);
                            }
                            selectorProgress.lazySet(taskId, i + 1);
                        }
                    } catch (Exception e) {
                        e.printStackTrace(System.err);
                    }
                });
            }
            pool.shutdown();
            long targetInsertCount = nInserters * insertCountPerThread;
            long targetSelectCount = nSelectors * selectCountPerThread;
            while (!pool.awaitTermination(1, TimeUnit.SECONDS)) {
                long doneInserts = IntStream.range(0, inserterProgress.length()).mapToLong(inserterProgress::get).sum();
                long doneSelects = IntStream.range(0, selectorProgress.length()).mapToLong(selectorProgress::get).sum();
                System.out.printf("%2d%% inserts %2d%% selects\n",
                        doneInserts * 100 / targetInsertCount, doneSelects * 100 / targetSelectCount);
            }
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    private static Connection createConnection() throws Exception {
        Class.forName("org.postgresql.Driver");
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        properties.setProperty("sslmode", "disable");
        properties.setProperty("binaryTransfer", Boolean.toString(true));
        // "simple", "extended", "extendedForPrepared", "extendedCacheEverything"
        properties.setProperty("preferQueryMode", "extended");
//        properties.setProperty("prepareThreshold", String.valueOf(-1));

        TimeZone.setDefault(TimeZone.getTimeZone("EDT"));
        final String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", 8812);
        return DriverManager.getConnection(url, properties);
    }
}
