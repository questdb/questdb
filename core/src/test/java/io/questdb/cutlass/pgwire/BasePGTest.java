/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.pgwire;

import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Rnd;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.TimeZone;

public class BasePGTest extends AbstractGriffinTest {

    protected PGWireServer createPGServer(PGWireConfiguration configuration) {
        return PGWireServer.create(
                configuration,
                null,
                LOG,
                engine,
                compiler.getFunctionFactoryCache(),
                snapshotAgent,
                metrics
        );
    }

    protected PGWireServer createPGServer(int workerCount) {
        return createPGServer(workerCount, Long.MAX_VALUE);
    }

    protected PGWireServer createPGServer(int workerCount, long maxQueryTime) {

        final int[] affinity = new int[workerCount];
        Arrays.fill(affinity, -1);

        final SqlExecutionCircuitBreakerConfiguration circuitBreakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public long getMaxTime() {
                return maxQueryTime;
            }
        };

        final PGWireConfiguration conf = new DefaultPGWireConfiguration() {
            @Override
            public SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
                return circuitBreakerConfiguration;
            }

            @Override
            public int[] getWorkerAffinity() {
                return affinity;
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }

            @Override
            public Rnd getRandom() {
                return new Rnd();
            }
        };

        return createPGServer(conf);
    }

    protected void execSelectWithParam(PreparedStatement select, int value) throws SQLException {
        sink.clear();
        select.setInt(1, value);
        try (ResultSet resultSet = select.executeQuery()) {
            sink.clear();
            while (resultSet.next()) {
                sink.put(resultSet.getInt(1));
                sink.put('\n');
            }
        }
    }

    enum Mode {
        Simple("simple"), Extended("extended"), ExtendedForPrepared("extendedForPrepared"), ExtendedCacheEverything("extendedCacheEverything");

        private final String value;

        Mode(String value) {
            this.value = value;
        }
    }

    protected Connection getConnection(boolean simple, boolean binary) throws SQLException {
        if (simple) {
            return getConnection(Mode.Simple, binary, -2);
        } else {
            return getConnection(Mode.Extended, binary, -2);
        }
    }

    protected Connection getConnection(Mode mode, boolean binary, int prepareThreshold) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        properties.setProperty("sslmode", "disable");
        properties.setProperty("binaryTransfer", Boolean.toString(binary));
        properties.setProperty("preferQueryMode", mode.value);
        if (prepareThreshold > -2) {//-1 has special meaning in pg jdbc ...
            properties.setProperty("prepareThreshold", String.valueOf(prepareThreshold));
        }

        TimeZone.setDefault(TimeZone.getTimeZone("EDT"));
        //use this line to switch to local postgres
        //return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/qdb", properties);
        return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:8812/qdb", properties);
    }

    @NotNull
    protected NetworkFacade getFragmentedSendFacade() {
        return new NetworkFacadeImpl() {
            @Override
            public int send(long fd, long buffer, int bufferLen) {
                int total = 0;
                for (int i = 0; i < bufferLen; i++) {
                    int n = super.send(fd, buffer + i, 1);
                    if (n < 0) {
                        return n;
                    }
                    total += n;
                }
                return total;
            }
        };
    }

    @NotNull
    protected DefaultPGWireConfiguration getHexPgWireConfig() {
        return new DefaultPGWireConfiguration() {
            @Override
            public String getDefaultPassword() {
                return "oh";
            }

            @Override
            public String getDefaultUsername() {
                return "xyz";
            }

            @Override
            public IODispatcherConfiguration getDispatcherConfiguration() {
                return new DefaultIODispatcherConfiguration() {
                    @Override
                    public int getBindPort() {
                        return 8812;
                    }

                    @Override
                    public boolean getPeerNoLinger() {
                        return false;
                    }
                };
            }
        };
    }

}
