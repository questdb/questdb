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

package io.questdb.test;

import io.questdb.DefaultServerConfiguration;
import io.questdb.FactoryProvider;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.cutlass.http.DefaultHttpContextConfiguration;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import io.questdb.cutlass.line.tcp.DefaultLineTcpReceiverConfiguration;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.cutlass.line.udp.DefaultLineUdpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.cutlass.pgwire.DefaultPGConfiguration;
import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.Numbers;
import io.questdb.std.StationaryMillisClock;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.nanotime.StationaryNanosClock;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;

import java.util.function.LongSupplier;

public class TestServerConfiguration extends DefaultServerConfiguration {

    public static final long importID = 100L;

    @SuppressWarnings("unused")
    public static final String importIDStr = Numbers.toHexStrPadded(importID);
    private final CairoConfiguration cairoConfiguration;
    private final HttpFullFatServerConfiguration confHttp;
    private final HttpServerConfiguration confHttpMin;
    private final LineTcpReceiverConfiguration confLineTcp;
    private final WorkerPoolConfiguration confLineTcpIOPool;
    private final WorkerPoolConfiguration confLineTcpWriterPool;
    private final LineUdpReceiverConfiguration confLineUdp = new DefaultLineUdpReceiverConfiguration() {
        @Override
        public boolean isEnabled() {
            return false;
        }
    };
    private final WorkerPoolConfiguration confMatViewRefreshPool;
    private final WorkerPoolConfiguration confSharedPool;
    private final WorkerPoolConfiguration confWalApplyPool;
    private final boolean enablePgWire;
    private final FactoryProvider factoryProvider;
    private final PGConfiguration confPgWire = new DefaultPGConfiguration() {
        @Override
        public FactoryProvider getFactoryProvider() {
            return factoryProvider;
        }

        @Override
        public boolean isEnabled() {
            return enablePgWire;
        }
    };

    public TestServerConfiguration(
            CharSequence dbRoot,
            CharSequence installRoot,
            boolean enableHttp,
            boolean enableLineTcp,
            boolean enablePgWire,
            int workerCountShared,
            int workerCountHttp,
            int workerCountLineTcpIO,
            int workerCountLineTcpWriter,
            FactoryProvider factoryProvider
    ) {
        super(dbRoot, installRoot);
        this.enablePgWire = enablePgWire;
        this.factoryProvider = factoryProvider;
        final SqlExecutionCircuitBreakerConfiguration circuitBreakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
            // do not check connection for SQLs executed via embedded API
            @Override
            public boolean checkConnection() {
                return false;
            }
        };
        this.cairoConfiguration = new DefaultCairoConfiguration(dbRoot) {
            @Override
            public @NotNull SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
                return circuitBreakerConfiguration;
            }

            // fix import ID
            @Override
            public @NotNull LongSupplier getCopyIDSupplier() {
                return () -> importID;
            }

            @Override
            public CharSequence getSqlCopyInputRoot() {
                return TestUtils.getCsvRoot();
            }
        };
        this.confHttp = new DefaultHttpServerConfiguration(
                cairoConfiguration,
                new DefaultHttpContextConfiguration() {
                    @Override
                    public FactoryProvider getFactoryProvider() {
                        return factoryProvider;
                    }

                    @Override
                    public MillisecondClock getMillisecondClock() {
                        return StationaryMillisClock.INSTANCE;
                    }

                    @Override
                    public Clock getNanosecondClock() {
                        return StationaryNanosClock.INSTANCE;
                    }
                }) {
            @Override
            public FactoryProvider getFactoryProvider() {
                return factoryProvider;
            }

            @Override
            public JsonQueryProcessorConfiguration getJsonQueryProcessorConfiguration() {
                return new DefaultJsonQueryProcessorConfiguration() {
                    @Override
                    public FactoryProvider getFactoryProvider() {
                        return factoryProvider;
                    }
                };
            }

            @Override
            public int getWorkerCount() {
                return workerCountHttp;
            }

            @Override
            public boolean isEnabled() {
                return enableHttp;
            }
        };

        this.confHttpMin = new DefaultHttpServerConfiguration(cairoConfiguration) {
            @Override
            public boolean isEnabled() {
                return false;
            }
        };


        this.confLineTcp = new DefaultLineTcpReceiverConfiguration(cairoConfiguration) {
            @Override
            public FactoryProvider getFactoryProvider() {
                return factoryProvider;
            }

            @Override
            public WorkerPoolConfiguration getNetworkWorkerPoolConfiguration() {
                return confLineTcpIOPool;
            }

            @Override
            public WorkerPoolConfiguration getWriterWorkerPoolConfiguration() {
                return confLineTcpWriterPool;
            }

            @Override
            public boolean isEnabled() {
                return enableLineTcp;
            }
        };

        this.confMatViewRefreshPool = () -> 0; // shared pool
        this.confWalApplyPool = () -> 0;
        this.confSharedPool = () -> workerCountShared;
        this.confLineTcpIOPool = () -> workerCountLineTcpIO;
        this.confLineTcpWriterPool = () -> workerCountLineTcpWriter;
    }

    @Override
    public CairoConfiguration getCairoConfiguration() {
        return cairoConfiguration;
    }

    @Override
    public FactoryProvider getFactoryProvider() {
        return factoryProvider;
    }

    @Override
    public HttpServerConfiguration getHttpMinServerConfiguration() {
        return confHttpMin;
    }

    @Override
    public HttpFullFatServerConfiguration getHttpServerConfiguration() {
        return confHttp;
    }

    @Override
    public LineTcpReceiverConfiguration getLineTcpReceiverConfiguration() {
        return confLineTcp;
    }

    @Override
    public LineUdpReceiverConfiguration getLineUdpReceiverConfiguration() {
        return confLineUdp;
    }

    @Override
    public WorkerPoolConfiguration getMatViewRefreshPoolConfiguration() {
        return confMatViewRefreshPool;
    }

    @Override
    public PGConfiguration getPGWireConfiguration() {
        return confPgWire;
    }

    @Override
    public WorkerPoolConfiguration getWalApplyPoolConfiguration() {
        return confWalApplyPool;
    }

    @Override
    public WorkerPoolConfiguration getSharedWorkerPoolNetworkConfiguration() {
        return confSharedPool;
    }
}
