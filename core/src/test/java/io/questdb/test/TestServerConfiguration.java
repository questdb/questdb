/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cutlass.http.DefaultHttpContextConfiguration;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpMinServerConfiguration;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.line.tcp.DefaultLineTcpReceiverConfiguration;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.cutlass.line.udp.DefaultLineUdpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.cutlass.pgwire.DefaultPGWireConfiguration;
import io.questdb.cutlass.pgwire.PGAuthenticatorFactory;
import io.questdb.cutlass.pgwire.PGWireConfiguration;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.StationaryMillisClock;
import io.questdb.std.datetime.millitime.MillisecondClock;

public class TestServerConfiguration extends DefaultServerConfiguration {

    private final HttpMinServerConfiguration confHttpMin = new DefaultHttpServerConfiguration() {
        @Override
        public boolean isEnabled() {
            return false;
        }
    };
    private final LineUdpReceiverConfiguration confLineUdp = new DefaultLineUdpReceiverConfiguration() {
        @Override
        public boolean isEnabled() {
            return false;
        }
    };
    private final WorkerPoolConfiguration confWalApplyPool = () -> 0;
    private final boolean enableHttp;
    private final boolean enableLineTcp;
    private final boolean enablePgWire;
    private final FactoryProvider factoryProvider;
    private final PGWireConfiguration confPgWire = new DefaultPGWireConfiguration() {
        @Override
        public PGAuthenticatorFactory getAuthenticatorFactory() {
            return factoryProvider.getPGAuthenticatorFactory();
        }

        @Override
        public SecurityContextFactory getSecurityContextFactory() {
            return factoryProvider.getSecurityContextFactory();
        }

        @Override
        public boolean isEnabled() {
            return enablePgWire;
        }
    };
    private final int workerCountHttp;
    private final HttpServerConfiguration confHttp = new DefaultHttpServerConfiguration(new DefaultHttpContextConfiguration() {
        @Override
        public MillisecondClock getClock() {
            return StationaryMillisClock.INSTANCE;
        }

        @Override
        public SecurityContextFactory getSecurityContextFactory() {
            return factoryProvider.getSecurityContextFactory();
        }
    }) {
        @Override
        public int getWorkerCount() {
            return workerCountHttp;
        }

        @Override
        public boolean isEnabled() {
            return enableHttp;
        }
    };
    private final int workerCountLineTcpIO;
    private final WorkerPoolConfiguration confLineTcpIOPool = new WorkerPoolConfiguration() {
        @Override
        public int getWorkerCount() {
            return workerCountLineTcpIO;
        }
    };
    private final int workerCountLineTcpWriter;
    private final WorkerPoolConfiguration confLineTcpWriterPool = new WorkerPoolConfiguration() {
        @Override
        public int getWorkerCount() {
            return workerCountLineTcpWriter;
        }
    };
    private final LineTcpReceiverConfiguration confLineTcp = new DefaultLineTcpReceiverConfiguration() {
        @Override
        public WorkerPoolConfiguration getIOWorkerPoolConfiguration() {
            return confLineTcpIOPool;
        }

        @Override
        public WorkerPoolConfiguration getWriterWorkerPoolConfiguration() {
            return confLineTcpWriterPool;
        }

        @Override
        public FactoryProvider getFactoryProvider() {
            return factoryProvider;
        }

        @Override
        public boolean isEnabled() {
            return enableLineTcp;
        }
    };
    private int workerCountShared;
    private final WorkerPoolConfiguration confSharedPool = () -> workerCountShared;

    public TestServerConfiguration(
            CharSequence root,
            boolean enableHttp,
            boolean enableLineTcp,
            boolean enablePgWire,
            int workerCountShared,
            int workerCountHttp,
            int workerCountLineTcpIO,
            int workerCountLineTcpWriter,
            FactoryProvider factoryProvider
    ) {
        super(root);
        this.workerCountHttp = workerCountHttp;
        this.workerCountShared = workerCountShared;
        this.enableHttp = enableHttp;
        this.enableLineTcp = enableLineTcp;
        this.enablePgWire = enablePgWire;
        this.workerCountLineTcpIO = workerCountLineTcpIO;
        this.workerCountLineTcpWriter = workerCountLineTcpWriter;
        this.factoryProvider = factoryProvider;
    }

    @Override
    public HttpMinServerConfiguration getHttpMinServerConfiguration() {
        return confHttpMin;
    }

    @Override
    public HttpServerConfiguration getHttpServerConfiguration() {
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
    public PGWireConfiguration getPGWireConfiguration() {
        return confPgWire;
    }

    @Override
    public WorkerPoolConfiguration getWalApplyPoolConfiguration() {
        return confWalApplyPool;
    }

    @Override
    public WorkerPoolConfiguration getWorkerPoolConfiguration() {
        return confSharedPool;
    }
}
