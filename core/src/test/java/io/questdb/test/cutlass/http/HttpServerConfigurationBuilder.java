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
package io.questdb.test.cutlass.http;

import io.questdb.DefaultFactoryProvider;
import io.questdb.FactoryProvider;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.SecurityContext;
import io.questdb.cutlass.http.DefaultHttpContextConfiguration;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpContextConfiguration;
import io.questdb.cutlass.http.MimeTypesCache;
import io.questdb.cutlass.http.WaitProcessorConfiguration;
import io.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import io.questdb.cutlass.http.processors.StaticContentProcessorConfiguration;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.FilesFacade;
import io.questdb.std.StationaryMillisClock;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.nanotime.StationaryNanosClock;
import io.questdb.test.std.TestFilesFacadeImpl;

public class HttpServerConfigurationBuilder {
    private boolean allowDeflateBeforeSend;
    private String baseDir;
    private long configuredMaxQueryResponseRowLimit = Long.MAX_VALUE;
    private boolean dumpTraffic;
    private FactoryProvider factoryProvider;
    private byte httpHealthCheckAuthType = SecurityContext.AUTH_TYPE_NONE;
    private String httpProtocolVersion = "HTTP/1.1 ";
    private byte httpStaticContentAuthType = SecurityContext.AUTH_TYPE_NONE;
    private long multipartIdleSpinCount = -1;
    private Clock nanosecondClock = StationaryNanosClock.INSTANCE;
    private NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
    private boolean pessimisticHealthCheck = false;
    private int port = -1;
    private int receiveBufferSize = 1024 * 1024;
    private int rerunProcessingQueueSize = 4096;
    private int sendBufferSize = 1024 * 1024;
    private boolean serverKeepAlive = true;
    private int tcpSndBufSize;
    private int workerCount;

    public DefaultHttpServerConfiguration build(CairoConfiguration cairoConfiguration) {
        return new DefaultHttpServerConfiguration(cairoConfiguration) {
            private final JsonQueryProcessorConfiguration jsonQueryProcessorConfiguration = new JsonQueryProcessorConfiguration() {
                @Override
                public int getConnectionCheckFrequency() {
                    return 1_000_000;
                }

                @Override
                public FactoryProvider getFactoryProvider() {
                    return DefaultFactoryProvider.INSTANCE;
                }

                @Override
                public FilesFacade getFilesFacade() {
                    return TestFilesFacadeImpl.INSTANCE;
                }

                @Override
                public CharSequence getKeepAliveHeader() {
                    return "Keep-Alive: timeout=5, max=10000\r\n";
                }

                @Override
                public long getMaxQueryResponseRowLimit() {
                    return configuredMaxQueryResponseRowLimit;
                }

                @Override
                public MillisecondClock getMillisecondClock() {
                    return StationaryMillisClock.INSTANCE;
                }

                @Override
                public Clock getNanosecondClock() {
                    return nanosecondClock;
                }
            };
            private final StaticContentProcessorConfiguration staticContentProcessorConfiguration = new StaticContentProcessorConfiguration() {
                @Override
                public FilesFacade getFilesFacade() {
                    return TestFilesFacadeImpl.INSTANCE;
                }

                @Override
                public String getKeepAliveHeader() {
                    return null;
                }

                @Override
                public MimeTypesCache getMimeTypesCache() {
                    return mimeTypesCache;
                }

                @Override
                public CharSequence getPublicDirectory() {
                    return baseDir;
                }

                @Override
                public byte getRequiredAuthType() {
                    return httpStaticContentAuthType;
                }
            };

            @Override
            public int getBindPort() {
                return port != -1 ? port : super.getBindPort();
            }

            @Override
            public HttpContextConfiguration getHttpContextConfiguration() {
                return new DefaultHttpContextConfiguration() {
                    @Override
                    public boolean allowDeflateBeforeSend() {
                        return allowDeflateBeforeSend;
                    }

                    @Override
                    public boolean getDumpNetworkTraffic() {
                        return dumpTraffic;
                    }

                    @Override
                    public FactoryProvider getFactoryProvider() {
                        return factoryProvider != null ? factoryProvider : super.getFactoryProvider();
                    }

                    @Override
                    public String getHttpVersion() {
                        return httpProtocolVersion;
                    }

                    @Override
                    public MillisecondClock getMillisecondClock() {
                        return StationaryMillisClock.INSTANCE;
                    }

                    @Override
                    public long getMultipartIdleSpinCount() {
                        if (multipartIdleSpinCount < 0) return super.getMultipartIdleSpinCount();
                        return multipartIdleSpinCount;
                    }

                    @Override
                    public Clock getNanosecondClock() {
                        return nanosecondClock;
                    }

                    @Override
                    public NetworkFacade getNetworkFacade() {
                        return nf;
                    }

                    @Override
                    public boolean getServerKeepAlive() {
                        return serverKeepAlive;
                    }
                };
            }

            @Override
            public JsonQueryProcessorConfiguration getJsonQueryProcessorConfiguration() {
                return jsonQueryProcessorConfiguration;
            }

            @Override
            public int getNetSendBufferSize() {
                return tcpSndBufSize == 0 ? super.getSendBufferSize() : tcpSndBufSize;
            }

            @Override
            public NetworkFacade getNetworkFacade() {
                return nf;
            }

            @Override
            public int getRecvBufferSize() {
                return receiveBufferSize;
            }

            @Override
            public byte getRequiredAuthType() {
                return httpHealthCheckAuthType;
            }

            @Override
            public int getSendBufferSize() {
                return sendBufferSize == 0 ? super.getSendBufferSize() : sendBufferSize;
            }

            @Override
            public StaticContentProcessorConfiguration getStaticContentProcessorConfiguration() {
                return staticContentProcessorConfiguration;
            }

            @Override
            public WaitProcessorConfiguration getWaitProcessorConfiguration() {
                return new WaitProcessorConfiguration() {
                    @Override
                    public MillisecondClock getClock() {
                        return MillisecondClockImpl.INSTANCE;
                    }

                    @Override
                    public double getExponentialWaitMultiplier() {
                        return 2.0;
                    }

                    @Override
                    public int getInitialWaitQueueSize() {
                        return 64;
                    }

                    @Override
                    public int getMaxProcessingQueueSize() {
                        return rerunProcessingQueueSize;
                    }

                    @Override
                    public long getMaxWaitCapMs() {
                        return 1000;
                    }
                };
            }

            @Override
            public int getWorkerCount() {
                return workerCount == 0 ? super.getWorkerCount() : workerCount;
            }

            @Override
            public boolean isPessimisticHealthCheckEnabled() {
                return pessimisticHealthCheck;
            }
        };
    }

    public HttpServerConfigurationBuilder withAllowDeflateBeforeSend(boolean allowDeflateBeforeSend) {
        this.allowDeflateBeforeSend = allowDeflateBeforeSend;
        return this;
    }

    public HttpServerConfigurationBuilder withBaseDir(String baseDir) {
        this.baseDir = baseDir;
        return this;
    }

    public HttpServerConfigurationBuilder withConfiguredMaxQueryResponseRowLimit(long configuredMaxQueryResponseRowLimit) {
        this.configuredMaxQueryResponseRowLimit = configuredMaxQueryResponseRowLimit;
        return this;
    }

    public HttpServerConfigurationBuilder withDumpingTraffic(boolean dumpTraffic) {
        this.dumpTraffic = dumpTraffic;
        return this;
    }

    public HttpServerConfigurationBuilder withFactoryProvider(FactoryProvider factoryProvider) {
        this.factoryProvider = factoryProvider;
        return this;
    }

    public HttpServerConfigurationBuilder withHealthCheckAuthRequired(byte httpHealthCheckAuthType) {
        this.httpHealthCheckAuthType = httpHealthCheckAuthType;
        return this;
    }

    public HttpServerConfigurationBuilder withHttpProtocolVersion(String httpProtocolVersion) {
        this.httpProtocolVersion = httpProtocolVersion;
        return this;
    }

    public HttpServerConfigurationBuilder withMultipartIdleSpinCount(long multipartIdleSpinCount) {
        this.multipartIdleSpinCount = multipartIdleSpinCount;
        return this;
    }

    public HttpServerConfigurationBuilder withNanosClock(Clock nanosecondClock) {
        this.nanosecondClock = nanosecondClock;
        return this;
    }

    public HttpServerConfigurationBuilder withNetwork(NetworkFacade nf) {
        this.nf = nf;
        return this;
    }

    public HttpServerConfigurationBuilder withPessimisticHealthCheck(boolean pessimisticHealthCheck) {
        this.pessimisticHealthCheck = pessimisticHealthCheck;
        return this;
    }

    public HttpServerConfigurationBuilder withPort(int port) {
        this.port = port;
        return this;
    }

    public HttpServerConfigurationBuilder withReceiveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
        return this;
    }

    public HttpServerConfigurationBuilder withRerunProcessingQueueSize(int rerunProcessingQueueSize) {
        this.rerunProcessingQueueSize = rerunProcessingQueueSize;
        return this;
    }

    public HttpServerConfigurationBuilder withSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
        return this;
    }

    public HttpServerConfigurationBuilder withServerKeepAlive(boolean serverKeepAlive) {
        this.serverKeepAlive = serverKeepAlive;
        return this;
    }

    public HttpServerConfigurationBuilder withStaticContentAuthRequired(byte httpStaticContentAuthType) {
        this.httpStaticContentAuthType = httpStaticContentAuthType;
        return this;
    }

    public HttpServerConfigurationBuilder withTcpSndBufSize(int tcpSndBufSize) {
        this.tcpSndBufSize = tcpSndBufSize;
        return this;
    }

    public HttpServerConfigurationBuilder withWorkerCount(int workerCount) {
        this.workerCount = workerCount;
        return this;
    }
}
