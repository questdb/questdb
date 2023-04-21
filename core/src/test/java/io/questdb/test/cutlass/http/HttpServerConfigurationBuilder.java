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
package io.questdb.test.cutlass.http;

import io.questdb.cutlass.http.*;
import io.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import io.questdb.cutlass.http.processors.StaticContentProcessorConfiguration;
import io.questdb.griffin.SqlParserFactory;
import io.questdb.griffin.SqlParserFactoryImpl;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.FilesFacade;
import io.questdb.std.Numbers;
import io.questdb.std.StationaryMillisClock;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;
import io.questdb.test.std.TestFilesFacadeImpl;

public class HttpServerConfigurationBuilder {
    private boolean allowDeflateBeforeSend;
    private String baseDir;
    private long configuredMaxQueryResponseRowLimit = Long.MAX_VALUE;
    private boolean dumpTraffic;
    private String httpProtocolVersion = "HTTP/1.1 ";
    private long multipartIdleSpinCount = -1;
    private NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
    private int receiveBufferSize = 1024 * 1024;
    private int rerunProcessingQueueSize = 4096;
    private int sendBufferSize = 1024 * 1024;
    private boolean serverKeepAlive = true;

    public DefaultHttpServerConfiguration build() {
        final IODispatcherConfiguration ioDispatcherConfiguration = new DefaultIODispatcherConfiguration() {
            @Override
            public NetworkFacade getNetworkFacade() {
                return nf;
            }
        };

        return new DefaultHttpServerConfiguration() {
            private final JsonQueryProcessorConfiguration jsonQueryProcessorConfiguration = new JsonQueryProcessorConfiguration() {

                @Override
                public MillisecondClock getClock() {
                    return () -> 0;
                }

                @Override
                public int getConnectionCheckFrequency() {
                    return 1_000_000;
                }

                @Override
                public int getDoubleScale() {
                    return Numbers.MAX_SCALE;
                }

                @Override
                public FilesFacade getFilesFacade() {
                    return TestFilesFacadeImpl.INSTANCE;
                }

                @Override
                public int getFloatScale() {
                    return 10;
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
                public SqlParserFactory getSqlParserFactory() {
                    return SqlParserFactoryImpl.INSTANCE;
                }
            };
            private final StaticContentProcessorConfiguration staticContentProcessorConfiguration = new StaticContentProcessorConfiguration() {
                @Override
                public FilesFacade getFilesFacade() {
                    return TestFilesFacadeImpl.INSTANCE;
                }

                @Override
                public CharSequence getIndexFileName() {
                    return null;
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
            };

            @Override
            public IODispatcherConfiguration getDispatcherConfiguration() {
                return ioDispatcherConfiguration;
            }

            @Override
            public HttpContextConfiguration getHttpContextConfiguration() {
                return new DefaultHttpContextConfiguration() {
                    @Override
                    public boolean allowDeflateBeforeSend() {
                        return allowDeflateBeforeSend;
                    }

                    @Override
                    public MillisecondClock getClock() {
                        return StationaryMillisClock.INSTANCE;
                    }

                    @Override
                    public boolean getDumpNetworkTraffic() {
                        return dumpTraffic;
                    }

                    @Override
                    public String getHttpVersion() {
                        return httpProtocolVersion;
                    }

                    @Override
                    public long getMultipartIdleSpinCount() {
                        if (multipartIdleSpinCount < 0) return super.getMultipartIdleSpinCount();
                        return multipartIdleSpinCount;
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
                    public int getSendBufferSize() {
                        return sendBufferSize;
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

    public HttpServerConfigurationBuilder withHttpProtocolVersion(String httpProtocolVersion) {
        this.httpProtocolVersion = httpProtocolVersion;
        return this;
    }

    public HttpServerConfigurationBuilder withMultipartIdleSpinCount(long multipartIdleSpinCount) {
        this.multipartIdleSpinCount = multipartIdleSpinCount;
        return this;
    }

    public HttpServerConfigurationBuilder withNetwork(NetworkFacade nf) {
        this.nf = nf;
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
}
