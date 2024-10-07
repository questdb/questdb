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

package io.questdb.cutlass.http;

import io.questdb.DefaultFactoryProvider;
import io.questdb.FactoryProvider;
import io.questdb.cairo.SecurityContext;
import io.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.http.processors.StaticContentProcessorConfiguration;
import io.questdb.cutlass.line.LineTcpTimestampAdapter;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.NanosecondClock;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;

import java.io.IOException;
import java.io.InputStream;

public class DefaultHttpServerConfiguration implements HttpServerConfiguration {

    protected final MimeTypesCache mimeTypesCache;
    private final IODispatcherConfiguration dispatcherConfiguration;
    private final HttpContextConfiguration httpContextConfiguration;
    private final JsonQueryProcessorConfiguration jsonQueryProcessorConfiguration = new DefaultJsonQueryProcessorConfiguration() {
    };
    private final LineHttpProcessorConfiguration lineHttpProcessorConfiguration = new DefaultLineHttpProcessorConfiguration();
    private final StaticContentProcessorConfiguration staticContentProcessorConfiguration = new StaticContentProcessorConfiguration() {
        @Override
        public FilesFacade getFilesFacade() {
            return FilesFacadeImpl.INSTANCE;
        }

        @Override
        public CharSequence getIndexFileName() {
            return "index.html";
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
            return ".";
        }

        @Override
        public byte getRequiredAuthType() {
            return SecurityContext.AUTH_TYPE_CREDENTIALS;
        }
    };

    public DefaultHttpServerConfiguration() {
        this(new DefaultHttpContextConfiguration());
    }

    public DefaultHttpServerConfiguration(HttpContextConfiguration httpContextConfiguration) {
        this(httpContextConfiguration, DefaultIODispatcherConfiguration.INSTANCE);
    }

    public DefaultHttpServerConfiguration(
            HttpContextConfiguration httpContextConfiguration,
            IODispatcherConfiguration dispatcherConfiguration
    ) {
        try (InputStream inputStream = DefaultHttpServerConfiguration.class.getResourceAsStream("/io/questdb/site/conf/mime.types")) {
            this.mimeTypesCache = new MimeTypesCache(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.httpContextConfiguration = httpContextConfiguration;
        this.dispatcherConfiguration = dispatcherConfiguration;
    }

    @Override
    public IODispatcherConfiguration getDispatcherConfiguration() {
        return dispatcherConfiguration;
    }

    @Override
    public FactoryProvider getFactoryProvider() {
        return DefaultFactoryProvider.INSTANCE;
    }

    @Override
    public HttpContextConfiguration getHttpContextConfiguration() {
        return httpContextConfiguration;
    }

    @Override
    public JsonQueryProcessorConfiguration getJsonQueryProcessorConfiguration() {
        return jsonQueryProcessorConfiguration;
    }

    @Override
    public LineHttpProcessorConfiguration getLineHttpProcessorConfiguration() {
        return lineHttpProcessorConfiguration;
    }

    @Override
    public String getPassword() {
        return "";
    }

    @Override
    public String getPoolName() {
        return "http";
    }

    @Override
    public int getQueryCacheBlockCount() {
        return 2;
    }

    @Override
    public int getQueryCacheRowCount() {
        return 8;
    }

    @Override
    public byte getRequiredAuthType() {
        return SecurityContext.AUTH_TYPE_NONE;
    }

    @Override
    public StaticContentProcessorConfiguration getStaticContentProcessorConfiguration() {
        return staticContentProcessorConfiguration;
    }

    @Override
    public String getUsername() {
        return "";
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
                return 4096;
            }

            @Override
            public long getMaxWaitCapMs() {
                return 1000;
            }
        };
    }

    @Override
    public int getWorkerCount() {
        return 2;
    }

    @Override
    public boolean isPessimisticHealthCheckEnabled() {
        return false;
    }

    @Override
    public boolean preAllocateBuffers() {
        return false;
    }

    @Override
    public boolean isQueryCacheEnabled() {
        return true;
    }

    public class DefaultJsonQueryProcessorConfiguration implements JsonQueryProcessorConfiguration {
        @Override
        public int getConnectionCheckFrequency() {
            return 1_000_000;
        }

        @Override
        public int getDoubleScale() {
            return Numbers.MAX_DOUBLE_SCALE;
        }

        @Override
        public FactoryProvider getFactoryProvider() {
            return DefaultFactoryProvider.INSTANCE;
        }

        @Override
        public FilesFacade getFilesFacade() {
            return FilesFacadeImpl.INSTANCE;
        }

        @Override
        public int getFloatScale() {
            return Numbers.MAX_FLOAT_SCALE;
        }

        @Override
        public CharSequence getKeepAliveHeader() {
            return "Keep-Alive: timeout=5, max=10000\r\n";
        }

        @Override
        public long getMaxQueryResponseRowLimit() {
            return Long.MAX_VALUE;
        }

        @Override
        public MillisecondClock getMillisecondClock() {
            return httpContextConfiguration.getMillisecondClock();
        }

        @Override
        public NanosecondClock getNanosecondClock() {
            return httpContextConfiguration.getNanosecondClock();
        }
    }

    public class DefaultLineHttpProcessorConfiguration implements LineHttpProcessorConfiguration {
        @Override
        public boolean autoCreateNewColumns() {
            return lineHttpProcessorConfiguration.autoCreateNewColumns();
        }

        @Override
        public boolean autoCreateNewTables() {
            return lineHttpProcessorConfiguration.autoCreateNewTables();
        }

        @Override
        public short getDefaultColumnTypeForFloat() {
            return lineHttpProcessorConfiguration.getDefaultColumnTypeForInteger();
        }

        @Override
        public short getDefaultColumnTypeForInteger() {
            return lineHttpProcessorConfiguration.getDefaultColumnTypeForInteger();
        }

        @Override
        public int getDefaultPartitionBy() {
            return lineHttpProcessorConfiguration.getDefaultPartitionBy();
        }

        @Override
        public CharSequence getInfluxPingVersion() {
            return "v2.7.4";
        }

        @Override
        public MicrosecondClock getMicrosecondClock() {
            return lineHttpProcessorConfiguration.getMicrosecondClock();
        }

        @Override
        public long getSymbolCacheWaitUsBeforeReload() {
            return lineHttpProcessorConfiguration.getSymbolCacheWaitUsBeforeReload();
        }

        @Override
        public LineTcpTimestampAdapter getTimestampAdapter() {
            return lineHttpProcessorConfiguration.getTimestampAdapter();
        }

        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public boolean isStringToCharCastAllowed() {
            return lineHttpProcessorConfiguration.isStringToCharCastAllowed();
        }

        @Override
        public boolean isUseLegacyStringDefault() {
            return true;
        }
    }
}
