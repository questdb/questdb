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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SecurityContext;
import io.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.http.processors.StaticContentProcessorConfiguration;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.std.ConcurrentCacheConfiguration;
import io.questdb.std.DefaultConcurrentCacheConfiguration;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;
import io.questdb.std.datetime.Clock;

import java.io.IOException;
import java.io.InputStream;

public class DefaultHttpServerConfiguration extends DefaultIODispatcherConfiguration implements HttpFullFatServerConfiguration {
    protected final MimeTypesCache mimeTypesCache;
    private final HttpContextConfiguration httpContextConfiguration;
    private final JsonQueryProcessorConfiguration jsonQueryProcessorConfiguration = new DefaultJsonQueryProcessorConfiguration() {
    };
    private final LineHttpProcessorConfiguration lineHttpProcessorConfiguration;
    private final StaticContentProcessorConfiguration staticContentProcessorConfiguration = new StaticContentProcessorConfiguration() {
        @Override
        public FilesFacade getFilesFacade() {
            return FilesFacadeImpl.INSTANCE;
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

    public DefaultHttpServerConfiguration(CairoConfiguration cairoConfiguration) {
        this(cairoConfiguration, new DefaultHttpContextConfiguration());
    }

    public DefaultHttpServerConfiguration(CairoConfiguration cairoConfiguration, HttpContextConfiguration httpContextConfiguration) {
        try (InputStream inputStream = DefaultHttpServerConfiguration.class.getResourceAsStream("/io/questdb/site/conf/mime.types")) {
            this.mimeTypesCache = new MimeTypesCache(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.lineHttpProcessorConfiguration = new DefaultLineHttpProcessorConfiguration(cairoConfiguration);
        this.httpContextConfiguration = httpContextConfiguration;
    }

    @Override
    public ConcurrentCacheConfiguration getConcurrentCacheConfiguration() {
        return DefaultConcurrentCacheConfiguration.DEFAULT;
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
    public boolean isQueryCacheEnabled() {
        return true;
    }

    @Override
    public boolean isSettingsReadOnly() {
        return false;
    }

    @Override
    public boolean preAllocateBuffers() {
        return false;
    }

    public static class DefaultLineHttpProcessorConfiguration implements LineHttpProcessorConfiguration {
        private final CairoConfiguration cairoConfiguration;

        public DefaultLineHttpProcessorConfiguration(CairoConfiguration cairoConfiguration) {
            this.cairoConfiguration = cairoConfiguration;
        }

        @Override
        public boolean autoCreateNewColumns() {
            return true;
        }

        @Override
        public boolean autoCreateNewTables() {
            return true;
        }

        @Override
        public CairoConfiguration getCairoConfiguration() {
            return cairoConfiguration;
        }

        @Override
        public short getDefaultColumnTypeForFloat() {
            return ColumnType.DOUBLE;
        }

        @Override
        public short getDefaultColumnTypeForInteger() {
            return ColumnType.LONG;
        }

        @Override
        public int getDefaultColumnTypeForTimestamp() {
            return ColumnType.TIMESTAMP_MICRO;
        }

        @Override
        public int getDefaultPartitionBy() {
            return PartitionBy.DAY;
        }

        @Override
        public CharSequence getInfluxPingVersion() {
            return "v2.7.4";
        }

        @Override
        public long getMaxRecvBufferSize() {
            return Numbers.SIZE_1GB;
        }

        @Override
        public io.questdb.std.datetime.Clock getMicrosecondClock() {
            return MicrosecondClockImpl.INSTANCE;
        }

        @Override
        public long getSymbolCacheWaitUsBeforeReload() {
            return 500_000;
        }

        @Override
        public byte getTimestampUnit() {
            return CommonUtils.TIMESTAMP_UNIT_NANOS;
        }

        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public boolean isStringToCharCastAllowed() {
            return false;
        }

        @Override
        public boolean isUseLegacyStringDefault() {
            return true;
        }

        @Override
        public boolean logMessageOnError() {
            return true;
        }
    }

    public class DefaultJsonQueryProcessorConfiguration implements JsonQueryProcessorConfiguration {

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
            return FilesFacadeImpl.INSTANCE;
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
        public Clock getNanosecondClock() {
            return httpContextConfiguration.getNanosecondClock();
        }
    }
}
