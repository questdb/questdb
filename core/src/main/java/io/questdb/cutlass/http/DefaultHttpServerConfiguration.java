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

package io.questdb.cutlass.http;

import io.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import io.questdb.cutlass.http.processors.StaticContentProcessorConfiguration;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;
import io.questdb.std.str.Path;

public class DefaultHttpServerConfiguration implements HttpServerConfiguration {
    protected final MimeTypesCache mimeTypesCache;
    private final IODispatcherConfiguration dispatcherConfiguration;
    private final HttpContextConfiguration httpContextConfiguration;

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
        public MimeTypesCache getMimeTypesCache() {
            return mimeTypesCache;
        }

        @Override
        public CharSequence getPublicDirectory() {
            return ".";
        }

        @Override
        public String getKeepAliveHeader() {
            return null;
        }
    };
    private final JsonQueryProcessorConfiguration jsonQueryProcessorConfiguration = new JsonQueryProcessorConfiguration() {
        @Override
        public MillisecondClock getClock() {
            return httpContextConfiguration.getClock();
        }

        @Override
        public int getConnectionCheckFrequency() {
            return 1_000_000;
        }

        @Override
        public FilesFacade getFilesFacade() {
            return FilesFacadeImpl.INSTANCE;
        }

        @Override
        public int getFloatScale() {
            return 10;
        }

        @Override
        public int getDoubleScale() {
            return Numbers.MAX_SCALE;
        }

        @Override
        public CharSequence getKeepAliveHeader() {
            return "Keep-Alive: timeout=5, max=10000\r\n";
        }

        @Override
        public long getMaxQueryResponseRowLimit() {
            return Long.MAX_VALUE;
        }
    };

    public DefaultHttpServerConfiguration() {
        this(new DefaultHttpContextConfiguration());
    }

    public DefaultHttpServerConfiguration(HttpContextConfiguration httpContextConfiguration) {
        this(httpContextConfiguration, new DefaultIODispatcherConfiguration());
    }

    public DefaultHttpServerConfiguration(HttpContextConfiguration httpContextConfiguration, IODispatcherConfiguration ioDispatcherConfiguration) {
        String defaultFilePath = this.getClass().getResource("/site/conf/mime.types").getFile();
        if (Os.type == Os.WINDOWS) {
            // on Windows Java returns "/C:/dir/file". This leading slash is Java specific and doesn't bode well
            // with OS file open methods.
            defaultFilePath = defaultFilePath.substring(1);
        }
        try (Path path = new Path().of(defaultFilePath).$()) {
            this.mimeTypesCache = new MimeTypesCache(FilesFacadeImpl.INSTANCE, path);
        }
        this.httpContextConfiguration = httpContextConfiguration;
        this.dispatcherConfiguration = ioDispatcherConfiguration;
    }

    @Override
    public IODispatcherConfiguration getDispatcherConfiguration() {
        return dispatcherConfiguration;
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
    public boolean isQueryCacheEnabled() {
        return true;
    }

    @Override
    public int getQueryCacheBlockCount() {
        return 4;
    }

    @Override
    public int getQueryCacheRowCount() {
        return 4;
    }

    @Override
    public WaitProcessorConfiguration getWaitProcessorConfiguration() {
        return new WaitProcessorConfiguration() {
            @Override
            public MillisecondClock getClock() {
                return MillisecondClockImpl.INSTANCE;
            }

            @Override
            public long getMaxWaitCapMs() {
                return 1000;
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
        };
    }

    @Override
    public StaticContentProcessorConfiguration getStaticContentProcessorConfiguration() {
        return staticContentProcessorConfiguration;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public int[] getWorkerAffinity() {
        return new int[]{-1, -1};
    }

    @Override
    public int getWorkerCount() {
        return 2;
    }

    @Override
    public boolean haltOnError() {
        return false;
    }

    @Override
    public String getPoolName() {
        return "http";
    }
}
