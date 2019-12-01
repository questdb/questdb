/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cutlass.http.processors.DefaultTextImportProcessorConfiguration;
import io.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import io.questdb.cutlass.http.processors.StaticContentProcessorConfiguration;
import io.questdb.cutlass.http.processors.TextImportProcessorConfiguration;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.std.time.MillisecondClock;
import io.questdb.std.time.MillisecondClockImpl;

class DefaultHttpServerConfiguration implements HttpServerConfiguration {
    protected final MimeTypesCache mimeTypesCache;
    private final IODispatcherConfiguration dispatcherConfiguration = new DefaultIODispatcherConfiguration();
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
    private final TextImportProcessorConfiguration textImportProcessorConfiguration = new DefaultTextImportProcessorConfiguration();
    private final JsonQueryProcessorConfiguration jsonQueryProcessorConfiguration = new JsonQueryProcessorConfiguration() {
        @Override
        public MillisecondClock getClock() {
            return DefaultHttpServerConfiguration.this.getClock();
        }

        @Override
        public int getConnectionCheckFrequency() {
            return 1_000_000;
        }

        @Override
        public int getDoubleScale() {
            return 10;
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
        public CharSequence getKeepAliveHeader() {
            return "Keep-Alive: timeout=5, max=10000\r\n";
        }
    };

    public DefaultHttpServerConfiguration() {
        String defaultFilePath = this.getClass().getResource("/site/conf/mime.types").getFile();
        if (Os.type == Os.WINDOWS) {
            // on Windows Java returns "/C:/dir/file". This leading slash is Java specific and doesn't bode well
            // with OS file open methods.
            defaultFilePath = defaultFilePath.substring(1);
        }
        try (Path path = new Path().of(defaultFilePath).$()) {
            this.mimeTypesCache = new MimeTypesCache(FilesFacadeImpl.INSTANCE, path);
        }
    }

    @Override
    public int getConnectionPoolInitialCapacity() {
        return 16;
    }

    @Override
    public int getConnectionStringPoolCapacity() {
        return 128;
    }

    @Override
    public int getMultipartHeaderBufferSize() {
        return 512;
    }

    @Override
    public long getMultipartIdleSpinCount() {
        return 10_000;
    }

    @Override
    public int getRecvBufferSize() {
        return 1024 * 1024;
    }

    @Override
    public int getRequestHeaderBufferSize() {
        return 1024;
    }

    @Override
    public int getResponseHeaderBufferSize() {
        return 1024;
    }

    @Override
    public MillisecondClock getClock() {
        return MillisecondClockImpl.INSTANCE;
    }

    @Override
    public IODispatcherConfiguration getDispatcherConfiguration() {
        return dispatcherConfiguration;
    }

    @Override
    public StaticContentProcessorConfiguration getStaticContentProcessorConfiguration() {
        return staticContentProcessorConfiguration;
    }

    @Override
    public TextImportProcessorConfiguration getTextImportProcessorConfiguration() {
        return textImportProcessorConfiguration;
    }

    @Override
    public JsonQueryProcessorConfiguration getJsonQueryProcessorConfiguration() {
        return jsonQueryProcessorConfiguration;
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
    public int[] getWorkerAffinity() {
        return new int[]{-1, -1};
    }

    @Override
    public int getSendBufferSize() {
        return 1024 * 1024;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public boolean getDumpNetworkTraffic() {
        return false;
    }

    @Override
    public boolean allowDeflateBeforeSend() {
        return false;
    }
}
