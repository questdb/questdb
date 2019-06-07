/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cutlass.http;

import com.questdb.cutlass.http.processors.StaticContentProcessorConfiguration;
import com.questdb.cutlass.http.processors.TextImportProcessorConfiguration;
import com.questdb.cutlass.text.TextConfiguration;
import com.questdb.network.*;
import com.questdb.std.FilesFacade;
import com.questdb.std.FilesFacadeImpl;
import com.questdb.std.Numbers;
import com.questdb.std.NumericException;
import com.questdb.std.time.MillisecondClock;
import com.questdb.std.time.MillisecondClockImpl;

import java.util.Properties;

public class PropHttpServerConfiguration implements HttpServerConfiguration {

    private final IODispatcherConfiguration ioDispatcherConfiguration = new PropIODispatcherConfiguration();
    private final TextImportProcessorConfiguration textImportProcessorConfiguration = new PropTextImportProcessorConfiguration();
    private final StaticContentProcessorConfiguration staticContentProcessorConfiguration = new PropStaticContentProcessorConfiguration();
    private final PropTextConfiguration textConfiguration = new PropTextConfiguration();
    private final int connectionPoolInitialSize;
    private final int connectionStringPoolSize;
    private final int multipartHeaderBufferSize;
    private final long multipartIdleSpinCount;
    private final int recvBufferSize;
    private final int requestHeaderBufferSize;
    private final int responseHeaderBufferSize;
    private final int workerCount;
    private final int sendBufferSize;
    private final CharSequence indexFileName;
    private final CharSequence publicDirectory;
    private final boolean abortBrokenUploads;
    private final int activeConnectionLimit;
    private final CharSequence bindIPv4Address;
    private final int bindPort;
    private final int eventCapacity;
    private final int ioQueueCapacity;
    private final long idleConnectionTimeout;
    private final int interestQueueCapacity;
    private final int listenBacklog;
    private final int sndBufSize;
    private final int rcvBufSize;
    private final String adapterSetConfigurationFileName;
    private final int dateAdapterPoolSize;
    private final int jsonCacheLimit;
    private final int jsonCacheSize;
    private final double maxRequiredDelimiterStdDev;
    private final int metadataStringPoolSize;
    private final long rollBufferLimit;
    private final long rollBufferSize;
    private final int textAnalysisMaxLines;
    private final int textLexerStringPoolSize;
    private final int timestampAdapterPoolSize;
    private final int utf8SinkCapacity;

    public PropHttpServerConfiguration(Properties properties) {
        this.connectionPoolInitialSize = getInt(properties, "http.connection.pool.initial.size");
        this.connectionStringPoolSize = getInt(properties, "http.connection.string.pool.size");
        this.multipartHeaderBufferSize = getInt(properties, "http.multipart.header.buffer.size");
        this.multipartIdleSpinCount = Long.parseLong(properties.getProperty("http.multipart.idle.spin.count"));
        this.recvBufferSize = getInt(properties, "http.receive.buffer.size");
        this.requestHeaderBufferSize = getInt(properties, "http.request.header.buffer.size");
        this.responseHeaderBufferSize = getInt(properties, "http.response.header.buffer.size");
        this.workerCount = getInt(properties, "http.worker.count");
        this.sendBufferSize = getInt(properties, "http.send.buffer.size");
        this.indexFileName = getString(properties, "http.static.index.file.name");
        this.publicDirectory = getString(properties, "http.static.pubic.directory");
        this.abortBrokenUploads = Boolean.parseBoolean(properties.getProperty("http.text.abort.broken.uploads"));
        this.activeConnectionLimit = getInt(properties, "http.net.active.connection.limit");
        this.eventCapacity = getInt(properties, "http.net.event.capacity");
        this.ioQueueCapacity = getInt(properties, "http.net.io.queue.capacity");
        this.idleConnectionTimeout = Long.parseLong(properties.getProperty("http.net.idle.connection.timeout"));
        this.interestQueueCapacity = getInt(properties, "http.net.interest.queue.capacity");
        this.listenBacklog = getInt(properties, "http.net.listen.backlog");
        this.sndBufSize = getInt(properties, "http.net.snd.buf.size");
        this.rcvBufSize = getInt(properties, "http.net.rcv.buf.size");
        this.adapterSetConfigurationFileName = getString(properties, "http.text.adapter.set.config");
        this.dateAdapterPoolSize = getInt(properties, "http.text.date.adapter.pool.size");
        this.jsonCacheLimit = getInt(properties, "http.text.json.cache.limit");
        this.jsonCacheSize = getInt(properties, "http.text.json.cache.size");
        this.maxRequiredDelimiterStdDev = Double.parseDouble(properties.getProperty("http.text.max.required.delimiter.stddev"));
        this.metadataStringPoolSize = getInt(properties, "http.text.metadata.string.pool.size");
        this.rollBufferLimit = Long.parseLong(properties.getProperty("http.text.roll.buffer.limit"));
        this.rollBufferSize = Long.getLong(properties.getProperty("http.text.roll.buffer.size"));
        this.textAnalysisMaxLines = getInt(properties, "http.text.analysis.max.lines");
        this.textLexerStringPoolSize = getInt(properties, "http.text.lexer.string.pool.size");
        this.timestampAdapterPoolSize = getInt(properties, "http.text.timestamp.adapter.pool.size");
        this.utf8SinkCapacity = getInt(properties, "http.text.utf8.sink.capacity");
        final String httpBindTo = getString(properties, "http.bind.to");
        final int colonIndex = httpBindTo.indexOf(':');
        if (colonIndex == -1) {
            throw new IllegalStateException();
        }
        this.bindIPv4Address = httpBindTo.substring(0, colonIndex);
        this.bindPort = Integer.parseInt(httpBindTo.substring(colonIndex + 1));

    }

    @Override
    public int getConnectionPoolInitialSize() {
        return connectionPoolInitialSize;
    }

    @Override
    public int getConnectionStringPoolSize() {
        return connectionStringPoolSize;
    }

    @Override
    public int getMultipartHeaderBufferSize() {
        return multipartHeaderBufferSize;
    }

    @Override
    public long getMultipartIdleSpinCount() {
        return multipartIdleSpinCount;
    }

    @Override
    public int getRecvBufferSize() {
        return recvBufferSize;
    }

    @Override
    public int getRequestHeaderBufferSize() {
        return requestHeaderBufferSize;
    }

    @Override
    public int getResponseHeaderBufferSize() {
        return responseHeaderBufferSize;
    }

    @Override
    public MillisecondClock getClock() {
        return MillisecondClockImpl.INSTANCE;
    }

    @Override
    public IODispatcherConfiguration getDispatcherConfiguration() {
        return ioDispatcherConfiguration;
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
    public int getWorkerCount() {
        return workerCount;
    }

    @Override
    public int getSendBufferSize() {
        return sendBufferSize;
    }

    private int getInt(Properties properties, String key) {
        try {
            return Numbers.parseInt(properties.getProperty(key));
        } catch (NumericException e) {
            return 0;
        }
    }

    private String getString(Properties properties, String key) {
        return properties.getProperty(key);
    }

    private class PropStaticContentProcessorConfiguration implements StaticContentProcessorConfiguration {
        @Override
        public FilesFacade getFilesFacade() {
            return FilesFacadeImpl.INSTANCE;
        }

        @Override
        public CharSequence getIndexFileName() {
            return indexFileName;
        }

        @Override
        public MimeTypesCache getMimeTypesCache() {
            return null;
        }

        @Override
        public CharSequence getPublicDirectory() {
            return publicDirectory;
        }
    }

    private class PropTextImportProcessorConfiguration implements TextImportProcessorConfiguration {
        @Override
        public boolean abortBrokenUploads() {
            return abortBrokenUploads;
        }

        @Override
        public TextConfiguration getTextConfiguration() {
            return textConfiguration;
        }
    }

    private class PropIODispatcherConfiguration implements IODispatcherConfiguration {
        @Override
        public int getActiveConnectionLimit() {
            return activeConnectionLimit;
        }

        @Override
        public CharSequence getBindIPv4Address() {
            return bindIPv4Address;
        }

        @Override
        public int getBindPort() {
            return bindPort;
        }

        @Override
        public MillisecondClock getClock() {
            return MillisecondClockImpl.INSTANCE;
        }

        @Override
        public int getEventCapacity() {
            return eventCapacity;
        }

        @Override
        public int getIOQueueCapacity() {
            return ioQueueCapacity;
        }

        @Override
        public long getIdleConnectionTimeout() {
            return idleConnectionTimeout;
        }

        @Override
        public int getInterestQueueCapacity() {
            return interestQueueCapacity;
        }

        @Override
        public int getListenBacklog() {
            return listenBacklog;
        }

        @Override
        public NetworkFacade getNetworkFacade() {
            return NetworkFacadeImpl.INSTANCE;
        }

        @Override
        public EpollFacade getEpollFacade() {
            return EpollFacadeImpl.INSTANCE;
        }

        @Override
        public SelectFacade getSelectFacade() {
            return SelectFacadeImpl.INSTANCE;
        }

        @Override
        public int getInitialBias() {
            return IOOperation.READ;
        }

        @Override
        public int getSndBufSize() {
            return sndBufSize;
        }

        @Override
        public int getRcvBufSize() {
            return rcvBufSize;
        }
    }

    private class PropTextConfiguration implements TextConfiguration {
        @Override
        public String getAdapterSetConfigurationFileName() {
            return adapterSetConfigurationFileName;
        }

        @Override
        public int getDateAdapterPoolSize() {
            return dateAdapterPoolSize;
        }

        @Override
        public int getJsonCacheLimit() {
            return jsonCacheLimit;
        }

        @Override
        public int getJsonCacheSize() {
            return jsonCacheSize;
        }

        @Override
        public double getMaxRequiredDelimiterStdDev() {
            return maxRequiredDelimiterStdDev;
        }

        @Override
        public int getMetadataStringPoolSize() {
            return metadataStringPoolSize;
        }

        @Override
        public long getRollBufferLimit() {
            return rollBufferLimit;
        }

        @Override
        public long getRollBufferSize() {
            return rollBufferSize;
        }

        @Override
        public int getTextAnalysisMaxLines() {
            return textAnalysisMaxLines;
        }

        @Override
        public int getTextLexerStringPoolSize() {
            return textLexerStringPoolSize;
        }

        @Override
        public int getTimestampAdapterPoolSize() {
            return timestampAdapterPoolSize;
        }

        @Override
        public int getUtf8SinkCapacity() {
            return utf8SinkCapacity;
        }
    }
}
