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

package io.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.HttpMinServerConfiguration;
import io.questdb.cutlass.http.HttpMinServerConfigurationWrapper;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.http.HttpServerConfigurationWrapper;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfigurationWrapper;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfigurationWrapper;
import io.questdb.cutlass.pgwire.PGWireConfiguration;
import io.questdb.cutlass.pgwire.PGWireConfigurationWrapper;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.metrics.MetricsConfiguration;
import io.questdb.metrics.MetricsConfigurationWrapper;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolConfigurationWrapper;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;


public class DynamicPropServerConfiguration implements ServerConfiguration, ConfigReloader {
    private static final Log LOG = LogFactory.getLog(DynamicPropServerConfiguration.class);
    private static final Function<String, ? extends ConfigPropertyKey> keyResolver = (k) -> {
        Optional<PropertyKey> prop = PropertyKey.getByString(k);
        return prop.orElse(null);
    };
    private static final Set<PropertyKey> reloadableProps = new HashSet<>(Arrays.asList(
            PropertyKey.PG_USER,
            PropertyKey.PG_PASSWORD,
            PropertyKey.PG_RO_USER_ENABLED,
            PropertyKey.PG_RO_USER,
            PropertyKey.PG_RO_PASSWORD,
            PropertyKey.PG_NAMED_STATEMENT_LIMIT,
            PropertyKey.PG_RECV_BUFFER_SIZE,
            PropertyKey.PG_SEND_BUFFER_SIZE,
            PropertyKey.PG_NET_CONNECTION_LIMIT,
            PropertyKey.HTTP_REQUEST_HEADER_BUFFER_SIZE,
            PropertyKey.HTTP_MULTIPART_HEADER_BUFFER_SIZE,
            PropertyKey.HTTP_RECV_BUFFER_SIZE,
            PropertyKey.HTTP_SEND_BUFFER_SIZE,
            PropertyKey.HTTP_NET_CONNECTION_LIMIT,
            PropertyKey.LINE_TCP_NET_CONNECTION_LIMIT
    ));
    private final BuildInformation buildInformation;
    private final CairoConfigurationImpl cairoConfig;
    private final java.nio.file.Path confPath;
    private final boolean configReloadEnabled;
    private final @Nullable Map<String, String> env;
    private final FilesFacade filesFacade;
    private final FactoryProviderFactory fpf;
    private final HttpServerConfigurationImpl httpServerConfig;
    private final LineTcpReceiverConfigurationImpl lineTcpConfig;
    private final LineUdpReceiverConfigurationImpl lineUdpConfig;
    private final boolean loadAdditionalConfigurations;
    private final Log log;
    private final MemoryConfigurationImpl memoryConfig;
    private final MetricsConfigurationImpl metricsConfig;
    private final MicrosecondClock microsecondClock;
    private final HttpMinServerConfigurationImpl minHttpServerConfig;
    private final PGWireConfigurationImpl pgWireConfig;
    private final Properties properties;
    private final PublicPassthroughConfigurationImpl publicPassThroughConfig;
    private final Object reloadLock = new Object();
    private final String root;
    private final AtomicReference<PropServerConfiguration> serverConfig;
    private final WorkerPoolConfigurationImpl walApplyPoolConfig;
    private final WorkerPoolConfigurationImpl workerPoolConfig;
    private long lastModified;
    private long version;

    public DynamicPropServerConfiguration(
            String root,
            Properties properties,
            @Nullable Map<String, String> env,
            Log log,
            BuildInformation buildInformation,
            FilesFacade filesFacade,
            MicrosecondClock microsecondClock,
            FactoryProviderFactory fpf,
            boolean loadAdditionalConfigurations
    ) throws ServerConfigurationException, JsonException {
        this.root = root;
        this.properties = properties;
        this.env = env;
        this.log = log;
        this.buildInformation = buildInformation;
        this.filesFacade = filesFacade;
        this.microsecondClock = microsecondClock;
        this.fpf = fpf;
        this.loadAdditionalConfigurations = loadAdditionalConfigurations;
        PropServerConfiguration serverConfig = new PropServerConfiguration(
                root,
                properties,
                env,
                log,
                buildInformation,
                filesFacade,
                microsecondClock,
                fpf,
                loadAdditionalConfigurations
        );
        this.serverConfig = new AtomicReference<>(serverConfig);
        this.cairoConfig = new CairoConfigurationImpl();
        this.minHttpServerConfig = new HttpMinServerConfigurationImpl();
        this.httpServerConfig = new HttpServerConfigurationImpl();
        this.lineTcpConfig = new LineTcpReceiverConfigurationImpl();
        this.lineUdpConfig = new LineUdpReceiverConfigurationImpl();
        this.memoryConfig = new MemoryConfigurationImpl();
        this.metricsConfig = new MetricsConfigurationImpl();
        this.pgWireConfig = new PGWireConfigurationImpl();
        this.publicPassThroughConfig = new PublicPassthroughConfigurationImpl();
        this.workerPoolConfig = new WorkerPoolConfigurationImpl();
        this.walApplyPoolConfig = new WorkerPoolConfigurationImpl();
        reloadNestedConfigurations(serverConfig);
        this.version = 0;
        this.confPath = Paths.get(this.getCairoConfiguration().getConfRoot().toString(), Bootstrap.CONFIG_FILE);
        this.configReloadEnabled = serverConfig.isConfigReloadEnabled();
        try (Path p = new Path()) {
            // we assume the config file does exist, otherwise we should not
            // get to this code. This constructor is passed properties object,
            // loaded from the same file. We are not expecting races here either.
            p.of(confPath.toString());
            lastModified = Files.getLastModified(p.$());
        }
    }

    public DynamicPropServerConfiguration(
            String root,
            Properties properties,
            @Nullable Map<String, String> env,
            Log log,
            BuildInformation buildInformation,
            FilesFacade filesFacade,
            MicrosecondClock microsecondClock,
            FactoryProviderFactory fpf
    ) throws ServerConfigurationException, JsonException {
        this(
                root,
                properties,
                env,
                log,
                buildInformation,
                filesFacade,
                microsecondClock,
                fpf,
                true
        );
    }

    public DynamicPropServerConfiguration(
            String root,
            Properties properties,
            @Nullable Map<String, String> env,
            Log log,
            BuildInformation buildInformation
    ) throws ServerConfigurationException, JsonException {
        this(
                root,
                properties,
                env,
                log,
                buildInformation,
                FilesFacadeImpl.INSTANCE,
                MicrosecondClockImpl.INSTANCE,
                (configuration, engine, freeOnExitList) -> DefaultFactoryProvider.INSTANCE,
                true
        );
    }

    // used in ent
    public static boolean updateSupportedProperties(
            Properties oldProperties,
            Properties newProperties,
            Set<? extends ConfigPropertyKey> reloadableProps,
            Function<String, ? extends ConfigPropertyKey> keyResolver,
            Log log
    ) {
        if (newProperties.equals(oldProperties)) {
            return false;
        }

        boolean changed = false;
        // Compare the new and existing properties
        for (Map.Entry<Object, Object> entry : newProperties.entrySet()) {
            String key = (String) entry.getKey();
            String oldVal = oldProperties.getProperty(key);
            if (oldVal == null || !oldVal.equals(entry.getValue())) {
                ConfigPropertyKey config = keyResolver.apply(key);
                if (config == null) {
                    return false;
                }

                if (reloadableProps.contains(config)) {
                    log.info().$("loaded new value of ").$(key).$();
                    oldProperties.setProperty(key, (String) entry.getValue());
                    changed = true;
                } else {
                    log.advisory().$("property ").$(key).$(" was modified in the config file but cannot be reloaded. Ignoring new value").$();
                }
            }
        }

        // Check for any old reloadable properties that have been removed in the new config
        Iterator<Object> oldPropsIter = oldProperties.keySet().iterator();
        while (oldPropsIter.hasNext()) {
            Object key = oldPropsIter.next();
            if (!newProperties.containsKey(key)) {
                ConfigPropertyKey prop = keyResolver.apply((String) key);
                if (prop == null) {
                    continue;
                }
                if (reloadableProps.contains(prop)) {
                    log.info().$("removed property ").$(key).$();
                    oldPropsIter.remove();
                    changed = true;
                } else {
                    log.advisory().$("property ").$(key).$(" was removed from the config file but cannot be reloaded. Ignoring").$();
                }
            }
        }
        return changed;
    }

    @Override
    public CairoConfiguration getCairoConfiguration() {
        return cairoConfig;
    }

    @Override
    public FactoryProvider getFactoryProvider() {
        return serverConfig.get().getFactoryProvider();
    }

    @Override
    public HttpMinServerConfiguration getHttpMinServerConfiguration() {
        return minHttpServerConfig;
    }

    @Override
    public HttpServerConfiguration getHttpServerConfiguration() {
        return httpServerConfig;
    }

    @Override
    public LineTcpReceiverConfiguration getLineTcpReceiverConfiguration() {
        return lineTcpConfig;
    }

    @Override
    public LineUdpReceiverConfiguration getLineUdpReceiverConfiguration() {
        return lineUdpConfig;
    }

    @Override
    public MemoryConfiguration getMemoryConfiguration() {
        return memoryConfig;
    }

    @Override
    public MetricsConfiguration getMetricsConfiguration() {
        return metricsConfig;
    }

    @Override
    public PGWireConfiguration getPGWireConfiguration() {
        return pgWireConfig;
    }

    @Override
    public PublicPassthroughConfiguration getPublicPassthroughConfiguration() {
        return publicPassThroughConfig;
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public WorkerPoolConfiguration getWalApplyPoolConfiguration() {
        return walApplyPoolConfig;
    }

    @Override
    public WorkerPoolConfiguration getWorkerPoolConfiguration() {
        return workerPoolConfig;
    }

    @Override
    public void init(CairoEngine engine, FreeOnExit freeOnExit) {
        serverConfig.get().init(this, engine, freeOnExit);
        if (configReloadEnabled) {
            engine.setConfigReloader(this);
        }
    }

    @Override
    public boolean reload() {
        try (Path p = new Path()) {
            p.of(confPath.toString());

            synchronized (reloadLock) {
                // Check that the file has been modified since the last trigger
                long newLastModified = Files.getLastModified(p.$());
                if (newLastModified > lastModified) {
                    // If it has, update the cached value
                    lastModified = newLastModified;

                    // Then load the config properties
                    Properties newProperties = new Properties();
                    try (InputStream is = java.nio.file.Files.newInputStream(confPath)) {
                        newProperties.load(is);
                    } catch (IOException exc) {
                        LOG.error().$(exc).$();
                        return false;
                    }

                    if (updateSupportedProperties(properties, newProperties, reloadableProps, keyResolver, LOG)) {
                        reload0();
                        LOG.info().$("QuestDB configuration reloaded, [file=").$(confPath).$(", modifiedAt=").$ts(newLastModified * 1000).I$();
                        return true;
                    }
                } else if (newLastModified == -1) {
                    LOG.critical().$("Server configuration file is inaccessible! This is dangerous as server will likely not boot on restart. Make sure the current user can access the configuration file [path=").$(confPath).I$();
                }
            }
        }
        return false;
    }

    private void reload0() {
        PropServerConfiguration newConfig;
        try {
            newConfig = new PropServerConfiguration(
                    root,
                    properties,
                    env,
                    log,
                    buildInformation,
                    filesFacade,
                    microsecondClock,
                    fpf,
                    loadAdditionalConfigurations
            );
        } catch (ServerConfigurationException | JsonException e) {
            log.error().$(e).$();
            return;
        }

        final PropServerConfiguration oldConfig = serverConfig.get();
        // Move factory provider to the new config instead of creating a new one.
        newConfig.reinit(oldConfig.getFactoryProvider());
        serverConfig.set(newConfig);
        reloadNestedConfigurations(newConfig);
        version++;
    }

    private void reloadNestedConfigurations(PropServerConfiguration serverConfig) {
        cairoConfig.setDelegate(serverConfig.getCairoConfiguration());
        minHttpServerConfig.setDelegate(serverConfig.getHttpMinServerConfiguration());
        httpServerConfig.setDelegate(serverConfig.getHttpServerConfiguration());
        lineTcpConfig.setDelegate(serverConfig.getLineTcpReceiverConfiguration());
        lineUdpConfig.setDelegate(serverConfig.getLineUdpReceiverConfiguration());
        memoryConfig.setDelegate(serverConfig.getMemoryConfiguration());
        metricsConfig.setDelegate(serverConfig.getMetricsConfiguration());
        pgWireConfig.setDelegate(serverConfig.getPGWireConfiguration());
        publicPassThroughConfig.setDelegate(serverConfig.getPublicPassthroughConfiguration());
        workerPoolConfig.setDelegate(serverConfig.getWorkerPoolConfiguration());
        walApplyPoolConfig.setDelegate(serverConfig.getWalApplyPoolConfiguration());
    }

    private static class CairoConfigurationImpl extends CairoConfigurationWrapper {
        private final AtomicReference<CairoConfiguration> delegate = new AtomicReference<>();

        @Override
        public CairoConfiguration getDelegate() {
            return delegate.get();
        }

        public void setDelegate(CairoConfiguration delegate) {
            this.delegate.set(delegate);
        }
    }

    private static class HttpMinServerConfigurationImpl extends HttpMinServerConfigurationWrapper {
        private final AtomicReference<HttpMinServerConfiguration> delegate = new AtomicReference<>();

        @Override
        public HttpMinServerConfiguration getDelegate() {
            return delegate.get();
        }

        public void setDelegate(HttpMinServerConfiguration delegate) {
            this.delegate.set(delegate);
        }
    }

    private static class HttpServerConfigurationImpl extends HttpServerConfigurationWrapper {
        private final AtomicReference<HttpServerConfiguration> delegate = new AtomicReference<>();

        @Override
        public HttpServerConfiguration getDelegate() {
            return delegate.get();
        }

        public void setDelegate(HttpServerConfiguration delegate) {
            this.delegate.set(delegate);
        }
    }

    private static class LineTcpReceiverConfigurationImpl extends LineTcpReceiverConfigurationWrapper {
        private final AtomicReference<LineTcpReceiverConfiguration> delegate = new AtomicReference<>();

        @Override
        public LineTcpReceiverConfiguration getDelegate() {
            return delegate.get();
        }

        public void setDelegate(LineTcpReceiverConfiguration delegate) {
            this.delegate.set(delegate);
        }
    }

    private static class LineUdpReceiverConfigurationImpl extends LineUdpReceiverConfigurationWrapper {
        private final AtomicReference<LineUdpReceiverConfiguration> delegate = new AtomicReference<>();

        @Override
        public LineUdpReceiverConfiguration getDelegate() {
            return delegate.get();
        }

        public void setDelegate(LineUdpReceiverConfiguration delegate) {
            this.delegate.set(delegate);
        }
    }

    private static class MemoryConfigurationImpl extends MemoryConfigurationWrapper {
        private final AtomicReference<MemoryConfiguration> delegate = new AtomicReference<>();

        @Override
        public MemoryConfiguration getDelegate() {
            return delegate.get();
        }

        public void setDelegate(MemoryConfiguration delegate) {
            this.delegate.set(delegate);
        }
    }

    private static class MetricsConfigurationImpl extends MetricsConfigurationWrapper {
        private final AtomicReference<MetricsConfiguration> delegate = new AtomicReference<>();

        @Override
        public MetricsConfiguration getDelegate() {
            return delegate.get();
        }

        public void setDelegate(MetricsConfiguration delegate) {
            this.delegate.set(delegate);
        }
    }

    private static class PGWireConfigurationImpl extends PGWireConfigurationWrapper {
        private final AtomicReference<PGWireConfiguration> delegate = new AtomicReference<>();

        @Override
        public PGWireConfiguration getDelegate() {
            return delegate.get();
        }

        public void setDelegate(PGWireConfiguration delegate) {
            this.delegate.set(delegate);
        }
    }

    private static class PublicPassthroughConfigurationImpl extends PublicPassthroughConfigurationWrapper {
        private final AtomicReference<PublicPassthroughConfiguration> delegate = new AtomicReference<>();

        @Override
        public PublicPassthroughConfiguration getDelegate() {
            return delegate.get();
        }

        public void setDelegate(PublicPassthroughConfiguration delegate) {
            this.delegate.set(delegate);
        }
    }

    private static class WorkerPoolConfigurationImpl extends WorkerPoolConfigurationWrapper {
        private final AtomicReference<WorkerPoolConfiguration> delegate = new AtomicReference<>();

        @Override
        public WorkerPoolConfiguration getDelegate() {
            return delegate.get();
        }

        public void setDelegate(WorkerPoolConfiguration delegate) {
            this.delegate.set(delegate);
        }
    }
}
