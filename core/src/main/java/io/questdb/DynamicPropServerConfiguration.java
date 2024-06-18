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
import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.HttpMinServerConfiguration;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.cutlass.pgwire.PGWireConfiguration;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.metrics.MetricsConfiguration;
import io.questdb.mp.WorkerPoolConfiguration;
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
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;


public class DynamicPropServerConfiguration implements DynamicServerConfiguration {

    private static final Log LOG = LogFactory.getLog(DynamicPropServerConfiguration.class);
    private final BuildInformation buildInformation;
    private final java.nio.file.Path confPath;
    private final boolean configReloadEnabled;
    private final AtomicReference<PropServerConfiguration> delegate;
    private final @Nullable Map<String, String> env;
    private final FilesFacade filesFacade;
    private final FactoryProviderFactory fpf;
    private final boolean loadAdditionalConfigurations;
    private final Log log;
    private final MicrosecondClock microsecondClock;
    private final Properties properties;
    private final Set<PropertyKey> reloadableProps = new HashSet<>(Arrays.asList(
            PropertyKey.PG_USER,
            PropertyKey.PG_PASSWORD,
            PropertyKey.PG_RO_USER_ENABLED,
            PropertyKey.PG_RO_USER,
            PropertyKey.PG_RO_PASSWORD
    ));
    private final String root;
    private Runnable afterConfigReloaded;
    private long lastModified;

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
        this.delegate = new AtomicReference<>(serverConfig);

        this.confPath = Paths.get(this.getCairoConfiguration().getConfRoot().toString(), Bootstrap.CONFIG_FILE);

        try (Path p = new Path()) {
            // we assume the config file does exist, otherwise we should not
            // get to this code. This constructor is passed properties object,
            // loaded from the same file. We are not expecting races here either.
            p.of(this.confPath.toString()).$();
            this.lastModified = Files.getLastModified(p);
        }
        this.configReloadEnabled = serverConfig.isConfigReloadEnabled();
    }

    public DynamicPropServerConfiguration(
            String root,
            Properties properties,
            @Nullable Map<String, String> env,
            Log log,
            final BuildInformation buildInformation,
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
            final BuildInformation buildInformation
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

    public DynamicPropServerConfiguration(
            String root,
            Properties properties,
            @Nullable Map<String, String> env,
            Log log,
            BuildInformation buildInformation,
            FilesFacade filesFacade,
            MicrosecondClock microsecondClock,
            FactoryProviderFactory fpf,
            boolean loadAdditionalConfigurations,
            Runnable afterConfigFileChanged
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
                loadAdditionalConfigurations
        );
        this.afterConfigReloaded = afterConfigFileChanged;
    }

    @Override
    public CairoConfiguration getCairoConfiguration() {
        return delegate.get().getCairoConfiguration();
    }

    @Override
    public FactoryProvider getFactoryProvider() {
        return delegate.get().getFactoryProvider();
    }

    @Override
    public HttpMinServerConfiguration getHttpMinServerConfiguration() {
        return delegate.get().getHttpMinServerConfiguration();
    }

    @Override
    public HttpServerConfiguration getHttpServerConfiguration() {
        return delegate.get().getHttpServerConfiguration();
    }

    @Override
    public LineTcpReceiverConfiguration getLineTcpReceiverConfiguration() {
        return delegate.get().getLineTcpReceiverConfiguration();
    }

    @Override
    public LineUdpReceiverConfiguration getLineUdpReceiverConfiguration() {
        return delegate.get().getLineUdpReceiverConfiguration();
    }

    @Override
    public MemoryConfiguration getMemoryConfiguration() {
        return delegate.get().getMemoryConfiguration();
    }

    @Override
    public MetricsConfiguration getMetricsConfiguration() {
        return delegate.get().getMetricsConfiguration();
    }

    @Override
    public PGWireConfiguration getPGWireConfiguration() {
        return delegate.get().getPGWireConfiguration();
    }

    @Override
    public PublicPassthroughConfiguration getPublicPassthroughConfiguration() {
        return delegate.get().getPublicPassthroughConfiguration();
    }

    @Override
    public WorkerPoolConfiguration getWalApplyPoolConfiguration() {
        return delegate.get().getWalApplyPoolConfiguration();
    }

    @Override
    public WorkerPoolConfiguration getWorkerPoolConfiguration() {
        return delegate.get().getWorkerPoolConfiguration();
    }

    @Override
    public void init(CairoEngine engine, FreeOnExit freeOnExit) {
        delegate.get().init(this, engine, freeOnExit);
    }

    @Override
    public boolean isConfigReloadEnabled() {
        return configReloadEnabled;
    }


    @Override
    public void onFileEvent() {
        try (Path p = new Path()) {
            p.of(this.confPath.toString()).$();

            // Check that the file has been modified since the last trigger
            long newLastModified = Files.getLastModified(p);
            if (newLastModified > this.lastModified) {
                // If it has, update the cached value
                this.lastModified = newLastModified;

                // Then load the config properties
                Properties newProperties = new Properties();
                try (InputStream is = java.nio.file.Files.newInputStream(this.confPath)) {
                    newProperties.load(is);
                } catch (IOException exc) {
                    LOG.error().$(exc).$();
                }


                if (!newProperties.equals(this.properties)) {
                    // Compare the new and existing properties
                    AtomicBoolean changed = new AtomicBoolean(false);
                    newProperties.forEach((k, v) -> {
                        String key = (String) k;
                        String oldVal = properties.getProperty(key);
                        if (oldVal == null || !oldVal.equals(newProperties.getProperty(key))) {
                            Optional<PropertyKey> prop = PropertyKey.getByString(key);
                            if (!prop.isPresent()) {
                                return;
                            }

                            if (reloadableProps.contains(prop.get())) {
                                LOG.info().$("loaded new value of ").$(k).$();
                                this.properties.setProperty(key, (String) v);
                                changed.set(true);
                            } else {
                                LOG.advisory().$("property ").$(k).$(" was modified in the config file but cannot be reloaded. ignoring new value").$();
                            }
                        }
                    });


                    // Check for any old reloadable properties that have been removed in the new config
                    properties.forEach((k, v) -> {
                        if (!newProperties.containsKey(k)) {
                            Optional<PropertyKey> prop = PropertyKey.getByString((String) k);
                            if (!prop.isPresent()) {
                                return;
                            }
                            if (reloadableProps.contains(prop.get())) {
                                LOG.info().$("removed property ").$(k).$();
                                this.properties.remove(k);
                                changed.set(true);
                            } else {
                                LOG.advisory().$("property ").$(k).$(" was removed from the config file but cannot be reloaded. ignoring").$();
                            }
                        }
                    });

                    // If they are different, reload the config in place
                    if (changed.get()) {
                        this.reload(this.properties);
                        LOG.info().$("config reloaded!").$();
                        if (this.afterConfigReloaded != null) {
                            afterConfigReloaded.run();
                        }
                    }

                }
            } else if (newLastModified == -1) {
                LOG.critical().$("Server configuration file is inaccessible! This is dangerous as server will likely not boot on restart. Make sure the current user can access the configuration file [path=").$(this.confPath).I$();
            }
        }
    }

    public void reload(Properties properties) {
        PropServerConfiguration newConfig;
        try {
            newConfig = new PropServerConfiguration(
                    this.root,
                    properties,
                    this.env,
                    this.log,
                    this.buildInformation,
                    this.filesFacade,
                    this.microsecondClock,
                    this.fpf,
                    this.loadAdditionalConfigurations
            );
        } catch (ServerConfigurationException | JsonException e) {
            this.log.error().$(e);
            return;
        }

        delegate.set(newConfig);
    }

}
