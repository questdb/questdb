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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;


public class DynamicPropServerConfiguration implements DynamicServerConfiguration {

    private static final Function<String, ? extends ConfigPropertyKey> KEY_RESOLVER = (k) -> {
        Optional<PropertyKey> prop = PropertyKey.getByString(k);
        return prop.orElse(null);
    };
    private static final Log LOG = LogFactory.getLog(DynamicPropServerConfiguration.class);
    private static final Set<PropertyKey> RELOADABLE_PROPS = new HashSet<>(Arrays.asList(
            PropertyKey.PG_USER,
            PropertyKey.PG_PASSWORD,
            PropertyKey.PG_RO_USER_ENABLED,
            PropertyKey.PG_RO_USER,
            PropertyKey.PG_RO_PASSWORD,
            PropertyKey.PG_NAMED_STATEMENT_LIMIT
    ));
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
                    log.info().$("loaded new value of ").$(entry.getKey()).$();
                    oldProperties.setProperty(key, (String) entry.getValue());
                    changed = true;
                } else {
                    log.advisory().$("property ").$(entry.getKey()).$(" was modified in the config file but cannot be reloaded. ignoring new value").$();
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
                    log.advisory().$("property ").$(key).$(" was removed from the config file but cannot be reloaded. ignoring").$();
                }
            }
        }
        return changed;
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
            p.of(confPath.toString());

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
                    return;
                }

                if (updateSupportedProperties(properties, newProperties, RELOADABLE_PROPS, KEY_RESOLVER, LOG)) {
                    reload(properties);
                    LOG.info().$("QuestDB configuration reloaded, [file=").$(confPath).$(", modifiedAt=").$ts(newLastModified * 1000).$(']').$();
                    if (afterConfigReloaded != null) {
                        afterConfigReloaded.run();
                    }
                }
            } else if (newLastModified == -1) {
                LOG.critical().$("Server configuration file is inaccessible! This is dangerous as server will likely not boot on restart. Make sure the current user can access the configuration file [path=").$(confPath).I$();
            }
        }
    }

    public void reload(Properties properties) {
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
            log.error().$(e);
            return;
        }

        delegate.set(newConfig);
    }
}
