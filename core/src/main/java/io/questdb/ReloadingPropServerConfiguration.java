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

package io.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cutlass.http.HttpMinServerConfiguration;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.cutlass.pgwire.PGWireConfiguration;
import io.questdb.log.Log;
import io.questdb.metrics.MetricsConfiguration;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

public class ReloadingPropServerConfiguration implements ServerConfiguration {

    private ServerConfiguration config;
    private final String root;
    private final Log log;
    private final BuildInformation buildInformation;
    private final FilesFacade filesFacade;
    private final MicrosecondClock microsecondClock;
    private final FactoryProviderFactory fpf;


    public ReloadingPropServerConfiguration(
            String root,
            Properties properties,
            @Nullable Map<String, String> env,
            Log log,
            final BuildInformation buildInformation
    ) throws ServerConfigurationException, JsonException {
        this.config = new PropServerConfiguration(root, properties, env, log, buildInformation);
        this.root = root;
        this.log = log;
        this.buildInformation = buildInformation;
        this.filesFacade = FilesFacadeImpl.INSTANCE;
        this.microsecondClock = MicrosecondClockImpl.INSTANCE;
        this.fpf = (configuration, engine, freeOnExitList) -> DefaultFactoryProvider.INSTANCE;
    }

    public ReloadingPropServerConfiguration(
            String root,
            Properties properties,
            @Nullable Map<String, String> env,
            Log log,
            final BuildInformation buildInformation,
            FilesFacade filesFacade,
            MicrosecondClock microsecondClock,
            FactoryProviderFactory fpf
    ) throws ServerConfigurationException, JsonException {
        this.config = new PropServerConfiguration(root, properties, env, log, buildInformation, filesFacade, microsecondClock, fpf);
        this.root = root;
        this.log = log;
        this.buildInformation = buildInformation;
        this.filesFacade = filesFacade;
        this.microsecondClock = microsecondClock;
        this.fpf = fpf;
    }

    @Override
    public CairoConfiguration getCairoConfiguration() {
        return this.config.getCairoConfiguration();
    }

    @Override
    public FactoryProvider getFactoryProvider() {
        return this.config.getFactoryProvider();
    }

    @Override
    public HttpMinServerConfiguration getHttpMinServerConfiguration() {
        return this.config.getHttpMinServerConfiguration();
    }

    @Override
    public HttpServerConfiguration getHttpServerConfiguration() {
        return this.config.getHttpServerConfiguration();
    }

    @Override
    public LineTcpReceiverConfiguration getLineTcpReceiverConfiguration() {
        return this.config.getLineTcpReceiverConfiguration();
    }

    @Override
    public LineUdpReceiverConfiguration getLineUdpReceiverConfiguration() {
        return this.config.getLineUdpReceiverConfiguration();
    }

    @Override
    public MetricsConfiguration getMetricsConfiguration() {
        return this.config.getMetricsConfiguration();
    }

    @Override
    public PGWireConfiguration getPGWireConfiguration() {
        return this.config.getPGWireConfiguration();
    }

    @Override
    public WorkerPoolConfiguration getWalApplyPoolConfiguration() {
        return this.config.getWalApplyPoolConfiguration();
    }

    @Override
    public WorkerPoolConfiguration getWorkerPoolConfiguration() {
        return this.config.getWorkerPoolConfiguration();
    }

    public boolean reload() {
        final Properties properties = new Properties();
        java.nio.file.Path configFile = Paths.get(this.root, PropServerConfiguration.CONFIG_DIRECTORY, Bootstrap.CONFIG_FILE);
        log.advisoryW().$("Server config: ").$(configFile).$();

        try (InputStream is = java.nio.file.Files.newInputStream(configFile)) {
            properties.load(is);
        } catch (IOException exc) {
            this.log.error().$(exc);
            return false;
        }

        return reload(
            properties,
            System.getenv()
        );


    }

    public boolean reload(
            Properties properties,
            @Nullable Map<String, String> env
    ) {
        try {
            this.config = new PropServerConfiguration(
                    this.root,
                    properties,
                    env,
                    this.log,
                    this.buildInformation,
                    this.filesFacade,
                    this.microsecondClock,
                    this.fpf
            );

        } catch (ServerConfigurationException|JsonException exc) {
            this.log.error().$(exc.toString());
            return false;
        }

        return true;
    }
}