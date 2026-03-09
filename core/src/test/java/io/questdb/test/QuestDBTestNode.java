/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test;

import io.questdb.ConfigPropertyKey;
import io.questdb.DefaultTelemetryConfiguration;
import io.questdb.MessageBus;
import io.questdb.Metrics;
import io.questdb.TelemetryConfiguration;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.Misc;
import io.questdb.test.cairo.Overrides;
import io.questdb.test.tools.TestUtils;

public class QuestDBTestNode {
    private final int nodeId;

    private Cairo cairo;
    private Griffin griffin;

    public QuestDBTestNode(int nodeId) {
        this.nodeId = nodeId;
    }

    public void closeCairo() {
        cairo.close();
    }

    public BindVariableService getBindVariableService() {
        return griffin.bindVariableService;
    }

    public CairoConfiguration getConfiguration() {
        return cairo.configuration;
    }

    public Overrides getConfigurationOverrides() {
        return cairo.overrides;
    }

    public CairoEngine getEngine() {
        return cairo.engine;
    }

    public int getId() {
        return nodeId;
    }

    public MessageBus getMessageBus() {
        return cairo.messageBus;
    }

    public Metrics getMetrics() {
        return cairo.configuration.getMetrics();
    }

    public CharSequence getRoot() {
        return cairo.root;
    }

    public SqlExecutionContext getSqlExecutionContext() {
        return griffin.sqlExecutionContext;
    }

    public void initCairo(
            String root,
            boolean ownRoot,
            Overrides overrides,
            TestCairoEngineFactory engineFactory,
            TestCairoConfigurationFactory configurationFactory
    ) {
        if (root == null || root.isEmpty()) {
            throw new IllegalArgumentException("must specify dbRoot");
        }
        cairo = new Cairo(root, ownRoot, overrides, engineFactory, configurationFactory);
    }

    public void initGriffin() {
        initGriffin(null);
    }

    public void initGriffin(SqlExecutionCircuitBreaker circuitBreaker) {
        if (cairo == null) {
            throw new IllegalStateException("Cairo is not initialised yet");
        }
        griffin = new Griffin(cairo, circuitBreaker);
    }

    public void setProperty(ConfigPropertyKey propertyKey, long value) {
        getConfigurationOverrides().setProperty(propertyKey, value);
    }

    public void setProperty(ConfigPropertyKey propertyKey, String value) {
        getConfigurationOverrides().setProperty(propertyKey, value);
    }

    public void setProperty(ConfigPropertyKey propertyKey, boolean value) {
        getConfigurationOverrides().setProperty(propertyKey, value);
    }

    public void setUpCairo() {
        cairo.setUp();
    }

    public void setUpGriffin() {
        griffin.setUp();
    }

    public void tearDownCairo(boolean removeDir) {
        cairo.tearDown(removeDir);
    }

    @Override
    public String toString() {
        return "QuestDBTestNode{" +
                "nodeId=" + nodeId +
                ", cairo=" + cairo +
                ", griffin=" + griffin +
                '}';
    }

    private static class Cairo {
        private final CairoConfiguration configuration;
        private final MessageBus messageBus;
        private final Overrides overrides;
        private final boolean ownRoot;
        private final CharSequence root;
        private CairoEngine engine;

        private Cairo(
                String root,
                boolean ownRoot,
                Overrides overrides,
                TestCairoEngineFactory engineFactory,
                TestCairoConfigurationFactory configurationFactory
        ) {
            this.root = root;
            this.ownRoot = ownRoot;
            this.overrides = overrides;
            final TelemetryConfiguration telemetryConfiguration = new DefaultTelemetryConfiguration() {
                @Override
                public boolean hideTables() {
                    return overrides.isHidingTelemetryTable();
                }
            };

            configuration = configurationFactory.getInstance(root, telemetryConfiguration, overrides);
            engine = engineFactory.getInstance(configuration);
            messageBus = engine.getMessageBus();
        }

        public void setUp() {
            if (ownRoot) {
                TestUtils.createTestPath(root);
            }
            engine.getTableIdGenerator().open();
            engine.getTableIdGenerator().reset();
            engine.resetNameRegistryMemory();
            engine.getWalLocker().clear();
            engine.setUp();
        }

        public void tearDown(boolean removeDir) {
            engine.getTableIdGenerator().close();
            engine.clear();
            engine.closeNameRegistry();
            engine.getMetrics().clear();
            if (removeDir && ownRoot) {
                TestUtils.removeTestPath(root);
            }
            overrides.reset();
        }

        private void close() {
            engine = Misc.free(engine);
        }
    }

    private static class Griffin {
        private final BindVariableService bindVariableService;
        private final SqlExecutionContext sqlExecutionContext;

        private Griffin(Cairo cairo, SqlExecutionCircuitBreaker circuitBreaker) {
            bindVariableService = new BindVariableServiceImpl(cairo.configuration);
            sqlExecutionContext = new SqlExecutionContextImpl(cairo.engine, 1)
                    .with(
                            cairo.configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            bindVariableService,
                            null,
                            -1,
                            circuitBreaker
                    );
            sqlExecutionContext.initNow();
            bindVariableService.clear();
        }

        public void setUp() {
            bindVariableService.clear();
        }
    }
}
