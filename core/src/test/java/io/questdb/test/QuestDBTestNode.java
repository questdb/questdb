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

package io.questdb.test;

import io.questdb.*;
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
import org.jetbrains.annotations.NotNull;

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
        return cairo.metrics;
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

    public void setProperty(PropertyKey propertyKey, long value) {
        getConfigurationOverrides().setProperty(propertyKey, value);
    }

    public void setProperty(PropertyKey propertyKey, String value) {
        getConfigurationOverrides().setProperty(propertyKey, value);
    }

    public void setProperty(PropertyKey propertyKey, boolean value) {
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
        private final Metrics metrics;
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
            metrics = Metrics.enabled();
            engine = engineFactory.getInstance(configuration, metrics);
            messageBus = engine.getMessageBus();
        }

        public void setUp() {
            if (ownRoot) {
                TestUtils.createTestPath(root);
            }
            engine.getTableIdGenerator().open();
            engine.getTableIdGenerator().reset();
            engine.resetNameRegistryMemory();
            engine.setUp();
        }

        public void tearDown(boolean removeDir) {
            engine.getTableIdGenerator().close();
            engine.clear();
            engine.closeNameRegistry();
            if (removeDir && ownRoot) {
                TestUtils.removeTestPath(root);
            }
            overrides.reset();
            clearWalQueue();
        }

        private void clearWalQueue() {
            long seq;
            while ((seq = engine.getMessageBus().getWalTxnNotificationSubSequence().next()) > -1) {
                engine.getMessageBus().getWalTxnNotificationSubSequence().done(seq);
            }
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
            bindVariableService.clear();
        }

        public void setUp() {
            bindVariableService.clear();
        }
    }
}
