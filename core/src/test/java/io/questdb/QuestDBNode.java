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

package io.questdb;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.DatabaseSnapshotAgent;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.Misc;
import io.questdb.test.tools.TestUtils;

import java.io.IOException;

public class QuestDBNode {
    private final int nodeId;

    private Cairo cairo;
    private Griffin griffin;

    public QuestDBNode(int nodeId) {
        this.nodeId = nodeId;
    }

    public void closeCairo() {
        cairo.close();
    }

    public void closeGriffin() {
        griffin.close();
    }

    public BindVariableService getBindVariableService() {
        return griffin.bindVariableService;
    }

    public CairoConfiguration getConfiguration() {
        return cairo.configuration;
    }

    public ConfigurationOverrides getConfigurationOverrides() {
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

    public DatabaseSnapshotAgent getSnapshotAgent() {
        return cairo.snapshotAgent;
    }

    public SqlCompiler getSqlCompiler() {
        return griffin.compiler;
    }

    public SqlExecutionContext getSqlExecutionContext() {
        return griffin.sqlExecutionContext;
    }

    public void initCairo(String dbRootName, ConfigurationOverrides overrides) {
        if (dbRootName == null || dbRootName.isEmpty()) {
            throw new IllegalArgumentException("must specify dbRoot");
        }
        cairo = new Cairo(dbRootName, overrides);
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

    public void setUpCairo() {
        cairo.setUp();
    }

    public void setUpGriffin() {
        griffin.setUp();
    }

    public void tearDownCairo(boolean removeDir) {
        cairo.tearDown(removeDir);
    }

    public void tearDownGriffin() {
        griffin.tearDown();
    }

    private static class Cairo {
        private final CairoConfiguration configuration;
        private final MessageBus messageBus;
        private final Metrics metrics;
        private final ConfigurationOverrides overrides;
        private final CharSequence root;
        private CairoEngine engine;
        private DatabaseSnapshotAgent snapshotAgent;

        private Cairo(String dbRootName, ConfigurationOverrides overrides) {
            try {
                root = AbstractCairoTest.temp.newFolder(dbRootName).getAbsolutePath();
            } catch (IOException e) {
                throw new ExceptionInInitializerError();
            }

            this.overrides = overrides;
            final TelemetryConfiguration telemetryConfiguration = new DefaultTelemetryConfiguration() {
                @Override
                public boolean hideTables() {
                    return overrides.isHidingTelemetryTable();
                }
            };

            configuration = new CairoTestConfiguration(root, telemetryConfiguration, overrides);
            metrics = Metrics.enabled();
            engine = new CairoEngine(configuration, metrics);
            snapshotAgent = new DatabaseSnapshotAgent(engine);
            messageBus = engine.getMessageBus();
        }

        public void setUp() {
            TestUtils.createTestPath(root);
            engine.getTableIdGenerator().open();
            engine.getTableIdGenerator().reset();
            engine.resetNameRegistryMemory();
        }

        public void tearDown(boolean removeDir) {
            snapshotAgent.clear();
            engine.getTableIdGenerator().close();
            engine.clear();
            engine.closeNameRegistry();
            if (removeDir) {
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
            snapshotAgent = Misc.free(snapshotAgent);
            engine = Misc.free(engine);
        }
    }

    private static class Griffin {
        private final BindVariableService bindVariableService;
        private final SqlCompiler compiler;
        private final SqlExecutionContext sqlExecutionContext;

        private Griffin(Cairo cairo, SqlExecutionCircuitBreaker circuitBreaker) {
            compiler = new SqlCompiler(cairo.engine, null, cairo.snapshotAgent);
            bindVariableService = new BindVariableServiceImpl(cairo.configuration);
            sqlExecutionContext = new SqlExecutionContextImpl(cairo.engine, 1)
                    .with(
                            AllowAllCairoSecurityContext.INSTANCE,
                            bindVariableService,
                            null,
                            -1,
                            circuitBreaker);
            bindVariableService.clear();
        }

        public void setUp() {
            bindVariableService.clear();
        }

        public void tearDown() {
        }

        private void close() {
            compiler.close();
        }
    }
}
