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

import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
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
    private Cairo cairo;
    private Griffin griffin;

    public void initCairo(String dbRootName) {
        if (dbRootName == null || dbRootName.isEmpty()) {
            throw new IllegalArgumentException("must specify dbRoot");
        }
        cairo = new Cairo(dbRootName);
    }

    public void closeCairo() {
        cairo.close();
    }

    public void setUpCairo() {
        cairo.setUp();
    }

    public void tearDownCairo(boolean removeDir) {
        cairo.tearDown(removeDir);
    }

    public void initGriffin(SqlExecutionCircuitBreaker circuitBreaker) {
        if (cairo == null) {
            throw new IllegalStateException("Cairo is not initialised yet");
        }
        griffin = new Griffin(cairo, circuitBreaker);
    }

    public void closeGriffin() {
        griffin.close();
    }

    public void setUpGriffin() {
        griffin.setUp();
    }

    public void tearDownGriffin() {
        griffin.tearDown();
    }

    private static class Cairo {
        private final CharSequence root;
        private final CairoConfiguration configuration;
        private final MessageBus messageBus;
        private CairoEngine engine;
        private DatabaseSnapshotAgent snapshotAgent;
        private final Metrics metrics;

        private Cairo(String dbRootName) {
            try {
                root = AbstractCairoTest.temp.newFolder(dbRootName).getAbsolutePath();
            } catch (IOException e) {
                throw new ExceptionInInitializerError();
            }

            final TelemetryConfiguration telemetryConfiguration = new DefaultTelemetryConfiguration() {
                @Override
                public boolean hideTables() {
                    return AbstractCairoTest.hideTelemetryTable;
                }
            };

            configuration = new AbstractCairoTest.CairoTestConfiguration(root, telemetryConfiguration);
            metrics = Metrics.enabled();
            engine = new CairoEngine(configuration, metrics, 2);
            snapshotAgent = new DatabaseSnapshotAgent(engine);
            messageBus = engine.getMessageBus();
        }

        private void close() {
            snapshotAgent = Misc.free(snapshotAgent);
            engine = Misc.free(engine);
        }

        public void setUp() {
            TestUtils.createTestPath(root);
            engine.getTableIdGenerator().open();
            engine.getTableIdGenerator().reset();
        }

        public void tearDown(boolean removeDir) {
            snapshotAgent.clear();
            engine.getTableIdGenerator().close();
            engine.clear();
            if (removeDir) {
                TestUtils.removeTestPath(root);
            }
        }
    }

    private static class Griffin {
        private final BindVariableService bindVariableService;
        private final SqlExecutionContext sqlExecutionContext;
        private final SqlCompiler compiler;

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

        private void close() {
            compiler.close();
        }

        public void setUp() {
            bindVariableService.clear();
        }

        public void tearDown() {
        }
    }

    public CharSequence getRoot() {
        return cairo.root;
    }

    public CairoConfiguration getConfiguration() {
        return cairo.configuration;
    }

    public MessageBus getMessageBus() {
        return cairo.messageBus;
    }

    public CairoEngine getEngine() {
        return cairo.engine;
    }

    public DatabaseSnapshotAgent getSnapshotAgent() {
        return cairo.snapshotAgent;
    }

    public Metrics getMetrics() {
        return cairo.metrics;
    }

    public BindVariableService getBindVariableService() {
        return griffin.bindVariableService;
    }

    public SqlExecutionContext getSqlExecutionContext() {
        return griffin.sqlExecutionContext;
    }

    public SqlCompiler getSqlCompiler() {
        return griffin.compiler;
    }
}
