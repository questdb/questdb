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

package io.questdb.cutlass.pgwire;

import io.questdb.Metrics;
import io.questdb.WorkerPoolAwareConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.text.TextImportRequestJob;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public final class TextImportRequestJobRule implements TestRule {

    private final CairoEngine engine;

    private TextImportRequestJobRule(CairoEngine engine) {
        this.engine = engine;
    }

    public static TextImportRequestJobRule forEngine(CairoEngine engine) {
        return new TextImportRequestJobRule(engine);
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                WorkerPoolConfiguration config = new WorkerPoolAwareConfiguration() {
                    @Override
                    public boolean isEnabled() {
                        return true;
                    }

                    @Override
                    public int[] getWorkerAffinity() {
                        return new int[1];
                    }

                    @Override
                    public int getWorkerCount() {
                        return 1;
                    }

                    @Override
                    public boolean haltOnError() {
                        return true;
                    }
                };
                WorkerPool pool = new WorkerPool(config, Metrics.disabled());
                try (TextImportRequestJob processingJob = new TextImportRequestJob(engine, 1, null)){
                    pool.assign(processingJob);
                    pool.start(null);
                    base.evaluate();
                } finally {
                    pool.halt();
                }
            }
        };
    }
}
