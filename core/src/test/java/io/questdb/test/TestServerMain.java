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

import io.questdb.Bootstrap;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.mp.WorkerPool;
import org.jetbrains.annotations.Nullable;

public class TestServerMain extends ServerMain {
    public TestServerMain(String... args) {
        super(args);
    }

    public TestServerMain(final Bootstrap bootstrap) {
        super(bootstrap);
    }

    @Override
    protected void setupWalApplyJob(
            WorkerPool workerPool,
            CairoEngine engine,
            int sharedWorkerCount,
            @Nullable FunctionFactoryCache ffCache
    ) {
        // do nothing
    }
}
