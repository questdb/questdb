/*+*****************************************************************************
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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;

/**
 * Runs a test step at the points the compiler consults the context once binding is done: as it reads whether the
 * statement requires the designated timestamp of the plan it is about to plan and generate, and each time planning
 * or generation asks whether parallel filters are enabled; so a test can act between binding and a chosen point of
 * generation.
 */
public class GenerationStepContext extends SqlExecutionContextImpl {
    private Runnable step;

    public GenerationStepContext(CairoEngine engine) {
        super(engine, 1);
        with(AllowAllSecurityContext.INSTANCE, new BindVariableServiceImpl(engine.getConfiguration()));
    }

    @Override
    public boolean isParallelFilterEnabled() {
        runStep();
        return super.isParallelFilterEnabled();
    }

    @Override
    public boolean isTimestampRequired() {
        runStep();
        return super.isTimestampRequired();
    }

    public void setStep(Runnable step) {
        this.step = step;
    }

    private void runStep() {
        if (step != null) {
            step.run();
        }
    }
}
