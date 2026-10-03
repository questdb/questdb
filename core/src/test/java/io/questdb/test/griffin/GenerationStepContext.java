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
 * Runs a test step each time code generation enters a join input, a sort input or a sub-query,
 * the only callers of {@link #pushTimestampRequiredFlag}, so a test can act between binding and
 * a chosen point of generation.
 */
public class GenerationStepContext extends SqlExecutionContextImpl {
    private Runnable step;

    public GenerationStepContext(CairoEngine engine) {
        super(engine, 1);
        with(AllowAllSecurityContext.INSTANCE, new BindVariableServiceImpl(engine.getConfiguration()));
    }

    @Override
    public void pushTimestampRequiredFlag(boolean flag) {
        if (step != null) {
            step.run();
        }
        super.pushTimestampRequiredFlag(flag);
    }

    public void setStep(Runnable step) {
        this.step = step;
    }
}
