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

package io.questdb.cairo.view;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.griffin.SqlExecutionContextImpl;

// Execution context used by the view compiler job.
// Since there is no user while the job is compiling views, we are switching the security context to ReadOnlySecurityContext.
// This execution context is also used by the COMPILE VIEW command.
// In that case we only care if the user has the COMPILE VIEW permission on the view, and no need to check for SELECT permission too.
public class ViewCompilerExecutionContext extends SqlExecutionContextImpl {
    public ViewCompilerExecutionContext(CairoEngine engine, int sharedQueryWorkerCount) {
        super(engine, sharedQueryWorkerCount);
        securityContext = ReadOnlySecurityContext.INSTANCE;
    }
}
