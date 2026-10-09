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

package io.questdb.griffin;

import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.ObjList;

/**
 * Binds new calls over bound expressions, so a stage after binding can build them without depending on the binder.
 */
public interface CallBinder {

    /**
     * Binds a call over already-bound arguments as if its SQL text had them as
     * children: the same overload selection, implicit casts, constant folding and
     * errors. A group-by name binds as an aggregate root. Arguments, their
     * columns' input and the argument list are borrowed only for this call.
     */
    BoundExpression bindCall(
            CharSequence name,
            int position,
            ObjList<? extends BoundExpression> args,
            OutputSchema input,
            SqlExecutionContext executionContext
    ) throws SqlException;
}
