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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.plan.logical.Subquery;

/**
 * The compiler operations the binder and the function instantiator need: binding a sub-query one depth deeper, and
 * generating an optimised sub-query for one of its consumers.
 */
interface SubqueryCompiler {

    /**
     * Binds a sub-query of the query binding now with the scope one depth deeper and returns it.
     */
    Subquery bindSubquery(QueryModel model, int position, SqlExecutionContext executionContext) throws SqlException;

    /**
     * Binds, optimises and generates a sub-query whose rows binding consumes; the caller owns the factory.
     */
    RecordCursorFactory compileSubqueryFactory(QueryModel model, int position, SqlExecutionContext executionContext) throws SqlException;

    /**
     * Generates a new factory of an optimised sub-query for one of its consumers; the caller owns the factory.
     */
    RecordCursorFactory generateSubquery(Subquery subquery, SqlExecutionContext executionContext) throws SqlException;
}
