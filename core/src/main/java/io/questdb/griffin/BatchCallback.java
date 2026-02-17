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

package io.questdb.griffin;


/**
 * Interface used to add steps before and/or after query compilation, e.g. cache checks and query result sending to jdbc client .
 */
public interface BatchCallback {

    /**
     * Callback to notify a query from a script has been compiled.
     * <p>
     * It's called after the query has been compiled and before it's executed.
     * If {#link #preCompile(SqlCompiler, CharSequence)} returned false, this method will not be called.
     *
     * @param compiler  sql compiler
     * @param cq        compiled query
     * @param queryText query text
     * @throws Exception if callback fails
     */
    void postCompile(SqlCompiler compiler, CompiledQuery cq, CharSequence queryText) throws Exception;

    /**
     * Callback to notify a query from a script is about to be compiled. It allows to skip compilation of the query.
     *
     * @param compiler sql compiler
     * @param sqlText  query text
     * @return true if query should be compiled, false if it should be skipped and move to next query
     */
    boolean preCompile(SqlCompiler compiler, CharSequence sqlText);
}
