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

package io.questdb.test.griffin.fuzz;

import io.questdb.std.ObjList;

/**
 * A single generated query plus a flag describing whether two runs of it
 * are guaranteed to return the same row content (in any order). Queries
 * that emit {@code LIMIT} without a fully-disambiguating {@code ORDER BY}
 * over a parallel GROUP BY / hash join may pick a different valid subset
 * on each run; for those, the diff-JIT oracle compares row counts and
 * exception classes only.
 * <p>
 * When the bind-variable differential variant fires for the query,
 * {@code bindSql} carries the same query rewritten with {@code :bN::TYPE}
 * placeholders, {@code bindNames} holds the placeholder names and
 * {@code bindValues} holds the matching {@code setStr} values, both in
 * emission order. {@code hasBind()} returns false when bindSql is null
 * (most queries) or no bindable constant rolled the per-constant coin
 * flip.
 */
public record GeneratedQuery(
        String sql,
        boolean deterministic,
        String bindSql,
        ObjList<String> bindNames,
        ObjList<String> bindValues
) {

    public GeneratedQuery(String sql, boolean deterministic) {
        this(sql, deterministic, null, null, null);
    }

    public boolean hasBind() {
        return bindSql != null && bindValues != null && bindValues.size() > 0;
    }

    public GeneratedQuery withBind(String bindSql, ObjList<String> bindNames, ObjList<String> bindValues) {
        return new GeneratedQuery(this.sql, this.deterministic, bindSql, bindNames, bindValues);
    }
}
