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
 * emission order. {@code bindLimitValue} is the value of the positional
 * {@code $1} that the bind form's LIMIT and SELECT list share, which the
 * runner leaves undefined at compile and binds after it, or null. {@code
 * hasBind()} returns false when bindSql is null (most queries) or the bind
 * form binds nothing: no bindable constant rolled the per-constant coin flip
 * and the LIMIT reads no {@code $1}.
 * <p>
 * {@code shape} names the generator the query came from. The clause generators leave it
 * {@link QueryShape#SIMPLE}; {@link QueryGenerator} stamps the real shape on the way out, so the
 * driver can hold each generator to a minimum accepted-query rate.
 * <p>
 * {@code referenceSql}, when set, is an independent rewrite of the query that must return the same
 * rows, such as the UNION ALL of plain GROUP BYs that {@code GroupingSetsClause} writes for a
 * grouping sets query. QueryRunner compares the two when both compile and run.
 * <p>
 * {@code spellingSql}, when set, is the same query in plain spelling: the searched form
 * {@code CASE WHEN x = 1 ...} for a simple CASE, {@code extract('day', ts)} for
 * {@code extract(day FROM ts)} and {@code ::TYPE} for {@code cast(x AS TYPE)}. QueryRunner
 * requires the two to agree on whether they compile, as well as on their rows.
 */
public record GeneratedQuery(
        String sql,
        boolean deterministic,
        String bindSql,
        ObjList<String> bindNames,
        ObjList<String> bindValues,
        String bindLimitValue,
        QueryShape shape,
        String referenceSql,
        String spellingSql
) {

    public GeneratedQuery(String sql, boolean deterministic) {
        this(sql, deterministic, null, null, null, null, QueryShape.SIMPLE, null, null);
    }

    public boolean hasBind() {
        return bindSql != null && ((bindValues != null && bindValues.size() > 0) || bindLimitValue != null);
    }

    public GeneratedQuery withBind(String bindSql, ObjList<String> bindNames, ObjList<String> bindValues, String bindLimitValue) {
        return new GeneratedQuery(this.sql, this.deterministic, bindSql, bindNames, bindValues, bindLimitValue, this.shape, this.referenceSql, this.spellingSql);
    }

    public GeneratedQuery withReference(String referenceSql) {
        return new GeneratedQuery(this.sql, this.deterministic, this.bindSql, this.bindNames, this.bindValues, this.bindLimitValue, this.shape, referenceSql, this.spellingSql);
    }

    public GeneratedQuery withShape(QueryShape shape) {
        return new GeneratedQuery(this.sql, this.deterministic, this.bindSql, this.bindNames, this.bindValues, this.bindLimitValue, shape, this.referenceSql, this.spellingSql);
    }

    public GeneratedQuery withSpelling(String spellingSql) {
        return new GeneratedQuery(this.sql, this.deterministic, this.bindSql, this.bindNames, this.bindValues, this.bindLimitValue, this.shape, this.referenceSql, spellingSql);
    }
}
