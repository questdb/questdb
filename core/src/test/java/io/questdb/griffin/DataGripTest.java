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

package io.questdb.griffin;

import org.junit.Ignore;
import org.junit.Test;

public class DataGripTest extends AbstractGriffinTest {

    @Test
    @Ignore
    public void testStartUpUnknownDBMS() throws SqlException {
        assertQuery(
                "",
                "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM," +
                        "   ct.relname AS TABLE_NAME," +
                        " NOT i.indisunique AS NON_UNIQUE," +
                        "   NULL AS INDEX_QUALIFIER, ci.relname AS INDEX_NAME," +
                        "   CASE i.indisclustered" +
                        "     WHEN true THEN 1" +
                        "    ELSE CASE am.amname" +
                        "       WHEN 'hash' THEN 2" +
                        "      ELSE 3" +
                        "    END" +
                        "   END AS TYPE," +
                        "   (i.keys).n AS ORDINAL_POSITION," +
                        "   trim(both '\"' from pg_catalog.pg_get_indexdef(ci.oid, (i.keys).n, false)) AS COLUMN_NAME," +
                        "   CASE am.amname" +
                        "     WHEN 'btree' THEN CASE i.indoption[(i.keys).n - 1] & 1" +
                        "       WHEN 1 THEN 'D'" +
                        "       ELSE 'A'" +
                        "     END" +
                        "     ELSE NULL" +
                        "   END AS ASC_OR_DESC," +
                        "   ci.reltuples AS CARDINALITY," +
                        "   ci.relpages AS PAGES," +
                        "   pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS FILTER_CONDITION" +
                        " FROM pg_catalog.pg_class ct" +
                        "   JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)" +
                        "   JOIN (SELECT i.indexrelid, i.indrelid, i.indoption," +
                        "           i.indisunique, i.indisclustered, i.indpred," +
                        "           i.indexprs," +
                        "           information_schema._pg_expandarray(i.indkey) AS keys" +
                        "         FROM pg_catalog.pg_index i) i" +
                        "     ON (ct.oid = i.indrelid)" +
                        "   JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)" +
                        "   JOIN pg_catalog.pg_am am ON (ci.relam = am.oid)" +
                        " WHERE true" +
                        "  AND n.nspname = E'\"public\"' AND ct.relname = E'\"xx\"'" +
                        " ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION",
                null,
                true,
                sqlExecutionContext,
                false,
                false
        );
    }

    @Test
    public void testUpperCaseCount() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table y as (select x from long_sequence(10))", sqlExecutionContext);
            assertSql(
                    "select COUNT(*) from y",
                    "count\n" +
                            "10\n"
            );
        });
    }

    @Test
    public void testLowerCaseCount() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table y as (select x from long_sequence(10))", sqlExecutionContext);
            assertSql(
                    "select COUNT(*) from y",
                    "count\n" +
                            "10\n"
            );
        });
    }
}
