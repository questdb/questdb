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

package io.questdb.griffin;

import io.questdb.griffin.engine.functions.catalogue.TxIDCurrentFunctionFactory;
import io.questdb.test.tools.TestUtils;
import org.junit.Ignore;
import org.junit.Test;

public class DataGripTest extends AbstractGriffinTest {

    @Test
    public void testGetCurrentDatabase() throws SqlException {
        assertQuery(
                "id\tname\tdescription\tis_template\tallow_connections\towner\n" +
                        "1\tquestdb\t\tfalse\ttrue\tpublic\n",
                "select N.oid::bigint as id,\n" +
                        "       datname as name,\n" +
                        "       D.description,\n" +
                        "       datistemplate as is_template,\n" +
                        "       datallowconn as allow_connections,\n" +
                        "       pg_catalog.pg_get_userbyid(N.datdba) as \"owner\"\n" +
                        "from pg_catalog.pg_database N\n" +
                        "  left join pg_catalog.pg_shdescription D on N.oid = D.objoid\n" +
                        "order by case when datname = pg_catalog.current_database() then -1::bigint else N.oid::bigint end;\n",
                null,
                true,
                sqlExecutionContext,
                false,
                false
        );
    }

    @Test
    public void testGetDatabaseOwner() throws SqlException {
        assertQuery(
                "id\tstate_number\tname\tdescription\towner\n" +
                        "2200\t0\tpublic\t\tpublic\n" +
                        "11\t0\tpg_catalog\t\tpublic\n",
                "select N.oid::bigint as id,\n" +
                        "       N.xmin as state_number,\n" +
                        "       nspname as name,\n" +
                        "       D.description,\n" +
                        "       pg_catalog.pg_get_userbyid(N.nspowner) as \"owner\"\n" +
                        "from pg_catalog.pg_namespace N\n" +
                        "  left join pg_catalog.pg_description D on N.oid = D.objoid\n" +
                        "/* */where N.nspname not like 'pg_toast%' and N.nspname not like 'pg_temp%' \n" +
                        "order by case when nspname = current_schema then -1::bigint else N.oid::bigint end",
                null,
                true,
                sqlExecutionContext,
                false,
                false
        );
    }

    @Test
    public void testGetDatabases() throws SqlException {
        assertQuery(
                "id\tname\tdescription\tis_template\tallow_connections\towner\n" +
                        "1\tquestdb\t\tfalse\ttrue\tpublic\n",
                "select N.oid::bigint as id,\n" +
                        "       datname as name,\n" +
                        "       D.description,\n" +
                        "       datistemplate as is_template,\n" +
                        "       datallowconn as allow_connections,\n" +
                        "       pg_catalog.pg_get_userbyid(N.datdba) as \"owner\"\n" +
                        "from pg_catalog.pg_database N\n" +
                        "  left join pg_catalog.pg_shdescription D on N.oid = D.objoid",
                null,
                false,
                sqlExecutionContext,
                false,
                false
        );
    }

    @Test
    public void testGetTxId() throws Exception {
        assertMemoryLeak(
                () -> TestUtils.assertSql(
                        compiler,
                        sqlExecutionContext,
                        "select case\n" +
                                "  when pg_catalog.pg_is_in_recovery()\n" +
                                "    then null\n" +
                                "  else\n" +
                                "    pg_catalog.txid_current()::varchar::bigint\n" +
                                "  end as current_txid",
                        sink,
                        "current_txid\n" + (TxIDCurrentFunctionFactory.getTxID() + 1) + "\n"
                )
        );
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

    @Test
    public void testShowDateStyles() throws SqlException {
        assertQuery(
                "DateStyle\n" +
                        "ISO,YMD\n",
                "show datestyle",
                null,
                false,
                sqlExecutionContext,
                false,
                true
        );
    }

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
}
