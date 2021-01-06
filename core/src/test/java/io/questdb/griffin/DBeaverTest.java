/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import org.junit.Test;

public class DBeaverTest extends AbstractGriffinTest {
    @Test
    public void testFrequentSql() throws SqlException {
        assertQuery(
                "current_schema\tsession_user\n" +
                        "public\tadmin\n",
                "SELECT current_schema(),session_user",
                null,
                true,
                sqlExecutionContext,
                false,
                true
        );
    }

    @Test
    public void testListColumns() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table xyz(a int, t timestamp)", sqlExecutionContext);
            compiler.compile("create table tab2(b long, z binary)", sqlExecutionContext);

            assertQuery(
                    "relname\tattrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tdef_value\tdescription\n" +
                            "xyz\t1\ta\t1\t23\tfalse\t0\t4\t\tfalse\t\t\n" +
                            "xyz\t1\tt\t2\t1114\tfalse\t0\t-1\t\tfalse\t\t\n",
                    "SELECT \n" +
                            "    c.relname,\n" +
                            "    a.*,\n" +
                            "    pg_catalog.pg_get_expr(ad.adbin, ad.adrelid, true) as def_value,\n" +
                            "    dsc.description\n" +
                            "FROM pg_catalog.pg_attribute a\n" +
                            "INNER JOIN pg_catalog.pg_class c ON (a.attrelid=c.oid)\n" +
                            "LEFT OUTER JOIN pg_catalog.pg_attrdef ad ON (a.attrelid=ad.adrelid AND a.attnum = ad.adnum)\n" +
                            "LEFT OUTER JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)\n" +
                            "WHERE NOT a.attisdropped AND c.oid=1 ORDER BY a.attnum",
                    null,
                    true,
                    sqlExecutionContext,
                    false,
                    false
            );
        });
    }

    @Test
    public void testNamespaceListSql() throws SqlException {
        assertQuery(
                "oid\tnspname\toid1\tdescription\n" +
                        "11\tpg_catalog\t11\t\n" +
                        "2200\tpublic\t2200\t\n",
                "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n\n" +
                        "LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass\n" +
                        " ORDER BY nspname",
                null,
                true,
                sqlExecutionContext,
                false,
                false
        );
    }

    @Test
    public void testShowSearchPath() throws SqlException {
        assertQuery(
                "search_path\n" +
                        "\"$user\", public\n",
                "SHOW search_path",
                null,
                false,
                sqlExecutionContext,
                false,
                true
        );
    }

    @Test
    public void testListTables() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table xyz(a int)", sqlExecutionContext);
            compiler.compile("create table tab2(b long)", sqlExecutionContext);
            assertQuery(
                    "oid\trelname\trelnamespace\trelkind\trelowner\toid1\trelpartbound\tdescription\tpartition_expr\tpartition_key\n" +
                            "2\ttab2\t2200\tr\t0\t2\t\t\t\t\n" +
                            "1\txyz\t2200\tr\t0\t1\t\t\t\t\n",
                    "SELECT c.oid,c.*,d.description,pg_catalog.pg_get_expr(c.relpartbound, c.oid) as partition_expr,  pg_catalog.pg_get_partkeydef(c.oid) as partition_key \n" +
                            "FROM pg_catalog.pg_class c\n" +
                            "LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=c.oid AND d.objsubid=0 AND d.classoid='pg_class'::regclass\n" +
                            "WHERE c.relnamespace=2200 AND c.relkind not in ('i','I','c')",
                    null,
                    false,
                    sqlExecutionContext,
                    false,
                    false
            );

        });
    }

    @Test
    public void testListTypes() throws SqlException {
        assertQuery(
                "oid\toid1\ttypname\ttypbasetype\ttyparray\ttypnamespace\ttypnotnull\ttyptypmod\ttyptype\ttyprelid\trelkind\tbase_type_name\tdescription\n" +
                        "16\t16\tBOOL\t0\t0\t2200\tfalse\t0\tb\tNaN\t\t\t\n" +
                        "17\t17\tBINARY\t0\t0\t2200\tfalse\t0\tb\tNaN\t\t\t\n" +
                        "18\t18\tCHAR\t0\t0\t2200\tfalse\t0\tb\tNaN\t\t\t\n" +
                        "20\t20\tINT8\t0\t0\t2200\tfalse\t0\tb\tNaN\t\t\t\n" +
                        "21\t21\tINT2\t0\t0\t2200\tfalse\t0\tb\tNaN\t\t\t\n" +
                        "23\t23\tINT4\t0\t0\t2200\tfalse\t0\tb\tNaN\t\t\t\n" +
                        "700\t700\tFLOAT4\t0\t0\t2200\tfalse\t0\tb\tNaN\t\t\t\n" +
                        "701\t701\tFLOAT8\t0\t0\t2200\tfalse\t0\tb\tNaN\t\t\t\n" +
                        "1043\t1043\tVARCHAR\t0\t0\t2200\tfalse\t0\tb\tNaN\t\t\t\n" +
                        "1082\t1082\tDATE\t0\t0\t2200\tfalse\t0\tb\tNaN\t\t\t\n" +
                        "1114\t1114\tTIMESTAMP\t0\t0\t2200\tfalse\t0\tb\tNaN\t\t\t\n" +
                        "1700\t1700\tNUMERIC\t0\t0\t2200\tfalse\t0\tb\tNaN\t\t\t\n",
                "SELECT t.oid,t.*,c.relkind,format_type(nullif(t.typbasetype, 0), t.typtypmod) as base_type_name, d.description\n" +
                        "FROM pg_catalog.pg_type t\n" +
                        "LEFT OUTER JOIN pg_catalog.pg_class c ON c.oid=t.typrelid\n" +
                        "LEFT OUTER JOIN pg_catalog.pg_description d ON t.oid=d.objoid\n" +
                        "WHERE typnamespace=2200\n" +
                        "ORDER by t.oid",
                null,
                true,
                sqlExecutionContext,
                false,
                false
        );

    }
}
