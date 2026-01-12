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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class DBeaverTest extends AbstractCairoTest {

    @Test
    public void testDotNetGetTypes() throws Exception {
        assertQuery(
                """
                        nspname\toid\ttypnamespace\ttypname\ttyptype\ttyprelid\ttypnotnull\trelkind\telemtypoid\telemtypname\telemrelkind\telemtyptype\tord
                        pg_catalog\t1043\t11\tvarchar\tb\tnull\tfalse\t\tnull\t\t\t\t0
                        pg_catalog\t1114\t11\ttimestamp\tb\tnull\tfalse\t\tnull\t\t\t\t0
                        pg_catalog\t701\t11\tfloat8\tb\tnull\tfalse\t\tnull\t\t\t\t0
                        pg_catalog\t700\t11\tfloat4\tb\tnull\tfalse\t\tnull\t\t\t\t0
                        pg_catalog\t23\t11\tint4\tb\tnull\tfalse\t\tnull\t\t\t\t0
                        pg_catalog\t21\t11\tint2\tb\tnull\tfalse\t\tnull\t\t\t\t0
                        pg_catalog\t1042\t11\tbpchar\tb\tnull\tfalse\t\tnull\t\t\t\t0
                        pg_catalog\t20\t11\tint8\tb\tnull\tfalse\t\tnull\t\t\t\t0
                        pg_catalog\t16\t11\tbool\tb\tnull\tfalse\t\tnull\t\t\t\t0
                        pg_catalog\t17\t11\tbinary\tb\tnull\tfalse\t\tnull\t\t\t\t0
                        pg_catalog\t1082\t11\tdate\tb\tnull\tfalse\t\tnull\t\t\t\t0
                        pg_catalog\t2950\t11\tuuid\tb\tnull\tfalse\t\tnull\t\t\t\t0
                        pg_catalog\t2281\t11\tinternal\tb\tnull\tfalse\t\tnull\t\t\t\t0
                        pg_catalog\t26\t11\toid\tb\tnull\tfalse\t\tnull\t\t\t\t0
                        pg_catalog\t1022\t11\t_float8\tb\tnull\tfalse\t\tnull\t\t\t\t0
                        pg_catalog\t1700\t11\tnumeric\tb\tnull\tfalse\t\tnull\t\t\t\t0
                        pg_catalog\t1015\t11\t_varchar\tb\tnull\tfalse\t\tnull\t\t\t\t0
                        """,
                """
                        SELECT ns.nspname, typ_and_elem_type.*,
                           CASE
                               WHEN typtype IN ('b', 'e', 'p') THEN 0           -- First base types, enums, pseudo-types
                               WHEN typtype = 'r' THEN 1                        -- Ranges after
                               WHEN typtype = 'c' THEN 2                        -- Composites after
                               WHEN typtype = 'd' AND elemtyptype <> 'a' THEN 3 -- Domains over non-arrays after
                               WHEN typtype = 'a' THEN 4                        -- Arrays before
                               WHEN typtype = 'd' AND elemtyptype = 'a' THEN 5  -- Domains over arrays last
                            END AS ord
                        FROM (
                            -- Arrays have typtype=b - this subquery identifies them by their typreceive and converts their typtype to a
                            -- We first do this for the type (innermost subquery), and then for its element type
                            -- This also returns the array element, range subtype and domain base type as elemtypoid
                            SELECT
                                typ.oid, typ.typnamespace, typ.typname, typ.typtype, typ.typrelid, typ.typnotnull, typ.relkind,
                                elemtyp.oid AS elemtypoid, elemtyp.typname AS elemtypname, elemcls.relkind AS elemrelkind,
                                CASE WHEN elemproc.proname='array_recv' THEN 'a' ELSE elemtyp.typtype END AS elemtyptype
                            FROM (
                                SELECT typ.oid, typnamespace, typname, typrelid, typnotnull, relkind, typelem AS elemoid,
                                    CASE WHEN proc.proname='array_recv' THEN 'a' ELSE typ.typtype END AS typtype,
                                    CASE
                                        WHEN proc.proname='array_recv' THEN typ.typelem
                                        WHEN typ.typtype='r' THEN rngsubtype
                                        WHEN typ.typtype='d' THEN typ.typbasetype
                                    END AS elemtypoid
                                FROM pg_type AS typ
                                LEFT JOIN pg_class AS cls ON (cls.oid = typ.typrelid)
                                LEFT JOIN pg_proc AS proc ON proc.oid = typ.typreceive
                                LEFT JOIN pg_range ON (pg_range.rngtypid = typ.oid)
                            ) AS typ
                            LEFT JOIN pg_type AS elemtyp ON elemtyp.oid = elemtypoid
                            LEFT JOIN pg_class AS elemcls ON (elemcls.oid = elemtyp.typrelid)
                            LEFT JOIN pg_proc AS elemproc ON elemproc.oid = elemtyp.typreceive
                        ) AS typ_and_elem_type
                        JOIN pg_namespace AS ns ON (ns.oid = typnamespace)
                        WHERE
                            typtype IN ('b', 'r', 'e', 'd') OR -- Base, range, enum, domain
                            (typtype = 'c' AND relkind='c') OR -- User-defined free-standing composites (not table composites) by default
                            (typtype = 'p' AND typname IN ('record', 'void')) OR -- Some special supported pseudo-types
                            (typtype = 'a' AND (  -- Array of...
                                elemtyptype IN ('b', 'r', 'e', 'd') OR -- Array of base, range, enum, domain
                                (elemtyptype = 'p' AND elemtypname IN ('record', 'void')) OR -- Arrays of special supported pseudo-types
                                (elemtyptype = 'c' AND elemrelkind='c') -- Array of user-defined free-standing composites (not table composites) by default
                            ))
                        ORDER BY ord""",
                null,
                true,
                true
        );
    }

    @Test
    public void testFrequentSql() throws Exception {
        assertQuery(
                """
                        current_schema\tsession_user
                        public\tadmin
                        """,
                "SELECT current_schema(),session_user",
                null,
                true,
                true
        );
    }

    @Test
    public void testListColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(a int, t timestamp)");
            execute("create table tab2(b long, z binary)");

            assertQueryNoLeakCheck(
                    """
                            relname\tattrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef\tdef_value\tdescription
                            xyz\t1\ta\t1\t23\tfalse\t-1\t4\t\tfalse\ttrue\t\t
                            xyz\t1\tt\t2\t1114\tfalse\t-1\t8\t\tfalse\ttrue\t\t
                            """,
                    """
                            SELECT\s
                                c.relname,
                                a.*,
                                pg_catalog.pg_get_expr(ad.adbin, ad.adrelid, true) as def_value,
                                dsc.description
                            FROM pg_catalog.pg_attribute a
                            INNER JOIN pg_catalog.pg_class c ON (a.attrelid=c.oid)
                            LEFT OUTER JOIN pg_catalog.pg_attrdef ad ON (a.attrelid=ad.adrelid AND a.attnum = ad.adnum)
                            LEFT OUTER JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)
                            WHERE NOT a.attisdropped AND c.oid=1 ORDER BY a.attnum""",
                    null,
                    true,
                    false
            );
        });
    }

    @Test
    public void testListTables() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xyz(a int)");
            execute("create table tab2(b long)");
            assertQueryNoLeakCheck(
                    """
                            oid tral\toid\trelname\trelnamespace\treltype\treloftype\trelowner\trelam\trelfilenode\treltablespace\trelpages\treltuples\trelallvisible\treltoastrelid\trelhasindex\trelisshared\trelpersistence\trelkind\trelnatts\trelchecks\trelhasrules\trelhastriggers\trelhassubclass\trelrowsecurity\trelforcerowsecurity\trelispopulated\trelreplident\trelispartition\trelrewrite\trelfrozenxid\trelminmxid\trelacl\treloptions\trelpartbound\trelhasoids\txmin\tdescription\tpartition_expr\tpartition_key
                            2\t2\ttab2\t2200\t0\t0\t0\t0\t0\t0\tfalse\t-1.0\t0\t0\tfalse\tfalse\tp\tr\t0\t0\tfalse\tfalse\tfalse\tfalse\tfalse\ttrue\td\tfalse\t0\t0\t0\t\t\t\tfalse\t0\t\t\t
                            1\t1\txyz\t2200\t0\t0\t0\t0\t0\t0\tfalse\t-1.0\t0\t0\tfalse\tfalse\tp\tr\t0\t0\tfalse\tfalse\tfalse\tfalse\tfalse\ttrue\td\tfalse\t0\t0\t0\t\t\t\tfalse\t0\t\t\t
                            """,
                    """
                            SELECT c.oid "oid tral",c.*,d.description,pg_catalog.pg_get_expr(c.relpartbound, c.oid) as partition_expr,  pg_catalog.pg_get_partkeydef(c.oid) as partition_key\s
                            FROM pg_catalog.pg_class c
                            LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=c.oid AND d.objsubid=0 AND d.classoid='pg_class'::regclass
                            WHERE c.relnamespace=2200 AND c.relkind not in ('i','I','c') order by relname""",
                    null,
                    true,
                    false
            );
        });
    }

    @Test
    public void testListTypes() throws Exception {
        assertQuery(
                """
                        oid1\toid\ttypname\ttypbasetype\ttyparray\ttypnamespace\ttypnotnull\ttyptypmod\ttyptype\ttypcategory\ttyprelid\ttypelem\ttypreceive\ttypdelim\ttypinput\ttypowner\ttyplen\ttypbyval\ttypispreferred\ttypisdefined\ttypalign\ttypstorage\ttypndims\ttypcollation\ttypdefault\trelkind\tbase_type_name\tdescription
                        16\t16\tbool\t0\t1000\t11\tfalse\t0\tb\tB\tnull\t0\t0\t0\t0\t0\t1\tfalse\tfalse\ttrue\tc\tp\t0\t0\tfalse\t\t\t
                        17\t17\tbinary\t0\t0\t11\tfalse\t0\tb\tU\tnull\t0\t0\t0\t0\t0\t-1\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\t\t\t
                        20\t20\tint8\t0\t1016\t11\tfalse\t0\tb\tN\tnull\t0\t0\t0\t0\t0\t8\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\t\t\t
                        21\t21\tint2\t0\t1005\t11\tfalse\t0\tb\tN\tnull\t0\t0\t0\t0\t0\t2\tfalse\tfalse\ttrue\tc\tp\t0\t0\t0\t\t\t
                        23\t23\tint4\t0\t1007\t11\tfalse\t0\tb\tN\tnull\t0\t0\t0\t0\t0\t4\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\t\t\t
                        26\t26\toid\t0\t0\t11\tfalse\t0\tb\tN\tnull\t0\t0\t0\t0\t0\t4\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\t\t\t
                        700\t700\tfloat4\t0\t1021\t11\tfalse\t0\tb\tN\tnull\t0\t0\t0\t0\t0\t4\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\t\t\t
                        701\t701\tfloat8\t0\t1022\t11\tfalse\t0\tb\tN\tnull\t0\t0\t0\t0\t0\t8\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\t\t\t
                        1015\t1015\t_varchar\t0\t0\t11\tfalse\t0\tb\tA\tnull\t1043\t0\t0\t0\t0\t-1\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\t\t\t
                        1022\t1022\t_float8\t0\t0\t11\tfalse\t0\tb\tA\tnull\t701\t0\t0\t0\t0\t-1\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\t\t\t
                        1042\t1042\tbpchar\t0\t0\t11\tfalse\t0\tb\tZ\tnull\t0\t0\t0\t0\t0\t2\tfalse\tfalse\ttrue\tc\tp\t0\t0\t0\t\t\t
                        1043\t1043\tvarchar\t0\t1015\t11\tfalse\t0\tb\tS\tnull\t0\t0\t0\t0\t0\t-1\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\t\t\t
                        1082\t1082\tdate\t0\t0\t11\tfalse\t0\tb\tD\tnull\t0\t0\t0\t0\t0\t8\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\t\t\t
                        1114\t1114\ttimestamp\t0\t1115\t11\tfalse\t0\tb\tD\tnull\t0\t0\t0\t0\t0\t8\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\t\t\t
                        1700\t1700\tnumeric\t0\t0\t11\tfalse\t0\tb\tN\tnull\t0\t0\t0\t0\t0\t-1\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\t\t\t
                        2281\t2281\tinternal\t0\t0\t11\tfalse\t0\tb\tP\tnull\t0\t0\t0\t0\t0\t8\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\t\t\t
                        2950\t2950\tuuid\t0\t0\t11\tfalse\t0\tb\tU\tnull\t0\t0\t0\t0\t0\t16\tfalse\tfalse\ttrue\tc\tp\t0\t0\t\t\t\t
                        """,
                """
                        SELECT t.oid as oid1,t.*,c.relkind,format_type(nullif(t.typbasetype, 0), t.typtypmod) as base_type_name, d.description
                        FROM pg_catalog.pg_type t
                        LEFT OUTER JOIN pg_catalog.pg_class c ON c.oid=t.typrelid
                        LEFT OUTER JOIN pg_catalog.pg_description d ON t.oid=d.objoid
                        WHERE typnamespace=11
                        ORDER by t.oid""",
                null,
                true,
                false
        );
    }

    @Test
    public void testNamespaceListSql() throws Exception {
        assertQuery(
                """
                        n_oid\tnspname\toid\txmin\tnspowner\tdescription
                        11\tpg_catalog\t11\t0\t1\t
                        2200\tpublic\t2200\t0\t1\t
                        """,
                """
                        SELECT n.oid "n_oid",n.*,d.description FROM pg_catalog.pg_namespace n
                        LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass
                         ORDER BY nspname""",
                null,
                true,
                false
        );
    }

    @Test
    public void testShowDefaultTransactionReadOnly() throws Exception {
        assertQuery(
                """
                        default_transaction_read_only
                        off
                        """,
                "SHOW default_transaction_read_only",
                null,
                false,
                true
        );
    }

    @Test
    public void testShowSearchPath() throws Exception {
        assertQuery(
                """
                        search_path
                        "$user", public
                        """,
                "SHOW search_path",
                null,
                false,
                true
        );
    }
}
