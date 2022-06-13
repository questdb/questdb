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

import org.junit.Test;

public class DBeaverTest extends AbstractGriffinTest {
    @Test
    public void testDotNetGetTypes() throws SqlException {
        assertQuery(
                "nspname\toid\ttypnamespace\ttypname\ttyptype\ttyprelid\ttypnotnull\trelkind\telemtypoid\telemtypname\telemrelkind\telemtyptype\tord\n" +
                        "public\t1043\t2200\tvarchar\tb\tNaN\tfalse\t\tNaN\t\t\t\t0\n" +
                        "public\t1114\t2200\ttimestamp\tb\tNaN\tfalse\t\tNaN\t\t\t\t0\n" +
                        "public\t701\t2200\tfloat8\tb\tNaN\tfalse\t\tNaN\t\t\t\t0\n" +
                        "public\t700\t2200\tfloat4\tb\tNaN\tfalse\t\tNaN\t\t\t\t0\n" +
                        "public\t23\t2200\tint4\tb\tNaN\tfalse\t\tNaN\t\t\t\t0\n" +
                        "public\t21\t2200\tint2\tb\tNaN\tfalse\t\tNaN\t\t\t\t0\n" +
                        "public\t18\t2200\tchar\tb\tNaN\tfalse\t\tNaN\t\t\t\t0\n" +
                        "public\t20\t2200\tint8\tb\tNaN\tfalse\t\tNaN\t\t\t\t0\n" +
                        "public\t16\t2200\tbool\tb\tNaN\tfalse\t\tNaN\t\t\t\t0\n" +
                        "public\t17\t2200\tbinary\tb\tNaN\tfalse\t\tNaN\t\t\t\t0\n" +
                        "public\t1082\t2200\tdate\tb\tNaN\tfalse\t\tNaN\t\t\t\t0\n",
                "SELECT ns.nspname, typ_and_elem_type.*,\n" +
                        "   CASE\n" +
                        "       WHEN typtype IN ('b', 'e', 'p') THEN 0           -- First base types, enums, pseudo-types\n" +
                        "       WHEN typtype = 'r' THEN 1                        -- Ranges after\n" +
                        "       WHEN typtype = 'c' THEN 2                        -- Composites after\n" +
                        "       WHEN typtype = 'd' AND elemtyptype <> 'a' THEN 3 -- Domains over non-arrays after\n" +
                        "       WHEN typtype = 'a' THEN 4                        -- Arrays before\n" +
                        "       WHEN typtype = 'd' AND elemtyptype = 'a' THEN 5  -- Domains over arrays last\n" +
                        "    END AS ord\n" +
                        "FROM (\n" +
                        "    -- Arrays have typtype=b - this subquery identifies them by their typreceive and converts their typtype to a\n" +
                        "    -- We first do this for the type (innerest-most subquery), and then for its element type\n" +
                        "    -- This also returns the array element, range subtype and domain base type as elemtypoid\n" +
                        "    SELECT\n" +
                        "        typ.oid, typ.typnamespace, typ.typname, typ.typtype, typ.typrelid, typ.typnotnull, typ.relkind,\n" +
                        "        elemtyp.oid AS elemtypoid, elemtyp.typname AS elemtypname, elemcls.relkind AS elemrelkind,\n" +
                        "        CASE WHEN elemproc.proname='array_recv' THEN 'a' ELSE elemtyp.typtype END AS elemtyptype\n" +
                        "    FROM (\n" +
                        "        SELECT typ.oid, typnamespace, typname, typrelid, typnotnull, relkind, typelem AS elemoid,\n" +
                        "            CASE WHEN proc.proname='array_recv' THEN 'a' ELSE typ.typtype END AS typtype,\n" +
                        "            CASE\n" +
                        "                WHEN proc.proname='array_recv' THEN typ.typelem\n" +
                        "                WHEN typ.typtype='r' THEN rngsubtype\n" +
                        "                WHEN typ.typtype='d' THEN typ.typbasetype\n" +
                        "            END AS elemtypoid\n" +
                        "        FROM pg_type AS typ\n" +
                        "        LEFT JOIN pg_class AS cls ON (cls.oid = typ.typrelid)\n" +
                        "        LEFT JOIN pg_proc AS proc ON proc.oid = typ.typreceive\n" +
                        "        LEFT JOIN pg_range ON (pg_range.rngtypid = typ.oid)\n" +
                        "    ) AS typ\n" +
                        "    LEFT JOIN pg_type AS elemtyp ON elemtyp.oid = elemtypoid\n" +
                        "    LEFT JOIN pg_class AS elemcls ON (elemcls.oid = elemtyp.typrelid)\n" +
                        "    LEFT JOIN pg_proc AS elemproc ON elemproc.oid = elemtyp.typreceive\n" +
                        ") AS typ_and_elem_type\n" +
                        "JOIN pg_namespace AS ns ON (ns.oid = typnamespace)\n" +
                        "WHERE\n" +
                        "    typtype IN ('b', 'r', 'e', 'd') OR -- Base, range, enum, domain\n" +
                        "    (typtype = 'c' AND relkind='c') OR -- User-defined free-standing composites (not table composites) by default\n" +
                        "    (typtype = 'p' AND typname IN ('record', 'void')) OR -- Some special supported pseudo-types\n" +
                        "    (typtype = 'a' AND (  -- Array of...\n" +
                        "        elemtyptype IN ('b', 'r', 'e', 'd') OR -- Array of base, range, enum, domain\n" +
                        "        (elemtyptype = 'p' AND elemtypname IN ('record', 'void')) OR -- Arrays of special supported pseudo-types\n" +
                        "        (elemtyptype = 'c' AND elemrelkind='c') -- Array of user-defined free-standing composites (not table composites) by default\n" +
                        "    ))\n" +
                        "ORDER BY ord",
                null,
                true,
                sqlExecutionContext,
                false,
                false
        );
    }

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
                    "relname\tattrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef\tdef_value\tdescription\n" +
                            "xyz\t1\ta\t1\t23\tfalse\t0\t4\t\tfalse\ttrue\t\t\n" +
                            "xyz\t1\tt\t2\t1114\tfalse\t0\t-1\t\tfalse\ttrue\t\t\n",
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
    public void testListTables() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table xyz(a int)", sqlExecutionContext);
            compiler.compile("create table tab2(b long)", sqlExecutionContext);
            assertQuery(
                    "oid tral\trelname\trelnamespace\trelkind\trelowner\toid\trelpartbound\trelhasrules\trelhasoids\trelhassubclass\tdescription\tpartition_expr\tpartition_key\n" +
                            "2\ttab2\t2200\tr\t0\t2\t\tfalse\tfalse\tfalse\t\t\t\n" +
                            "1\txyz\t2200\tr\t0\t1\t\tfalse\tfalse\tfalse\t\t\t\n",
                    "SELECT c.oid \"oid tral\",c.*,d.description,pg_catalog.pg_get_expr(c.relpartbound, c.oid) as partition_expr,  pg_catalog.pg_get_partkeydef(c.oid) as partition_key \n" +
                            "FROM pg_catalog.pg_class c\n" +
                            "LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=c.oid AND d.objsubid=0 AND d.classoid='pg_class'::regclass\n" +
                            "WHERE c.relnamespace=2200 AND c.relkind not in ('i','I','c') order by relname",
                    null,
                    true,
                    sqlExecutionContext,
                    false,
                    false
            );

        });
    }

    @Test
    public void testListTypes() throws SqlException {
        assertQuery(
                "oid1\toid\ttypname\ttypbasetype\ttyparray\ttypnamespace\ttypnotnull\ttyptypmod\ttyptype\ttyprelid\ttypelem\ttypreceive\ttypdelim\ttypinput\trelkind\tbase_type_name\tdescription\n" +
                        "16\t16\tbool\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\t\t\t\n" +
                        "17\t17\tbinary\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\t\t\t\n" +
                        "18\t18\tchar\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\t\t\t\n" +
                        "20\t20\tint8\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\t\t\t\n" +
                        "21\t21\tint2\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\t\t\t\n" +
                        "23\t23\tint4\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\t\t\t\n" +
                        "700\t700\tfloat4\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\t\t\t\n" +
                        "701\t701\tfloat8\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\t\t\t\n" +
                        "1043\t1043\tvarchar\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\t\t\t\n" +
                        "1082\t1082\tdate\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\t\t\t\n" +
                        "1114\t1114\ttimestamp\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\t\t\t\n",
                "SELECT t.oid as oid1,t.*,c.relkind,format_type(nullif(t.typbasetype, 0), t.typtypmod) as base_type_name, d.description\n" +
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

    @Test
    public void testNamespaceListSql() throws SqlException {
        assertQuery(
                "n_oid\tnspname\toid\txmin\tnspowner\tdescription\n" +
                        "11\tpg_catalog\t11\t0\t1\t\n" +
                        "2200\tpublic\t2200\t0\t1\t\n",
                "SELECT n.oid \"n_oid\",n.*,d.description FROM pg_catalog.pg_namespace n\n" +
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
}
