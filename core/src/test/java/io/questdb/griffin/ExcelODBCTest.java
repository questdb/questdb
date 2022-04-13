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

public class ExcelODBCTest extends AbstractGriffinTest {

    @Test
    public void testGetTableMetaDataQ1() throws SqlException {
        compiler.compile("create table mytab (a int, b float)", sqlExecutionContext);
        assertQuery(
                "nspname\trelname\tattname\tatttypid\ttypname\tattnum\tattlen\tatttypmod\tattnotnull\trelhasrules\trelkind\toid\tpg_get_expr\tswitch\ttyptypmod\trelhasoids\tattidentity\trelhassubclass\n" +
                        "public\tmytab\ta\t23\tint4\t1\t4\t0\tfalse\tfalse\tr\t1\t\t0\t0\tfalse\t\tfalse\n" +
                        "public\tmytab\tb\t700\tfloat4\t2\t4\t0\tfalse\tfalse\tr\t1\t\t0\t0\tfalse\t\tfalse\n",
                "select\n" +
                        "  n.nspname,\n" +
                        "  c.relname,\n" +
                        "  a.attname,\n" +
                        "  a.atttypid,\n" +
                        "  t.typname,\n" +
                        "  a.attnum,\n" +
                        "  a.attlen,\n" +
                        "  a.atttypmod,\n" +
                        "  a.attnotnull,\n" +
                        "  c.relhasrules,\n" +
                        "  c.relkind,\n" +
                        "  c.oid,\n" +
                        "  pg_get_expr(d.adbin, d.adrelid),\n" +
                        "  case\n" +
                        "    t.typtype\n" +
                        "    when 'd' then t.typbasetype\n" +
                        "    else 0\n" +
                        "  end,\n" +
                        "  t.typtypmod,\n" +
                        "  c.relhasoids,\n" +
                        "  attidentity,\n" +
                        "  c.relhassubclass\n" +
                        "from\n" +
                        "  (\n" +
                        "    (\n" +
                        "      (\n" +
                        "        pg_catalog.pg_class c\n" +
                        "        inner join pg_catalog.pg_namespace n on n.oid = c.relnamespace\n" +
                        "        and c.relname like 'mytab'\n" +
                        "        and n.nspname like 'public'\n" +
                        "      )\n" +
                        "      inner join pg_catalog.pg_attribute a on (not a.attisdropped)\n" +
                        "      and a.attnum > 0\n" +
                        "      and a.attrelid = c.oid\n" +
                        "    )\n" +
                        "    inner join pg_catalog.pg_type t on t.oid = a.atttypid\n" +
                        "  )\n" +
                        "  left outer join pg_attrdef d on a.atthasdef\n" +
                        "  and d.adrelid = a.attrelid\n" +
                        "  and d.adnum = a.attnum\n" +
                        "order by\n" +
                        "  n.nspname,\n" +
                        "  c.relname,\n" +
                        "  attnum;",
                null,
                true,
                sqlExecutionContext,
                false,
                false
        );
    }

    @Test
    public void testGetTablesIndexesQ2() throws SqlException {
        assertQuery(
                "attname\tattnum\trelname\tnspname\trelname1\n",
                "select\n" +
                        "  ta.attname,\n" +
                        "  ia.attnum,\n" +
                        "  ic.relname,\n" +
                        "  n.nspname,\n" +
                        "  tc.relname\n" +
                        "from\n" +
                        "  pg_catalog.pg_attribute ta,\n" +
                        "  pg_catalog.pg_attribute ia,\n" +
                        "  pg_catalog.pg_class tc,\n" +
                        "  pg_catalog.pg_index i,\n" +
                        "  pg_catalog.pg_namespace n,\n" +
                        "  pg_catalog.pg_class ic\n" +
                        "where\n" +
                        "  tc.relname = 'telemetry_config'\n" +
                        "  AND n.nspname = 'public'\n" +
                        "  AND tc.oid = i.indrelid\n" +
                        "  AND n.oid = tc.relnamespace\n" +
                        "  AND i.indisprimary = 't'\n" +
                        "  AND ia.attrelid = i.indexrelid\n" +
                        "  AND ta.attrelid = i.indrelid\n" +
                        "  AND ta.attnum = i.indkey [ ia.attnum -1 ]\n" +
                        "  AND (NOT ta.attisdropped)\n" +
                        "  AND (NOT ia.attisdropped)\n" +
                        "  AND ic.oid = i.indexrelid\n" +
                        "order by\n" +
                        "  ia.attnum;",
                null,
                true,
                sqlExecutionContext,
                false,
                false
        );
    }

    @Test
    public void testGetTablesIndexesQ3() throws SqlException {
        assertQuery(
                "attname\tattnum\trelname\tnspname\tNULL\n",
                "select\n" +
                        " ta.attname,\n" +
                        " ia.attnum,\n" +
                        " ic.relname,\n" +
                        " n.nspname,\n" +
                        " NULL\n" +
                        "from\n" +
                        "  pg_catalog.pg_attribute ta,\n" +
                        "  pg_catalog.pg_attribute ia,\n" +
                        "  pg_catalog.pg_class ic,\n" +
                        "  pg_catalog.pg_index i,\n" +
                        " pg_catalog.pg_namespace n\n" +
                        "where\n" +
                        " ic.relname = 'telemetry_config_pkey'\n" +
                        " AND n.nspname = 'public'\n" +
                        " AND ic.oid = i.indexrelid\n" +
                        " AND n.oid = ic.relnamespace\n" +
                        " AND ia.attrelid = i.indexrelid\n" +
                        " AND ta.attrelid = i.indrelid\n" +
                        " AND ta.attnum = i.indkey [ ia.attnum -1 ]\n" +
                        " AND (NOT ta.attisdropped)\n" +
                        " AND (NOT ia.attisdropped)\n" +
                        "order by\n" +
                        "  ia.attnum\n",
                null,
                true,
                sqlExecutionContext,
                false,
                false
        );
    }
}
