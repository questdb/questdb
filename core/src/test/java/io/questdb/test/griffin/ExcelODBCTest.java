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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class ExcelODBCTest extends AbstractCairoTest {

    @Test
    public void testGetTableMetaDataQ1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table mytab (a int, b float)");
            assertQuery("""
                    select
                      n.nspname,
                      c.relname,
                      a.attname,
                      a.atttypid,
                      t.typname,
                      a.attnum,
                      a.attlen,
                      a.atttypmod,
                      a.attnotnull,
                      c.relhasrules,
                      c.relkind,
                      c.oid,
                      pg_get_expr(d.adbin, d.adrelid),
                      case
                        t.typtype
                        when 'd' then t.typbasetype
                        else 0
                      end,
                      t.typtypmod,
                      c.relhasoids,
                      attidentity,
                      c.relhassubclass
                    from
                      (
                        (
                          (
                            pg_catalog.pg_class c
                            inner join pg_catalog.pg_namespace n on n.oid = c.relnamespace
                            and c.relname like 'mytab'
                            and n.nspname like 'public'
                          )
                          inner join pg_catalog.pg_attribute a on (not a.attisdropped)
                          and a.attnum > 0
                          and a.attrelid = c.oid
                        )
                        inner join pg_catalog.pg_type t on t.oid = a.atttypid
                      )
                      left outer join pg_attrdef d on a.atthasdef
                      and d.adrelid = a.attrelid
                      and d.adnum = a.attnum
                    order by
                      n.nspname,
                      c.relname,
                      attnum;""")
                    .noLeakCheck()
                    .returns("""
                            nspname\trelname\tattname\tatttypid\ttypname\tattnum\tattlen\tatttypmod\tattnotnull\trelhasrules\trelkind\toid\tpg_get_expr\tswitch\ttyptypmod\trelhasoids\tattidentity\trelhassubclass
                            public\tmytab\ta\t23\tint4\t1\t4\t-1\tfalse\tfalse\tr\t1\t\t0\t0\tfalse\t\tfalse
                            public\tmytab\tb\t700\tfloat4\t2\t4\t-1\tfalse\tfalse\tr\t1\t\t0\t0\tfalse\t\tfalse
                            """);
        });
    }

    @Test
    public void testGetTablesIndexesQ2() throws Exception {
        assertQuery("""
                select
                  ta.attname,
                  ia.attnum,
                  ic.relname,
                  n.nspname,
                  tc.relname
                from
                  pg_catalog.pg_attribute ta,
                  pg_catalog.pg_attribute ia,
                  pg_catalog.pg_class tc,
                  pg_catalog.pg_index i,
                  pg_catalog.pg_namespace n,
                  pg_catalog.pg_class ic
                where
                  tc.relname = 'telemetry_config'
                  AND n.nspname = 'public'
                  AND tc.oid = i.indrelid
                  AND n.oid = tc.relnamespace
                  AND i.indisprimary = 't'
                  AND ia.attrelid = i.indexrelid
                  AND ta.attrelid = i.indrelid
                  AND ta.attnum = i.indkey [ ia.attnum -1 ]
                  AND (NOT ta.attisdropped)
                  AND (NOT ia.attisdropped)
                  AND ic.oid = i.indexrelid
                order by
                  ia.attnum;""")
                .returns("attname\tattnum\trelname\tnspname\trelname1\n");
    }

    @Test
    public void testGetTablesIndexesQ3() throws Exception {
        assertQuery("""
                select
                 ta.attname,
                 ia.attnum,
                 ic.relname,
                 n.nspname,
                 NULL
                from
                  pg_catalog.pg_attribute ta,
                  pg_catalog.pg_attribute ia,
                  pg_catalog.pg_class ic,
                  pg_catalog.pg_index i,
                 pg_catalog.pg_namespace n
                where
                 ic.relname = 'telemetry_config_pkey'
                 AND n.nspname = 'public'
                 AND ic.oid = i.indexrelid
                 AND n.oid = ic.relnamespace
                 AND ia.attrelid = i.indexrelid
                 AND ta.attrelid = i.indrelid
                 AND ta.attnum = i.indkey [ ia.attnum -1 ]
                 AND (NOT ta.attisdropped)
                 AND (NOT ia.attisdropped)
                order by
                  ia.attnum
                """)
                .returns("attname\tattnum\trelname\tnspname\tNULL\n");
    }
}
