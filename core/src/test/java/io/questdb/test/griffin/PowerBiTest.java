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

public class PowerBiTest extends AbstractCairoTest {

    @Test
    public void testGetTypes() throws Exception {
        // PowerBI runs this SQL on connection startup
        assertQuery("""
                SELECT ns.nspname, a.typname, a.oid, a.typrelid, a.typbasetype,
                CASE WHEN pg_proc.proname='array_recv' THEN 'a' ELSE a.typtype END AS type,
                CASE
                  WHEN pg_proc.proname='array_recv' THEN a.typelem
                  WHEN a.typtype='r' THEN rngsubtype
                  ELSE 0
                END AS elemoid,
                CASE
                  WHEN pg_proc.proname IN ('array_recv','oidvectorrecv') THEN 3    /* Arrays last */
                  WHEN a.typtype='r' THEN 2                                        /* Ranges before */
                  WHEN a.typtype='d' THEN 1                                        /* Domains before */
                  ELSE 0                                                           /* Base types first */
                END AS ord
                FROM pg_type AS a
                JOIN pg_namespace AS ns ON (ns.oid = a.typnamespace)
                JOIN pg_proc ON pg_proc.oid = a.typreceive
                LEFT OUTER JOIN pg_class AS cls ON (cls.oid = a.typrelid)
                LEFT OUTER JOIN pg_type AS b ON (b.oid = a.typelem)
                LEFT OUTER JOIN pg_class AS elemcls ON (elemcls.oid = b.typrelid)
                LEFT OUTER JOIN pg_range ON (pg_range.rngtypid = a.oid)\s
                WHERE
                  a.typtype IN ('b', 'r', 'e', 'd') OR         /* Base, range, enum, domain */
                  (a.typtype = 'c' AND cls.relkind='c') OR /* User-defined free-standing composites (not table composites) by default */
                  (pg_proc.proname='array_recv' AND (
                    b.typtype IN ('b', 'r', 'e', 'd') OR       /* Array of base, range, enum, domain */
                    (b.typtype = 'p' AND b.typname IN ('record', 'void')) OR /* Arrays of special supported pseudo-types */
                    (b.typtype = 'c' AND elemcls.relkind='c')  /* Array of user-defined free-standing composites (not table composites) */
                  )) OR
                  (a.typtype = 'p' AND a.typname IN ('record', 'void'))  /* Some special supported pseudo-types */
                ORDER BY ord""")
                .returns("""
                        nspname\ttypname\toid\ttyprelid\ttypbasetype\ttype\telemoid\tord
                        pg_catalog\tvarchar\t1043\tnull\t0\tb\t0\t0
                        pg_catalog\ttimestamp\t1114\tnull\t0\tb\t0\t0
                        pg_catalog\tfloat8\t701\tnull\t0\tb\t0\t0
                        pg_catalog\tfloat4\t700\tnull\t0\tb\t0\t0
                        pg_catalog\tint4\t23\tnull\t0\tb\t0\t0
                        pg_catalog\tint2\t21\tnull\t0\tb\t0\t0
                        pg_catalog\tbpchar\t1042\tnull\t0\tb\t0\t0
                        pg_catalog\tint8\t20\tnull\t0\tb\t0\t0
                        pg_catalog\tbool\t16\tnull\t0\tb\t0\t0
                        pg_catalog\tbinary\t17\tnull\t0\tb\t0\t0
                        pg_catalog\tdate\t1082\tnull\t0\tb\t0\t0
                        pg_catalog\tuuid\t2950\tnull\t0\tb\t0\t0
                        pg_catalog\tinternal\t2281\tnull\t0\tb\t0\t0
                        pg_catalog\toid\t26\tnull\t0\tb\t0\t0
                        pg_catalog\t_float8\t1022\tnull\t0\tb\t0\t0
                        pg_catalog\tnumeric\t1700\tnull\t0\tb\t0\t0
                        pg_catalog\t_varchar\t1015\tnull\t0\tb\t0\t0
                        """);
    }
}
