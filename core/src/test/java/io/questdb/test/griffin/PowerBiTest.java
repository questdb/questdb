/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
        assertQuery(
                "nspname\ttypname\toid\ttyprelid\ttypbasetype\ttype\telemoid\tord\n" +
                        "pg_catalog\tvarchar\t1043\tnull\t0\tb\t0\t0\n" +
                        "pg_catalog\ttimestamp\t1114\tnull\t0\tb\t0\t0\n" +
                        "pg_catalog\tfloat8\t701\tnull\t0\tb\t0\t0\n" +
                        "pg_catalog\tfloat4\t700\tnull\t0\tb\t0\t0\n" +
                        "pg_catalog\tint4\t23\tnull\t0\tb\t0\t0\n" +
                        "pg_catalog\tint2\t21\tnull\t0\tb\t0\t0\n" +
                        "pg_catalog\tbpchar\t1042\tnull\t0\tb\t0\t0\n" +
                        "pg_catalog\tint8\t20\tnull\t0\tb\t0\t0\n" +
                        "pg_catalog\tbool\t16\tnull\t0\tb\t0\t0\n" +
                        "pg_catalog\tbinary\t17\tnull\t0\tb\t0\t0\n" +
                        "pg_catalog\tdate\t1082\tnull\t0\tb\t0\t0\n" +
                        "pg_catalog\tuuid\t2950\tnull\t0\tb\t0\t0\n" +
                        "pg_catalog\tinternal\t2281\tnull\t0\tb\t0\t0\n" +
                        "pg_catalog\toid\t26\tnull\t0\tb\t0\t0\n" +
                        "pg_catalog\t_float8\t1022\tnull\t0\tb\t0\t0\n" +
                        "pg_catalog\tnumeric\t1700\tnull\t0\tb\t0\t0\n",
                "SELECT ns.nspname, a.typname, a.oid, a.typrelid, a.typbasetype,\n" +
                        "CASE WHEN pg_proc.proname='array_recv' THEN 'a' ELSE a.typtype END AS type,\n" +
                        "CASE\n" +
                        "  WHEN pg_proc.proname='array_recv' THEN a.typelem\n" +
                        "  WHEN a.typtype='r' THEN rngsubtype\n" +
                        "  ELSE 0\n" +
                        "END AS elemoid,\n" +
                        "CASE\n" +
                        "  WHEN pg_proc.proname IN ('array_recv','oidvectorrecv') THEN 3    /* Arrays last */\n" +
                        "  WHEN a.typtype='r' THEN 2                                        /* Ranges before */\n" +
                        "  WHEN a.typtype='d' THEN 1                                        /* Domains before */\n" +
                        "  ELSE 0                                                           /* Base types first */\n" +
                        "END AS ord\n" +
                        "FROM pg_type AS a\n" +
                        "JOIN pg_namespace AS ns ON (ns.oid = a.typnamespace)\n" +
                        "JOIN pg_proc ON pg_proc.oid = a.typreceive\n" +
                        "LEFT OUTER JOIN pg_class AS cls ON (cls.oid = a.typrelid)\n" +
                        "LEFT OUTER JOIN pg_type AS b ON (b.oid = a.typelem)\n" +
                        "LEFT OUTER JOIN pg_class AS elemcls ON (elemcls.oid = b.typrelid)\n" +
                        "LEFT OUTER JOIN pg_range ON (pg_range.rngtypid = a.oid) \n" +
                        "WHERE\n" +
                        "  a.typtype IN ('b', 'r', 'e', 'd') OR         /* Base, range, enum, domain */\n" +
                        "  (a.typtype = 'c' AND cls.relkind='c') OR /* User-defined free-standing composites (not table composites) by default */\n" +
                        "  (pg_proc.proname='array_recv' AND (\n" +
                        "    b.typtype IN ('b', 'r', 'e', 'd') OR       /* Array of base, range, enum, domain */\n" +
                        "    (b.typtype = 'p' AND b.typname IN ('record', 'void')) OR /* Arrays of special supported pseudo-types */\n" +
                        "    (b.typtype = 'c' AND elemcls.relkind='c')  /* Array of user-defined free-standing composites (not table composites) */\n" +
                        "  )) OR\n" +
                        "  (a.typtype = 'p' AND a.typname IN ('record', 'void'))  /* Some special supported pseudo-types */\n" +
                        "ORDER BY ord",
                null,
                true,
                false
        );
    }
}
