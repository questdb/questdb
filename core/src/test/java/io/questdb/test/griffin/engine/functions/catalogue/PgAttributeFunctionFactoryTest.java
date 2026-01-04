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

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class PgAttributeFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCachedWindowQueryOrderedByColumnNotOnSelectList() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y (a int, b int)");
            execute("insert into y select x/4, x from long_sequence(10)");
            engine.releaseAllWriters();

            String query = "select b.a, row_number() OVER (PARTITION BY b.a ORDER BY b.b desc) as b " +
                    " from y b " +
                    "order by b.b";

            assertPlanNoLeakCheck(query,
                    """
                            SelectedRecord
                                Radix sort light
                                  keys: [b1]
                                    CachedWindow
                                      orderedFunctions: [[b desc] => [row_number() over (partition by [a1])]]
                                        SelectedRecord
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: y
                            """);

            assertQueryNoLeakCheck(
                    """
                            a\tb
                            0\t3
                            0\t2
                            0\t1
                            1\t4
                            1\t3
                            1\t2
                            1\t1
                            2\t3
                            2\t2
                            2\t1
                            """,
                    query,
                    null,
                    true,
                    true,
                    false
            );
        });
    }

    @Test
    public void testDropAndRecreateTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (old int)");

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery compile = compiler.compile("select * from pg_catalog.pg_attribute", sqlExecutionContext);

                // we use a single instance of RecordCursorFactory before and after table drop
                // this mimic behavior of a query cache.
                try (RecordCursorFactory recordCursorFactory = compile.getRecordCursorFactory()) {
                    try (RecordCursor cursor = recordCursorFactory.getCursor(sqlExecutionContext)) {
                        assertCursor("""
                                        attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef
                                        1\told\t1\t23\tfalse\t-1\t4\t\tfalse\ttrue
                                        """,
                                false, true, true, cursor, recordCursorFactory.getMetadata(), false);
                    }

                    // recreate the same table again, this time with a different column
                    execute("drop table x");
                    execute("create table x (new long)");
                    drainWalQueue();

                    try (RecordCursor cursor = recordCursorFactory.getCursor(sqlExecutionContext)) {
                        // note the ID is 2 now!
                        assertCursor("""
                                        attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef
                                        2\tnew\t1\t20\tfalse\t-1\t8\t\tfalse\ttrue
                                        """,
                                false, true, true, cursor, recordCursorFactory.getMetadata(), false);
                    }
                }
            }
        });
    }

    @Test
    public void testKafkaMetadataQuery() throws Exception {
        String query = """
                
                SELECT
                    result.TABLE_CAT,       \s
                    result.TABLE_SCHEM,       \s
                    result.TABLE_NAME,       \s
                    result.COLUMN_NAME,       \s
                    result.KEY_SEQ,       \s
                    result.PK_NAME,
                    result.KEYS,
                    result.A_ATTNUM,
                    RAW\s
                FROM
                    (SELECT\s
                        NULL AS TABLE_CAT,\s
                        n.nspname AS TABLE_SCHEM,  \s
                        ct.relname AS TABLE_NAME,\s
                        a.attname AS COLUMN_NAME,  \s
                        (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ,\s
                        ci.relname AS PK_NAME,  \s
                        information_schema._pg_expandarray(i.indkey) AS KEYS,\s
                        a.attnum AS A_ATTNUM,
                        i.indkey AS RAW\s
                    FROM pg_catalog.pg_class ct
                        JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)  \s
                        JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)  \s
                        JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid)\s
                        JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)\s
                    WHERE\s
                        true \s
                        AND ct.relname = E'po_items'\s
                        AND i.indisprimary \s
                    ) result;\s
                """;

        assertQuery(
                "TABLE_CAT\tTABLE_SCHEM\tTABLE_NAME\tCOLUMN_NAME\tKEY_SEQ\tPK_NAME\tKEYS\tA_ATTNUM\tRAW\n",
                query,
                "create table x(a int)",
                null,
                false,
                false
        );
    }

    @Test
    public void testKafkaMetadataQueryCaseInsensitivity1() throws Exception {
        String query = """
                SELECT
                    result.TABLE_CAT,       \s
                    result.TABLE_SCHEM,       \s
                    result.TABLE_NAME,       \s
                    result.COLUMN_NAME,       \s
                    result.KEY_SEQ,       \s
                    result.PK_NAME
                FROM
                    (SELECT\s
                        NULL AS TABLE_CAT,\s
                        n.nspname AS TABLE_SCHEM,  \s
                        ct.relname AS TABLE_NAME,\s
                        a.attname AS COLUMN_NAME,  \s
                        (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ,\s
                        ci.relname AS PK_NAME,  \s
                        information_schema._pg_expandarray(i.indkey) AS KEYS,\s
                        a.attnum AS A_ATTNUM,
                        i.indkey AS RAW\s
                    FROM pg_catalog.pg_class ct
                        JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)  \s
                        JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)  \s
                        JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid)\s
                        JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)\s
                    WHERE\s
                        true \s
                        AND ct.relname = E'po_items'\s
                        AND i.indisprimary \s
                    ) result\s
                WHERE A_ATTNUM = (result.KEYS).x \s
                ORDER BY result.table_name, result.PK_NAME, result.KEY_SEQ;""";

        assertQuery(
                "TABLE_CAT\tTABLE_SCHEM\tTABLE_NAME\tCOLUMN_NAME\tKEY_SEQ\tPK_NAME\n",
                query,
                "create table x(a int)",
                null,
                true
        );
    }

    @Test
    public void testKafkaMetadataQueryCaseInsensitivity2() throws Exception {
        assertMemoryLeak(() -> {
            String query = """
                    SELECT
                        result.TABLE_CAT,       \s
                        result.TABLE_SCHEM,       \s
                        result.TABLE_NAME,       \s
                        result.COLUMN_NAME,       \s
                        result.KEY_SEQ,       \s
                        result.PK_NAME,
                        result.KEYS,
                        result.A_ATTNUM,
                        RAW\s
                    FROM
                        (SELECT\s
                            NULL AS TABLE_CAT,\s
                            n.nspname AS TABLE_SCHEM,  \s
                            ct.relname AS TABLE_NAME,\s
                            a.attname AS COLUMN_NAME,  \s
                            (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ,\s
                            ci.relname AS PK_NAME,  \s
                            information_schema._pg_expandarray(i.indkey) AS KEYS,\s
                            a.attnum AS A_ATTNUM,
                            i.indkey AS RAW\s
                        FROM pg_catalog.pg_class ct
                            JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)  \s
                            JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)  \s
                            JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid)\s
                            JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)\s
                        WHERE\s
                            true \s
                            AND ct.relname = E'po_items'\s
                            AND i.indisprimary \s
                        ) result\s
                    WHERE A_ATTNUM = (result.KEYS).x \s
                    ORDER BY result.TABLE_NAME, result.pk_name, result.KEY_SEQ;""";

            assertQuery(
                    "TABLE_CAT\tTABLE_SCHEM\tTABLE_NAME\tCOLUMN_NAME\tKEY_SEQ\tPK_NAME\tKEYS\tA_ATTNUM\tRAW\n",
                    query,
                    "create table x(a int)",
                    null,
                    true,
                    false
            );
        });
    }

    @Test
    public void testKafkaQuery3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y (a int, b short, c byte, d long, e char, f string, g boolean, h long256, i float, j double, k date, l timestamp)");
            engine.releaseAllWriters();

            assertQueryNoLeakCheck(
                    """
                            nspname\trelname\tattname\tatttypid\tattnotnull\tatttypmod\tattlen\ttyptypmod\tattnum\tattidentity\tadsrc\tdescription\ttypbasetype\ttyptype
                            public\ty\ta\t23\tfalse\t-1\t4\t0\t1\t\t\t\t0\tb
                            public\ty\tb\t21\tfalse\t-1\t2\t0\t2\t\t\t\t0\tb
                            public\ty\tc\t21\tfalse\t-1\t2\t0\t3\t\t\t\t0\tb
                            public\ty\td\t20\tfalse\t-1\t8\t0\t4\t\t\t\t0\tb
                            public\ty\te\t1042\tfalse\t5\t-1\t0\t5\t\t\t\t0\tb
                            public\ty\tf\t1043\tfalse\t-1\t-1\t0\t6\t\t\t\t0\tb
                            public\ty\tg\t16\tfalse\t-1\t1\t0\t7\t\t\t\t0\tb
                            public\ty\th\t1043\tfalse\t-1\t-1\t0\t8\t\t\t\t0\tb
                            public\ty\ti\t700\tfalse\t-1\t4\t0\t9\t\t\t\t0\tb
                            public\ty\tj\t701\tfalse\t-1\t8\t0\t10\t\t\t\t0\tb
                            public\ty\tk\t1114\tfalse\t-1\t8\t0\t11\t\t\t\t0\tb
                            public\ty\tl\t1114\tfalse\t-1\t8\t0\t12\t\t\t\t0\tb
                            """,
                    """
                            SELECT * FROM (
                                SELECT\s
                                    n.nspname,
                                    c.relname,
                                    a.attname,
                                    a.atttypid,
                                    a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,
                                    a.atttypmod,
                                    a.attlen,
                                    t.typtypmod,
                                    row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum,\s
                                    nullif(a.attidentity, '') as attidentity,
                                    pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,
                                    dsc.description,
                                    t.typbasetype,
                                    t.typtype \s
                                FROM pg_catalog.pg_namespace n \s
                                    JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid) \s
                                    JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid) \s
                                    JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid) \s
                                    LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum) \s
                                    LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid) \s
                                    LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class') \s
                                    LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog') \s
                                WHERE\s
                                    c.relkind in ('r','p','v','f','m')\s
                                    and a.attnum > 0\s
                                    AND NOT a.attisdropped \s
                                    AND c.relname LIKE E'y'
                                ) c\s
                            WHERE true \s
                            ORDER BY\s
                                nspname,
                                c.relname,
                                attnum""",
                    "create table x(a int)",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testKafkaQuery31() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y (a int, b short, c byte, d long, e char, f string, g boolean, h long256, i float, j double, k date, l timestamp)");
            engine.releaseAllWriters();

            assertQueryNoLeakCheck(
                    """
                            nspname\trelname\tattname\tatttypid\tattnotnull\tatttypmod\tattlen\ttyptypmod\tattnum\tattidentity\tadsrc\tdescription\ttypbasetype\ttyptype
                            public\ty\ta\t23\tfalse\t-1\t4\t0\t1\t\t\t\t0\tb
                            public\ty\tb\t21\tfalse\t-1\t2\t0\t2\t\t\t\t0\tb
                            public\ty\tc\t21\tfalse\t-1\t2\t0\t3\t\t\t\t0\tb
                            public\ty\td\t20\tfalse\t-1\t8\t0\t4\t\t\t\t0\tb
                            public\ty\te\t1042\tfalse\t5\t-1\t0\t5\t\t\t\t0\tb
                            public\ty\tf\t1043\tfalse\t-1\t-1\t0\t6\t\t\t\t0\tb
                            public\ty\tg\t16\tfalse\t-1\t1\t0\t7\t\t\t\t0\tb
                            public\ty\th\t1043\tfalse\t-1\t-1\t0\t8\t\t\t\t0\tb
                            public\ty\ti\t700\tfalse\t-1\t4\t0\t9\t\t\t\t0\tb
                            public\ty\tj\t701\tfalse\t-1\t8\t0\t10\t\t\t\t0\tb
                            public\ty\tk\t1114\tfalse\t-1\t8\t0\t11\t\t\t\t0\tb
                            public\ty\tl\t1114\tfalse\t-1\t8\t0\t12\t\t\t\t0\tb
                            """,
                    """
                            SELECT * FROM (
                                SELECT\s
                                    n.nspname,
                                    c.relname,
                                    a.attname,
                                    a.atttypid,
                                    a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,
                                    a.atttypmod,
                                    a.attlen,
                                    t.typtypmod,
                                    row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum,\s
                                    nullif(a.attidentity, '') as attidentity,
                                    pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,
                                    dsc.description,
                                    t.typbasetype,
                                    t.typtype \s
                                FROM pg_catalog.pg_namespace n \s
                                    JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid) \s
                                    JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid) \s
                                    JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid) \s
                                    LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum) \s
                                    LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid) \s
                                    LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class') \s
                                    LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog') \s
                                WHERE\s
                                    c.relkind in ('r','p','v','f','m')\s
                                    and a.attnum > 0\s
                                    AND NOT a.attisdropped \s
                                    AND c.relname LIKE E'y'
                                order by a.attnum\
                                ) c\s
                            """,
                    "create table x(a int)",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testPgAttributeFunc() throws Exception {
        assertQuery(
                """
                        attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef
                        1\ta\t1\t23\tfalse\t-1\t4\t\tfalse\ttrue
                        """,
                "pg_catalog.pg_attribute;",
                "create table x(a int)",
                null,
                false
        );
    }

    @Test
    public void testPgAttributeFuncNoPrefix() throws Exception {
        assertQuery(
                "attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef\n",
                "pg_attribute;",
                null,
                null,
                false
        );
    }

    @Test
    public void testPgAttributeFuncNoTables() throws Exception {
        assertQuery(
                "attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef\n",
                "pg_catalog.pg_attribute;",
                null,
                null,
                false
        );
    }

    @Test
    public void testPgAttributeFuncWith2Tables() throws Exception {
        assertQuery("""
                        attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef
                        1\ta\t1\t23\tfalse\t-1\t4\t\tfalse\ttrue
                        """,
                "pg_catalog.pg_attribute order by 1;",
                "create table x(a int)",
                null,
                "create table y(a double, b string)",
                """
                        attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef
                        1\ta\t1\t23\tfalse\t-1\t4\t\tfalse\ttrue
                        2\ta\t1\t701\tfalse\t-1\t8\t\tfalse\ttrue
                        2\tb\t2\t1043\tfalse\t-1\t-1\t\tfalse\ttrue
                        """, true, false, false);
    }

    @Test
    public void testPgAttributeFuncWith2TablesLimit1() throws Exception {
        assertQuery("""
                attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef
                1\ta\t1\t23\tfalse\t-1\t4\t\tfalse\ttrue
                """, "pg_catalog.pg_attribute order by 1 limit 1;", "create table x(a int)", null, "create table y(a double, b string)", """
                attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef
                1\ta\t1\t23\tfalse\t-1\t4\t\tfalse\ttrue
                """, true, false, false);
    }

    @Test
    public void testSecondKafkaMetadataQuery() throws Exception {
        assertMemoryLeak(() -> {
            String query = """
                    SELECT * FROM (
                        SELECT\s
                            n.nspname,
                            c.relname,
                            a.attname,
                            a.atttypid,
                            a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,
                            a.atttypmod,
                            a.attlen,
                            t.typtypmod,
                         --   row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum,\s
                            nullif(a.attidentity, '') as attidentity,
                            pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,
                            dsc.description,
                            t.typbasetype,t.typtype \s
                        FROM pg_catalog.pg_namespace n \s
                            JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid) \s
                            JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid) \s
                            JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid) \s
                            LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum) \s
                            LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid) \s
                            LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class') \s
                            LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog') \s
                        WHERE\s
                            c.relkind in ('r','p','v','f','m')\s
                            and a.attnum > 0\s
                            AND NOT a.attisdropped \s
                     --       AND c.relname LIKE E'x'
                        ) c\s
                    WHERE true \s
                    ORDER BY nspname,c.relname --,attnum""";

            assertQuery(
                    """
                            nspname\trelname\tattname\tatttypid\tattnotnull\tatttypmod\tattlen\ttyptypmod\tattidentity\tadsrc\tdescription\ttypbasetype\ttyptype
                            public\tx\ta\t23\tfalse\t-1\t4\t0\t\t\t\t0\tb
                            """,
                    query,
                    "create table x(a int)",
                    null,
                    true
            );
        });
    }
}
