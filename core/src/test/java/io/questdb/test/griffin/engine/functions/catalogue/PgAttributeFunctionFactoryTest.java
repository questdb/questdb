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

package io.questdb.test.griffin.engine.functions.catalogue;

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
                                Encode sort light
                                  keys: [b1]
                                    CachedWindow
                                      orderedFunctions: [[b desc] => [row_number() over (partition by [a1])]]
                                        SelectedRecord
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: y
                            """);

            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
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
                            """);
        });
    }

    @Test
    public void testDropAndRecreateTable() throws Exception {
        // A single cached pg_attribute factory must reflect table x after it is dropped and
        // recreated with a different column - the attrelid moves from 1 to 2. The builder's
        // mutateWith(...) path reuses one factory across the DDL, mimicking a query cache.
        assertQuery("select * from pg_catalog.pg_attribute")
                .ddl("create table x (old int)")
                .mutateWith("drop table x; create table x (new long)")
                .noRandomAccess()
                .expectSize()
                .sizeMayVary()
                .returns(
                        """
                                attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef
                                1\told\t1\t23\tfalse\t-1\t4\t\tfalse\ttrue
                                """,
                        // note the ID is 2 now!
                        """
                                attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef
                                2\tnew\t1\t20\tfalse\t-1\t8\t\tfalse\ttrue
                                """);
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

        assertQuery(query)
                .ddl("create table x(a int)")
                .noRandomAccess()
                .returns("TABLE_CAT\tTABLE_SCHEM\tTABLE_NAME\tCOLUMN_NAME\tKEY_SEQ\tPK_NAME\tKEYS\tA_ATTNUM\tRAW\n");
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

        assertQuery(query)
                .ddl("create table x(a int)")
                .returns("TABLE_CAT\tTABLE_SCHEM\tTABLE_NAME\tCOLUMN_NAME\tKEY_SEQ\tPK_NAME\n");
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

            assertQuery(query)
                    .ddl("create table x(a int)")
                    .returns("TABLE_CAT\tTABLE_SCHEM\tTABLE_NAME\tCOLUMN_NAME\tKEY_SEQ\tPK_NAME\tKEYS\tA_ATTNUM\tRAW\n");
        });
    }

    @Test
    public void testKafkaQuery3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y (a int, b short, c byte, d long, e char, f string, g boolean, h long256, i float, j double, k date, l timestamp)");
            engine.releaseAllWriters();

            assertQuery("""
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
                        attnum""")
                    .noLeakCheck()
                    .ddl("create table x(a int)")
                    .expectSize()
                    .returns("""
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
                            """);
        });
    }

    @Test
    public void testKafkaQuery31() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y (a int, b short, c byte, d long, e char, f string, g boolean, h long256, i float, j double, k date, l timestamp)");
            engine.releaseAllWriters();

            assertQuery("""
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
                    """)
                    .noLeakCheck()
                    .ddl("create table x(a int)")
                    .expectSize()
                    .returns("""
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
                            """);
        });
    }

    @Test
    public void testPgAttributeFunc() throws Exception {
        assertQuery("pg_catalog.pg_attribute;")
                .ddl("create table x(a int)")
                .noRandomAccess()
                .returns("""
                        attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef
                        1\ta\t1\t23\tfalse\t-1\t4\t\tfalse\ttrue
                        """);
    }

    @Test
    public void testPgAttributeFuncNoPrefix() throws Exception {
        assertQuery("pg_attribute;")
                .ddl(null)
                .noRandomAccess()
                .returns("attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef\n");
    }

    @Test
    public void testPgAttributeFuncNoTables() throws Exception {
        assertQuery("pg_catalog.pg_attribute;")
                .ddl(null)
                .noRandomAccess()
                .returns("attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef\n");
    }

    @Test
    public void testPgAttributeFuncWith2Tables() throws Exception {
        assertQuery("pg_catalog.pg_attribute order by 1;")
                .ddl("create table x(a int)")
                .mutateWith("create table y(a double, b string)")
                .returns("""
                        attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef
                        1\ta\t1\t23\tfalse\t-1\t4\t\tfalse\ttrue
                        """, """
                        attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef
                        1\ta\t1\t23\tfalse\t-1\t4\t\tfalse\ttrue
                        2\ta\t1\t701\tfalse\t-1\t8\t\tfalse\ttrue
                        2\tb\t2\t1043\tfalse\t-1\t-1\t\tfalse\ttrue
                        """);
    }

    @Test
    public void testPgAttributeFuncWith2TablesLimit1() throws Exception {
        assertQuery("pg_catalog.pg_attribute order by 1 limit 1;")
                .ddl("create table x(a int)")
                .mutateWith("create table y(a double, b string)")
                .returns("""
                        attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef
                        1\ta\t1\t23\tfalse\t-1\t4\t\tfalse\ttrue
                        """, """
                        attrelid\tattname\tattnum\tatttypid\tattnotnull\tatttypmod\tattlen\tattidentity\tattisdropped\tatthasdef
                        1\ta\t1\t23\tfalse\t-1\t4\t\tfalse\ttrue
                        """);
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

            assertQuery(query)
                    .ddl("create table x(a int)")
                    .returns("""
                            nspname\trelname\tattname\tatttypid\tattnotnull\tatttypmod\tattlen\ttyptypmod\tattidentity\tadsrc\tdescription\ttypbasetype\ttyptype
                            public\tx\ta\t23\tfalse\t-1\t4\t0\t\t\t\t0\tb
                            """);
        });
    }
}
