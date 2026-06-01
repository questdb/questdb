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

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class PrefixedPgClassFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testJoinReorderNoStackOverflow() throws Exception {
        // LEFT JOIN is outer, therefore it does not reduce one of other tables to 0 rows, hence we
        // expect row duplication
        assertQuery("""
                    pg_catalog.pg_namespace n,\s
                    pg_catalog.pg_class c \s
                    LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0)\s
                """)
                .ddl("create table beta(a int)")
                .noRandomAccess()
                .returns("""
                        nspname\toid\txmin\tnspowner\toid1\trelname\trelnamespace\treltype\treloftype\trelowner\trelam\trelfilenode\treltablespace\trelpages\treltuples\trelallvisible\treltoastrelid\trelhasindex\trelisshared\trelpersistence\trelkind\trelnatts\trelchecks\trelhasrules\trelhastriggers\trelhassubclass\trelrowsecurity\trelforcerowsecurity\trelispopulated\trelreplident\trelispartition\trelrewrite\trelfrozenxid\trelminmxid\trelacl\treloptions\trelpartbound\trelhasoids\txmin1\tobjoid\tclassoid\tobjsubid\tdescription
                        pg_catalog\t11\t0\t1\t1259\tpg_class\t11\t0\t0\t0\t0\t0\t0\tfalse\t-1.0\t0\t0\tfalse\tfalse\tu\tr\t0\t0\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\td\tfalse\t0\t0\t0\t\t\t\tfalse\t0\tnull\tnull\t0\t
                        public\t2200\t0\t1\t1259\tpg_class\t11\t0\t0\t0\t0\t0\t0\tfalse\t-1.0\t0\t0\tfalse\tfalse\tu\tr\t0\t0\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\td\tfalse\t0\t0\t0\t\t\t\tfalse\t0\tnull\tnull\t0\t
                        pg_catalog\t11\t0\t1\t1\tbeta\t2200\t0\t0\t0\t0\t0\t0\tfalse\t-1.0\t0\t0\tfalse\tfalse\tp\tr\t0\t0\tfalse\tfalse\tfalse\tfalse\tfalse\ttrue\td\tfalse\t0\t0\t0\t\t\t\tfalse\t0\tnull\tnull\t0\t
                        public\t2200\t0\t1\t1\tbeta\t2200\t0\t0\t0\t0\t0\t0\tfalse\t-1.0\t0\t0\tfalse\tfalse\tp\tr\t0\t0\tfalse\tfalse\tfalse\tfalse\tfalse\ttrue\td\tfalse\t0\t0\t0\t\t\t\tfalse\t0\tnull\tnull\t0\t
                        """);
    }

    @Test
    public void testKafkaJdbcTableQuery() throws Exception {
        assertQuery("""
                SELECT\s
                     NULL AS TABLE_CAT,\s
                     n.nspname AS TABLE_SCHEM,\s
                    \s
                     c.relname AS TABLE_NAME, \s
                     CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema' \s
                        WHEN true THEN\s
                           CASE \s
                                WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN\s
                                    CASE c.relkind  \s
                                        WHEN 'r' THEN 'SYSTEM TABLE'\s
                                        WHEN 'v' THEN 'SYSTEM VIEW'
                                        WHEN 'i' THEN 'SYSTEM INDEX'
                                        ELSE NULL  \s
                                    END
                                WHEN n.nspname = 'pg_toast' THEN\s
                                    CASE c.relkind  \s
                                        WHEN 'r' THEN 'SYSTEM TOAST TABLE'
                                        WHEN 'i' THEN 'SYSTEM TOAST INDEX'
                                        ELSE NULL  \s
                                    END
                                ELSE\s
                                    CASE c.relkind
                                        WHEN 'r' THEN 'TEMPORARY TABLE'
                                        WHEN 'p' THEN 'TEMPORARY TABLE'
                                        WHEN 'i' THEN 'TEMPORARY INDEX'
                                        WHEN 'S' THEN 'TEMPORARY SEQUENCE'
                                        WHEN 'v' THEN 'TEMPORARY VIEW'
                                        ELSE NULL  \s
                                    END \s
                            END \s
                        WHEN false THEN\s
                            CASE c.relkind \s
                                WHEN 'r' THEN 'TABLE' \s
                                WHEN 'p' THEN 'PARTITIONED TABLE' \s
                                WHEN 'i' THEN 'INDEX' \s
                                WHEN 'S' THEN 'SEQUENCE' \s
                                WHEN 'v' THEN 'VIEW' \s
                                WHEN 'c' THEN 'TYPE' \s
                                WHEN 'f' THEN 'FOREIGN TABLE' \s
                                WHEN 'm' THEN 'MATERIALIZED VIEW' \s
                                ELSE NULL \s
                            END \s
                        ELSE NULL \s
                    END AS TABLE_TYPE,\s
                    d.description AS REMARKS,
                    '' as TYPE_CAT,
                    '' as TYPE_SCHEM,
                    '' as TYPE_NAME,
                    '' AS SELF_REFERENCING_COL_NAME,
                    '' AS REF_GENERATION
                FROM\s
                    pg_catalog.pg_namespace n,\s
                    pg_catalog.pg_class c \s
                    LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0)\s
                    LEFT JOIN pg_catalog.pg_class dc ON (d.classoid=dc.oid AND dc.relname='pg_class')
                    LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog')
                WHERE\s
                    c.relnamespace = n.oid \s
                    AND c.relname LIKE E'alpha'\s
                    AND (
                        false \s
                        OR  ( c.relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' )\s
                        )\s
                ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME;
                """)
                .ddl("create table alpha(col string)")
                .returns("""
                        TABLE_CAT\tTABLE_SCHEM\tTABLE_NAME\tTABLE_TYPE\tREMARKS\tTYPE_CAT\tTYPE_SCHEM\tTYPE_NAME\tSELF_REFERENCING_COL_NAME\tREF_GENERATION
                        null\tpublic\talpha\tTABLE\t\t\t\t\t\t
                        """);
    }

    @Test
    public void testLeakAfterIncompleteFetch() throws Exception {
        assertMemoryLeak(() -> {
            try (RecordCursorFactory factory = select("select * from pg_catalog.pg_class")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                    TestUtils.assertEquals(
                            """
                                    oid\trelname\trelnamespace\treltype\treloftype\trelowner\trelam\trelfilenode\treltablespace\trelpages\treltuples\trelallvisible\treltoastrelid\trelhasindex\trelisshared\trelpersistence\trelkind\trelnatts\trelchecks\trelhasrules\trelhastriggers\trelhassubclass\trelrowsecurity\trelforcerowsecurity\trelispopulated\trelreplident\trelispartition\trelrewrite\trelfrozenxid\trelminmxid\trelacl\treloptions\trelpartbound\trelhasoids\txmin
                                    1259\tpg_class\t11\t0\t0\t0\t0\t0\t0\tfalse\t-1.0\t0\t0\tfalse\tfalse\tu\tr\t0\t0\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\td\tfalse\t0\t0\t0\t\t\t\tfalse\t0
                                    """,
                            sink
                    );

                    execute("create table xyz (a int)", sqlExecutionContext);
                    engine.clear();

                    cursor.toTop();
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertFalse(cursor.hasNext());

                    try (Path path = new Path()) {
                        path.of(configuration.getDbRoot());
                        path.concat("test").$();
                        Assert.assertEquals(0, TestFilesFacadeImpl.INSTANCE.mkdirs(path, 0));
                    }

                    execute("create table abc (b double)");

                    cursor.toTop();
                    Assert.assertTrue(cursor.hasNext());

                    execute("drop table abc;");

                    cursor.toTop();
                    Assert.assertTrue(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testPSQLTableList() throws Exception {
        assertQuery("""
                SELECT n.nspname as "Schema",
                  c.relname as "Name",
                  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
                  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
                FROM pg_catalog.pg_class c
                     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind IN ('r','p','v','m','S','f','')
                      AND n.nspname <> 'pg_catalog'
                      AND n.nspname <> 'information_schema'
                      AND n.nspname !~ '^pg_toast'
                  AND pg_catalog.pg_table_is_visible(c.oid)
                ORDER BY 1,2;""")
                .ddl("create table x(a int)")
                .returns("""
                        Schema\tName\tType\tOwner
                        public\tx\ttable\tpublic
                        """);
    }

    @Test
    public void testPgClassOneTable() throws Exception {
        assertQuery("pg_catalog.pg_class")
                .ddl("create table x(a int)")
                .noRandomAccess()
                .returns("""
                        oid\trelname\trelnamespace\treltype\treloftype\trelowner\trelam\trelfilenode\treltablespace\trelpages\treltuples\trelallvisible\treltoastrelid\trelhasindex\trelisshared\trelpersistence\trelkind\trelnatts\trelchecks\trelhasrules\trelhastriggers\trelhassubclass\trelrowsecurity\trelforcerowsecurity\trelispopulated\trelreplident\trelispartition\trelrewrite\trelfrozenxid\trelminmxid\trelacl\treloptions\trelpartbound\trelhasoids\txmin
                        1259\tpg_class\t11\t0\t0\t0\t0\t0\t0\tfalse\t-1.0\t0\t0\tfalse\tfalse\tu\tr\t0\t0\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\td\tfalse\t0\t0\t0\t\t\t\tfalse\t0
                        1\tx\t2200\t0\t0\t0\t0\t0\t0\tfalse\t-1.0\t0\t0\tfalse\tfalse\tp\tr\t0\t0\tfalse\tfalse\tfalse\tfalse\tfalse\ttrue\td\tfalse\t0\t0\t0\t\t\t\tfalse\t0
                        """);
    }

    @Test
    public void testPgClassTwoTables() throws Exception {
        assertQuery("pg_catalog.pg_class order by 1")
                .ddl("create table x(a int)")
                .mutateWith("create table y(a int)")
                .returns("""
                        oid\trelname\trelnamespace\treltype\treloftype\trelowner\trelam\trelfilenode\treltablespace\trelpages\treltuples\trelallvisible\treltoastrelid\trelhasindex\trelisshared\trelpersistence\trelkind\trelnatts\trelchecks\trelhasrules\trelhastriggers\trelhassubclass\trelrowsecurity\trelforcerowsecurity\trelispopulated\trelreplident\trelispartition\trelrewrite\trelfrozenxid\trelminmxid\trelacl\treloptions\trelpartbound\trelhasoids\txmin
                        1\tx\t2200\t0\t0\t0\t0\t0\t0\tfalse\t-1.0\t0\t0\tfalse\tfalse\tp\tr\t0\t0\tfalse\tfalse\tfalse\tfalse\tfalse\ttrue\td\tfalse\t0\t0\t0\t\t\t\tfalse\t0
                        1259\tpg_class\t11\t0\t0\t0\t0\t0\t0\tfalse\t-1.0\t0\t0\tfalse\tfalse\tu\tr\t0\t0\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\td\tfalse\t0\t0\t0\t\t\t\tfalse\t0
                        """, """
                        oid\trelname\trelnamespace\treltype\treloftype\trelowner\trelam\trelfilenode\treltablespace\trelpages\treltuples\trelallvisible\treltoastrelid\trelhasindex\trelisshared\trelpersistence\trelkind\trelnatts\trelchecks\trelhasrules\trelhastriggers\trelhassubclass\trelrowsecurity\trelforcerowsecurity\trelispopulated\trelreplident\trelispartition\trelrewrite\trelfrozenxid\trelminmxid\trelacl\treloptions\trelpartbound\trelhasoids\txmin
                        1\tx\t2200\t0\t0\t0\t0\t0\t0\tfalse\t-1.0\t0\t0\tfalse\tfalse\tp\tr\t0\t0\tfalse\tfalse\tfalse\tfalse\tfalse\ttrue\td\tfalse\t0\t0\t0\t\t\t\tfalse\t0
                        2\ty\t2200\t0\t0\t0\t0\t0\t0\tfalse\t-1.0\t0\t0\tfalse\tfalse\tp\tr\t0\t0\tfalse\tfalse\tfalse\tfalse\tfalse\ttrue\td\tfalse\t0\t0\t0\t\t\t\tfalse\t0
                        1259\tpg_class\t11\t0\t0\t0\t0\t0\t0\tfalse\t-1.0\t0\t0\tfalse\tfalse\tu\tr\t0\t0\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\td\tfalse\t0\t0\t0\t\t\t\tfalse\t0
                        """);
    }

    @Test
    public void testPythonInitialSql() throws Exception {
        assertMemoryLeak(() -> assertQuery("""
                SELECT t.oid, typarray
                FROM pg_type t JOIN pg_namespace ns
                    ON typnamespace = ns.oid
                WHERE typname = 'hstore';""")
                .noRandomAccess()
                .returns("oid\ttyparray\n"));
    }

    @Test
    public void testShowMaxIdentifierLength() throws Exception {
        assertQuery("show max_identifier_length")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        max_identifier_length
                        63
                        """);
    }

    @Test
    public void testShowTransactionIsolationLevel() throws Exception {
        assertQuery("show transaction isolation level")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        transaction_isolation
                        read committed
                        """);
    }

    @Test
    public void testShowTransactionIsolationLevelErr1() throws Exception {
        assertQuery("show transaction")
                .fails(16, "expected 'isolation'");
    }

    @Test
    public void testShowTransactionIsolationLevelErr2() throws Exception {
        assertQuery("show transaction oh")
                .fails(17, "expected 'isolation'");
    }

    @Test
    public void testShowTransactionIsolationLevelErr3() throws Exception {
        assertQuery("show transaction isolation")
                .fails(26, "expected 'level'");
    }

    @Test
    public void testShowTransactionIsolationLevelErr4() throws Exception {
        assertQuery("show transaction isolation oops")
                .fails(27, "expected 'level'");
    }

    @Test
    public void testShowTransactionIsolationUnderscore() throws Exception {
        assertQuery("show transaction_isolation")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        transaction_isolation
                        read committed
                        """);
    }

    @Test
    public void testSimple() throws Exception {
        assertMemoryLeak(() -> {
            try (RecordCursorFactory factory = select("select * from pg_catalog.pg_class() order by relname")) {
                RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                try {
                    println(factory, cursor);
                    TestUtils.assertEquals(
                            """
                                    oid\trelname\trelnamespace\treltype\treloftype\trelowner\trelam\trelfilenode\treltablespace\trelpages\treltuples\trelallvisible\treltoastrelid\trelhasindex\trelisshared\trelpersistence\trelkind\trelnatts\trelchecks\trelhasrules\trelhastriggers\trelhassubclass\trelrowsecurity\trelforcerowsecurity\trelispopulated\trelreplident\trelispartition\trelrewrite\trelfrozenxid\trelminmxid\trelacl\treloptions\trelpartbound\trelhasoids\txmin
                                    1259\tpg_class\t11\t0\t0\t0\t0\t0\t0\tfalse\t-1.0\t0\t0\tfalse\tfalse\tu\tr\t0\t0\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\td\tfalse\t0\t0\t0\t\t\t\tfalse\t0
                                    """,
                            sink
                    );

                    execute("create table xyz (a int)");

                    cursor.close();
                    cursor = factory.getCursor(sqlExecutionContext);

                    println(factory, cursor);
                    TestUtils.assertEquals(
                            """
                                    oid\trelname\trelnamespace\treltype\treloftype\trelowner\trelam\trelfilenode\treltablespace\trelpages\treltuples\trelallvisible\treltoastrelid\trelhasindex\trelisshared\trelpersistence\trelkind\trelnatts\trelchecks\trelhasrules\trelhastriggers\trelhassubclass\trelrowsecurity\trelforcerowsecurity\trelispopulated\trelreplident\trelispartition\trelrewrite\trelfrozenxid\trelminmxid\trelacl\treloptions\trelpartbound\trelhasoids\txmin
                                    1259\tpg_class\t11\t0\t0\t0\t0\t0\t0\tfalse\t-1.0\t0\t0\tfalse\tfalse\tu\tr\t0\t0\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\td\tfalse\t0\t0\t0\t\t\t\tfalse\t0
                                    1\txyz\t2200\t0\t0\t0\t0\t0\t0\tfalse\t-1.0\t0\t0\tfalse\tfalse\tp\tr\t0\t0\tfalse\tfalse\tfalse\tfalse\tfalse\ttrue\td\tfalse\t0\t0\t0\t\t\t\tfalse\t0
                                    """,
                            sink
                    );

                    try (Path path = new Path()) {
                        CharSequence dirName = "test" + TableUtils.SYSTEM_TABLE_NAME_SUFFIX;
                        path.of(configuration.getDbRoot()).concat(dirName).$();
                        Assert.assertEquals(0, TestFilesFacadeImpl.INSTANCE.mkdirs(path, 0));
                    }

                    execute("create table автомобилей (b double)");

                    cursor.close();
                    cursor = factory.getCursor(sqlExecutionContext);

                    println(factory, cursor);

                    TestUtils.assertEquals(
                            """
                                    oid\trelname\trelnamespace\treltype\treloftype\trelowner\trelam\trelfilenode\treltablespace\trelpages\treltuples\trelallvisible\treltoastrelid\trelhasindex\trelisshared\trelpersistence\trelkind\trelnatts\trelchecks\trelhasrules\trelhastriggers\trelhassubclass\trelrowsecurity\trelforcerowsecurity\trelispopulated\trelreplident\trelispartition\trelrewrite\trelfrozenxid\trelminmxid\trelacl\treloptions\trelpartbound\trelhasoids\txmin
                                    1259\tpg_class\t11\t0\t0\t0\t0\t0\t0\tfalse\t-1.0\t0\t0\tfalse\tfalse\tu\tr\t0\t0\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\td\tfalse\t0\t0\t0\t\t\t\tfalse\t0
                                    1\txyz\t2200\t0\t0\t0\t0\t0\t0\tfalse\t-1.0\t0\t0\tfalse\tfalse\tp\tr\t0\t0\tfalse\tfalse\tfalse\tfalse\tfalse\ttrue\td\tfalse\t0\t0\t0\t\t\t\tfalse\t0
                                    2\tавтомобилей\t2200\t0\t0\t0\t0\t0\t0\tfalse\t-1.0\t0\t0\tfalse\tfalse\tp\tr\t0\t0\tfalse\tfalse\tfalse\tfalse\tfalse\ttrue\td\tfalse\t0\t0\t0\t\t\t\tfalse\t0
                                    """,
                            sink
                    );

                    execute("drop table автомобилей;");

                    cursor.close();
                    cursor = factory.getCursor(sqlExecutionContext);

                    println(factory, cursor);

                    TestUtils.assertEquals(
                            """
                                    oid\trelname\trelnamespace\treltype\treloftype\trelowner\trelam\trelfilenode\treltablespace\trelpages\treltuples\trelallvisible\treltoastrelid\trelhasindex\trelisshared\trelpersistence\trelkind\trelnatts\trelchecks\trelhasrules\trelhastriggers\trelhassubclass\trelrowsecurity\trelforcerowsecurity\trelispopulated\trelreplident\trelispartition\trelrewrite\trelfrozenxid\trelminmxid\trelacl\treloptions\trelpartbound\trelhasoids\txmin
                                    1259\tpg_class\t11\t0\t0\t0\t0\t0\t0\tfalse\t-1.0\t0\t0\tfalse\tfalse\tu\tr\t0\t0\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\td\tfalse\t0\t0\t0\t\t\t\tfalse\t0
                                    1\txyz\t2200\t0\t0\t0\t0\t0\t0\tfalse\t-1.0\t0\t0\tfalse\tfalse\tp\tr\t0\t0\tfalse\tfalse\tfalse\tfalse\tfalse\ttrue\td\tfalse\t0\t0\t0\t\t\t\tfalse\t0
                                    """,
                            sink
                    );
                } finally {
                    cursor.close();
                }
            }
        });
    }

    @Test
    public void testVarcharCast() throws Exception {
        assertQuery("SELECT CAST('test plain returns' AS VARCHAR(60)) AS anon_1")
                .expectSize()
                .returns("""
                        anon_1
                        test plain returns
                        """);
    }
}
