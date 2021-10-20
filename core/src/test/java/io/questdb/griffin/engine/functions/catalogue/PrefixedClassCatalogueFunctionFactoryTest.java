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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class PrefixedClassCatalogueFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testJoinReorderNoStackOverflow() throws Exception {
        // LEFT JOIN is outer, therefore it does not reduce one of other tables to 0 rows, hence we
        // expect row duplication
        assertQuery(
                "nspname\toid\trelname\trelnamespace\trelkind\trelowner\toid1\trelpartbound\tobjoid\tclassoid\tobjsubid\tdescription\n" +
                        "pg_catalog\t11\tpg_class\t11\tr\t0\t1259\t\tNaN\tNaN\t0\t\n" +
                        "public\t2200\tpg_class\t11\tr\t0\t1259\t\tNaN\tNaN\t0\t\n" +
                        "pg_catalog\t11\tbeta\t2200\tr\t0\t1\t\tNaN\tNaN\t0\t\n" +
                        "public\t2200\tbeta\t2200\tr\t0\t1\t\tNaN\tNaN\t0\t\n",
                "    pg_catalog.pg_namespace n, \n" +
                        "    pg_catalog.pg_class c  \n" +
                        "    LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0) \n",
                "create table beta(a int)",
                null,
                false,
                false
        );
    }

    @Test
    public void testKafkaJdbcTableQuery() throws Exception {
        assertQuery(
                "TABLE_CAT\tTABLE_SCHEM\tTABLE_NAME\tTABLE_TYPE\tREMARKS\tTYPE_CAT\tTYPE_SCHEM\tTYPE_NAME\tSELF_REFERENCING_COL_NAME\tREF_GENERATION\n" +
                        "null\tpublic\talpha\tTABLE\t\t\t\t\t\t\n",
                "SELECT \n" +
                        "     NULL AS TABLE_CAT, \n" +
                        "     n.nspname AS TABLE_SCHEM, \n" +
                        "     \n" +
                        "     c.relname AS TABLE_NAME,  \n" +
                        "     CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema'  \n" +
                        "        WHEN true THEN \n" +
                        "           CASE  \n" +
                        "                WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN \n" +
                        "                    CASE c.relkind   \n" +
                        "                        WHEN 'r' THEN 'SYSTEM TABLE' \n" +
                        "                        WHEN 'v' THEN 'SYSTEM VIEW'\n" +
                        "                        WHEN 'i' THEN 'SYSTEM INDEX'\n" +
                        "                        ELSE NULL   \n" +
                        "                    END\n" +
                        "                WHEN n.nspname = 'pg_toast' THEN \n" +
                        "                    CASE c.relkind   \n" +
                        "                        WHEN 'r' THEN 'SYSTEM TOAST TABLE'\n" +
                        "                        WHEN 'i' THEN 'SYSTEM TOAST INDEX'\n" +
                        "                        ELSE NULL   \n" +
                        "                    END\n" +
                        "                ELSE \n" +
                        "                    CASE c.relkind\n" +
                        "                        WHEN 'r' THEN 'TEMPORARY TABLE'\n" +
                        "                        WHEN 'p' THEN 'TEMPORARY TABLE'\n" +
                        "                        WHEN 'i' THEN 'TEMPORARY INDEX'\n" +
                        "                        WHEN 'S' THEN 'TEMPORARY SEQUENCE'\n" +
                        "                        WHEN 'v' THEN 'TEMPORARY VIEW'\n" +
                        "                        ELSE NULL   \n" +
                        "                    END  \n" +
                        "            END  \n" +
                        "        WHEN false THEN \n" +
                        "            CASE c.relkind  \n" +
                        "                WHEN 'r' THEN 'TABLE'  \n" +
                        "                WHEN 'p' THEN 'PARTITIONED TABLE'  \n" +
                        "                WHEN 'i' THEN 'INDEX'  \n" +
                        "                WHEN 'S' THEN 'SEQUENCE'  \n" +
                        "                WHEN 'v' THEN 'VIEW'  \n" +
                        "                WHEN 'c' THEN 'TYPE'  \n" +
                        "                WHEN 'f' THEN 'FOREIGN TABLE'  \n" +
                        "                WHEN 'm' THEN 'MATERIALIZED VIEW'  \n" +
                        "                ELSE NULL  \n" +
                        "            END  \n" +
                        "        ELSE NULL  \n" +
                        "    END AS TABLE_TYPE, \n" +
                        "    d.description AS REMARKS,\n" +
                        "    '' as TYPE_CAT,\n" +
                        "    '' as TYPE_SCHEM,\n" +
                        "    '' as TYPE_NAME,\n" +
                        "    '' AS SELF_REFERENCING_COL_NAME,\n" +
                        "    '' AS REF_GENERATION\n" +
                        "FROM \n" +
                        "    pg_catalog.pg_namespace n, \n" +
                        "    pg_catalog.pg_class c  \n" +
                        "    LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0) \n" +
                        "    LEFT JOIN pg_catalog.pg_class dc ON (d.classoid=dc.oid AND dc.relname='pg_class')\n" +
                        "    LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog')\n" +
                        "WHERE \n" +
                        "    c.relnamespace = n.oid  \n" +
                        "    AND c.relname LIKE E'alpha' \n" +
                        "    AND (\n" +
                        "        false  \n" +
                        "        OR  ( c.relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ) \n" +
                        "        ) \n" +
                        "ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME;\n",
                "create table alpha(col string)",
                null,
                true,
                false
        );
    }

    @Test
    public void testLeakAfterIncompleteFetch() throws Exception {
        assertMemoryLeak(() -> {
            sink.clear();
            try (RecordCursorFactory factory = compiler.compile("select * from pg_catalog.pg_class", sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    printer.print(cursor, factory.getMetadata(), true, sink);
                    TestUtils.assertEquals("relname\trelnamespace\trelkind\trelowner\toid\trelpartbound\n" +
                            "pg_class\t11\tr\t0\t1259\t\n", sink);

                    compiler.compile("create table xyz (a int)", sqlExecutionContext);
                    engine.clear();

                    cursor.toTop();
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertFalse(cursor.hasNext());

                    try (Path path = new Path()) {
                        path.of(configuration.getRoot());
                        path.concat("test").$();
                        Assert.assertEquals(0, FilesFacadeImpl.INSTANCE.mkdirs(path, 0));
                    }

                    compiler.compile("create table abc (b double)", sqlExecutionContext);

                    cursor.toTop();
                    Assert.assertTrue(cursor.hasNext());

                    compiler.compile("drop table abc;", sqlExecutionContext);

                    cursor.toTop();
                    Assert.assertTrue(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testPSQLTableList() throws Exception {
        assertQuery(
                "Schema\tName\tType\tOwner\n" +
                        "public\tx\ttable\tpublic\n",
                "SELECT n.nspname as \"Schema\",\n" +
                        "  c.relname as \"Name\",\n" +
                        "  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as \"Type\",\n" +
                        "  pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\"\n" +
                        "FROM pg_catalog.pg_class c\n" +
                        "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n" +
                        "WHERE c.relkind IN ('r','p','v','m','S','f','')\n" +
                        "      AND n.nspname <> 'pg_catalog'\n" +
                        "      AND n.nspname <> 'information_schema'\n" +
                        "      AND n.nspname !~ '^pg_toast'\n" +
                        "  AND pg_catalog.pg_table_is_visible(c.oid)\n" +
                        "ORDER BY 1,2;",
                "create table x(a int)",
                null,
                true
        );
    }

    @Test
    public void testPythonInitialSql() throws SqlException {
        assertQuery(
                "oid\ttyparray\n",
                "SELECT t.oid, typarray\n" +
                        "FROM pg_type t JOIN pg_namespace ns\n" +
                        "    ON typnamespace = ns.oid\n" +
                        "WHERE typname = 'hstore';",
                null, false
        );
    }

    @Test
    public void testShowTransactionIsolationLevel() throws SqlException {
        assertQuery(
                "transaction_isolation\n" +
                        "read committed\n",
                "show transaction isolation level",
                null,
                false,
                sqlExecutionContext,
                false,
                true
        );
    }

    @Test
    public void testShowTransactionIsolationLevelErr1() throws Exception {
        assertFailure("show transaction", null, 16, "expected 'isolation'");
    }

    @Test
    public void testShowTransactionIsolationLevelErr2() throws Exception {
        assertFailure("show transaction oh", null, 17, "expected 'isolation'");
    }

    @Test
    public void testShowTransactionIsolationLevelErr3() throws Exception {
        assertFailure("show transaction isolation", null, 26, "expected 'level'");
    }

    @Test
    public void testShowTransactionIsolationLevelErr4() throws Exception {
        assertFailure("show transaction isolation oops", null, 27, "expected 'level'");
    }

    @Test
    public void testSimple() throws Exception {
        assertMemoryLeak(() -> {
            sink.clear();
            try (RecordCursorFactory factory = compiler.compile("select * from pg_catalog.pg_class() order by relname", sqlExecutionContext).getRecordCursorFactory()) {
                RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                try {
                    printer.print(cursor, factory.getMetadata(), true, sink);
                    TestUtils.assertEquals("relname\trelnamespace\trelkind\trelowner\toid\trelpartbound\n" +
                            "pg_class\t11\tr\t0\t1259\t\n", sink);

                    compiler.compile("create table xyz (a int)", sqlExecutionContext);

                    cursor.close();
                    cursor = factory.getCursor(sqlExecutionContext);

                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true, sink);
                    TestUtils.assertEquals("relname\trelnamespace\trelkind\trelowner\toid\trelpartbound\n" +
                            "pg_class\t11\tr\t0\t1259\t\n" +
                            "xyz\t2200\tr\t0\t1\t\n", sink);

                    try (Path path = new Path()) {
                        path.of(configuration.getRoot());
                        path.concat("test").$();
                        Assert.assertEquals(0, FilesFacadeImpl.INSTANCE.mkdirs(path, 0));
                    }

                    compiler.compile("create table автомобилей (b double)", sqlExecutionContext);

                    cursor.close();
                    cursor = factory.getCursor(sqlExecutionContext);

                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true, sink);

                    TestUtils.assertEquals("relname\trelnamespace\trelkind\trelowner\toid\trelpartbound\n" +
                                    "pg_class\t11\tr\t0\t1259\t\n" +
                                    "xyz\t2200\tr\t0\t1\t\n" +
                                    "автомобилей\t2200\tr\t0\t2\t\n"
                            , sink);

                    compiler.compile("drop table автомобилей;", sqlExecutionContext);

                    cursor.close();
                    cursor = factory.getCursor(sqlExecutionContext);

                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true, sink);

                    TestUtils.assertEquals("relname\trelnamespace\trelkind\trelowner\toid\trelpartbound\n" +
                            "pg_class\t11\tr\t0\t1259\t\n" +
                            "xyz\t2200\tr\t0\t1\t\n", sink);

                } finally {
                    cursor.close();
                }
            }
        });
    }

    @Test
    public void testVarcharCast() throws SqlException {
        assertQuery("anon_1\n" +
                        "test plain returns\n", "SELECT CAST('test plain returns' AS VARCHAR(60)) AS anon_1",
                null,
                true,
                sqlExecutionContext,
                false,
                true
        );
    }

    @Test
    public void testPgClassOneTable() throws Exception {
        assertQuery(
                "relname\trelnamespace\trelkind\trelowner\toid\trelpartbound\n" +
                        "pg_class\t11\tr\t0\t1259\t\n" +
                        "x\t2200\tr\t0\t1\t\n",
                "pg_catalog.pg_class",
                "create table x(a int)",
                null,
                false,
                false
        );
    }

    @Test
    public void testPgClassTwoTables() throws Exception {
        assertQuery(
                "relname\trelnamespace\trelkind\trelowner\toid\trelpartbound\n" +
                        "pg_class\t11\tr\t0\t1259\t\n" +
                        "x\t2200\tr\t0\t1\t\n",
                "pg_catalog.pg_class order by 1",
                "create table x(a int)",
                null,
                "create table y(a int)",
                "relname\trelnamespace\trelkind\trelowner\toid\trelpartbound\n" +
                        "pg_class\t11\tr\t0\t1259\t\n" +
                        "x\t2200\tr\t0\t1\t\n" +
                        "y\t2200\tr\t0\t2\t\n",
                true,
                false,
                false
        );
    }
}
