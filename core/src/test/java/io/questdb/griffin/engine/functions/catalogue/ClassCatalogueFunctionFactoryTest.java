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

public class ClassCatalogueFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testLeakAfterIncompleteFetch() throws Exception {
        assertMemoryLeak(() -> {
            sink.clear();
            try (RecordCursorFactory factory = compiler.compile("select * from pg_catalog.pg_class", sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    printer.print(cursor, factory.getMetadata(), true);
                    TestUtils.assertEquals("relname\trelnamespace\trelkind\trelowner\toid\n", sink);

                    compiler.compile("create table xyz (a int)", sqlExecutionContext);
                    engine.releaseAllReaders();
                    engine.releaseAllWriters();

                    cursor.toTop();
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
    public void testSimple() throws Exception {
        assertMemoryLeak(() -> {
            sink.clear();
            try (RecordCursorFactory factory = compiler.compile("select * from pg_catalog.pg_class() order by relname", sqlExecutionContext).getRecordCursorFactory()) {
                RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                try {
                    printer.print(cursor, factory.getMetadata(), true);
                    TestUtils.assertEquals("relname\trelnamespace\trelkind\trelowner\toid\n", sink);

                    compiler.compile("create table xyz (a int)", sqlExecutionContext);

                    cursor.close();
                    cursor = factory.getCursor(sqlExecutionContext);

                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true);
                    TestUtils.assertEquals("relname\trelnamespace\trelkind\trelowner\toid\n" +
                            "xyz\t1\tr\t0\t0\n", sink);

                    try (Path path = new Path()) {
                        path.of(configuration.getRoot());
                        path.concat("test").$();
                        Assert.assertEquals(0, FilesFacadeImpl.INSTANCE.mkdirs(path, 0));
                    }

                    compiler.compile("create table автомобилей (b double)", sqlExecutionContext);

                    cursor.close();
                    cursor = factory.getCursor(sqlExecutionContext);

                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true);

                    TestUtils.assertEquals("relname\trelnamespace\trelkind\trelowner\toid\n" +
                                    "xyz\t1\tr\t0\t0\n" +
                                    "автомобилей\t1\tr\t0\t0\n"
                            , sink);

                    compiler.compile("drop table автомобилей;", sqlExecutionContext);

                    cursor.close();
                    cursor = factory.getCursor(sqlExecutionContext);

                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true);

                    TestUtils.assertEquals("relname\trelnamespace\trelkind\trelowner\toid\n" +
                            "xyz\t1\tr\t0\t0\n", sink);

                } finally {
                    cursor.close();
                }
            }
        });
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
}