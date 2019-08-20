/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.functions.catalogue;

import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.griffin.AbstractGriffinTest;
import com.questdb.std.FilesFacadeImpl;
import com.questdb.std.str.Path;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ClassCatalogueFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testLeakAfterIncompleteFetch() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            sink.clear();
            try (RecordCursorFactory factory = compiler.compile("select * from pg_catalog.pg_class")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    printer.print(cursor, factory.getMetadata(), true);
                    TestUtils.assertEquals("relname\trelnamespace\trelkind\trelowner\toid\n", sink);

                    compiler.compile("create table xyz (a int)");
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

                    compiler.compile("create table abc (b double)");

                    cursor.toTop();
                    Assert.assertTrue(cursor.hasNext());

                    compiler.compile("drop table abc;");

                    cursor.toTop();
                    Assert.assertTrue(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testSimple() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            sink.clear();
            try (RecordCursorFactory factory = compiler.compile("select * from pg_catalog.pg_class() order by relname")) {
                RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                try {
                    printer.print(cursor, factory.getMetadata(), true);
                    TestUtils.assertEquals("relname\trelnamespace\trelkind\trelowner\toid\n", sink);

                    compiler.compile("create table xyz (a int)");

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

                    compiler.compile("create table автомобилей (b double)");

                    cursor.close();
                    cursor = factory.getCursor(sqlExecutionContext);

                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true);

                    TestUtils.assertEquals("relname\trelnamespace\trelkind\trelowner\toid\n" +
                                    "xyz\t1\tr\t0\t0\n" +
                                    "автомобилей\t1\tr\t0\t0\n"
                            , sink);

                    compiler.compile("drop table автомобилей;");

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
}