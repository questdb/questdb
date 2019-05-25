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

package com.questdb.cairo;

import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.std.Chars;
import com.questdb.std.FilesFacade;
import com.questdb.std.FilesFacadeImpl;
import com.questdb.std.Rnd;
import com.questdb.std.str.LPSZ;
import com.questdb.std.str.Path;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TableReadFailTest extends AbstractCairoTest {
    @Test
    public void testMetaFileCannotOpenConstructor() throws Exception {
        FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Chars.endsWith(name, TableUtils.META_FILE_NAME)) {
                    return -1;
                }
                return super.openRO(name);
            }
        };
        assertConstructorFail(ff);
    }

    @Test
    public void testMetaFileMissingConstructor() throws Exception {
        FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                return !Chars.endsWith(path, TableUtils.META_FILE_NAME) && super.exists(path);
            }
        };
        assertConstructorFail(ff);
    }

    @Test
    public void testReloadTimeout() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.LONG)
                    .timestamp()) {
                CairoTestUtils.create(model);
            }

            try (Path path = new Path();
                 TableReader reader = new TableReader(configuration, "x");
                 ReadWriteMemory mem = new ReadWriteMemory()) {

                final Rnd rnd = new Rnd();
                final int N = 1000;

                // home path at txn file
                path.of(configuration.getRoot()).concat("x").concat(TableUtils.TXN_FILE_NAME).$();

                try (TableWriter w = new TableWriter(configuration, "x")) {


                    for (int i = 0; i < N; i++) {
                        TableWriter.Row r = w.newRow();
                        r.putInt(0, rnd.nextInt());
                        r.putLong(1, rnd.nextLong());
                        r.append();
                    }
                    w.commit();
                }


                Assert.assertTrue(reader.reload());


                RecordCursor cursor = reader.getCursor();
                final Record record = cursor.getRecord();
                rnd.reset();
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(rnd.nextInt(), record.getInt(0));
                    Assert.assertEquals(rnd.nextLong(), record.getLong(1));
                    count++;
                }

                Assert.assertEquals(N, count);

                mem.of(configuration.getFilesFacade(), path, configuration.getFilesFacade().getPageSize());

                // keep txn file parameters
                long offset = configuration.getFilesFacade().length(mem.getFd());
                long txn = mem.getLong(TableUtils.TX_OFFSET_TXN);

                // corrupt the txn file
                mem.jumpTo(TableUtils.TX_OFFSET_TXN);
                mem.putLong(123);
                mem.jumpTo(offset);
                mem.close();

                // this should time out
                try {
                    reader.reload();
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getMessage(), "timeout");
                }

                // restore txn file to its former glory

                mem.of(configuration.getFilesFacade(), path, configuration.getFilesFacade().getPageSize());
                mem.jumpTo(TableUtils.TX_OFFSET_TXN);
                mem.putLong(txn);
                mem.jumpTo(offset);
                mem.close();
                mem.close();

                // make sure reload functions correctly
                Assert.assertFalse(reader.reload());

                try (TableWriter w = new TableWriter(configuration, "x")) {
                    // add more data
                    for (int i = 0; i < N; i++) {
                        TableWriter.Row r = w.newRow();
                        r.putInt(0, rnd.nextInt());
                        r.putLong(1, rnd.nextLong());
                        r.append();
                    }
                    w.commit();
                }

                // does positive reload work?
                Assert.assertTrue(reader.reload());

                // can reader still see correct data?
                cursor = reader.getCursor();
                rnd.reset();
                count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(rnd.nextInt(), record.getInt(0));
                    Assert.assertEquals(rnd.nextLong(), record.getLong(1));
                    count++;
                }

                Assert.assertEquals(2 * N, count);
            }
        });
    }

    @Test
    public void testTodoPresentConstructor() throws Exception {
        FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                return Chars.endsWith(path, TableUtils.TODO_FILE_NAME) || super.exists(path);
            }
        };

        assertConstructorFail(ff);
    }

    @Test
    public void testTxnFileCannotOpenConstructor() throws Exception {
        FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Chars.endsWith(name, TableUtils.TXN_FILE_NAME)) {
                    return -1;
                }
                return super.openRO(name);
            }
        };
        assertConstructorFail(ff);
    }

    @Test
    public void testTxnFileMissingConstructor() throws Exception {
        FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                return !Chars.endsWith(path, TableUtils.TXN_FILE_NAME) && super.exists(path);
            }
        };
        assertConstructorFail(ff);
    }

    private void assertConstructorFail(FilesFacade ff) throws Exception {
        CairoTestUtils.createAllTable(configuration, PartitionBy.DAY);
        TestUtils.assertMemoryLeak(() -> {
            try {
                new TableReader(new DefaultCairoConfiguration(root) {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return ff;
                    }
                }, "all");
                Assert.fail();
            } catch (CairoException ignore) {
            }
        });
    }
}
