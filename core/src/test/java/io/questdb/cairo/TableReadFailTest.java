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

package io.questdb.cairo;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
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

            try (
                    Path path = new Path();
                    TableReader reader = new TableReader(configuration, "x");
                    MemoryCMARW mem = Vm.getCMARWInstance()
            ) {

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

                mem.smallFile(configuration.getFilesFacade(), path, MemoryTag.MMAP_DEFAULT);

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
                    TestUtils.assertContains(e.getFlyweightMessage(), "timeout");
                }

                // restore txn file to its former glory

                mem.smallFile(configuration.getFilesFacade(), path, MemoryTag.MMAP_DEFAULT);
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
