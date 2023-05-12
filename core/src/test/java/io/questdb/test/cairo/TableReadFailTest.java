/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.cairo;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TableReadFailTest extends AbstractCairoTest {
    @Test
    public void testMetaFileCannotOpenConstructor() throws Exception {
        spinLockTimeout = 1;
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int openRO(LPSZ name) {
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
        spinLockTimeout = 1;
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int openRO(LPSZ path) {
                if (Chars.endsWith(path, TableUtils.META_FILE_NAME)) {
                    return -1;
                }
                return super.openRO(path);
            }
        };
        assertConstructorFail(ff);
    }

    @Test
    public void testReloadTimeout() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            spinLockTimeout = 1;
            String x = "x";
            try (TableModel model = new TableModel(configuration, x, PartitionBy.NONE)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.LONG)
                    .timestamp()) {
                CreateTableTestUtils.create(model);
            }

            try (
                    Path path = new Path();
                    TableReader reader = newTableReader(configuration, x);
                    MemoryCMARW mem = Vm.getCMARWInstance()
            ) {

                final Rnd rnd = new Rnd();
                final int N = 1000;

                // home path at txn file
                TableToken tableToken = engine.verifyTableName(x);
                path.of(configuration.getRoot()).concat(tableToken).concat(TableUtils.TXN_FILE_NAME).$();

                try (TableWriter w = newTableWriter(configuration, x, metrics)) {
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

                // corrupt the txn file
                long txn = mem.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
                int recOffset = txn % 2 == 0 ? mem.getInt(TableUtils.TX_BASE_OFFSET_A_32) : mem.getInt(TableUtils.TX_BASE_OFFSET_B_32);
                mem.jumpTo(recOffset + TableUtils.TX_OFFSET_TXN_64);
                mem.putLong(txn + 123);
                mem.putLong(TableUtils.TX_BASE_OFFSET_VERSION_64, txn + 2);
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
                mem.jumpTo(recOffset + TableUtils.TX_OFFSET_TXN_64);
                mem.putLong(txn + 2);
                mem.jumpTo(offset);
                mem.close();
                mem.close();

                // make sure reload functions correctly. Txn changed from 1 to 3, reload should return true
                Assert.assertTrue(reader.reload());

                try (TableWriter w = newTableWriter(configuration, x, metrics)) {
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
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int openRO(LPSZ name) {
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
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                return !Chars.endsWith(path, TableUtils.TXN_FILE_NAME) && super.exists(path);
            }
        };
        assertConstructorFail(ff);
    }

    private void assertConstructorFail(FilesFacade ff) throws Exception {
        CreateTableTestUtils.createAllTable(engine, PartitionBy.DAY);
        TestUtils.assertMemoryLeak(() -> {
            try {
                newTableReader(new DefaultTestCairoConfiguration(root) {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return ff;
                    }

                    @Override
                    public long getSpinLockTimeout() {
                        return 1;
                    }
                }, "all");
                Assert.fail();
            } catch (CairoException ignore) {
            }
        });
    }
}
