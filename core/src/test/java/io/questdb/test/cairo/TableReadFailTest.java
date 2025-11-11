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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

public class TableReadFailTest extends AbstractCairoTest {
    @Test
    public void testMetaFileCannotOpenConstructor() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, 1);
        spinLockTimeout = 1;
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, TableUtils.META_FILE_NAME)) {
                    return -1;
                }
                return super.openRO(name);
            }
        };
        assertConstructorFail(ff);
    }

    @Test
    public void testMetaFileMissingConstructor() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, 1);
        spinLockTimeout = 1;
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ path) {
                if (Utf8s.endsWithAscii(path, TableUtils.META_FILE_NAME)) {
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
            node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, 1);
            spinLockTimeout = 1;
            String x = "x";
            TableModel model = new TableModel(configuration, x, PartitionBy.NONE)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.LONG)
                    .timestamp();
            AbstractCairoTest.create(model);

            try (
                    Path path = new Path();
                    TableReader reader = newOffPoolReader(configuration, x);
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader);
                    MemoryCMARW mem = Vm.getCMARWInstance()
            ) {
                final Rnd rnd = new Rnd();
                final int N = 1000;

                // home path at txn file
                TableToken tableToken = engine.verifyTableName(x);
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.TXN_FILE_NAME).$();

                try (TableWriter writer = newOffPoolWriter(configuration, x)) {
                    for (int i = 0; i < N; i++) {
                        TableWriter.Row r = writer.newRow();
                        r.putInt(0, rnd.nextInt());
                        r.putLong(1, rnd.nextLong());
                        r.append();
                    }
                    writer.commit();
                }

                Assert.assertTrue(reader.reload());

                final Record record = cursor.getRecord();
                rnd.reset();
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(rnd.nextInt(), record.getInt(0));
                    Assert.assertEquals(rnd.nextLong(), record.getLong(1));
                    count++;
                }

                Assert.assertEquals(N, count);

                mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);

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
                    spinLockTimeout = 100;
                    reader.reload();
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "timeout");
                }

                // restore txn file to its former glory

                mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
                mem.jumpTo(recOffset + TableUtils.TX_OFFSET_TXN_64);
                mem.putLong(txn + 2);
                mem.jumpTo(offset);
                mem.close();
                mem.close();

                // Make sure reload functions correctly. Txn changed from 1 to 3, reload should return true
                Assert.assertTrue(reader.reload());

                try (TableWriter writer = newOffPoolWriter(configuration, x)) {
                    // add more data
                    for (int i = 0; i < N; i++) {
                        TableWriter.Row r = writer.newRow();
                        r.putInt(0, rnd.nextInt());
                        r.putLong(1, rnd.nextLong());
                        r.append();
                    }
                    writer.commit();
                }

                // does positive reload work?
                Assert.assertTrue(reader.reload());

                // can reader still see the correct data?
                cursor.toTop();
                rnd.reset();
                count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(rnd.nextInt(), record.getInt(0));
                    Assert.assertEquals(rnd.nextLong(), record.getLong(1));
                    count++;
                }

                Assert.assertEquals(2 * N, count);
            }
            engine.clear();
        });
    }

    @Test
    public void testTxnFileCannotOpenConstructor() throws Exception {
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, TableUtils.TXN_FILE_NAME)) {
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
            public long length(LPSZ name) {
                return Utf8s.endsWithAscii(name, TableUtils.TXN_FILE_NAME) ? 0 : super.length(name);
            }
        };
        assertConstructorFail(ff);
    }

    private void assertConstructorFail(FilesFacade ff) throws Exception {
        CreateTableTestUtils.createAllTable(engine, PartitionBy.DAY, ColumnType.TIMESTAMP_MICRO);
        assertMemoryLeak(() -> {
            try {
                newOffPoolReader(
                        new DefaultTestCairoConfiguration(root) {
                            @Override
                            public @NotNull FilesFacade getFilesFacade() {
                                return ff;
                            }

                            @Override
                            public long getSpinLockTimeout() {
                                return 1;
                            }
                        }, "all"
                ).close();
                Assert.fail();
            } catch (CairoException ignore) {
            }
        });
    }
}
