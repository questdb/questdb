/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cairo.mig;

import io.questdb.cairo.*;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static io.questdb.cairo.TableUtils.META_OFFSET_VERSION;
import static io.questdb.cairo.TableUtils.openFileRWOrFail;

public class Mig655Test {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testMigrate() throws IOException {

        final String dbRoot = temp.newFolder("dbRoot").getAbsolutePath();
        final FilesFacade ff = FilesFacadeImpl.INSTANCE;

        final int currentVersion;
        try (
                final MemoryARW virtualMem = Vm.getARWInstance(Files.PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_MIG_MMAP);
                final Path path = new Path().of(dbRoot);
                final Path other = new Path();
                final MemoryMARW rwMemory = Vm.getMARWInstance();
                final InputStream txnIs = Mig655Test.class.getResourceAsStream("/migration/txn_v426/_txn");
                final InputStream metaIs = Mig655Test.class.getResourceAsStream("/migration/txn_v426/_meta")
        ) {
            // copy resources _txn and _meta into temp local files
            Assert.assertNotNull(txnIs);
            Assert.assertNotNull(metaIs);
            final byte[] buffer = new byte[(int) Files.PAGE_SIZE];
            copyToTempFile(txnIs, new File(dbRoot + Files.SEPARATOR + TableUtils.TXN_FILE_NAME), buffer);
            copyToTempFile(metaIs, new File(dbRoot + Files.SEPARATOR + TableUtils.META_FILE_NAME), buffer);

            // open _meta file and read current version, which should be 426
            final long metaFd = openFileRWOrFail(
                    ff,
                    other.of(path).concat(TableUtils.META_FILE_NAME).$(),
                    Os.type != Os.WINDOWS ? CairoConfiguration.O_ASYNC : CairoConfiguration.O_NONE
            );
            final long metaVersionMem = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_MIG);
            currentVersion = TableUtils.readIntOrFail(ff, metaFd, META_OFFSET_VERSION, metaVersionMem, other);
            Assert.assertEquals(426, currentVersion);

            try {
                Mig655.migrate(new MigrationContext(null, metaVersionMem, Long.BYTES, virtualMem, rwMemory) {
                    @Override
                    public FilesFacade getFf() {
                        return ff;
                    }

                    @Override
                    public long getMetadataFd() {
                        return metaFd;
                    }

                    @Override
                    public Path getTablePath() {
                        return path;
                    }

                    @Override
                    public Path getTablePath2() {
                        return other;
                    }
                });

                // update _meta's version
                TableUtils.writeIntOrFail(
                        ff,
                        metaFd,
                        META_OFFSET_VERSION,
                        currentVersion + 1,
                        metaVersionMem,
                        path
                );
            } finally {
                ff.close(metaFd);
                Unsafe.free(metaVersionMem, Long.BYTES, MemoryTag.NATIVE_MIG);
            }
        }


        // original partition table:
        // timestamp, size, txn, column_version
        final long[][] expected = {
                {0L, 2, 1, 0},
                {1987200000000L, 2, 1, 0}, // 1970-01-24T00:00:00.000Z
                {3974400000000L, 2, 1, 0}, // 1970-02-16T00:00:00.000Z
                {5961600000000L, 2, 1, 0}, // 1970-03-11T00:00:00.000Z
                {7948800000000L, 2, 1, 0}, // 1970-04-03T00:00:00.000Z
                {9936000000000L, 2, 1, 0}, // 1970-04-26T00:00:00.000Z
                {11923200000000L, 2, 1, 0}, // 1970-05-19T00:00:00.000Z
                {13996800000000L, 2, 1, 0}, // 1970-06-12T00:00:00.000Z
                {15984000000000L, 2, 1, 0}, // 1970-07-05T00:00:00.000Z
                {17971200000000L, 2, 1, 0}, // 1970-07-28T00:00:00.000Z
                {19958400000000L, 2, 1, 0}, // 1970-08-20T00:00:00.000Z
                {21945600000000L, 2, 1, 0}, // 1970-09-12T00:00:00.000Z
                {23932800000000L, 2, 1, 0}, // 1970-10-05T00:00:00.000Z
                {25920000000000L, 2, 1, 0}, // 1970-10-28T00:00:00.000Z
                {27993600000000L, 2, 1, 1}  // 1970-11-21T00:00:00.000Z
        };

        // check migrated _txn content
        try (
                final TableReaderMetadata metaReader = new TableReaderMetadata(ff);
                final TxReader txReader = new TxReader(ff);
                final Path path = new Path().of(dbRoot).concat(TableUtils.META_FILE_NAME).$()
        ) {
            metaReader.deferredInit(path, currentVersion + 1);
            txReader.ofRO(path.parent().concat(TableUtils.TXN_FILE_NAME).$(), metaReader.getPartitionBy());
            txReader.unsafeLoadAll();
            Assert.assertEquals(2, txReader.getSymbolColumnCount());
            Assert.assertEquals(15, txReader.getPartitionCount());
            List<Long> vals = new ArrayList<>();
            for (int i = 0; i < txReader.getPartitionCount(); i++) {
                vals.add(txReader.getPartitionTimestamp(i));
                vals.add(txReader.getPartitionSize(i));
                vals.add(txReader.getPartitionNameTxn(i));
                vals.add(txReader.getPartitionColumnVersion(i));
                Assert.assertEquals(expected[i][0], txReader.getPartitionTimestamp(i));
                Assert.assertEquals(expected[i][1], txReader.getPartitionSize(i));
                Assert.assertEquals(expected[i][2], txReader.getPartitionNameTxn(i));
                Assert.assertEquals(expected[i][3], txReader.getPartitionColumnVersion(i));
                Assert.assertEquals(0L, txReader.getPartitionMask(i));
                Assert.assertFalse(txReader.getPartitionIsRO(i));
                Assert.assertEquals(0L, txReader.getPartitionAvailable0(i));
                Assert.assertEquals(0L, txReader.getPartitionAvailable1(i));
                Assert.assertEquals(0L, txReader.getPartitionAvailable2(i));
            }
        }
    }

    private static void copyToTempFile(InputStream is, File src, byte[] buffer) throws IOException {
        Assert.assertEquals(Files.PAGE_SIZE, buffer.length);
        try (FileOutputStream fos = new FileOutputStream(src)) {
            for (int n; (n = is.read(buffer, 0, buffer.length)) > 0; ) {
                fos.write(buffer, 0, n);
            }
        }
        Assert.assertTrue(src.exists());
        Assert.assertEquals(Files.PAGE_SIZE, src.length());
    }
}
