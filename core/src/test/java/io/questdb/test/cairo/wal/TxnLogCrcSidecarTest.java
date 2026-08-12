/*******************************************************************************
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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.wal.seq.TxnLogCrcSidecar;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class TxnLogCrcSidecarTest extends AbstractCairoTest {

    @Test
    public void testAppendedCrcReadsBack() throws Exception {
        assertMemoryLeak(() -> {
            final long size = 28; // V1 RECORD_SIZE
            final long addr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try (Path path = new Path(); TxnLogCrcSidecar sidecar = new TxnLogCrcSidecar()) {
                Unsafe.getUnsafe().setMemory(addr, size, (byte) 9);
                sidecar.of(configuration.getFilesFacade(), sidecarPath(path, "readback"), 5L);
                sidecar.append(5L, addr, size);
                Assert.assertEquals(
                        "the stored CRC must be the checksum of the record bytes",
                        TableUtils.calculateCvAreaChecksum(addr, size),
                        sidecar.readCrc(5L)
                );
                Assert.assertNotEquals(0L, sidecar.readCrc(5L));
            } finally {
                Unsafe.free(addr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testHeaderIsWriteOnceAcrossReopen() throws Exception {
        // The watermark classifies every record already covered. Reopening with a different value must
        // NOT rewrite it, or a later open would retroactively claim coverage the file never had.
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                final Path p = sidecarPath(path, "reopen");
                try (TxnLogCrcSidecar sidecar = new TxnLogCrcSidecar()) {
                    sidecar.of(configuration.getFilesFacade(), p, 5L);
                    Assert.assertEquals(5L, sidecar.firstCoveredTxn());
                }
                try (TxnLogCrcSidecar reopened = new TxnLogCrcSidecar()) {
                    reopened.of(configuration.getFilesFacade(), sidecarPath(path, "reopen"), 99L);
                    Assert.assertEquals(
                            "the recorded watermark must win over the caller's",
                            5L,
                            reopened.firstCoveredTxn()
                    );
                }
            }
        });
    }

    @Test
    public void testTxnBelowWatermarkReadsZero() throws Exception {
        // Below the watermark there is no claim of coverage: 0 means "legacy, read unverified", which is
        // what keeps a pre-sidecar txnlog readable.
        assertMemoryLeak(() -> {
            try (Path path = new Path(); TxnLogCrcSidecar sidecar = new TxnLogCrcSidecar()) {
                sidecar.of(configuration.getFilesFacade(), sidecarPath(path, "belowwm"), 5L);
                Assert.assertEquals(0L, sidecar.readCrc(4L));
                Assert.assertEquals(5L, sidecar.firstCoveredTxn());
            }
        });
    }

    @Test
    public void testUnwrittenSlotAtOrAboveWatermarkReadsZero() throws Exception {
        // At or above the watermark a 0 is the torn/absent signal the verifier acts on -- it must not be
        // confused with a real checksum, which calculateCvAreaChecksum never returns as 0.
        assertMemoryLeak(() -> {
            try (Path path = new Path(); TxnLogCrcSidecar sidecar = new TxnLogCrcSidecar()) {
                sidecar.of(configuration.getFilesFacade(), sidecarPath(path, "unwritten"), 1L);
                Assert.assertEquals(0L, sidecar.readCrc(7L));
            }
        });
    }

    private Path sidecarPath(Path path, String name) {
        return path.of(configuration.getDbRoot()).concat("txnlog_crc_" + name + ".c");
    }
}
