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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class WalEventChecksumTest extends AbstractCairoTest {

    @Test
    public void testEventRecordsCarryChecksumTrailer() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            execute("insert into x values ('2024-01-01T00:01:00.000000Z', 2)");
            drainWalQueue();
            TableToken tt = engine.verifyTableName("x");
            byte[] bytes = Files.readAllBytes(findEventFile(tt.getDirName()));
            int magic = countMagic(bytes, WalUtils.WALE_CHECKSUM_MAGIC);
            // 2 inserts -> 2 data records -> >= 2 trailers. Kept as >= (not ==) so the test stays
            // non-flaky if a record's pseudo-random checksum bytes ever coincide with the magic.
            Assert.assertTrue("expected a checksum trailer per record, got " + magic, magic >= 2);
        });
    }

    static Path findEventFile(CharSequence tableDirName) throws Exception {
        Path tableDir = Paths.get(engine.getConfiguration().getDbRoot().toString(), tableDirName.toString());
        try (Stream<Path> s = Files.walk(tableDir)) {
            return s.filter(p -> p.getFileName().toString().equals(WalUtils.EVENT_FILE_NAME))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("no _event file under " + tableDir));
        }
    }

    @Test
    public void testTornEventRecordSuspendsTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            TableToken tt = engine.verifyTableName("x");
            Path event = findEventFile(tt.getDirName());
            byte[] bytes = Files.readAllBytes(event);
            // Corrupt the first byte of the stored xxh3 checksum (the 8 bytes immediately after
            // WALE_CHECKSUM_MAGIC). The magic is still intact, so the record is recognised as
            // "new-format with trailer", but the stored hash no longer matches the body.
            // This is the only change: no other validator fires on a mismatched hash value.
            corruptFirstChecksumByte(bytes, WalUtils.WALE_CHECKSUM_MAGIC);
            Files.write(event, bytes);
            drainWalQueue();
            // torn record must be detected on apply and suspend the table, not silently mis-apply.
            Assert.assertTrue("expected table to be suspended after torn record", engine.getTableSequencerAPI().isSuspended(tt));
        });
    }

    @Test
    public void testTornBodyByteSuspendsTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            TableToken tt = engine.verifyTableName("x");
            Path event = findEventFile(tt.getDirName());
            byte[] bytes = Files.readAllBytes(event);
            corruptLastBodyByte(bytes, WalUtils.WALE_CHECKSUM_MAGIC);
            Files.write(event, bytes);
            drainWalQueue();
            Assert.assertTrue("expected table to be suspended after torn body", engine.getTableSequencerAPI().isSuspended(tt));
        });
    }

    @Test
    public void testLegacyRecordWithoutTrailerStillReads() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            TableToken tt = engine.verifyTableName("x");
            Path event = findEventFile(tt.getDirName());
            byte[] bytes = Files.readAllBytes(event);
            zeroFirstMagic(bytes, WalUtils.WALE_CHECKSUM_MAGIC); // simulate a pre-checksum (legacy) record
            Files.write(event, bytes);
            drainWalQueue();
            Assert.assertFalse("table should not be suspended for legacy (no-trailer) record", engine.getTableSequencerAPI().isSuspended(tt));
            assertQuery("select count() from x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n1\n");
        });
    }

    static int countMagic(byte[] bytes, long magic) {
        int count = 0;
        for (int i = 0; i + 8 <= bytes.length; i++) {
            if (readLeLong(bytes, i) == magic) {
                count++;
            }
        }
        return count;
    }

    static void zeroFirstMagic(byte[] bytes, long magic) {
        for (int i = 0; i + 8 <= bytes.length; i++) {
            if (readLeLong(bytes, i) == magic) {
                for (int b = 0; b < 8; b++) {
                    bytes[i + b] = 0;
                }
                return;
            }
        }
        throw new AssertionError("magic not found");
    }

    // Flip one bit in the stored xxh3 checksum immediately after the first occurrence of magic.
    // The magic itself is left intact so the reader still sees a "new-format" trailer; the stored
    // hash value no longer matches the body, but no other validator touches it.
    static void corruptFirstChecksumByte(byte[] bytes, long magic) {
        for (int i = 0; i + 8 <= bytes.length; i++) {
            if (readLeLong(bytes, i) == magic) {
                // The stored checksum starts at i + 8 (right after the 8-byte magic).
                if (i + 9 <= bytes.length) {
                    bytes[i + 8] ^= 0xFF;
                    return;
                }
            }
        }
        throw new AssertionError("magic not found");
    }

    // Corrupt the last byte of maxTimestamp in the DATA record body. Layout before the magic trailer:
    //   ...[maxTimestamp: 8 bytes][outOfOrder: 1 byte][END_OF_SYMBOL_DIFFS: 4 bytes][MAGIC: 8 bytes]...
    // So maxTimestamp's last byte is at magic_position - 1(END_OF_SYMBOL_DIFFS byte 3) - 3(bytes 2..0)
    // - 1(outOfOrder) - 1 = magic_position - 6.
    // The magic is left intact so the record is still recognised as new-format; the recomputed hash
    // of the (now-changed) body no longer matches the stored checksum.
    // This byte is pure payload: without verification the apply proceeds with a wrong maxTimestamp,
    // silently — no suspension unless the checksum catches it.
    static void corruptLastBodyByte(byte[] bytes, long magic) {
        for (int i = 0; i + 8 <= bytes.length; i++) {
            if (readLeLong(bytes, i) == magic) {
                // Last byte of maxTimestamp: outOfOrder(1) + END_OF_SYMBOL_DIFFS(4) + 1 = 6 bytes before magic.
                int target = i - 6;
                if (target < 0) {
                    throw new AssertionError("no payload byte before magic at expected offset");
                }
                bytes[target] ^= 0xFF;
                return;
            }
        }
        throw new AssertionError("magic not found");
    }

    static long readLeLong(byte[] bytes, int i) {
        long v = 0;
        for (int b = 0; b < 8; b++) {
            v |= (bytes[i + b] & 0xFFL) << (8 * b);
        }
        return v;
    }
}
