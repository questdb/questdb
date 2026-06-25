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

    static int countMagic(byte[] bytes, long magic) {
        int count = 0;
        for (int i = 0; i + 8 <= bytes.length; i++) {
            long v = 0;
            for (int b = 0; b < 8; b++) {
                v |= (bytes[i + b] & 0xFFL) << (8 * b);
            }
            if (v == magic) {
                count++;
            }
        }
        return count;
    }
}
