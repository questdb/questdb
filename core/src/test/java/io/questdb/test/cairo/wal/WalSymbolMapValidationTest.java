/*+*****************************************************************************
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

import io.questdb.cairo.EmptySymbolMapReader;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class WalSymbolMapValidationTest extends AbstractCairoTest {
    // Reports "rithmic" at key 1 while the symbol map holds no symbols, which
    // makes the WAL writer emit a sparse symbol diff.
    private static final SymbolMapReader STALE_READER = new EmptySymbolMapReader() {
        @Override
        public int keyOf(CharSequence value) {
            return value != null && Chars.equals(value, "rithmic") ? 1 : super.keyOf(value);
        }
    };

    @Test
    public void testSparseSymbolDiffAppliesHealthyPrefixBeforeSuspend() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (source SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");

            final TableToken tableToken = engine.verifyTableName("x");
            final int walId;
            try (WalWriter writer = engine.getWalWriter(tableToken)) {
                walId = writer.getWalId();
                commitRithmicRows(writer, 3_600_000_000L, 2);
                final SymbolMapReader originalReader = writer.getSymbolMapReader(0);
                writer.setSymbolMapReader(0, STALE_READER);
                try {
                    commitRithmicRows(writer, 3_600_000_002L, 2);
                } finally {
                    writer.setSymbolMapReader(0, originalReader);
                }
            }

            drainWalQueue();

            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertInvalidSymbolDiffError(tableToken, 3, walId);
            assertQuery("SELECT count() FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n2\n");
        });
    }

    @Test
    public void testSparseSymbolDiffSuspendsTableWithDiagnostic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (source SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");

            final TableToken tableToken = engine.verifyTableName("x");
            final int walId;
            try (WalWriter writer = engine.getWalWriter(tableToken)) {
                walId = writer.getWalId();
                final SymbolMapReader originalReader = writer.getSymbolMapReader(0);
                writer.setSymbolMapReader(0, STALE_READER);
                try {
                    commitRithmicRows(writer, 3_600_000_000L, 2);
                } finally {
                    writer.setSymbolMapReader(0, originalReader);
                }
            }

            drainWalQueue();

            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertInvalidSymbolDiffError(tableToken, 1, walId);
            assertQuery("SELECT count() FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n0\n");
        });
    }

    private static void assertInvalidSymbolDiffError(TableToken tableToken, long seqTxn, int walId) {
        final String errorMessage = engine.getTableSequencerAPI().getTxnTracker(tableToken).getErrorMessage();
        TestUtils.assertContains(errorMessage, "invalid WAL symbol diff key");
        TestUtils.assertContains(errorMessage, "seqTxn=" + seqTxn);
        TestUtils.assertContains(errorMessage, "walId=" + walId);
        TestUtils.assertContains(errorMessage, "segmentId=0");
    }

    private static void commitRithmicRows(WalWriter writer, long fromTs, int count) {
        for (int i = 0; i < count; i++) {
            final TableWriter.Row row = writer.newRow(fromTs + i);
            row.putSym(0, "rithmic");
            row.append();
            writer.commit();
        }
    }
}
