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

import io.questdb.PropertyKey;
import io.questdb.cairo.EmptySymbolMapReader;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

public class WalSymbolMapValidationTest extends AbstractCairoTest {

    @Test
    public void testSparseSymbolDiffSuspendsTableWithDiagnostic() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 8);
        setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 0);
        setProperty(PropertyKey.CAIRO_WAL_SQUASH_UNCOMMITTED_ROWS_MULTIPLIER, 1);

        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (source SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");

            final Field symbolMapReadersField = WalWriter.class.getDeclaredField("symbolMapReaders");
            symbolMapReadersField.setAccessible(true);
            final SymbolMapReader staleReader = new EmptySymbolMapReader() {
                @Override
                public int getSymbolCount() {
                    return 0;
                }

                @Override
                public int keyOf(CharSequence value) {
                    return Chars.equals(value, "rithmic") ? 1 : SymbolTable.VALUE_NOT_FOUND;
                }
            };

            final TableToken tableToken = engine.verifyTableName("x");
            try (WalWriter writer = engine.getWalWriter(tableToken)) {
                @SuppressWarnings("unchecked") final ObjList<SymbolMapReader> readers = (ObjList<SymbolMapReader>) symbolMapReadersField.get(writer);
                final SymbolMapReader originalReader = readers.getQuick(0);
                readers.setQuick(0, staleReader);
                try {
                    for (int i = 0; i < 4; i++) {
                        final TableWriter.Row row = writer.newRow(3_600_000_000L + i);
                        row.putSym(0, "rithmic");
                        row.append();
                        writer.commit();
                    }
                } finally {
                    readers.setQuick(0, originalReader);
                }
            }

            drainWalQueue();

            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            final String errorMessage = engine.getTableSequencerAPI().getTxnTracker(tableToken).getErrorMessage();
            TestUtils.assertContains(errorMessage, "invalid WAL symbol diff key");
            TestUtils.assertContains(errorMessage, "columnIndex=0");
            TestUtils.assertContains(errorMessage, "cleanSymbolCount=0");
            TestUtils.assertContains(errorMessage, "expectedKey=0");
            TestUtils.assertContains(errorMessage, "actualKey=1");
        });
    }
}
