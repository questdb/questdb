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

package io.questdb.test.cairo.crash;

import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 * Proves the harness reproduces the ARRAY torn-aux bug: zeroing the last aux
 * entry's data-offset bytes (first 8 bytes of the 16-byte entry) via tornTail
 * and reopening must NOT silently overwrite committed rows.
 * <p>
 * ARRAY aux entries are 16 bytes (ARRAY_AUX_WIDTH_BYTES=4*Integer.BYTES=16).
 * The data-offset is the first 8 bytes of each entry; zeroing it simulates a
 * torn partial write of the offset word.
 * <p>
 * Red on unguarded HEAD (setAppendPosition silently places cursor inside committed data).
 * Green after adding the crash-consistency guard (mirrors VarcharTypeDriver).
 */
public class ArrayCrashConsistencyTest extends AbstractCrashConsistencyTest {

    @Test
    public void testTornLastAuxEntryNeverSilentlyCorrupts() throws Exception {
        runWithCrashFacade(() -> {
            final int rows = 10;
            execute("create table a (ts timestamp, arr double[]) timestamp(ts) partition by none");

            for (int i = 0; i < rows; i++) {
                execute("insert into a values (" + (i * 1_000_000L) + ", ARRAY[1.0,2.0,3.0])");
            }
            markDurableBaseline();

            // Queue torn-tail: zero bytes [0,8) of the last committed aux entry.
            // The data-offset is the first 8 bytes (readDataOffset = Unsafe.getLong(addr) & OFFSET_MAX).
            // ARRAY_AUX_WIDTH_BYTES = 16 (4 * Integer.BYTES).
            TableToken tt = engine.verifyTableName("a");
            try (Path aux = new Path()) {
                aux.of(engine.getConfiguration().getDbRoot())
                        .concat(tt)
                        .concat(TableUtils.DEFAULT_PARTITION_NAME)
                        .slash();
                TableUtils.iFile(aux, "arr", TableUtils.COLUMN_NAME_TXN_NONE);
                // Offset of last entry in aux file, then zero the 8-byte offset word
                long lastEntryOffset = (long) (rows - 1) * ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES;
                crashFf.tornTail(aux.$(), lastEntryOffset, 8L);
            }

            crashAndReopen();

            // Attempt to append after crash: setAppendPosition should detect the torn offset
            // and throw loudly instead of silently placing the write cursor inside committed data.
            boolean detected = false;
            try {
                execute("insert into a values (" + (rows * 1_000_000L) + ", ARRAY[9.0,9.0,9.0])");
            } catch (CairoException | CairoError e) {
                detected = true;
            }
            engine.releaseAllWriters();

            // Secondary check (no-silent-corruption): either row 0 reads back correctly,
            // or a loud CairoException/CairoError is thrown (both are acceptable outcomes).
            // Arrays are not readable via getStrA/getVarcharA; we use assertQuery which renders
            // them via their text representation (e.g. "[1.0,2.0,3.0]").
            try {
                assertQuery("select arr from a where ts = 0")
                        .noLeakCheck()
                        .returns("arr\n[1.0,2.0,3.0]\n");
            } catch (CairoException | CairoError e) {
                // loud detection on read path: acceptable, not silent corruption
            } catch (RuntimeException e) {
                // assertQuery may wrap a CairoException/CairoError in a RuntimeException
                if (!(e.getCause() instanceof CairoException) && !(e.getCause() instanceof CairoError)) {
                    throw e;
                }
            }

            // Primary assertion: the torn entry must be detected, never silently accepted.
            Assert.assertTrue("torn last aux entry must be detected on reopen", detected);
        });
    }
}
