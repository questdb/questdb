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

package io.questdb.test.cairo;

import io.questdb.cairo.CompositeDictionaries;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Task 5 (Plan 2): the composite dedicated dictionaries and the {@code _cell} registry are
 * first-class {@code _txn} symbol maps. Two halves of one invariant:
 * <ul>
 *     <li><b>Part A</b> (create path): the initial {@code _txn} counts the dedicated dicts + the
 *     registry, so its symbol-count region reserves a zero-count slot for each.</li>
 *     <li><b>Part B</b> (writer open): the writer registers those interners into
 *     {@code denseSymbolMapWriters}, in layout order (dedicated dicts by dimension, then the
 *     registry), and exposes them through {@link CompositeDictionaries}.</li>
 * </ul>
 * A plain (non-composite) table has no interners: its {@code _txn} is unchanged and
 * {@code getCompositeDictionaries()} is null.
 */
public class CompositeDictionariesTest extends AbstractCairoTest {

    @Test
    public void testInitialTxnCountsInterners() throws Exception {          // Part A
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
                    "timestamp(ts) partition by day, exchange, truncate(symbol, 3) wal");
            // 2 real SYMBOL cols (exchange, symbol) + 1 dedicated dict (truncate) + 1 registry = 4
            try (TableReader r = getReader("t")) {
                Assert.assertEquals(4, r.getTxFile().getSymbolColumnCount());
            }
        });
    }

    @Test
    public void testPlainTableRegistersNoInterners() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p (ts timestamp, s symbol) timestamp(ts) partition by day wal");
            try (TableWriter w = getWriter("p")) {
                Assert.assertNull(w.getCompositeDictionaries());        // no interners for a plain table
                Assert.assertEquals(1, w.getDenseSymbolMapCount());     // exactly the 1 SYMBOL column
            }
            try (TableReader r = getReader("p")) {
                Assert.assertEquals(1, r.getTxFile().getSymbolColumnCount()); // _txn unchanged for plain
            }
        });
    }

    @Test
    public void testWriterRegistersDedicatedInternersInOrder() throws Exception {   // Part B
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
                    "timestamp(ts) partition by day, exchange, truncate(symbol, 3) wal");
            try (TableWriter w = getWriter("t")) {
                // dim0 = identity(exchange) [reuses column dict, no dedicated]; dim1 = truncate(symbol,3) [dedicated]
                CompositeDictionaries d = w.getCompositeDictionaries();
                Assert.assertNotNull(d);
                Assert.assertNull(d.dedicatedDictFor(0));               // identity -> no dedicated dict
                Assert.assertNotNull(d.dedicatedDictFor(1));            // truncate -> dedicated dict
                Assert.assertNotNull(d.cellRegistry());
                Assert.assertEquals(2 + 2, w.getDenseSymbolMapCount()); // 2 real symbols + dict + registry
            }
        });
    }
}
