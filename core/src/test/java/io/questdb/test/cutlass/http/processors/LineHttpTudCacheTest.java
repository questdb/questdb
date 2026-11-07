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

package io.questdb.test.cutlass.http.processors;

import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.http.processors.LineHttpTudCache;
import io.questdb.cutlass.line.tcp.DefaultColumnTypes;
import io.questdb.cutlass.line.tcp.LineTcpParser;
import io.questdb.cutlass.line.tcp.SymbolCache;
import io.questdb.cutlass.line.tcp.WalTableUpdateDetails;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.WeakClosableObjectPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.ref.WeakReference;

public class LineHttpTudCacheTest extends AbstractCairoTest {

    /**
     * reset() and the distressed clear() free every table entry and leave none of them
     * reachable.
     */
    @Test
    public void testResetAndDistressedClearReleaseFreedEntries() throws Exception {
        // Only writer closes that succeed: when a free throws, reset() and clear() stop at
        // that entry and do not clear the table map.
        assertMemoryLeak(() -> {
            final LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);
            final DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(lineConfig);
            // The 16th lookup grows the table map, and the 17th lands in the grown map.
            final int tableCount = 17;
            for (int t = 0; t < tableCount; t++) {
                execute("CREATE TABLE l_t" + t + " (val LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            }
            for (boolean isDistressedClear : new boolean[]{false, true}) {
                final String freeAll = isDistressedClear ? "distressed clear" : "reset";
                // The parser and the symbol cache pool as LineHttpProcessorState builds them, and
                // closed in its order: the cache, the pool, the parser.
                try (
                        LineTcpParser parser = new LineTcpParser();
                        WeakClosableObjectPool<SymbolCache> symbolCachePool = new WeakClosableObjectPool<>(
                                () -> new SymbolCache(lineConfig.getMicrosecondClock(), lineConfig.getSymbolCacheWaitUsBeforeReload()),
                                5
                        );
                        LineHttpTudCache cache = new LineHttpTudCache(engine, true, false, defaultColumnTypes, PartitionBy.DAY)
                ) {
                    final ObjList<WeakReference<Object>> freed = openAndFreeAll(
                            freeAll, cache, parser, symbolCachePool, tableCount, isDistressedClear);
                    assertUnreachable(freeAll + ": freed entries still reachable", freed);

                    // The cache goes on serving tables. Using it here also keeps it reachable
                    // through the collection above.
                    Assert.assertNotNull(freeAll, lookUp(cache, parser, symbolCachePool, "l_t0"));
                }
            }
        });
    }

    // Collects garbage until no referent is reachable, then asserts that none is. A full
    // collection, which System.gc() runs under the suite's ParallelGC, clears the weak
    // reference of every unreachable object; the retries cover a request the JVM skipped.
    private static void assertUnreachable(String what, ObjList<WeakReference<Object>> refs) {
        int reachableCount = refs.size();
        for (int attempt = 0; attempt < 20 && reachableCount > 0; attempt++) {
            System.gc();
            reachableCount = 0;
            for (int i = 0, n = refs.size(); i < n; i++) {
                if (refs.getQuick(i).get() != null) {
                    reachableCount++;
                }
            }
        }
        Assert.assertEquals(what, 0, reachableCount);
    }

    // Parses one ILP line for the table and looks the table up with the parsed line, as
    // LineHttpProcessorState does for every line.
    private static WalTableUpdateDetails lookUp(
            LineHttpTudCache cache,
            LineTcpParser parser,
            WeakClosableObjectPool<SymbolCache> symbolCachePool,
            String tableName
    ) throws LineHttpTudCache.TableCreateException {
        final String line = tableName + " val=1i 1000000\n";
        final long lo = TestUtils.toMemory(line);
        try {
            Assert.assertEquals(LineTcpParser.ParseResult.MEASUREMENT_COMPLETE, parser.of(lo).parseMeasurement(lo + line.length()));
            return cache.getTableUpdateDetails(AllowAllSecurityContext.INSTANCE, parser, symbolCachePool);
        } finally {
            Unsafe.free(lo, line.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    // Opens an entry for each table, then frees them all with reset() or the distressed
    // clear(). Returns weak references to the freed entries and nothing else: no frame of
    // the caller keeps them.
    private static ObjList<WeakReference<Object>> openAndFreeAll(
            String freeAll,
            LineHttpTudCache cache,
            LineTcpParser parser,
            WeakClosableObjectPool<SymbolCache> symbolCachePool,
            int tableCount,
            boolean isDistressedClear
    ) throws LineHttpTudCache.TableCreateException {
        final ObjList<WeakReference<Object>> refs = new ObjList<>();
        for (int t = 0; t < tableCount; t++) {
            final WalTableUpdateDetails tud = lookUp(cache, parser, symbolCachePool, "l_t" + t);
            Assert.assertNotNull(freeAll, tud);
            refs.add(new WeakReference<>(tud));
        }
        if (isDistressedClear) {
            cache.setDistressed();
            cache.clear();
        } else {
            cache.reset();
        }
        return refs;
    }
}
