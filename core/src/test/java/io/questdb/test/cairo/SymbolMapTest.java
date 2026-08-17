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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.SymbolMapReaderImpl;
import io.questdb.cairo.SymbolMapUtil;
import io.questdb.cairo.SymbolMapWriter;
import io.questdb.cairo.SymbolValueCountCollector;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.nanotime.NanosecondClockImpl;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.SymbolMapWriter.keyToOffset;
import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static io.questdb.cairo.TableUtils.offsetFileName;

public class SymbolMapTest extends AbstractCairoTest {
    private final static SymbolValueCountCollector NOOP_COLLECTOR = (_, _) -> {
    };

    public static void create(Path path, CharSequence name, int symbolCapacity, boolean useCache) {
        int plen = path.size();
        try {
            try (
                    MemoryCMARW mem = Vm.getSmallCMARWInstance(
                            configuration.getFilesFacade(),
                            path.concat(name).put(".o").$(),
                            MemoryTag.MMAP_DEFAULT,
                            configuration.getWriterFileOpenOpts()
                    )
            ) {
                mem.putInt(symbolCapacity);
                mem.putBool(useCache);
                mem.jumpTo(SymbolMapWriter.HEADER_SIZE);
            }

            configuration.getFilesFacade().touch(path.trimTo(plen).concat(name).put(".c").$());
            BitmapIndexTest.create(configuration, path.trimTo(plen), name, 4);
        } finally {
            path.trimTo(plen);
        }
    }

    @Test
    public void testAppend() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 1000;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", N, true);
                Rnd rnd = new Rnd();

                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    int prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }
                }

                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                N,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    int prev = N - 1;
                    // append second batch and check that symbol keys start with N
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }

                    // try to append first batch - this should return symbol keys starting with 0
                    rnd.reset();
                    prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        prev = key;
                    }
                    Assert.assertEquals(SymbolTable.VALUE_IS_NULL, writer.put(null));
                }
            }
        });
    }

    @Test
    public void testCacheExhaustionFallsBackToTheOnDiskIndex() throws Exception {
        // The cache addresses its keys by 32-bit word offsets, so a column that interns
        // enough distinct symbols exhausts it and the writer drops it and looks every later
        // symbol up on disk instead. Reaching that in production takes gigabytes of
        // symbols; the limit is lowered here so the transition itself can be walked over.
        final long previousLimit = SymbolMapWriter.setCacheKeyBufferLimit(256);
        try {
            TestUtils.assertMemoryLeak(() -> {
                try (Path path = new Path().of(configuration.getDbRoot())) {
                    create(path, "x", 128, true);
                    final int symbolCount;
                    final int uncachedFrom;
                    try (
                            SymbolMapWriter writer = new SymbolMapWriter(
                                    configuration,
                                    path,
                                    "x",
                                    COLUMN_NAME_TXN_NONE,
                                    0,
                                    -1,
                                    NOOP_COLLECTOR,
                                    -1
                            )
                    ) {
                        Assert.assertTrue("the column asked for a cache", writer.isCached());
                        Assert.assertTrue("...and got one", writer.isCacheAllocated());

                        // Intern until the key buffer runs out. Every symbol is distinct, so
                        // each one takes buffer the previous ones did not.
                        final ObjList<String> cached = new ObjList<>();
                        int key = -1;
                        while (writer.isCacheAllocated() && cached.size() < 64) {
                            final String symbol = "cached-symbol-" + cached.size();
                            Assert.assertEquals(++key, writer.put(symbol));
                            cached.add(symbol);
                        }
                        Assert.assertFalse(
                                "the cache must be dropped once its key buffer is exhausted,"
                                        + " rather than grown past what it can address",
                                writer.isCacheAllocated()
                        );
                        // A concrete floor rather than "more than one". The limit above admits
                        // six of these symbols and the seventh is the one that trips the drop -
                        // the list holds all seven, since the tripping symbol is interned too,
                        // just through the fallback. A change to the cache's sizing that
                        // collapsed the cached run to a single insert would otherwise leave this
                        // case asserting the fallback over almost nothing.
                        Assert.assertTrue(
                                "the cached run was " + cached.size() + " symbols, too short to"
                                        + " exercise the cache before the drop",
                                cached.size() >= 5
                        );
                        // The column's own CACHE flag is untouched: dropping the accelerator
                        // is the writer's business, not a change to what the column declared.
                        Assert.assertTrue(writer.isCached());

                        // Existing symbols: every key interned while the cache was alive must
                        // still resolve to the same key, now off the on-disk index.
                        for (int i = 0, n = cached.size(); i < n; i++) {
                            Assert.assertEquals(
                                    "symbol " + i + " changed key after the cache was dropped",
                                    i,
                                    writer.put(cached.getQuick(i))
                            );
                        }

                        // New symbols: still appended, still sequential, still uncached.
                        final int firstUncached = cached.size();
                        for (int i = 0; i < 32; i++) {
                            Assert.assertEquals(firstUncached + i, writer.put("uncached-symbol-" + i));
                            Assert.assertFalse(writer.isCacheAllocated());
                        }

                        // Duplicates of both eras resolve to their own key rather than
                        // appending a second copy.
                        Assert.assertEquals(0, writer.put(cached.getQuick(0)));
                        Assert.assertEquals(firstUncached, writer.put("uncached-symbol-0"));
                        Assert.assertEquals(firstUncached + 31, writer.put("uncached-symbol-31"));
                        Assert.assertEquals(firstUncached + 32, writer.getSymbolCount());
                        Assert.assertEquals(SymbolTable.VALUE_IS_NULL, writer.put(null));
                        symbolCount = writer.getSymbolCount();
                        uncachedFrom = firstUncached;
                    }

                    // ...and the whole map reads back holding the values the writer was given,
                    // in the keys it handed out. Reading each value back by name is what says
                    // the fallback wrote the same thing the cache would: an identity
                    // round-trip alone cannot tell a correct map from a consistently wrong one.
                    try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(
                            configuration,
                            path,
                            "x",
                            COLUMN_NAME_TXN_NONE,
                            symbolCount
                    )) {
                        Assert.assertEquals(symbolCount, reader.getSymbolCount());
                        for (int i = 0; i < symbolCount; i++) {
                            TestUtils.assertEquals(
                                    i < uncachedFrom
                                            ? "cached-symbol-" + i
                                            : "uncached-symbol-" + (i - uncachedFrom),
                                    reader.valueOf(i)
                            );
                            Assert.assertEquals(i, reader.keyOf(reader.valueOf(i)));
                        }
                    }
                }
            });
        } finally {
            SymbolMapWriter.setCacheKeyBufferLimit(previousLimit);
        }
    }

    @Test
    public void testCloseReleasesCacheWhenCharFileReleaseFails() throws Exception {
        assertCloseReleasesCacheWhenReleaseFails(".c");
    }

    @Test
    public void testCloseReleasesCacheWhenIndexValueFileReleaseFails() throws Exception {
        assertCloseReleasesCacheWhenReleaseFails(".v");
    }

    @Test
    public void testConcurrentSymbolTableAccess() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int keys = 1000;
            final int iterations = 10000;
            final int readerCount = 3;

            CountDownLatch stopLatch = new CountDownLatch(readerCount);
            CyclicBarrier startBarrier = new CyclicBarrier(readerCount);
            AtomicInteger errors = new AtomicInteger();

            IntObjHashMap<String> symbols = new IntObjHashMap<>();

            SymbolMapReaderImpl reader;
            try (final Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", keys, false);

                // Obtain the reader when there are no symbols yet.
                reader = new SymbolMapReaderImpl(
                        configuration,
                        path,
                        "x",
                        COLUMN_NAME_TXN_NONE,
                        0
                );

                // Write the symbols.
                try (final SymbolMapWriter writer = new SymbolMapWriter(
                        configuration,
                        path,
                        "x",
                        COLUMN_NAME_TXN_NONE,
                        0,
                        -1,
                        NOOP_COLLECTOR,
                        -1
                )) {
                    int prev = -1;
                    for (int i = 0; i < keys; i++) {
                        String symbol = "sym" + i;
                        int key = writer.put(symbol);
                        Assert.assertEquals(prev + 1, key);
                        prev = key;
                        symbols.put(key, symbol);
                    }
                }
            }

            // Reload the reader.
            reader.updateSymbolCount(keys);

            class ReaderThread extends Thread {
                final StaticSymbolTable symbolTable;

                ReaderThread(StaticSymbolTable symbolTable) {
                    this.symbolTable = symbolTable;
                }

                @Override
                public void run() {
                    Rnd rnd = new Rnd(NanosecondClockImpl.INSTANCE.getTicks(), MicrosecondClockImpl.INSTANCE.getTicks());
                    try {
                        startBarrier.await();
                        for (int i = 0; i < iterations; i++) {
                            int key = rnd.nextPositiveInt() % symbols.size();
                            int actualKey = symbolTable.keyOf(symbols.get(key));
                            Assert.assertEquals(key, actualKey);
                        }
                    } catch (Throwable e) {
                        errors.incrementAndGet();
                        e.printStackTrace();
                    } finally {
                        stopLatch.countDown();
                    }
                }
            }

            new ReaderThread(reader).start();
            for (int i = 0; i < readerCount - 1; i++) {
                new ReaderThread(reader.newSymbolTableView()).start();
            }

            try {
                Assert.assertTrue(stopLatch.await(20000, TimeUnit.SECONDS));
                Assert.assertEquals(0, errors.get());
            } finally {
                Misc.free(reader);
            }
        });
    }

    @Test
    public void testCorruptOffsetFile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 1024;
            try (Path path = new Path().of(configuration.getDbRoot()); Path path2 = new Path()) {
                create(path, "x", N, true);
                int pathSize = path.size();
                int symbolCount = 6;

                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    Assert.assertEquals(0, writer.put("A1"));
                    Assert.assertEquals(1, writer.put("A2"));
                    Assert.assertEquals(2, writer.put("A3"));
                    Assert.assertEquals(3, writer.put("A4"));
                    Assert.assertEquals(4, writer.put("A5"));
                    Assert.assertEquals(5, writer.put("A6"));
                }

                // Corrupt offset file, backup it first
                var oFile = offsetFileName(path.trimTo(pathSize), "x", -1);
                path2.of(path).put(".bak");
                FilesFacade ff = configuration.getFilesFacade();
                ff.copy(oFile, path2.$());

                try (MemoryCMARW mem = Vm.getSmallCMARWInstance(
                        configuration.getFilesFacade(),
                        offsetFileName(path.trimTo(pathSize), "x", -1),
                        MemoryTag.MMAP_DEFAULT,
                        configuration.getWriterFileOpenOpts()
                )) {
                    for (long l = SymbolMapWriter.HEADER_SIZE; l < mem.size(); l += 8) {
                        Unsafe.putLong(mem.addressOf(l), 0);
                    }
                    mem.jumpTo(mem.size());
                }

                try (
                        SymbolMapWriter ignore = new SymbolMapWriter(
                                configuration,
                                path.trimTo(pathSize),
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                symbolCount,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    Assert.fail("expected corrupt exception");
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "symbol column map is corrupt"));
                }

                // restore .o file
                oFile = offsetFileName(path.trimTo(pathSize), "x", -1);
                path2.of(path).put(".bak");
                ff.remove(oFile);
                ff.copy(path2.$(), oFile);

                // Check that .c file still has the values
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path.trimTo(pathSize),
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                symbolCount,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    Assert.assertEquals(5, writer.put("A6"));
                    Assert.assertEquals(4, writer.put("A5"));
                    Assert.assertEquals(3, writer.put("A4"));
                    Assert.assertEquals(2, writer.put("A3"));
                    Assert.assertEquals(1, writer.put("A2"));
                    Assert.assertEquals(0, writer.put("A1"));
                }
            }
        });
    }

    @Test
    public void testLookupPerformance() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10000000;
            int symbolCount = 1024;
            ObjList<String> symbols = new ObjList<>();
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", symbolCount, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    Rnd rnd = new Rnd();
                    int prev = -1;
                    for (int i = 0; i < symbolCount; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        int key = writer.put(cs);
                        symbols.add(cs.toString());
                        Assert.assertEquals(prev + 1, key);
                        prev = key;
                    }

                    long t = System.nanoTime();
                    for (int i = 0; i < N; i++) {
                        int key = rnd.nextPositiveInt() % symbolCount;
                        Assert.assertEquals(key, writer.put(symbols.getQuick(key)));
                    }
                    System.out.println("SymbolMapWriter lookup performance [10M <500ms]: " + (System.nanoTime() - t) / 1000000);
                }
            }
        });
    }

    @Test
    public void testMapDoesNotExist() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                try {
                    new SymbolMapWriter(
                            configuration,
                            path,
                            "x",
                            COLUMN_NAME_TXN_NONE,
                            0,
                            -1,
                            NOOP_COLLECTOR,
                            -1
                    );
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "does not exist"));
                }
            }
        });
    }

    @Test
    public void testMergeAppend() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", N, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    int prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = String.valueOf(i);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }
                }

                create(path, "y", N, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "y",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    int prev = -1;
                    for (int i = N; i < 2 * N; i++) {
                        CharSequence cs = String.valueOf(i);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }
                }

                create(path, "z", 2 * N, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "z",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, N)) {
                        boolean remapped = SymbolMapWriter.mergeSymbols(writer, reader);
                        Assert.assertFalse(remapped);
                    }

                    try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "y", COLUMN_NAME_TXN_NONE, N)) {
                        boolean remapped = SymbolMapWriter.mergeSymbols(writer, reader);
                        Assert.assertTrue(remapped);
                    }
                }

                try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "z", COLUMN_NAME_TXN_NONE, 2 * N)) {

                    for (int i = 0; i < 2 * N; i++) {
                        CharSequence cs = String.valueOf(i);
                        TestUtils.assertEquals(cs, reader.valueOf(i));
                        Assert.assertEquals(i, reader.keyOf(cs));
                    }

                    Assert.assertEquals(2 * N, reader.getSymbolCount());
                    Assert.assertNull(reader.valueOf(-1));
                    Assert.assertNull(reader.valueOf(2 * N));
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.keyOf("hola"));
                }
            }
        });
    }

    @Test
    public void testMergeIntoEmpty() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10000;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", N, false);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    int prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = String.valueOf(i);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        prev = key;
                    }

                }

                create(path, "y", N, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "y",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, N)) {
                        boolean remapped = SymbolMapWriter.mergeSymbols(writer, reader);
                        Assert.assertFalse(remapped);

                        for (int i = 0; i < N; i++) {
                            CharSequence cs = String.valueOf(i);
                            TestUtils.assertEquals(cs, reader.valueOf(i));
                            Assert.assertEquals(i, reader.keyOf(cs));
                        }

                        Assert.assertEquals(N, reader.getSymbolCount());
                        Assert.assertNull(reader.valueOf(-1));
                        Assert.assertNull(reader.valueOf(N));
                        Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.keyOf("hola"));
                    }
                }
            }
        });
    }

    @Test
    public void testMergeOverlapped() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                int plen = path.size();
                create(path, "x", N, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    int prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = String.valueOf(i);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }
                }

                create(path, "y", N, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "y",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    int prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = String.valueOf(i + N / 2);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }
                }

                int T = N + N / 2;
                create(path, "z", T, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "z",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, N)) {
                        try (
                                MemoryCMARW mem = Vm.getSmallCMARWInstance(
                                        configuration.getFilesFacade(),
                                        path.concat("x").put(TableUtils.SYMBOL_KEY_REMAP_FILE_SUFFIX).$(),
                                        MemoryTag.MMAP_DEFAULT,
                                        configuration.getWriterFileOpenOpts()
                                )
                        ) {
                            SymbolMapWriter.mergeSymbols(writer, reader, mem);
                            for (int i = 0; i < N; i++) {
                                long newId = mem.getInt(i * Integer.BYTES);
                                Assert.assertEquals(i, newId);
                            }
                        }
                    }

                    path.trimTo(plen);
                    try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "y", COLUMN_NAME_TXN_NONE, N)) {
                        try (
                                MemoryCMARW mem = Vm.getSmallCMARWInstance(
                                        configuration.getFilesFacade(),
                                        path.concat("y").put(TableUtils.SYMBOL_KEY_REMAP_FILE_SUFFIX).$(),
                                        MemoryTag.MMAP_DEFAULT,
                                        configuration.getWriterFileOpenOpts()
                                )
                        ) {
                            SymbolMapWriter.mergeSymbols(writer, reader, mem);
                            for (int i = 0; i < N; i++) {
                                long newId = mem.getInt(i * Integer.BYTES);
                                Assert.assertEquals(i + N / 2, newId);
                            }
                        }
                    }
                }

                path.trimTo(plen);
                try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "z", COLUMN_NAME_TXN_NONE, T)) {

                    for (int i = 0; i < T; i++) {
                        CharSequence cs = String.valueOf(i);
                        TestUtils.assertEquals(cs, reader.valueOf(i));
                        Assert.assertEquals(i, reader.keyOf(cs));
                    }

                    Assert.assertEquals(T, reader.getSymbolCount());
                    Assert.assertNull(reader.valueOf(-1));
                    Assert.assertNull(reader.valueOf(T));
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.keyOf("hola"));
                }
            }
        });
    }

    @Test
    public void testMergeWithEmpty() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10000;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", N, false);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    int prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = String.valueOf(i);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        prev = key;
                    }
                    create(path, "y", N, true);
                    try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "y", COLUMN_NAME_TXN_NONE, 0)) {
                        boolean remapped = SymbolMapWriter.mergeSymbols(writer, reader);
                        Assert.assertFalse(remapped);
                    }
                }

                try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, N)) {
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = String.valueOf(i);
                        TestUtils.assertEquals(cs, reader.valueOf(i));
                        Assert.assertEquals(i, reader.keyOf(cs));
                    }

                    Assert.assertEquals(N, reader.getSymbolCount());
                    Assert.assertNull(reader.valueOf(-1));
                    Assert.assertNull(reader.valueOf(N));
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.keyOf("hola"));
                }
            }
        });
    }

    @Test
    public void testReadEmptySymbolMap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10000;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", N, true);
                try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, 0)) {
                    Assert.assertEquals(N, reader.getSymbolCapacity());
                    Assert.assertNull(reader.valueOf(-1));
                    Assert.assertEquals(SymbolTable.VALUE_IS_NULL, reader.keyOf(null));
                }
            }
        });
    }

    @Test
    public void testReaderCache() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", 16, true);

                int[] keys = new int[16];
                try (SymbolMapWriter writer = new SymbolMapWriter(
                        configuration,
                        path,
                        "x",
                        COLUMN_NAME_TXN_NONE,
                        0,
                        -1,
                        NOOP_COLLECTOR,
                        -1
                )) {
                    for (int i = 0; i < keys.length; i++) {
                        keys[i] = writer.put("key" + i);
                    }
                }

                try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, keys.length)) {
                    Assert.assertTrue(reader.isCached());
                    Assert.assertEquals(0, reader.getCacheSize());
                    for (int i = 0; i < keys.length; i++) {
                        TestUtils.assertEquals("key" + i, reader.valueOf(keys[i]));
                        Assert.assertEquals(i + 1, reader.getCacheSize());
                    }
                }
            }
        });
    }

    @Test
    public void testReaderWhenMapDoesNotExist() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                try {
                    new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, 0);
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "does not exist"));
                }
            }
        });
    }

    @Test
    public void testReaderWithShortHeader() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                int plen = path.size();
                Assert.assertTrue(configuration.getFilesFacade().touch(path.concat("x").put(".o").$()));
                try {
                    new SymbolMapReaderImpl(configuration, path.trimTo(plen), "x", COLUMN_NAME_TXN_NONE, 0);
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "too short"));
                }
            }
        });
    }

    @Test
    public void testRollback() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 1024;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", N, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    Rnd rnd = new Rnd();
                    int prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }

                    writer.rollback(N / 2);

                    prev = N / 2 - 1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }
                }
            }
        });
    }

    @Test
    public void testRollbackAndRetry() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 1024;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", N, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    Assert.assertEquals(0, writer.put("A1"));
                    Assert.assertEquals(1, writer.put("A2"));
                    Assert.assertEquals(2, writer.put("A3"));
                    Assert.assertEquals(3, writer.put("A4"));
                    Assert.assertEquals(4, writer.put("A5"));

                    Assert.assertEquals(5, writer.put("A6"));
                    Assert.assertEquals(6, writer.put("A7"));
                    Assert.assertEquals(7, writer.put("A8"));
                    Assert.assertEquals(8, writer.put("A9"));
                    Assert.assertEquals(9, writer.put("A10"));

                    writer.rollback(5);

                    Assert.assertEquals(5, writer.put("A6"));
                    Assert.assertEquals(6, writer.put("A7"));
                    Assert.assertEquals(7, writer.put("A8"));
                    Assert.assertEquals(8, writer.put("A9"));
                    Assert.assertEquals(9, writer.put("A10"));

                }
            }
        });
    }

    @Test
    public void testRollbackFuzz() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        int symbols = 64 + rnd.nextInt(1024);

        int resets = 10 + rnd.nextInt(10);

        // Create longer symbols to hit various mapping page sizes
        int symbolPrefixSize = (rnd.nextInt(200) + 5) / 3;
        StringBuilder symbolPrefix = new StringBuilder("abc");
        symbolPrefix.repeat("abc", Math.max(0, symbolPrefixSize));
        String prefix = symbolPrefix.toString();

        TestUtils.assertMemoryLeak(() -> {
            int N = 128;
            ObjList<CharSequence> symbolList = new ObjList<>();
            IntList indexList = new IntList();

            try (Path path = new Path().of(configuration.getDbRoot())) {

                SymbolMapUtil smu = new SymbolMapUtil();
                create(path, "x", N, true);

                SymbolMapWriter w = new SymbolMapWriter(
                        configuration,
                        path,
                        "x",
                        COLUMN_NAME_TXN_NONE,
                        0,
                        -1,
                        NOOP_COLLECTOR,
                        -1
                );
                int hi = rnd.nextInt(symbols);
                hi = addRange(w, 0, hi, rnd, symbolList, indexList, prefix);

                for (int i = 0; i < resets; i++) {
                    int resetTo = Math.max(0, rnd.nextInt(Math.max(1, hi - 100)));
                    w.close();

                    destroySymbolFilesOffsets(path, "x", resetTo, rnd);
                    smu.rebuildSymbolFiles(configuration, path, "x", -1, resetTo, -1);

                    w = new SymbolMapWriter(
                            configuration,
                            path,
                            "x",
                            COLUMN_NAME_TXN_NONE,
                            resetTo,
                            -1,
                            NOOP_COLLECTOR,
                            -1
                    );

                    hi = resetTo + rnd.nextInt(symbols - resetTo);
                    hi = addRange(w, resetTo, Math.max(resetTo, hi), rnd, symbolList, indexList, prefix);
                }
                w.close();
            }
        });
    }

    @Test
    public void testShortHeader() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                int plen = path.size();
                Assert.assertTrue(configuration.getFilesFacade().touch(path.concat("x").put(".o").$()));
                try {
                    new SymbolMapWriter(
                            configuration,
                            path.trimTo(plen),
                            "x",
                            COLUMN_NAME_TXN_NONE,
                            0,
                            -1,
                            NOOP_COLLECTOR,
                            -1
                    );
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "too short"));
                }
            }
        });
    }

    @Test
    public void testSimpleAdd() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 1000000;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", N, false);
                try (SymbolMapWriter writer = new SymbolMapWriter(
                        configuration,
                        path,
                        "x",
                        COLUMN_NAME_TXN_NONE,
                        0,
                        -1,
                        NOOP_COLLECTOR,
                        -1
                )
                ) {
                    Rnd rnd = new Rnd();
                    int prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }
                }
            }
        });
    }

    @Test
    public void testSimpleRead() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 1000000;
            Rnd rnd = new Rnd();
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", N, false);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    int prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        prev = key;
                    }
                }
                rnd.reset();
                try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, N)) {
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        TestUtils.assertEquals(cs, reader.valueOf(i));
                        Assert.assertEquals(i, reader.keyOf(cs));
                    }

                    Assert.assertEquals(N, reader.getSymbolCount());
                    Assert.assertNull(reader.valueOf(-1));
                    Assert.assertNull(reader.valueOf(N));
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.keyOf("hola"));
                }
            }
        });
    }

    @Test
    public void testSymbolCapacityRebuildRestoresTheDroppedCache() throws Exception {
        // ALTER TABLE ... ALTER COLUMN ... SYMBOL CAPACITY re-opens the map's files and
        // re-indexes every value it holds, so it is the one non-destructive lever an
        // operator has over a writer that dropped its cache - and the only lever at all
        // on a WAL table, whose TRUNCATE keeps symbol maps. The rebuild used to skip
        // setupCache whenever the requested flag matched the one the writer already
        // carried, and the drop leaves that flag on, so the request could never differ.
        final long previousLimit = SymbolMapWriter.setCacheKeyBufferLimit(256);
        try {
            assertMemoryLeak(() -> {
                try (Path path = new Path().of(configuration.getDbRoot())) {
                    create(path, "x", 128, true);
                    try (
                            SymbolMapWriter writer = new SymbolMapWriter(
                                    configuration,
                                    path,
                                    "x",
                                    COLUMN_NAME_TXN_NONE,
                                    0,
                                    -1,
                                    NOOP_COLLECTOR,
                                    -1
                            )
                    ) {
                        final ObjList<String> exhausted = exhaustCache(writer);

                        writer.rebuildCapacity(configuration, path, "x", COLUMN_NAME_TXN_NONE, 1024, true);

                        Assert.assertTrue(
                                "a capacity rebuild that is still asked for a cache must hand one"
                                        + " back rather than leave the writer on the on-disk index",
                                writer.isCacheAllocated()
                        );
                        Assert.assertTrue(writer.isCached());
                        Assert.assertEquals(1024, writer.getSymbolCapacity());

                        // The rebuild re-indexed what the column already held, so the values
                        // survive at their original keys, and the fresh cache - which starts
                        // empty over a non-empty column - fills from the on-disk index rather
                        // than handing out keys of its own.
                        Assert.assertEquals(exhausted.size(), writer.getSymbolCount());
                        for (int i = 0, n = exhausted.size(); i < n; i++) {
                            Assert.assertEquals(i, writer.put(exhausted.getQuick(i)));
                            Assert.assertEquals(i, writer.put(exhausted.getQuick(i)));
                        }
                        Assert.assertEquals(exhausted.size(), writer.getSymbolCount());
                    }
                }
            });
        } finally {
            SymbolMapWriter.setCacheKeyBufferLimit(previousLimit);
        }
    }

    @Test
    public void testTransactionalRead() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 1000000;
            Rnd rnd = new Rnd();
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", N, false);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    int prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        prev = key;
                    }

                    rnd.reset();
                    try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", COLUMN_NAME_TXN_NONE, N)) {
                        for (int i = 0; i < N; i++) {
                            CharSequence cs = rnd.nextChars(10);
                            TestUtils.assertEquals(cs, reader.valueOf(i));
                            Assert.assertEquals(i, reader.keyOf(cs));
                        }

                        Assert.assertNull(reader.valueOf(N));
                        Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.keyOf("hola"));
                        Assert.assertEquals(N, writer.put("XYZ"));

                        // must not be able to read new symbol
                        Assert.assertNull(reader.valueOf(N));
                        Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.keyOf("XYZ"));

                        reader.updateSymbolCount(N + 1);
                        TestUtils.assertEquals("XYZ", reader.valueOf(N));
                        Assert.assertEquals(N, reader.keyOf("XYZ"));
                    }
                }
            }
        });
    }

    @Test
    public void testTruncate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 1024;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", N, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    Rnd rnd = new Rnd();
                    int prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }

                    Assert.assertEquals(N, writer.getSymbolCount());

                    writer.truncate();

                    Assert.assertEquals(0, writer.getSymbolCount());

                    // reset RND to exercise symbol cache
                    rnd.reset();
                    prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        int key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }
                    Assert.assertEquals(N, writer.getSymbolCount());
                }
            }
        });
    }

    @Test
    public void testTruncateRestoresTheDroppedCache() throws Exception {
        // A column that runs its cache's key buffer out keeps working off the on-disk
        // index, at roughly twice the cost per lookup when the column's declared
        // capacity matches its population and far more when it does not. TRUNCATE
        // empties the column, which retires the exhaustion outright, so a writer the
        // column still tells to cache has to come back cached rather than stay degraded
        // for as long as its table writer stays pooled.
        final long previousLimit = SymbolMapWriter.setCacheKeyBufferLimit(256);
        try {
            assertMemoryLeak(() -> {
                try (Path path = new Path().of(configuration.getDbRoot())) {
                    create(path, "x", 128, true);
                    try (
                            SymbolMapWriter writer = new SymbolMapWriter(
                                    configuration,
                                    path,
                                    "x",
                                    COLUMN_NAME_TXN_NONE,
                                    0,
                                    -1,
                                    NOOP_COLLECTOR,
                                    -1
                            )
                    ) {
                        final ObjList<String> exhausted = exhaustCache(writer);

                        writer.truncate();

                        Assert.assertEquals(0, writer.getSymbolCount());
                        Assert.assertTrue(
                                "TRUNCATE empties the key buffer the cache ran out of, so a column"
                                        + " that is still told to cache must get its cache back",
                                writer.isCacheAllocated()
                        );
                        Assert.assertTrue(writer.isCached());

                        // A working cache rather than an empty shell, and a bounded one:
                        // the same run of values interns from zero again, hands out the same
                        // keys, and runs the key buffer out a second time at the same point.
                        // A restored cache that could not exhaust again would mean TRUNCATE
                        // had handed out an unbounded one.
                        final ObjList<String> second = exhaustCache(writer);
                        Assert.assertEquals(exhausted.size(), second.size());
                        for (int i = 0, n = exhausted.size(); i < n; i++) {
                            Assert.assertEquals(exhausted.getQuick(i), second.getQuick(i));
                        }
                    }
                }
            });
        } finally {
            SymbolMapWriter.setCacheKeyBufferLimit(previousLimit);
        }
    }

    @Test
    public void testUpdateCacheFlagOffReleasesTheCache() throws Exception {
        // ALTER TABLE ... ALTER COLUMN ... NOCACHE lands here, on the writer already
        // serving the column - nothing re-opens it. put() dispatches on the cache rather
        // than on the flag, because a cache dropped on key-buffer exhaustion leaves the
        // flag on, so a NOCACHE that wrote only the flag would leave the writer caching
        // and leave the cache's native buffers charged to NATIVE_TABLE_WRITER until the
        // table writer leaves the pool - for a column the user just asked not to cache.
        assertMemoryLeak(() -> {
            final int values = 10_000;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", 16, true);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    for (int i = 0; i < values; i++) {
                        Assert.assertEquals(i, writer.put("key" + i));
                    }
                    Assert.assertTrue("the column asked for a cache and must have got one", writer.isCacheAllocated());

                    final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER);
                    writer.updateCacheFlag(false);
                    final long after = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER);

                    // The cache is the writer's only NATIVE_TABLE_WRITER allocation - its
                    // mapped files are charged to MMAP_INDEX_WRITER - so the whole of this
                    // difference is the cache going back to the allocator.
                    Assert.assertTrue(
                            "NOCACHE must hand the cache's native buffers back [before=" + before
                                    + ", after=" + after + ']',
                            after < before
                    );
                    Assert.assertFalse(writer.isCacheAllocated());
                    Assert.assertFalse(writer.isCached());

                    // Still a working map: every key interned while the cache was alive
                    // resolves to the same key off the on-disk index, new values still
                    // append sequentially, and neither re-establishes a cache.
                    for (int i = 0; i < values; i++) {
                        Assert.assertEquals(i, writer.put("key" + i));
                    }
                    for (int i = 0; i < 32; i++) {
                        Assert.assertEquals(values + i, writer.put("later" + i));
                    }
                    Assert.assertFalse(writer.isCacheAllocated());
                    Assert.assertEquals(values + 32, writer.getSymbolCount());

                    // A capacity rebuild re-runs the cache decision, and its gate admits a
                    // writer whose flag is on but whose cache is gone. A column turned
                    // NOCACHE carries the flag off, so the rebuild must leave it uncached
                    // rather than hand the cache back through that arm.
                    writer.rebuildCapacity(configuration, path, "x", COLUMN_NAME_TXN_NONE, 1024, false);
                    Assert.assertFalse(
                            "a capacity rebuild of a NOCACHE column must not re-establish its cache",
                            writer.isCacheAllocated()
                    );
                    Assert.assertFalse(writer.isCached());
                }

                // ...and the header carries the answer, so the next writer over the same
                // files opens uncached too.
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path.of(configuration.getDbRoot()),
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                values + 32,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    Assert.assertFalse(writer.isCached());
                    Assert.assertFalse(writer.isCacheAllocated());
                    Assert.assertEquals(0, writer.put("key0"));
                }
            }
        });
    }

    @Test
    public void testUpdateCacheFlagOnEstablishesTheCache() throws Exception {
        // The reverse direction, ALTER TABLE ... ALTER COLUMN ... CACHE. The writer
        // serving a NOCACHE column holds no cache, so raising the flag alone would leave
        // put() on the on-disk index and the column no faster than it was.
        assertMemoryLeak(() -> {
            final int values = 10_000;
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", 16, false);
                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path,
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                0,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    for (int i = 0; i < values; i++) {
                        Assert.assertEquals(i, writer.put("key" + i));
                    }
                    Assert.assertFalse("the column asked for no cache", writer.isCacheAllocated());

                    final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER);
                    writer.updateCacheFlag(true);
                    final long after = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER);

                    Assert.assertTrue(
                            "CACHE must build the cache the column now asks for [before=" + before
                                    + ", after=" + after + ']',
                            after > before
                    );
                    Assert.assertTrue(writer.isCacheAllocated());
                    Assert.assertTrue(writer.isCached());

                    // The new cache starts empty over a non-empty column, so the first
                    // lookup of each value comes off the on-disk index and the second off
                    // the cache. Both must give the key the value already had.
                    for (int i = 0; i < values; i++) {
                        Assert.assertEquals(i, writer.put("key" + i));
                        Assert.assertEquals(i, writer.put("key" + i));
                    }
                    Assert.assertEquals(values, writer.put("later"));
                    Assert.assertEquals(values, writer.put("later"));
                    Assert.assertEquals(values + 1, writer.getSymbolCount());
                }

                try (
                        SymbolMapWriter writer = new SymbolMapWriter(
                                configuration,
                                path.of(configuration.getDbRoot()),
                                "x",
                                COLUMN_NAME_TXN_NONE,
                                values + 1,
                                -1,
                                NOOP_COLLECTOR,
                                -1
                        )
                ) {
                    Assert.assertTrue(writer.isCached());
                    Assert.assertTrue(writer.isCacheAllocated());
                    Assert.assertEquals(0, writer.put("key0"));
                }
            }
        });
    }

    private int addRange(SymbolMapWriter w, int lo, int hi, Rnd rnd, ObjList<CharSequence> symbolList, IntList indexList, String prefix) {
        LOG.info().$("Resetting range [").$(lo).$(", ").$(hi).$("]").$();

        symbolList.setPos(hi);
        indexList.setPos(hi);
        for (int i = lo; i < hi; i++) {
            int id = i + rnd.nextInt(hi * 2);
            String symbol = id % 3 == 0 ? "" : prefix + id;
            int symi = w.put(symbol);
            symbolList.setQuick(i, symbol);
            indexList.setQuick(i, symi);
        }

        // Read back all and check
        int symMax = 0;
        for (int i = 0; i < hi; i++) {
            int symi = w.put(symbolList.getQuick(i));
            Assert.assertEquals(indexList.get(i), symi);
            symMax = Math.max(symMax, symi);
        }
        return symMax;
    }

    /**
     * Closes a cached symbol map writer while the release of one of its mapped files
     * fails, and asserts the off-heap value-to-key cache still went back to the
     * allocator. {@code failingFileSuffix} picks which file's release raises: ".c"
     * fails inside the char memory's own close, ".v" fails one level down, inside
     * {@link io.questdb.cairo.idx.BitmapIndexWriter#close()}.
     * <p>
     * The failure lands on the file's truncate, which
     * {@link io.questdb.cairo.vm.Vm#bestEffortClose} reaches after the mapping is gone
     * and before the descriptor's own close in the enclosing finally. So the file the
     * facade fails releases everything it owns and still raises, and every resource
     * this test can leak belongs to the writer rather than to the injection.
     */
    private void assertCloseReleasesCacheWhenReleaseFails(String failingFileSuffix) throws Exception {
        final AtomicLong failingFd = new AtomicLong(-1);
        final AtomicBoolean isArmed = new AtomicBoolean();
        final FilesFacade failingFf = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                final long fd = super.openRW(name, opts);
                if (Utf8s.endsWithAscii(name, failingFileSuffix)) {
                    failingFd.set(fd);
                }
                return fd;
            }

            @Override
            public boolean truncate(long fd, long size) {
                if (isArmed.get() && fd == failingFd.get()) {
                    throw CairoException.critical(0).put("injected truncate failure");
                }
                return super.truncate(fd, size);
            }
        };

        assertMemoryLeak(failingFf, () -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                create(path, "x", 64, true);
                final SymbolMapWriter writer = new SymbolMapWriter(
                        configuration,
                        path,
                        "x",
                        COLUMN_NAME_TXN_NONE,
                        0,
                        -1,
                        NOOP_COLLECTOR,
                        -1
                );
                for (int i = 0; i < 32; i++) {
                    Assert.assertEquals(i, writer.put("symbol" + i));
                }
                Assert.assertTrue("the column asked for a cache and must have got one", writer.isCacheAllocated());

                isArmed.set(true);
                try {
                    writer.close();
                    Assert.fail("close() must surface the release failure the facade injected");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "injected truncate failure");
                } finally {
                    isArmed.set(false);
                }

                // The cache is two native buffers. A release step that raises before the
                // writer gets to them strands them for the life of the process, since
                // nothing else references them once the writer is gone.
                Assert.assertFalse(
                        "close() must release the off-heap cache even when another release fails",
                        writer.isCacheAllocated()
                );
            }
        });
    }

    private void destroySymbolFilesOffsets(Path path, String name, int cleanCount, Rnd rnd) {
        int plen = path.size();
        try {
            FilesFacade ff = configuration.getFilesFacade();
            offsetFileName(path.trimTo(plen), name, -1);
            long size = ff.length(path.$());

            long fd = TableUtils.openRW(ff, path.$(), LOG, configuration.getWriterFileOpenOpts());
            long address = TableUtils.mapRW(ff, fd, size, MemoryTag.MMAP_DEFAULT);
            for (long i = keyToOffset(cleanCount); i + 4 < size; i += 4) {
                Unsafe.putInt(address + i, rnd.nextInt());
            }

            ff.munmap(address, size, MemoryTag.MMAP_DEFAULT);
            ff.close(fd);

        } finally {
            path.trimTo(plen);
        }
    }

    @Test
    public void testWriterCacheIsOffHeapAndReleased() throws Exception {
        // The writer's value-to-key cache holds its keys in unmanaged memory, so it
        // shows up under NATIVE_TABLE_WRITER rather than on the Java heap, and it has
        // to be released on close - the enclosing assertMemoryLeak is what proves the
        // second half. Measured as the difference between a cached and an uncached
        // writer over the same values, so the writer's other native structures, which
        // are identical between the two, cancel out.
        TestUtils.assertMemoryLeak(() -> {
            final int values = 10_000;
            Assert.assertTrue(taggedBytesForWriter("cached", true, values) > taggedBytesForWriter("plain", false, values));
        });
    }

    /**
     * Interns distinct values into {@code writer} until its cache runs the key buffer out
     * and the writer drops it, and returns every value interned along the way - the last of
     * them is the one whose insert tripped the drop, so that one went in through the on-disk
     * fallback. The caller has to have lowered
     * {@link SymbolMapWriter#setCacheKeyBufferLimit(long)} first; at the production ceiling
     * getting here takes eight gigabytes of distinct keys.
     */
    private static ObjList<String> exhaustCache(SymbolMapWriter writer) {
        Assert.assertTrue("the column asked for a cache", writer.isCached());
        Assert.assertTrue("...and got one", writer.isCacheAllocated());
        final ObjList<String> symbols = new ObjList<>();
        while (writer.isCacheAllocated() && symbols.size() < 64) {
            final String symbol = "cached-symbol-" + symbols.size();
            Assert.assertEquals(symbols.size(), writer.put(symbol));
            symbols.add(symbol);
        }
        Assert.assertFalse(
                "the cache must be dropped once its key buffer is exhausted, rather than"
                        + " grown past what it can address",
                writer.isCacheAllocated()
        );
        // The same floor the exhaustion case asserts: a sizing change that collapsed the
        // cached run to a single insert would leave every caller of this exercising the
        // fallback over almost nothing.
        Assert.assertTrue(
                "the cached run was " + symbols.size() + " symbols, too short to exercise"
                        + " the cache before the drop",
                symbols.size() >= 5
        );
        return symbols;
    }

    /**
     * Peak NATIVE_TABLE_WRITER bytes a symbol map writer holds after interning
     * {@code values} distinct values, over its own baseline. The writer is closed
     * before returning, so the caller's leak check also covers the release path.
     */
    private static long taggedBytesForWriter(CharSequence name, boolean useCache, int values) {
        try (Path path = new Path().of(configuration.getDbRoot())) {
            create(path, name, 16, useCache);
            final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER);
            try (SymbolMapWriter writer = new SymbolMapWriter(
                    configuration,
                    path,
                    name,
                    COLUMN_NAME_TXN_NONE,
                    0,
                    -1,
                    NOOP_COLLECTOR,
                    -1
            )) {
                Assert.assertEquals(useCache, writer.isCached());
                for (int i = 0; i < values; i++) {
                    writer.put("key" + i);
                }
                return Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TABLE_WRITER) - before;
            }
        }
    }

}