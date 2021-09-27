/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cairo;

import io.questdb.cairo.SymbolMapWriter.TransientSymbolCountChangeHandler;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SymbolMapTest extends AbstractCairoTest {
    private static final TransientSymbolCountChangeHandler TRANSIENT_SYMBOL_COUNT_CHANGE_HANDLER = (symCount) -> {
    };

    public static void create(Path path, CharSequence name, int symbolCapacity, boolean useCache) {
        int plen = path.length();
        try {
            try (
                    MemoryCMARW mem = Vm.getSmallCMARWInstance(
                            configuration.getFilesFacade(),
                            path.concat(name).put(".o").$(),
                            MemoryTag.MMAP_DEFAULT)
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
            try (Path path = new Path().of(configuration.getRoot())) {
                create(path, "x", N, true);
                Rnd rnd = new Rnd();

                try (SymbolMapWriter writer = new SymbolMapWriter(configuration, path, "x", 0, TRANSIENT_SYMBOL_COUNT_CHANGE_HANDLER)) {
                    long prev = -1L;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        long key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }
                }

                try (SymbolMapWriter writer = new SymbolMapWriter(configuration, path, "x", N, TRANSIENT_SYMBOL_COUNT_CHANGE_HANDLER)) {
                    long prev = N - 1;
                    // append second batch and check that symbol keys start with N
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        long key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }

                    // try append first batch - this should return symbol keys starting with 0
                    rnd.reset();
                    prev = -1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        long key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        prev = key;
                    }
                    Assert.assertEquals(SymbolTable.VALUE_IS_NULL, writer.put(null));
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
            try (Path path = new Path().of(configuration.getRoot())) {
                create(path, "x", symbolCount, true);
                try (SymbolMapWriter writer = new SymbolMapWriter(configuration, path, "x", 0, TRANSIENT_SYMBOL_COUNT_CHANGE_HANDLER)) {
                    Rnd rnd = new Rnd();
                    long prev = -1L;
                    for (int i = 0; i < symbolCount; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        long key = writer.put(cs);
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
            try (Path path = new Path().of(configuration.getRoot())) {
                try {
                    new SymbolMapWriter(configuration, path, "x", 0, TRANSIENT_SYMBOL_COUNT_CHANGE_HANDLER);
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "does not exist"));
                }
            }
        });
    }

    @Test
    public void testReadEmptySymbolMap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10000;
            try (Path path = new Path().of(configuration.getRoot())) {
                create(path, "x", N, true);
                try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", 0)) {
                    Assert.assertEquals(N, reader.getSymbolCapacity());
                    Assert.assertNull(reader.valueOf(-1));
                    Assert.assertEquals(SymbolTable.VALUE_IS_NULL, reader.keyOf(null));
                }
            }
        });
    }

    @Test
    public void testReaderWhenMapDoesNotExist() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getRoot())) {
                try {
                    new SymbolMapReaderImpl(configuration, path, "x", 0);
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
            try (Path path = new Path().of(configuration.getRoot())) {
                int plen = path.length();
                Assert.assertTrue(configuration.getFilesFacade().touch(path.concat("x").put(".o").$()));
                try {
                    new SymbolMapReaderImpl(configuration, path.trimTo(plen), "x", 0);
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
            try (Path path = new Path().of(configuration.getRoot())) {
                create(path, "x", N, true);
                try (SymbolMapWriter writer = new SymbolMapWriter(configuration, path, "x", 0, TRANSIENT_SYMBOL_COUNT_CHANGE_HANDLER)) {
                    Rnd rnd = new Rnd();
                    long prev = -1L;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        long key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }

                    writer.rollback(N / 2);

                    prev = N / 2 - 1;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        long key = writer.put(cs);
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
            try (Path path = new Path().of(configuration.getRoot())) {
                create(path, "x", N, true);
                try (SymbolMapWriter writer = new SymbolMapWriter(configuration, path, "x", 0, TRANSIENT_SYMBOL_COUNT_CHANGE_HANDLER)) {
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
    public void testShortHeader() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getRoot())) {
                int plen = path.length();
                Assert.assertTrue(configuration.getFilesFacade().touch(path.concat("x").put(".o").$()));
                try {
                    new SymbolMapWriter(configuration, path.trimTo(plen), "x", 0, TRANSIENT_SYMBOL_COUNT_CHANGE_HANDLER);
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
            try (Path path = new Path().of(configuration.getRoot())) {
                create(path, "x", N, false);
                try (SymbolMapWriter writer = new SymbolMapWriter(configuration, path, "x", 0, TRANSIENT_SYMBOL_COUNT_CHANGE_HANDLER)) {
                    Rnd rnd = new Rnd();
                    long prev = -1L;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        long key = writer.put(cs);
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
            try (Path path = new Path().of(configuration.getRoot())) {
                create(path, "x", N, false);
                try (SymbolMapWriter writer = new SymbolMapWriter(configuration, path, "x", 0, TRANSIENT_SYMBOL_COUNT_CHANGE_HANDLER)) {
                    long prev = -1L;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        long key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        prev = key;
                    }
                }
                rnd.reset();
                try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", N)) {
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        TestUtils.assertEquals(cs, reader.valueOf(i));
                        Assert.assertEquals(i, reader.keyOf(cs));
                    }

                    Assert.assertEquals(N, reader.size());
                    Assert.assertNull(reader.valueOf(-1));
                    Assert.assertNull(reader.valueOf(N));
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, reader.keyOf("hola"));
                }
            }
        });
    }

    @Test
    public void testTransactionalRead() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 1000000;
            Rnd rnd = new Rnd();
            try (Path path = new Path().of(configuration.getRoot())) {
                create(path, "x", N, false);
                try (SymbolMapWriter writer = new SymbolMapWriter(configuration, path, "x", 0, TRANSIENT_SYMBOL_COUNT_CHANGE_HANDLER)) {
                    long prev = -1L;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        long key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        prev = key;
                    }

                    rnd.reset();
                    try (SymbolMapReaderImpl reader = new SymbolMapReaderImpl(configuration, path, "x", N)) {
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
            try (Path path = new Path().of(configuration.getRoot())) {
                create(path, "x", N, true);
                try (SymbolMapWriter writer = new SymbolMapWriter(configuration, path, "x", 0, TRANSIENT_SYMBOL_COUNT_CHANGE_HANDLER)) {
                    Rnd rnd = new Rnd();
                    long prev = -1L;
                    for (int i = 0; i < N; i++) {
                        CharSequence cs = rnd.nextChars(10);
                        long key = writer.put(cs);
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
                        long key = writer.put(cs);
                        Assert.assertEquals(prev + 1, key);
                        Assert.assertEquals(key, writer.put(cs));
                        prev = key;
                    }
                    Assert.assertEquals(N, writer.getSymbolCount());
                }
            }
        });
    }
}