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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewCheckpointKeyDictionaryColumnSource;
import io.questdb.cairo.lv.LiveViewCheckpointKeyDictionaryReader;
import io.questdb.cairo.lv.LiveViewCheckpointKeyDictionaryWriter;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaSegmentWriter;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointRootBuilder;
import io.questdb.cairo.lv.LiveViewSymbolIdRegistry;
import io.questdb.cairo.lv.LiveViewSymbolIdSource;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.DirectSymbolMap;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/**
 * The LV-private symbol-key dictionary's durable format, on its own: the directory/chunk page
 * shapes {@link LiveViewCheckpointKeyDictionaryWriter} and
 * {@link LiveViewCheckpointKeyDictionaryReader} share, the append-only path-copy growth an
 * incremental seal requires, and {@link LiveViewCheckpointRoot}'s {@code keyDictionaryRef}.
 * <p>
 * Every test here drives the mechanism directly rather than through a live view, so a case
 * can build identities and growth patterns no single view would produce - a column that
 * shrank, a duplicate string, a malformed chunk. What a real seal writes is audited on the
 * bytes by {@link LiveViewCheckpointSymbolKeyWireFormatTest}, and what a real view does with
 * the restored ids by {@link LiveViewSymbolIdTranslationTest}.
 */
public class LiveViewCheckpointKeyDictionaryTest extends AbstractCairoTest {

    private static final int BASE_TABLE_ID = 5;
    private static final String LV_DIR = "lv_key_dictionary";
    private static final int WRITER_COLUMN_0 = 2;
    private static final int WRITER_COLUMN_1 = 9;

    @Before
    public void setUp() {
        super.setUp();
        try (Path path = new Path()) {
            checkpointsDir(path).concat(LiveViewCheckpointLayout.META_DIR_NAME).slash();
            configuration.getFilesFacade().mkdirs(path, configuration.getMkDirMode());
        }
    }

    @Test
    public void testAppendOnlyGrowthReusesPredecessorChunksByReference() throws Exception {
        assertMemoryLeak(() -> {
            final TestColumnSource columns = new TestColumnSource();
            columns.addColumn(BASE_TABLE_ID, WRITER_COLUMN_0, "sym", ColumnType.SYMBOL, "a", "b");
            final LiveViewCheckpointPageRef ref1 = new LiveViewCheckpointPageRef();
            final LongList referenced1;
            try (LiveViewCheckpointKeyDictionaryWriter writer = new LiveViewCheckpointKeyDictionaryWriter(configuration);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                writer.write(new LiveViewCheckpointPageRef(), columns, 10, ref1);
                referenced1 = new LongList(writer.getReferencedSegmentIds());
            }
            // Directory + one chunk, both freshly written into segment 10.
            assertLongList(referenced1, 10);

            final LiveViewCheckpointPageRef oldChunkRef;
            try (LiveViewCheckpointKeyDictionaryReader reader = new LiveViewCheckpointKeyDictionaryReader(configuration);
                 Path dir = new Path()) {
                reader.of(checkpointsDir(dir), ref1);
                Assert.assertEquals(1, reader.getColumnCount());
                Assert.assertEquals(2, reader.getSymbolCount(0));
                Assert.assertEquals(1, reader.getChunkCount(0));
                oldChunkRef = copy(reader.getChunkRef(0, 0));
            }

            // Grow the live dictionary and seal again against the first root as predecessor.
            columns.addEntry(0, "c");
            final LiveViewCheckpointPageRef ref2 = new LiveViewCheckpointPageRef();
            final LongList referenced2;
            try (LiveViewCheckpointKeyDictionaryWriter writer = new LiveViewCheckpointKeyDictionaryWriter(configuration);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                writer.write(ref1, columns, 11, ref2);
                referenced2 = new LongList(writer.getReferencedSegmentIds());
            }
            // The new directory names the old chunk's segment (10) plus its own new chunk's
            // segment (11) - the old chunk page itself is never rewritten.
            assertLongList(referenced2, 10, 11);

            try (LiveViewCheckpointKeyDictionaryReader reader = new LiveViewCheckpointKeyDictionaryReader(configuration);
                 Path dir = new Path()) {
                reader.of(checkpointsDir(dir), ref2);
                Assert.assertEquals(1, reader.getColumnCount());
                Assert.assertEquals(3, reader.getSymbolCount(0));
                Assert.assertEquals(2, reader.getChunkCount(0));
                assertRefEquals(oldChunkRef, reader.getChunkRef(0, 0));

                final LiveViewSymbolIdRegistry registry = registry();
                registry.bind(0, 0, WRITER_COLUMN_0, BASE_TABLE_ID);
                registry.restoreDictionary(reader);
                Assert.assertEquals(3, registry.getDictionarySize(0));
                Assert.assertEquals("a", registry.lookup(0, 0).toString());
                Assert.assertEquals("b", registry.lookup(0, 1).toString());
                Assert.assertEquals("c", registry.lookup(0, 2).toString());
                registry.close();
            }
        });
    }

    @Test
    public void testColumnCannotShrink() throws Exception {
        assertMemoryLeak(() -> {
            final TestColumnSource columns = new TestColumnSource();
            columns.addColumn(BASE_TABLE_ID, WRITER_COLUMN_0, "sym", ColumnType.SYMBOL, "a", "b", "c");
            final LiveViewCheckpointPageRef ref1 = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointKeyDictionaryWriter writer = new LiveViewCheckpointKeyDictionaryWriter(configuration);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                writer.write(new LiveViewCheckpointPageRef(), columns, 20, ref1);
            }

            final TestColumnSource shrunk = new TestColumnSource();
            shrunk.addColumn(BASE_TABLE_ID, WRITER_COLUMN_0, "sym", ColumnType.SYMBOL, "a");
            try (LiveViewCheckpointKeyDictionaryWriter writer = new LiveViewCheckpointKeyDictionaryWriter(configuration);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                try {
                    writer.write(ref1, shrunk, 21, new LiveViewCheckpointPageRef());
                    Assert.fail("a shrinking column must be refused");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "column shrank");
                }
            }
        });
    }

    @Test
    public void testCorruptChunkPageKindRejected() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef fakeChunk = writeRaw(30, 0x2b + 1, mem -> {
                mem.putInt(1);
                mem.putInt(0);
            });
            final LiveViewCheckpointPageRef directoryRef = writeRaw(31, LiveViewCheckpointKeyDictionaryReader.DIRECTORY_PAGE_KIND, mem -> {
                mem.putInt(1);
                mem.putInt(1);
                mem.putInt(BASE_TABLE_ID);
                mem.putInt(WRITER_COLUMN_0);
                mem.putInt(ColumnType.SYMBOL);
                mem.putInt(0);
                mem.putInt(0);
                mem.putInt(1);
                putRef(mem, fakeChunk);
            });
            try (LiveViewCheckpointKeyDictionaryReader reader = new LiveViewCheckpointKeyDictionaryReader(configuration);
                 Path dir = new Path();
                 DirectSymbolMap map = freshMap()) {
                reader.of(checkpointsDir(dir), directoryRef);
                try {
                    reader.restoreInto(0, map);
                    Assert.fail("a chunk with the wrong page kind must be rejected");
                } catch (CairoException e) {
                    Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
                    TestUtils.assertContains(e.getFlyweightMessage(), "chunk page kind unknown");
                }
            }
        });
    }

    @Test
    public void testDuplicateStringRejectedOnRestore() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef directoryRef = writeRaw(40, LiveViewCheckpointKeyDictionaryReader.DIRECTORY_PAGE_KIND, mem -> {
                mem.putInt(1);
                mem.putInt(1);
                mem.putInt(BASE_TABLE_ID);
                mem.putInt(WRITER_COLUMN_0);
                mem.putInt(ColumnType.SYMBOL);
                mem.putInt(0);
                mem.putInt(2); // symbolCount claims two entries
                mem.putInt(1);
                final LiveViewCheckpointPageRef chunk = writeRaw(41, LiveViewCheckpointKeyDictionaryReader.CHUNK_PAGE_KIND, chunkMem -> {
                    chunkMem.putInt(1);
                    chunkMem.putInt(2);
                    putUtf8Entry(chunkMem, "dup");
                    putUtf8Entry(chunkMem, "dup");
                });
                putRef(mem, chunk);
            });
            try (LiveViewCheckpointKeyDictionaryReader reader = new LiveViewCheckpointKeyDictionaryReader(configuration);
                 Path dir = new Path();
                 DirectSymbolMap map = freshMap()) {
                reader.of(checkpointsDir(dir), directoryRef);
                try {
                    reader.restoreInto(0, map);
                    Assert.fail("a duplicate string within one column must be rejected");
                } catch (CairoException e) {
                    Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
                    TestUtils.assertContains(e.getFlyweightMessage(), "duplicate string");
                }
            }
        });
    }

    @Test
    public void testEmptyDictionaryRoundTrips() throws Exception {
        assertMemoryLeak(() -> {
            final TestColumnSource columns = new TestColumnSource();
            final LiveViewCheckpointPageRef ref = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointKeyDictionaryWriter writer = new LiveViewCheckpointKeyDictionaryWriter(configuration);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                writer.write(new LiveViewCheckpointPageRef(), columns, 50, ref);
            }
            try (LiveViewCheckpointKeyDictionaryReader reader = new LiveViewCheckpointKeyDictionaryReader(configuration);
                 Path dir = new Path()) {
                reader.of(checkpointsDir(dir), ref);
                Assert.assertEquals(0, reader.getColumnCount());
            }
        });
    }

    @Test
    public void testMalformedUtf8Rejected() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef directoryRef = writeRaw(120, LiveViewCheckpointKeyDictionaryReader.DIRECTORY_PAGE_KIND, mem -> {
                mem.putInt(1);
                mem.putInt(1);
                mem.putInt(BASE_TABLE_ID);
                mem.putInt(WRITER_COLUMN_0);
                mem.putInt(ColumnType.SYMBOL);
                mem.putInt(0);
                mem.putInt(1);
                mem.putInt(1);
                final LiveViewCheckpointPageRef chunk = writeRaw(121, LiveViewCheckpointKeyDictionaryReader.CHUNK_PAGE_KIND, chunkMem -> {
                    chunkMem.putInt(1);
                    chunkMem.putInt(1);
                    // A truncated two-byte sequence: a lead byte claiming one continuation byte
                    // that never comes.
                    chunkMem.putInt(1);
                    chunkMem.putByte((byte) 0xc2);
                });
                putRef(mem, chunk);
            });
            try (LiveViewCheckpointKeyDictionaryReader reader = new LiveViewCheckpointKeyDictionaryReader(configuration);
                 Path dir = new Path();
                 DirectSymbolMap map = freshMap()) {
                reader.of(checkpointsDir(dir), directoryRef);
                try {
                    reader.restoreInto(0, map);
                    Assert.fail("a truncated UTF-8 sequence must be rejected");
                } catch (CairoException e) {
                    Assert.assertEquals(CairoException.LV_CHECKPOINT_TIMELINE_INVALID, e.getErrno());
                    TestUtils.assertContains(e.getFlyweightMessage(), "not valid UTF-8");
                }
            }
        });
    }

    @Test
    public void testMultiByteUtf8RoundTrips() throws Exception {
        assertMemoryLeak(() -> {
            final TestColumnSource columns = new TestColumnSource();
            // A 2-byte sequence (Latin-1 supplement), a 3-byte sequence (CJK) and a 4-byte
            // sequence that decodes to a UTF-16 surrogate pair.
            columns.addColumn(BASE_TABLE_ID, WRITER_COLUMN_0, "sym", ColumnType.SYMBOL, "café", "中文", "🎉");
            final LiveViewCheckpointPageRef ref = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointKeyDictionaryWriter writer = new LiveViewCheckpointKeyDictionaryWriter(configuration);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                writer.write(new LiveViewCheckpointPageRef(), columns, 130, ref);
            }
            try (LiveViewCheckpointKeyDictionaryReader reader = new LiveViewCheckpointKeyDictionaryReader(configuration);
                 Path dir = new Path();
                 DirectSymbolMap map = freshMap()) {
                reader.of(checkpointsDir(dir), ref);
                reader.restoreInto(0, map);
                Assert.assertEquals("café", map.valueOf(0).toString());
                Assert.assertEquals("中文", map.valueOf(1).toString());
                Assert.assertEquals("🎉", map.valueOf(2).toString());
            }
        });
    }

    @Test
    public void testRegistryRoundTripThroughWriterAndReader() throws Exception {
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef rootRef;
            try (LiveViewSymbolIdRegistry registry = registry()) {
                registry.bind(0, 0, WRITER_COLUMN_0, BASE_TABLE_ID);
                registry.bind(1, 1, WRITER_COLUMN_1, BASE_TABLE_ID);
                registry.armFor(sourceStatic(new Dictionary("x", "y", "z")));
                Assert.assertEquals(0, registry.translate(0, 0));
                Assert.assertEquals(1, registry.translate(0, 1));
                Assert.assertEquals(2, registry.translate(0, 2));
                registry.armFor(sourceStatic(new Dictionary("p", "q")));
                Assert.assertEquals(0, registry.translate(1, 0));
                Assert.assertEquals(1, registry.translate(1, 1));

                try (LiveViewCheckpointKeyDictionaryWriter writer = new LiveViewCheckpointKeyDictionaryWriter(configuration);
                     Path dir = new Path()) {
                    writer.of(checkpointsDir(dir));
                    final LiveViewCheckpointPageRef ref = new LiveViewCheckpointPageRef();
                    writer.write(new LiveViewCheckpointPageRef(), registry.newDictionaryColumnSource(), 60, ref);
                    rootRef = copy(ref);
                }
            }

            try (LiveViewCheckpointKeyDictionaryReader reader = new LiveViewCheckpointKeyDictionaryReader(configuration);
                 Path dir = new Path()) {
                reader.of(checkpointsDir(dir), rootRef);
                Assert.assertEquals(2, reader.getColumnCount());

                try (LiveViewSymbolIdRegistry restored = registry()) {
                    restored.bind(0, 0, WRITER_COLUMN_0, BASE_TABLE_ID);
                    restored.bind(1, 1, WRITER_COLUMN_1, BASE_TABLE_ID);
                    restored.restoreDictionary(reader);
                    Assert.assertEquals("x", restored.lookup(0, 0).toString());
                    Assert.assertEquals("y", restored.lookup(0, 1).toString());
                    Assert.assertEquals("z", restored.lookup(0, 2).toString());
                    Assert.assertEquals("p", restored.lookup(1, 0).toString());
                    Assert.assertEquals("q", restored.lookup(1, 1).toString());
                }
            }
        });
    }

    @Test
    public void testRestoreFromStrictPrefixRootDiscardsAbandonedIds() throws Exception {
        // Section 6.3: a rollback/A-B-fallback restore can land on a root whose dictionary is a
        // strict prefix of what the runtime was just using. Interning must resume at that
        // root's own symbolCount, so an id the abandoned root spent on one string can be spent
        // again on a different one - and nothing may keep reading the abandoned string through
        // that id afterward.
        assertMemoryLeak(() -> {
            final LiveViewCheckpointPageRef sealedRef;
            try (LiveViewSymbolIdRegistry registry = registry()) {
                registry.bind(0, 0, WRITER_COLUMN_0, BASE_TABLE_ID);
                registry.armFor(sourceStatic(new Dictionary("a", "b", "c")));
                Assert.assertEquals(0, registry.translate(0, 0));
                Assert.assertEquals(1, registry.translate(0, 1));
                Assert.assertEquals(2, registry.translate(0, 2));

                try (LiveViewCheckpointKeyDictionaryWriter writer = new LiveViewCheckpointKeyDictionaryWriter(configuration);
                     Path dir = new Path()) {
                    writer.of(checkpointsDir(dir));
                    final LiveViewCheckpointPageRef ref = new LiveViewCheckpointPageRef();
                    writer.write(new LiveViewCheckpointPageRef(), registry.newDictionaryColumnSource(), 70, ref);
                    sealedRef = copy(ref);
                }

                // The runtime keeps going past the seal, the way it would before an abandoned
                // repair or an A/B fallback discards this progress.
                registry.armFor(sourceStatic(new Dictionary("a", "b", "c", "abandoned")));
                Assert.assertEquals(3, registry.translate(0, 3));
                Assert.assertEquals(4, registry.getDictionarySize(0));

                try (LiveViewCheckpointKeyDictionaryReader reader = new LiveViewCheckpointKeyDictionaryReader(configuration);
                     Path dir = new Path()) {
                    reader.of(checkpointsDir(dir), sealedRef);
                    registry.restoreDictionary(reader);
                }
                Assert.assertEquals(3, registry.getDictionarySize(0));
                Assert.assertNull("the abandoned id must not resolve to its old string", registry.lookup(0, 3));

                // Interning resumes at the restored symbolCount and can mint id 3 again, for a
                // different string this time.
                registry.armFor(sourceStatic(new Dictionary("a", "b", "c", "different")));
                Assert.assertEquals(3, registry.translate(0, 3));
                Assert.assertEquals("different", registry.lookup(0, 3).toString());
            }
        });
    }

    @Test
    public void testRootCarriesKeyDictionaryRefAcrossFormatVersions() throws Exception {
        assertMemoryLeak(() -> {
            // A version-1 root, the shape written before the key dictionary existed, still
            // decodes with a null key dictionary reference.
            final LiveViewCheckpointPageRef legacyRoot = writeRaw(80, LiveViewCheckpointRoot.PAGE_KIND, mem -> {
                mem.putInt(1); // FORMAT_VERSION_1
                mem.putInt(0); // segmentCount
                mem.putLong(1); // checkpointId
                mem.putLong(100); // maxTimestamp
                mem.putLong(1); // definitionTxn
                putNullRef(mem); // anchorRootRef
                putRef(mem, fakeMetaRef()); // functionDirectoryRef
            });
            try (LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                 Path dir = new Path()) {
                root.of(checkpointsDir(dir), legacyRoot);
                final LiveViewCheckpointPageRef out = new LiveViewCheckpointPageRef();
                root.getKeyDictionaryRef(out);
                Assert.assertTrue(out.isNull());
            }

            // A freshly built root with no dictionary bound publishes a null reference too -
            // the RootBuilder call sites this optimization has not touched yet.
            final LiveViewCheckpointPageRef withoutDictionary;
            try (LiveViewCheckpointRootBuilder builder = new LiveViewCheckpointRootBuilder(configuration);
                 Path dir = new Path()) {
                builder.begin(checkpointsDir(dir), 2, 200, 1, new LiveViewCheckpointPageRef());
                final LiveViewCheckpointPageRef out = new LiveViewCheckpointPageRef();
                builder.build(81, out);
                withoutDictionary = copy(out);
            }
            try (LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                 Path dir = new Path()) {
                root.of(checkpointsDir(dir), withoutDictionary);
                final LiveViewCheckpointPageRef out = new LiveViewCheckpointPageRef();
                root.getKeyDictionaryRef(out);
                Assert.assertTrue(out.isNull());
            }

            // A root that does bind one round-trips it, and folds the dictionary's own
            // segments into the checkpoint's closure.
            final TestColumnSource columns = new TestColumnSource();
            columns.addColumn(BASE_TABLE_ID, WRITER_COLUMN_0, "sym", ColumnType.SYMBOL, "a");
            final LiveViewCheckpointPageRef dictionaryRef = new LiveViewCheckpointPageRef();
            final LongList dictionarySegments;
            try (LiveViewCheckpointKeyDictionaryWriter dictWriter = new LiveViewCheckpointKeyDictionaryWriter(configuration);
                 Path dir = new Path()) {
                dictWriter.of(checkpointsDir(dir));
                dictWriter.write(new LiveViewCheckpointPageRef(), columns, 90, dictionaryRef);
                dictionarySegments = new LongList(dictWriter.getReferencedSegmentIds());
            }
            final LiveViewCheckpointPageRef withDictionary;
            try (LiveViewCheckpointRootBuilder builder = new LiveViewCheckpointRootBuilder(configuration);
                 Path dir = new Path()) {
                builder.begin(checkpointsDir(dir), 3, 300, 1, new LiveViewCheckpointPageRef());
                builder.setKeyDictionaryRef(dictionaryRef, dictionarySegments);
                final LiveViewCheckpointPageRef out = new LiveViewCheckpointPageRef();
                builder.build(91, out);
                withDictionary = copy(out);
            }
            try (LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                 Path dir = new Path()) {
                root.of(checkpointsDir(dir), withDictionary);
                final LiveViewCheckpointPageRef out = new LiveViewCheckpointPageRef();
                root.getKeyDictionaryRef(out);
                assertRefEquals(dictionaryRef, out);
                boolean sawDictSegment = false;
                for (int i = 0; i < root.getSegmentIdCount(); i++) {
                    sawDictSegment |= root.getSegmentId(i) == 90;
                }
                Assert.assertTrue("the root's closure must name the dictionary's own segment", sawDictSegment);
            }
        });
    }

    @Test
    public void testRoundTripMultipleColumnsSorted() throws Exception {
        assertMemoryLeak(() -> {
            final TestColumnSource columns = new TestColumnSource();
            columns.addColumn(BASE_TABLE_ID, WRITER_COLUMN_1, "sym1", ColumnType.SYMBOL, "p", "q");
            columns.addColumn(BASE_TABLE_ID, WRITER_COLUMN_0, "sym0", ColumnType.SYMBOL, "x", "y", "z");
            final LiveViewCheckpointPageRef ref = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointKeyDictionaryWriter writer = new LiveViewCheckpointKeyDictionaryWriter(configuration);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                try {
                    writer.write(new LiveViewCheckpointPageRef(), columns, 100, ref);
                    Assert.fail("out-of-order columns must be refused");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "strictly increasing");
                }
            }

            final TestColumnSource sorted = new TestColumnSource();
            sorted.addColumn(BASE_TABLE_ID, WRITER_COLUMN_0, "sym0", ColumnType.SYMBOL, "x", "y", "z");
            sorted.addColumn(BASE_TABLE_ID, WRITER_COLUMN_1, "sym1", ColumnType.SYMBOL, "p", "q");
            try (LiveViewCheckpointKeyDictionaryWriter writer = new LiveViewCheckpointKeyDictionaryWriter(configuration);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                writer.write(new LiveViewCheckpointPageRef(), sorted, 101, ref);
            }
            try (LiveViewCheckpointKeyDictionaryReader reader = new LiveViewCheckpointKeyDictionaryReader(configuration);
                 Path dir = new Path()) {
                reader.of(checkpointsDir(dir), ref);
                Assert.assertEquals(2, reader.getColumnCount());
                final int col0 = reader.findColumn(BASE_TABLE_ID, WRITER_COLUMN_0);
                final int col1 = reader.findColumn(BASE_TABLE_ID, WRITER_COLUMN_1);
                Assert.assertEquals(0, col0);
                Assert.assertEquals(1, col1);
                Assert.assertEquals(-1, reader.findColumn(BASE_TABLE_ID, 999));
                Assert.assertEquals(3, reader.getSymbolCount(col0));
                Assert.assertEquals(2, reader.getSymbolCount(col1));

                final DirectSymbolMap map0 = freshMap();
                try {
                    reader.restoreInto(col0, map0);
                    Assert.assertEquals("x", map0.valueOf(0).toString());
                    Assert.assertEquals("y", map0.valueOf(1).toString());
                    Assert.assertEquals("z", map0.valueOf(2).toString());
                } finally {
                    map0.close();
                }
                final DirectSymbolMap map1 = freshMap();
                try {
                    reader.restoreInto(col1, map1);
                    Assert.assertEquals("p", map1.valueOf(0).toString());
                    Assert.assertEquals("q", map1.valueOf(1).toString());
                } finally {
                    map1.close();
                }
            }
        });
    }

    private static void assertLongList(LongList actual, long... expected) {
        Assert.assertEquals(expected.length, actual.size());
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals(expected[i], actual.getQuick(i));
        }
    }

    private static void assertRefEquals(LiveViewCheckpointPageRef expected, LiveViewCheckpointPageRef actual) {
        Assert.assertEquals(expected.getSegmentId(), actual.getSegmentId());
        Assert.assertEquals(expected.getOffset(), actual.getOffset());
        Assert.assertEquals(expected.getLength(), actual.getLength());
    }

    private static Path checkpointsDir(Path path) {
        return path.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
    }

    private static LiveViewCheckpointPageRef copy(LiveViewCheckpointPageRef ref) {
        return new LiveViewCheckpointPageRef().of(ref.getSegmentId(), ref.getOffset(), ref.getLength());
    }

    private static LiveViewCheckpointPageRef fakeMetaRef() {
        return new LiveViewCheckpointPageRef().of(99, LiveViewCheckpointLayout.SEG_HEADER_SIZE, LiveViewCheckpointLayout.PAGE_HEADER_SIZE);
    }

    private static DirectSymbolMap freshMap() {
        return new DirectSymbolMap(64, 8, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
    }

    private static void putNullRef(MemoryA mem) {
        mem.putLong(-1);
        mem.putLong(0);
        mem.putInt(0);
    }

    private static void putRef(MemoryA mem, LiveViewCheckpointPageRef ref) {
        mem.putLong(ref.getSegmentId());
        mem.putLong(ref.getOffset());
        mem.putInt(ref.getLength());
    }

    private static void putUtf8Entry(MemoryA mem, String value) {
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        mem.putInt(bytes.length);
        for (byte b : bytes) {
            mem.putByte(b);
        }
    }

    private static LiveViewSymbolIdRegistry registry() {
        return new LiveViewSymbolIdRegistry(new TableToken("lv", "lv~1", null, 1, true, false, false));
    }

    private static LiveViewSymbolIdSource sourceStatic(Dictionary dictionary) {
        return (registry, slot, scan, writer) -> registry.armStatic(slot, dictionary.size(), dictionary);
    }

    private LiveViewCheckpointPageRef writeRaw(long segmentId, int pageKind, PageWriter pageWriter) {
        final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
        try (LiveViewCheckpointMetaSegmentWriter writer = new LiveViewCheckpointMetaSegmentWriter(configuration);
             Path dir = new Path()) {
            writer.of(checkpointsDir(dir), segmentId);
            pageWriter.write(writer.beginPage(pageKind));
            writer.endPage(root);
            writer.commit();
        }
        return root;
    }

    @FunctionalInterface
    private interface PageWriter {
        void write(MemoryA mem);
    }

    /**
     * One column's symbol space, in the shape the registry consumes it. Mirrors
     * {@code LiveViewSymbolIdRegistryTest}'s own fixture.
     */
    private static final class Dictionary implements StaticSymbolTable, SymbolTableSource {
        private final ObjList<String> values = new ObjList<>();

        Dictionary(String... values) {
            for (String value : values) {
                this.values.add(value);
            }
        }

        @Override
        public boolean containsNullValue() {
            return false;
        }

        @Override
        public int getSymbolCount() {
            return values.size();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return this;
        }

        @Override
        public int keyOf(CharSequence value) {
            final int index = values.indexOf(value);
            return index < 0 ? SymbolTable.VALUE_NOT_FOUND : index;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return this;
        }

        int size() {
            return values.size();
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            return key >= 0 && key < values.size() ? values.getQuick(key) : null;
        }
    }

    /**
     * A minimal, directly-controlled {@link LiveViewCheckpointKeyDictionaryColumnSource} for
     * exercising the writer without a live registry.
     */
    private static final class TestColumnSource implements LiveViewCheckpointKeyDictionaryColumnSource {
        private final IntList baseTableIds = new IntList();
        private final IntList baseWriterColumnIndexes = new IntList();
        private final ObjList<String> names = new ObjList<>();
        private final IntList types = new IntList();
        private final ObjList<ObjList<String>> values = new ObjList<>();

        void addColumn(int baseTableId, int baseWriterColumnIndex, String name, int type, String... entries) {
            baseTableIds.add(baseTableId);
            baseWriterColumnIndexes.add(baseWriterColumnIndex);
            names.add(name);
            types.add(type);
            final ObjList<String> list = new ObjList<>();
            for (String e : entries) {
                list.add(e);
            }
            values.add(list);
        }

        void addEntry(int columnIndex, String value) {
            values.getQuick(columnIndex).add(value);
        }

        @Override
        public int getBaseTableId(int columnIndex) {
            return baseTableIds.getQuick(columnIndex);
        }

        @Override
        public int getBaseWriterColumnIndex(int columnIndex) {
            return baseWriterColumnIndexes.getQuick(columnIndex);
        }

        @Override
        public int getColumnCount() {
            return baseTableIds.size();
        }

        @Override
        public CharSequence getColumnName(int columnIndex) {
            return names.getQuick(columnIndex);
        }

        @Override
        public int getColumnType(int columnIndex) {
            return types.getQuick(columnIndex);
        }

        @Override
        public int getEntryCount(int columnIndex) {
            return values.getQuick(columnIndex).size();
        }

        @Override
        public CharSequence getEntryValue(int columnIndex, int lvId) {
            return values.getQuick(columnIndex).getQuick(lvId);
        }
    }
}
