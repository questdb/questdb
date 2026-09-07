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

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewCheckpointKeyDictionaryReader;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.DirectSymbolMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.zip.CRC32;

/**
 * Wire-format audit of the on-disk shapes a translated SYMBOL partition key adds, of the field
 * the checkpoint root grew to address them, and of the key bytes themselves: the key
 * dictionary's directory page ({@code 0x2a}), its chunk pages ({@code 0x2b}), the root's third
 * {@code LiveViewCheckpointPageRef}, and the four bytes a partition-map leaf ({@code 0x16})
 * carries for one translated key.
 * <p>
 * {@link LiveViewCheckpointWireFormatTest} audits the shapes a released 10.0.1 build wrote,
 * against bytes that build actually emitted. These shapes have no released bytes to check
 * against - no released build ever wrote one - so the question this class answers is the other
 * one: that the format written here is the format specified, field for field, when a real live
 * view seals rather than when a unit test drives the writer by hand.
 * <p>
 * That distinction is what separates this from {@link LiveViewCheckpointKeyDictionaryTest},
 * which builds a dictionary from a synthetic column source and round-trips it through the
 * writer and reader. A writer and a reader from the same tree agree with each other by
 * construction, and a synthetic source can hold identities no live view would produce. Here
 * every byte comes from an ordinary seal of an ordinary view, and every field is read twice:
 * once longhand below, with each offset and width as a literal, and once by the decoder under
 * audit. Only the longhand reading can say the decoder moved a field.
 * <p>
 * The page kinds, the segment magic and the framing sizes appear here as numbers rather than
 * as imports of the constants under audit, for the same reason.
 */
public class LiveViewCheckpointSymbolKeyWireFormatTest extends AbstractLiveViewTest {

    // Page framing: crc INT, payloadLength INT, pageKind INT.
    private static final int PAGE_HEADER_SIZE = 12;
    private static final int PAGE_KIND_CHECKPOINT_ROOT = 0x1a;
    private static final int PAGE_KIND_FUNCTION_ROOT = 0x18;
    private static final int PAGE_KIND_KEY_DICTIONARY_CHUNK = 0x2b;
    private static final int PAGE_KIND_KEY_DICTIONARY_DIRECTORY = 0x2a;
    private static final int PAGE_KIND_PARTITION_MAP_LEAF = 0x16;
    private static final int PAGE_KIND_WINDOW_ROOT = 0x1d;
    // A metadata page reference: segmentId LONG, offset LONG, length INT.
    private static final int PAGE_REF_BYTES = 20;
    // Segment header: magic INT, formatVersion INT, segmentId LONG, pageCount INT, headerCrc INT.
    private static final int SEG_HEADER_SIZE = 24;
    // ASCII "LVMS".
    private static final int SEG_MAGIC = 0x4c56_4d53;
    // A state page reference: segmentId LONG, offset LONG, then storedLength, decodedLength,
    // pageKind, codec, rowCount and flags as INTs.
    private static final int STATE_REF_BYTES = 40;

    @Before
    public void setUpCadence() {
        // One sealed boundary per commit, so a case that wants a predecessor root only has to
        // commit twice rather than fill a row budget.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
    }

    @Test
    public void testASecondSealPathCopiesThePredecessorsChunksAndAppendsOnlyTheDelta() throws Exception {
        // The append-only invariant, read off the bytes: ids already published cannot be
        // renumbered by a later seal, because a later seal does not rewrite the chunk that
        // holds them - it names the same page again and appends one covering the delta. A
        // writer that rewrote the whole dictionary per seal would agree with its own reader
        // and pass every round-trip case, while making the shared-leaf reuse this invariant
        // rests on impossible to check.
        assertMemoryLeak(() -> {
            createTranslatedView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES ('2024-01-01T00:00:00.000000Z', 'a', 1)");
                driveRefreshToQuiescence(job);

                final Directory first = readNewestDictionary();
                Assert.assertEquals("one bound slot, one directory column", 1, first.columns.size());
                Assert.assertEquals("only 'a' has been interned", 1, first.columns.getQuick(0).symbolCount);
                Assert.assertEquals(1, first.columns.getQuick(0).chunkRefs.size());
                Assert.assertArrayEquals(new String[]{"a"}, first.columns.getQuick(0).values());

                // A second commit introducing a second string, so the dictionary grows by
                // exactly one id and the seal has a predecessor to path-copy.
                execute("INSERT INTO base VALUES ('2024-01-01T00:00:01.000000Z', 'b', 2)");
                driveRefreshToQuiescence(job);

                final Directory second = readNewestDictionary();
                final Column before = first.columns.getQuick(0);
                final Column after = second.columns.getQuick(0);
                Assert.assertEquals("the second seal must see both strings", 2, after.symbolCount);
                Assert.assertEquals(
                        "the second seal must append exactly one chunk to the predecessor's list",
                        before.chunkRefs.size() + 1,
                        after.chunkRefs.size()
                );
                assertRefEquals(
                        "the predecessor's chunk must be named again rather than rewritten",
                        before.chunkRefs.getQuick(0),
                        after.chunkRefs.getQuick(0)
                );
                Assert.assertArrayEquals(new String[]{"a", "b"}, after.values());
                // The new chunk covers the delta alone. A chunk that carried 'a' as well would
                // still reconstruct correctly, and would still be wrong: seal cost would then
                // track the dictionary's total size rather than what the commit changed.
                Assert.assertArrayEquals(
                        new String[]{"b"},
                        second.chunkValues(after.chunkRefs.getQuick(1))
                );
            }
        });
    }

    @Test
    public void testACompositeKeySealsOneDirectoryColumnPerBoundSlotInIdentityOrder() throws Exception {
        // Two bound slots, so the directory carries two columns and the ordering the reader's
        // own lookup depends on is exercised rather than trivially true. The order is by
        // (baseTableId, baseWriterColumnIndex) rather than by slot, because the reader
        // matches a column by identity when it restores: a slot bound in a different order by
        // a later compile has to find the same dictionary, and an id resolved against the
        // wrong column's dictionary is in range for the one it lands in rather than being
        // refused.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym1 SYMBOL, x LONG, sym2 SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym1, sym2, sum(x) OVER (PARTITION BY sym2, sym1 ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES "
                        + "('2024-01-01T00:00:00.000000Z', 'a', 1, 'p'), "
                        + "('2024-01-01T00:00:01.000000Z', 'b', 2, 'q')");
                driveRefreshToQuiescence(job);
            }

            final Directory directory = readNewestDictionary();
            Assert.assertEquals("one directory column per bound slot", 2, directory.columns.size());

            final int baseTableId = engine.verifyTableName("base").getTableId();
            // sym1 is writer column 1 and sym2 writer column 3, so identity order puts sym1
            // first even though the PARTITION BY names sym2 first.
            final Column first = directory.columns.getQuick(0);
            Assert.assertEquals(baseTableId, first.baseTableId);
            Assert.assertEquals(1, first.baseWriterColumnIndex);
            Assert.assertArrayEquals(new String[]{"a", "b"}, first.values());

            final Column second = directory.columns.getQuick(1);
            Assert.assertEquals(baseTableId, second.baseTableId);
            Assert.assertEquals(3, second.baseWriterColumnIndex);
            Assert.assertArrayEquals(new String[]{"p", "q"}, second.values());
        });
    }

    @Test
    public void testASealedRootAddressesAKeyDictionaryWhoseDirectoryAndChunksDecodeFieldForField() throws Exception {
        assertMemoryLeak(() -> {
            createTranslatedView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES "
                        + "('2024-01-01T00:00:00.000000Z', 'alpha', 1), "
                        + "('2024-01-01T00:00:01.000000Z', 'beta', 2), "
                        + "('2024-01-01T00:00:02.000000Z', null, 3), "
                        + "('2024-01-01T00:00:03.000000Z', 'alpha', 4)");
                driveRefreshToQuiescence(job);
            }

            final ObjList<Page> pages = readPages();
            final Page rootPage = newest(pages, PAGE_KIND_CHECKPOINT_ROOT);
            final byte[] payload = rootPage.payload;

            // formatVersion INT, segmentCount INT, checkpointId LONG, maxTimestamp LONG,
            // definitionTxn LONG, stateRootRef, functionDirectoryRef, keyDictionaryRef,
            // segmentIds LONG[]. The dictionary reference is the field this optimization
            // added, and it is last of the three so that a version-1 root - which has only
            // the first two - stays decodable by truncation rather than by a shifted read.
            Assert.assertEquals("checkpoint root [formatVersion]", 2, leInt(payload, 0));
            final int segmentCount = leInt(payload, 4);
            final int fixedSize = 2 * Integer.BYTES + 3 * Long.BYTES + 3 * PAGE_REF_BYTES;
            Assert.assertEquals(
                    "checkpoint root [payload length]",
                    fixedSize + segmentCount * Long.BYTES,
                    payload.length
            );
            final LiveViewCheckpointPageRef longhandRef = readRef(payload, 2 * Integer.BYTES + 3 * Long.BYTES + 2 * PAGE_REF_BYTES);
            Assert.assertFalse(
                    "a view whose partition key translates must publish a key dictionary",
                    longhandRef.isNull()
            );

            try (
                    Path checkpointsDir = checkpointsDir();
                    LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(engine.getConfiguration())
            ) {
                root.of(checkpointsDir, rootPage.ref());
                final LiveViewCheckpointPageRef decoded = new LiveViewCheckpointPageRef();
                root.getKeyDictionaryRef(decoded);
                assertRefEquals("checkpoint root [keyDictionaryRef]", longhandRef, decoded);

                // The dictionary's own segments belong to the root's closure, or a
                // reclamation pass that reads only the root would delete pages the root
                // still points at.
                boolean namesDictionarySegment = false;
                for (int i = 0, n = root.getSegmentIdCount(); i < n; i++) {
                    namesDictionarySegment |= root.getSegmentId(i) == longhandRef.getSegmentId();
                }
                Assert.assertTrue(
                        "the root's closure must name the segment its dictionary directory lives in",
                        namesDictionarySegment
                );
                root.detach();
            }

            // The directory page the reference addresses, read longhand.
            final Page directoryPage = pageAt(pages, longhandRef);
            Assert.assertEquals(
                    "the key dictionary reference must address a directory page",
                    PAGE_KIND_KEY_DICTIONARY_DIRECTORY,
                    directoryPage.kind
            );
            final Directory directory = decodeDirectory(pages, directoryPage);
            Assert.assertEquals("one bound slot, one directory column", 1, directory.columns.size());
            final Column column = directory.columns.getQuick(0);

            final TableToken baseToken = engine.verifyTableName("base");
            Assert.assertEquals("directory column [baseTableId]", baseToken.getTableId(), column.baseTableId);
            // sym is the second column of base, and the identity is the writer index rather
            // than any query-side ordinal: a projection between the base scan and the window
            // shifts every query-side index, and an id read against the wrong column's
            // dictionary is in range for the one it lands in.
            Assert.assertEquals("directory column [baseWriterColumnIndex]", 1, column.baseWriterColumnIndex);
            Assert.assertEquals("directory column [columnType]", ColumnType.SYMBOL, column.columnType);
            // NULL is never interned - it keeps SymbolTable.VALUE_IS_NULL through the whole
            // key path - so the third row above contributes no entry here.
            Assert.assertEquals("directory column [symbolCount]", 2, column.symbolCount);
            Assert.assertArrayEquals(new String[]{"alpha", "beta"}, column.values());

            // And the same bytes through the decoder under audit, including the heavier
            // per-column pass that reconstructs the strings in id order.
            try (
                    Path checkpointsDir = checkpointsDir();
                    LiveViewCheckpointKeyDictionaryReader reader =
                            new LiveViewCheckpointKeyDictionaryReader(engine.getConfiguration());
                    DirectSymbolMap restored = new DirectSymbolMap(64, 8, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM)
            ) {
                reader.of(checkpointsDir, longhandRef);
                Assert.assertEquals(1, reader.getColumnCount());
                Assert.assertEquals(column.baseTableId, reader.getBaseTableId(0));
                Assert.assertEquals(column.baseWriterColumnIndex, reader.getBaseWriterColumnIndex(0));
                Assert.assertEquals(column.columnType, reader.getColumnType(0));
                Assert.assertEquals(column.symbolCount, reader.getSymbolCount(0));
                Assert.assertEquals(column.chunkRefs.size(), reader.getChunkCount(0));
                for (int i = 0, n = column.chunkRefs.size(); i < n; i++) {
                    assertRefEquals(
                            "directory column [chunkRef " + i + ']',
                            column.chunkRefs.getQuick(i),
                            reader.getChunkRef(0, i)
                    );
                }
                reader.restoreInto(0, restored);
                final String[] longhandValues = column.values();
                for (int id = 0; id < longhandValues.length; id++) {
                    Assert.assertEquals(
                            "restored id " + id,
                            longhandValues[id],
                            restored.valueOf(id).toString()
                    );
                }
                reader.detach();
            }
        });
    }

    @Test
    public void testAPartitionMapLeafCarriesTheSymbolIdMostSignificantByteFirst() throws Exception {
        // The key bytes themselves, which is what decides where a key lands in the tree. A
        // partition map orders its pages by an unsigned byte comparison, so a four-byte id
        // written in the platform's own little-endian order sorts on its low byte first and a
        // run of sequential ids scatters across every leaf; written most significant byte
        // first it sorts numerically, and a batch of new ids is an append at the right edge.
        // The strings below are interned in an order that is not their alphabetical one, so
        // an implementation that keyed by the resolved string, or that wrote the id in the
        // native order, produces a different leaf order than the one asserted here.
        assertMemoryLeak(() -> {
            createTranslatedView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES "
                        + "('2024-01-01T00:00:00.000000Z', 'zulu', 1), "
                        + "('2024-01-01T00:00:01.000000Z', 'alpha', 2), "
                        + "('2024-01-01T00:00:02.000000Z', 'mike', 3), "
                        + "('2024-01-01T00:00:03.000000Z', null, 4)");
                driveRefreshToQuiescence(job);
            }

            final ObjList<Page> pages = readPages();
            final Page rootPage = newest(pages, PAGE_KIND_CHECKPOINT_ROOT);
            final Directory dictionary = decodeDirectory(pages, pageAt(
                    pages,
                    readRef(rootPage.payload, 2 * Integer.BYTES + 3 * Long.BYTES + 2 * PAGE_REF_BYTES)
            ));
            Assert.assertArrayEquals(
                    "the dictionary hands its ids out in first-seen order",
                    new String[]{"zulu", "alpha", "mike"},
                    dictionary.columns.getQuick(0).values()
            );

            // formatVersion INT, stateFormatVersion INT, identityLength INT, keySchemaLength
            // INT, segmentCount INT, scalarStateRef, partitionMapRootRef.
            final byte[] functionRoot = newest(pages, PAGE_KIND_FUNCTION_ROOT).payload;
            final Page leaf = pageAt(pages, readRef(functionRoot, 5 * Integer.BYTES + STATE_REF_BYTES));
            Assert.assertEquals("four keys fit one leaf", PAGE_KIND_PARTITION_MAP_LEAF, leaf.kind);

            // Leaf payload: formatVersion INT, count INT, then per entry keyLength INT,
            // scalarLength INT, stateRefCount INT, the key bytes, the scalar state and that
            // many state page references.
            final byte[] payload = leaf.payload;
            Assert.assertEquals("partition map leaf [formatVersion]", 1, leInt(payload, 0));
            final int count = leInt(payload, 4);
            Assert.assertEquals("one entry per live key, the NULL key included", 4, count);
            final ObjList<byte[]> keys = new ObjList<>();
            int offset = 2 * Integer.BYTES;
            for (int i = 0; i < count; i++) {
                final int keyLength = leInt(payload, offset);
                final int scalarLength = leInt(payload, offset + 4);
                final int stateRefCount = leInt(payload, offset + 8);
                offset += 3 * Integer.BYTES;
                Assert.assertEquals(
                        "partition map leaf entry " + i + " [key length]",
                        Integer.BYTES,
                        keyLength
                );
                keys.add(Arrays.copyOfRange(payload, offset, offset + keyLength));
                offset += keyLength + scalarLength + stateRefCount * STATE_REF_BYTES;
            }
            Assert.assertEquals("partition map leaf [payload consumed]", payload.length, offset);

            // The four bytes of each key, stated as literals rather than through any decoder:
            // id 0 ('zulu'), id 1 ('alpha'), id 2 ('mike'), then VALUE_IS_NULL, which is
            // Integer.MIN_VALUE and so leads with 0x80 and sorts after every non-negative id.
            // Alphabetically 'alpha' would come first and 'zulu' last, and in the native byte
            // order id 1 would read 01 00 00 00 - neither is what a leaf holds.
            Assert.assertArrayEquals("leaf key 0 - id 0, 'zulu'", new byte[]{0, 0, 0, 0}, keys.getQuick(0));
            Assert.assertArrayEquals("leaf key 1 - id 1, 'alpha'", new byte[]{0, 0, 0, 1}, keys.getQuick(1));
            Assert.assertArrayEquals("leaf key 2 - id 2, 'mike'", new byte[]{0, 0, 0, 2}, keys.getQuick(2));
            Assert.assertArrayEquals(
                    "leaf key 3 - VALUE_IS_NULL",
                    new byte[]{(byte) 0x80, 0, 0, 0},
                    keys.getQuick(3)
            );
        });
    }

    @Test
    public void testTheSealedFunctionRootsKeySchemaSaysSymbolRatherThanString() throws Exception {
        // The counterpart of LiveViewCheckpointWireFormatTest's released-root cases, which
        // read STRING out of the same field: a released build resolved a SYMBOL partition
        // term to its string before keying, this one keys by the LV-private id. The schema is
        // what a restore validates the compiled runtime against, so the two readings together
        // are what say the upgrade is a schema change rather than a silent reinterpretation
        // of the same bytes.
        // <p>
        // A plain ROWS window seals a function root per window call, which is where the key
        // schema lives for this shape.
        assertMemoryLeak(() -> {
            createTranslatedView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES ('2024-01-01T00:00:00.000000Z', 'alpha', 1)");
                driveRefreshToQuiescence(job);
            }

            // formatVersion INT, stateFormatVersion INT, identityLength INT, keySchemaLength
            // INT, segmentCount INT, scalarStateRef, partitionMapRootRef, then the identity,
            // the key schema and (segmentId, useCount) LONG pairs.
            final byte[] functionRoot = newest(readPages(), PAGE_KIND_FUNCTION_ROOT).payload;
            Assert.assertEquals("function root [formatVersion]", 1, leInt(functionRoot, 0));
            assertSingleSymbolKeySchema(
                    "function root",
                    functionRoot,
                    5 * Integer.BYTES + STATE_REF_BYTES + PAGE_REF_BYTES + leInt(functionRoot, 8),
                    leInt(functionRoot, 12)
            );
        });
    }

    @Test
    public void testTheSealedWindowRootsKeySchemaSaysSymbolRatherThanString() throws Exception {
        // The other root shape that carries a key schema. A live view reaches one or the
        // other by its window's own shape rather than by anything about its key: an anchored
        // window seals the fused window root this branch writes where 10.0.1 wrote the anchor
        // root LiveViewCheckpointWireFormatTest reads STRING out of.
        assertMemoryLeak(() -> {
            createAnchoredTranslatedView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES ('2024-01-01T00:00:00.000000Z', 'alpha', 1)");
                driveRefreshToQuiescence(job);
            }

            // formatVersion INT, anchorValueType INT, totalInlineStateBytes INT,
            // windowIdentityLength INT, keySchemaLength INT, manifestLength INT, segmentCount
            // INT, partitionMapRootRef, then the identity, the key schema, the manifest and
            // (segmentId, useCount) LONG pairs.
            final byte[] windowRoot = newest(readPages(), PAGE_KIND_WINDOW_ROOT).payload;
            Assert.assertEquals("window root [formatVersion]", 1, leInt(windowRoot, 0));
            assertSingleSymbolKeySchema(
                    "window root",
                    windowRoot,
                    7 * Integer.BYTES + PAGE_REF_BYTES + leInt(windowRoot, 12),
                    leInt(windowRoot, 16)
            );
        });
    }

    /**
     * Reads a root's key schema out of its payload and asserts it names one SYMBOL column.
     * The schema is big-endian - the identity encoder writes it, not the page writer - and
     * reads count INT then one ColumnType INT per key column.
     */
    private static void assertSingleSymbolKeySchema(String at, byte[] payload, int offset, int length) {
        Assert.assertEquals(at + " [keySchema length]", 2 * Integer.BYTES, length);
        final byte[] keySchema = Arrays.copyOfRange(payload, offset, offset + length);
        Assert.assertEquals(at + " [keySchema columnCount]", 1, beInt(keySchema, 0));
        Assert.assertEquals(at + " [keySchema columnType]", ColumnType.SYMBOL, beInt(keySchema, 4));
    }

    private static int beInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24)
                | ((bytes[offset + 1] & 0xff) << 16)
                | ((bytes[offset + 2] & 0xff) << 8)
                | (bytes[offset + 3] & 0xff);
    }

    private static int crc32(byte[] bytes, int offset, int length) {
        final CRC32 crc = new CRC32();
        crc.update(bytes, offset, length);
        return (int) crc.getValue();
    }

    private static int leInt(byte[] bytes, int offset) {
        return (bytes[offset] & 0xff)
                | ((bytes[offset + 1] & 0xff) << 8)
                | ((bytes[offset + 2] & 0xff) << 16)
                | ((bytes[offset + 3] & 0xff) << 24);
    }

    private static long leLong(byte[] bytes, int offset) {
        return (leInt(bytes, offset) & 0xffff_ffffL) | ((long) leInt(bytes, offset + 4) << 32);
    }

    private static void assertRefEquals(String at, LiveViewCheckpointPageRef expected, LiveViewCheckpointPageRef actual) {
        Assert.assertEquals(at + " [segmentId]", expected.getSegmentId(), actual.getSegmentId());
        Assert.assertEquals(at + " [offset]", expected.getOffset(), actual.getOffset());
        Assert.assertEquals(at + " [length]", expected.getLength(), actual.getLength());
    }

    /**
     * The newest page of a kind, in the file order {@link #readPages} returns. A seal
     * publishes a new root per boundary, and every case here reads the one the last commit
     * wrote.
     */
    private static Page newest(ObjList<Page> pages, int kind) {
        Page newest = null;
        for (int i = 0, n = pages.size(); i < n; i++) {
            if (pages.getQuick(i).kind == kind) {
                newest = pages.getQuick(i);
            }
        }
        Assert.assertNotNull("no 0x" + Integer.toHexString(kind) + " page was sealed", newest);
        return newest;
    }

    private static Page pageAt(ObjList<Page> pages, LiveViewCheckpointPageRef ref) {
        for (int i = 0, n = pages.size(); i < n; i++) {
            final Page page = pages.getQuick(i);
            if (page.segmentId == ref.getSegmentId() && page.offset == ref.getOffset()) {
                Assert.assertEquals(
                        "the reference must carry the page's whole framed length",
                        page.payload.length + PAGE_HEADER_SIZE,
                        ref.getLength()
                );
                return page;
            }
        }
        throw new AssertionError("no page at segment " + ref.getSegmentId() + " offset " + ref.getOffset());
    }

    private static LiveViewCheckpointPageRef readRef(byte[] payload, int offset) {
        return new LiveViewCheckpointPageRef().of(
                leLong(payload, offset),
                leLong(payload, offset + Long.BYTES),
                leInt(payload, offset + 2 * Long.BYTES)
        );
    }

    private Path checkpointsDir() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return new Path().of(engine.getConfiguration().getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private void createAnchoredTranslatedView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                + "SELECT ts, sym, sum(x) OVER w AS s FROM base "
                + "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");
    }

    private void createTranslatedView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                + "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s "
                + "FROM base");
    }

    /**
     * Reads a directory page longhand: formatVersion INT, columnCount INT, then per column
     * baseTableId INT, baseWriterColumnIndex INT, columnType INT, nameLength INT, the name
     * bytes, symbolCount INT, chunkCount INT and that many page references.
     */
    private Directory decodeDirectory(ObjList<Page> pages, Page directoryPage) {
        final byte[] payload = directoryPage.payload;
        Assert.assertEquals("key dictionary directory [formatVersion]", 1, leInt(payload, 0));
        final int columnCount = leInt(payload, 4);
        final Directory directory = new Directory(pages);
        int offset = 8;
        for (int c = 0; c < columnCount; c++) {
            final Column column = new Column();
            column.baseTableId = leInt(payload, offset);
            column.baseWriterColumnIndex = leInt(payload, offset + 4);
            column.columnType = leInt(payload, offset + 8);
            final int nameLength = leInt(payload, offset + 12);
            offset += 16;
            column.name = new String(payload, offset, nameLength, StandardCharsets.UTF_8);
            offset += nameLength;
            column.symbolCount = leInt(payload, offset);
            final int chunkCount = leInt(payload, offset + 4);
            offset += 8;
            for (int k = 0; k < chunkCount; k++) {
                column.chunkRefs.add(readRef(payload, offset));
                offset += PAGE_REF_BYTES;
            }
            if (c > 0) {
                final Column previous = directory.columns.getQuick(c - 1);
                Assert.assertTrue(
                        "directory columns must be ordered by (baseTableId, baseWriterColumnIndex)",
                        previous.baseTableId < column.baseTableId
                                || (previous.baseTableId == column.baseTableId
                                && previous.baseWriterColumnIndex < column.baseWriterColumnIndex)
                );
            }
            directory.columns.add(column);
        }
        Assert.assertEquals("key dictionary directory [payload consumed]", payload.length, offset);
        for (int c = 0; c < columnCount; c++) {
            final Column column = directory.columns.getQuick(c);
            int reconstructed = 0;
            for (int k = 0, n = column.chunkRefs.size(); k < n; k++) {
                final String[] chunk = directory.chunkValues(column.chunkRefs.getQuick(k));
                for (int i = 0; i < chunk.length; i++) {
                    column.strings.add(chunk[i]);
                }
                reconstructed += chunk.length;
            }
            Assert.assertEquals(
                    "column " + column.name + ": the chunks must reconstruct exactly symbolCount ids",
                    column.symbolCount,
                    reconstructed
            );
        }
        return directory;
    }

    /**
     * Every metadata page the view's own {@code _checkpoints} tree holds, in file order, with
     * the segment header, its self-checksum and each page's own CRC checked longhand on the
     * way rather than through {@code LiveViewCheckpointMetaSegmentReader}.
     */
    private ObjList<Page> readPages() throws IOException {
        final File metaDir;
        try (Path dir = checkpointsDir()) {
            metaDir = new File(dir.toString(), LiveViewCheckpointLayout.META_DIR_NAME);
        }
        final String[] names = metaDir.list();
        Assert.assertNotNull("the view must have sealed a meta/ directory", names);
        Arrays.sort(names);
        final ObjList<Page> pages = new ObjList<>();
        for (int f = 0; f < names.length; f++) {
            final String name = names[f];
            if (!name.startsWith(LiveViewCheckpointLayout.META_SEGMENT_PREFIX)) {
                continue;
            }
            final byte[] bytes = Files.readAllBytes(new File(metaDir, name).toPath());
            Assert.assertEquals(name + " [segment magic]", SEG_MAGIC, leInt(bytes, 0));
            Assert.assertEquals(name + " [segment formatVersion]", 1, leInt(bytes, 4));
            final long segmentId = leLong(bytes, 8);
            Assert.assertEquals(
                    name + ": the segment must name the id its filename does",
                    Long.parseLong(name.substring(2)),
                    segmentId
            );
            final int pageCount = leInt(bytes, 16);
            Assert.assertEquals(name + " [segment header crc]", crc32(bytes, 0, 20), leInt(bytes, 20));

            int offset = SEG_HEADER_SIZE;
            for (int i = 0; i < pageCount; i++) {
                final int payloadLength = leInt(bytes, offset + 4);
                final int kind = leInt(bytes, offset + 8);
                Assert.assertEquals(
                        name + " page " + i + " [crc]",
                        crc32(bytes, offset + 4, PAGE_HEADER_SIZE - 4 + payloadLength),
                        leInt(bytes, offset)
                );
                pages.add(new Page(
                        segmentId,
                        offset,
                        kind,
                        Arrays.copyOfRange(bytes, offset + PAGE_HEADER_SIZE, offset + PAGE_HEADER_SIZE + payloadLength)
                ));
                offset += PAGE_HEADER_SIZE + payloadLength;
            }
            Assert.assertEquals(name + ": the segment must end on its last page", bytes.length, offset);
        }
        Assert.assertTrue("the view must have sealed metadata segments", pages.size() > 0);
        return pages;
    }

    /**
     * The dictionary the newest sealed root addresses, decoded longhand.
     */
    private Directory readNewestDictionary() throws IOException {
        final ObjList<Page> pages = readPages();
        final Page rootPage = newest(pages, PAGE_KIND_CHECKPOINT_ROOT);
        final LiveViewCheckpointPageRef ref =
                readRef(rootPage.payload, 2 * Integer.BYTES + 3 * Long.BYTES + 2 * PAGE_REF_BYTES);
        Assert.assertFalse("the newest root must publish a key dictionary", ref.isNull());
        return decodeDirectory(pages, pageAt(pages, ref));
    }

    /**
     * One column of a decoded directory page.
     */
    private static final class Column {
        final ObjList<LiveViewCheckpointPageRef> chunkRefs = new ObjList<>();
        final ObjList<String> strings = new ObjList<>();
        int baseTableId;
        int baseWriterColumnIndex;
        int columnType;
        String name;
        int symbolCount;

        String[] values() {
            final String[] values = new String[strings.size()];
            for (int i = 0; i < values.length; i++) {
                values[i] = strings.getQuick(i);
            }
            return values;
        }
    }

    /**
     * A decoded directory page, plus the segment pages its chunk references address.
     */
    private static final class Directory {
        final ObjList<Column> columns = new ObjList<>();
        private final ObjList<Page> pages;

        Directory(ObjList<Page> pages) {
            this.pages = pages;
        }

        /**
         * One chunk page, read longhand: formatVersion INT, count INT, then per entry a
         * length INT and that many UTF-8 bytes, in ascending id order.
         */
        String[] chunkValues(LiveViewCheckpointPageRef ref) {
            final Page page = pageAt(pages, ref);
            Assert.assertEquals(
                    "a directory chunk reference must address a chunk page",
                    PAGE_KIND_KEY_DICTIONARY_CHUNK,
                    page.kind
            );
            final byte[] payload = page.payload;
            Assert.assertEquals("key dictionary chunk [formatVersion]", 1, leInt(payload, 0));
            final int count = leInt(payload, 4);
            final String[] values = new String[count];
            int offset = 8;
            for (int i = 0; i < count; i++) {
                final int length = leInt(payload, offset);
                offset += 4;
                values[i] = new String(payload, offset, length, StandardCharsets.UTF_8);
                offset += length;
            }
            Assert.assertEquals("key dictionary chunk [payload consumed]", payload.length, offset);
            return values;
        }
    }

    /**
     * One page of a metadata segment, with its framing already validated and its payload
     * lifted out.
     */
    private static final class Page {
        final int kind;
        final long offset;
        final byte[] payload;
        final long segmentId;

        Page(long segmentId, long offset, int kind, byte[] payload) {
            this.segmentId = segmentId;
            this.offset = offset;
            this.kind = kind;
            this.payload = payload;
        }

        LiveViewCheckpointPageRef ref() {
            return new LiveViewCheckpointPageRef().of(segmentId, offset, payload.length + PAGE_HEADER_SIZE);
        }
    }
}
