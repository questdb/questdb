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
import io.questdb.cairo.lv.LiveViewCheckpointAnchorRoot;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionIdentity;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionRoot;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapReader;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.cairo.lv.LiveViewCheckpointWindowRoot;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.IntToLongFunction;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Wire-format audit of the checkpoint structures PR #6939 reworked, against the bytes an
 * unmodified 10.0.1 build actually wrote.
 * <p>
 * {@link LiveViewCheckpointReleaseCompatTest} proves the composite path: the released tree
 * restores rather than rebuilding. It cannot say <i>why</i> a field is right, and it cannot fail
 * on a field nothing in that view's restore happens to read. This class takes the other half of
 * the question - field order, width, signedness, alignment, optional/null encoding, page-kind
 * dispatch and checksum coverage, one structure at a time.
 * <p>
 * The rule the cases follow is that nothing here may be checked by production code alone. Each
 * page is parsed twice: once by a reader written out longhand below from the 10.0.x layout, with
 * every offset and width as a literal, and once by this branch's own decoder. A decoder that
 * changed a field's offset agrees with itself, so only the second, independent reading can say
 * it disagrees with the release. That is also why the page kinds, the segment magic and the
 * identity magic appear here as numbers rather than as imports of the constants under audit.
 * <p>
 * The structures the audit covers are the pre-existing on-disk shapes this PR touched:
 * <ul>
 *     <li>{@code 0x1a} {@link LiveViewCheckpointRoot} - the boundary root;</li>
 *     <li>{@code 0x1b} {@link LiveViewCheckpointAnchorRoot} - the legacy anchored-window root,
 *     which moved from {@code long[]} to {@code LongList} and gained a decode-buffer pool;</li>
 *     <li>{@code 0x18} {@link LiveViewCheckpointFunctionRoot} - likewise;</li>
 *     <li>{@code 0x19} {@link LiveViewCheckpointFunctionDirectory} - which moved to
 *     {@code ObjList} and retained reference shells;</li>
 *     <li>{@code 0x16} the partition-map leaf, which gained the arena-backed key path;</li>
 *     <li>{@link LiveViewCheckpointFunctionIdentity}, whose encoder was rewritten from
 *     {@code ByteBuffer} to a hand-rolled big-endian one. It is the single highest-risk change
 *     in the set: the identity is the directory's lookup key, so a byte that moved would leave a
 *     freshly compiled function unable to find the root the release wrote for it, with no
 *     corruption anywhere to explain it.</li>
 * </ul>
 * Two shapes the fixture does carry - the {@code 0x11} timeline leaf and the {@code 0x15}
 * segment-directory leaf - are counted by the census case but not decoded field for field: their
 * encoders live in {@code LiveViewCheckpointTimelineNode} and
 * {@code LiveViewCheckpointSegmentDirectoryNode}, which this PR does not touch, so they are
 * outside what the audit is for. The partition map's {@code 0x17} internal node is not in the
 * fixture at all - its trees are one page deep - but it shares its {@code writeTo} with the
 * {@code 0x16} leaf audited here. Neither is the {@code 0x13}/{@code 0x14} row-position delta,
 * whose view would need a ROWS frame; its encoder is untouched by this PR as well. The census
 * case pins all of this, so a regenerated fixture that changed what is present fails loudly
 * rather than turning a case that reads it into a no-op.
 */
public class LiveViewCheckpointWireFormatTest extends AbstractLiveViewTest {

    private static final String FIXTURE_RESOURCE = "/lv/lv_checkpoint_10_0_1.zip";
    // ASCII "LVFI", the first four bytes of an encoded function identity.
    private static final int IDENTITY_MAGIC = 0x4c56_4649;
    // Page framing: crc INT, payloadLength INT, pageKind INT.
    private static final int PAGE_HEADER_SIZE = 12;
    private static final int PAGE_KIND_ANCHOR_ROOT = 0x1b;
    private static final int PAGE_KIND_CHECKPOINT_ROOT = 0x1a;
    private static final int PAGE_KIND_FUNCTION_DIRECTORY = 0x19;
    private static final int PAGE_KIND_FUNCTION_ROOT = 0x18;
    private static final int PAGE_KIND_PARTITION_MAP_LEAF = 0x16;
    private static final int PAGE_KIND_WINDOW_ROOT = 0x1d;
    // A metadata page reference: segmentId LONG, offset LONG, length INT.
    private static final int PAGE_REF_BYTES = 20;
    /**
     * The released view's own SELECT. Compiling it here is what turns the identity case from a
     * self-consistency check into a compatibility one: the strings this branch's compiler feeds
     * the encoder have to be the strings 10.0.1 fed it, or a restored directory lookup misses.
     */
    private static final String RELEASED_VIEW_SQL = "SELECT created_at, account_id, "
            + "sum(amount) OVER w AS cumulative_sum, "
            + "count(account_id) OVER w AS cumulative_count "
            + "FROM tx WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')";
    // Segment header: magic INT, formatVersion INT, segmentId LONG, pageCount INT, headerCrc INT.
    private static final int SEG_HEADER_SIZE = 24;
    // ASCII "LVMS".
    private static final int SEG_MAGIC = 0x4c56_4d53;
    // A state page reference: segmentId LONG, offset LONG, then storedLength, decodedLength,
    // pageKind, codec, rowCount and flags as INTs.
    private static final int STATE_REF_BYTES = 40;

    @Test
    public void testACorruptedReleasedPageIsRejectedRatherThanMisread() throws Exception {
        assertMemoryLeak(() -> {
            unpackFixture();
            final ReleasedPage root = onlyPage(PAGE_KIND_CHECKPOINT_ROOT, 4);

            // The page CRC covers [payloadLength, pageKind, payload]. The kind is the one worth
            // saying out loud: a checksum that had narrowed to the payload alone would let a
            // flipped kind byte hand released bytes to another structure's decoder, which is the
            // exact failure a tagged union makes reachable.
            assertCorruptionRejected(root, PAGE_HEADER_SIZE, "payload", "metadata page checksum mismatch");
            assertCorruptionRejected(root, 8, "page kind field", "metadata page checksum mismatch");
            // The length field is guarded twice - the reference the caller descended through
            // carries the page's total length, and the CRC covers the field as well - so the
            // case pins the rejection rather than which of the two guards answers first.
            assertCorruptionRejected(root, 4, "payload length field", "metadata page");
        });
    }

    @Test
    public void testTheReleasedAnchorMapEntriesDecodeFieldForField() throws Exception {
        assertMemoryLeak(() -> {
            unpackFixture();
            final ObjList<ReleasedPage> pages = releasedPages(PAGE_KIND_PARTITION_MAP_LEAF);
            Assert.assertEquals("the fixture must carry partition map leaves", 12, pages.size());

            try (
                    Path checkpointsDir = checkpointsDir();
                    LiveViewCheckpointPartitionMapReader reader =
                            new LiveViewCheckpointPartitionMapReader(engine.getConfiguration())
            ) {
                reader.of(checkpointsDir);
                for (int p = 0, n = pages.size(); p < n; p++) {
                    final ReleasedPage page = pages.getQuick(p);
                    final byte[] payload = page.payload;
                    final String at = "partition map leaf in segment " + page.segmentId;
                    Assert.assertEquals(at + " [formatVersion]", 1, leInt(payload, 0));
                    final int count = leInt(payload, 4);
                    Assert.assertTrue(at + " [count]", count > 0);

                    // Entry framing: keyLength INT, scalarLength INT, statePageRefCount INT,
                    // then the key bytes, the scalar bytes and the references, in that order.
                    final byte[][] keys = new byte[count][];
                    final byte[][] scalars = new byte[count][];
                    final int[][] refs = new int[count][];
                    int offset = 8;
                    for (int i = 0; i < count; i++) {
                        final int keyLength = leInt(payload, offset);
                        final int scalarLength = leInt(payload, offset + 4);
                        final int refCount = leInt(payload, offset + 8);
                        offset += 12;
                        keys[i] = Arrays.copyOfRange(payload, offset, offset + keyLength);
                        offset += keyLength;
                        scalars[i] = Arrays.copyOfRange(payload, offset, offset + scalarLength);
                        offset += scalarLength;
                        refs[i] = new int[refCount];
                        for (int r = 0; r < refCount; r++) {
                            refs[i][r] = offset;
                            offset += STATE_REF_BYTES;
                        }
                    }
                    Assert.assertEquals(at + " [payload consumed]", payload.length, offset);

                    final int[] seen = {0};
                    reader.iterateAll(page.ref(), entry -> {
                        final int i = seen[0]++;
                        Assert.assertArrayEquals(at + " entry " + i + " [key]", keys[i], entry.getKey());
                        Assert.assertArrayEquals(at + " entry " + i + " [scalar]", scalars[i], entry.getScalarState());
                        Assert.assertEquals(
                                at + " entry " + i + " [statePageCount]",
                                refs[i].length,
                                entry.getStatePageCount()
                        );
                        for (int r = 0; r < refs[i].length; r++) {
                            assertStateRef(at + " entry " + i + " ref " + r, payload, refs[i][r], entry.getStatePageRef(r));
                        }
                    });
                    Assert.assertEquals(at + " [entry count]", count, seen[0]);
                }
            }
        });
    }

    @Test
    public void testTheReleasedAnchorRootsDecodeFieldForFieldAndNoFusedDecoderClaimsThem() throws Exception {
        assertMemoryLeak(() -> {
            unpackFixture();
            final ObjList<ReleasedPage> pages = releasedPages(PAGE_KIND_ANCHOR_ROOT);
            Assert.assertEquals("the fixture must carry an anchor root per boundary", 5, pages.size());

            try (
                    Path checkpointsDir = checkpointsDir();
                    LiveViewCheckpointAnchorRoot anchorRoot =
                            new LiveViewCheckpointAnchorRoot(engine.getConfiguration());
                    LiveViewCheckpointWindowRoot windowRoot =
                            new LiveViewCheckpointWindowRoot(engine.getConfiguration());
                    LiveViewCheckpointPartitionMapReader maps =
                            new LiveViewCheckpointPartitionMapReader(engine.getConfiguration())
            ) {
                maps.of(checkpointsDir);
                for (int p = 0, n = pages.size(); p < n; p++) {
                    final ReleasedPage page = pages.getQuick(p);
                    final byte[] payload = page.payload;
                    final String at = "anchor root in segment " + page.segmentId;

                    // formatVersion INT, anchorValueType INT, windowNameLength INT,
                    // keySchemaLength INT, segmentCount INT, partitionMapRootRef, then the
                    // window name, the key schema and (segmentId, useCount) LONG pairs.
                    Assert.assertEquals(at + " [formatVersion]", 1, leInt(payload, 0));
                    final int anchorValueType = leInt(payload, 4);
                    final int windowNameLength = leInt(payload, 8);
                    final int keySchemaLength = leInt(payload, 12);
                    final int segmentCount = leInt(payload, 16);
                    final int fixedSize = 5 * Integer.BYTES + PAGE_REF_BYTES;
                    Assert.assertEquals(
                            at + " [payload length]",
                            fixedSize + windowNameLength + keySchemaLength + segmentCount * 16,
                            payload.length
                    );
                    final byte[] windowName = Arrays.copyOfRange(payload, fixedSize, fixedSize + windowNameLength);
                    final byte[] keySchema = Arrays.copyOfRange(
                            payload,
                            fixedSize + windowNameLength,
                            fixedSize + windowNameLength + keySchemaLength
                    );

                    // The anchor value is a timestamp and the partition key a projected SYMBOL,
                    // which the projector writes in the STRING space. Both travel as raw
                    // ColumnType ids, so a renumbering of either would land here.
                    Assert.assertEquals(at + " [anchorValueType]", ColumnType.TIMESTAMP, anchorValueType);
                    Assert.assertEquals(at + " [windowName]", "w", new String(windowName, StandardCharsets.UTF_8));
                    // The key schema is big-endian - it is built by the identity encoder, not by
                    // the page writer - and reads count INT then one ColumnType INT per key.
                    Assert.assertEquals(at + " [keySchema columnCount]", 1, beInt(keySchema, 0));
                    Assert.assertEquals(at + " [keySchema columnType]", ColumnType.STRING, beInt(keySchema, 4));

                    anchorRoot.of(checkpointsDir, page.ref());
                    Assert.assertEquals(at + " [anchorValueType]", anchorValueType, anchorRoot.getAnchorValueType());
                    Assert.assertArrayEquals(at + " [windowName]", windowName, anchorRoot.getWindowName());
                    Assert.assertArrayEquals(at + " [keySchema]", keySchema, anchorRoot.getKeySchema());
                    final LiveViewCheckpointPageRef mapRef = new LiveViewCheckpointPageRef();
                    anchorRoot.getPartitionMapRootRef(mapRef);
                    assertPageRef(at + " [partitionMapRootRef]", payload, 20, mapRef);
                    assertSegmentUseCounts(
                            at,
                            payload,
                            fixedSize + windowNameLength + keySchemaLength,
                            segmentCount,
                            anchorRoot.getSegmentUseCountSize(),
                            anchorRoot::getSegmentId,
                            anchorRoot::getSegmentUseCount
                    );

                    // The fused root stands in the same slot, so the two are a tagged union read
                    // by page kind. A probe that claimed a released anchor root would restore it
                    // as the wrong shape rather than fail.
                    Assert.assertFalse(
                            at + ": the fused window-root probe must decline a legacy anchor root",
                            windowRoot.ofIfWindowRoot(checkpointsDir, page.ref())
                    );

                    // The anchor value itself: eight little-endian bytes of scalar state, and no
                    // data page. A misread here restores a partition to the wrong bucket.
                    final int[] entries = {0};
                    maps.iterateAll(mapRef, entry -> {
                        entries[0]++;
                        Assert.assertEquals(at + " [anchor entry state pages]", 0, entry.getStatePageCount());
                        Assert.assertEquals(at + " [anchor entry scalar length]", 8, entry.getScalarState().length);
                        Assert.assertEquals(
                                at + " [anchor value]",
                                leLong(entry.getScalarState(), 0),
                                LiveViewCheckpointAnchorRoot.readAnchorValue(entry)
                        );
                    });
                    Assert.assertTrue(at + " [anchor map is not empty]", entries[0] > 0);
                }
                anchorRoot.detach();
                windowRoot.detach();
            }
        });
    }

    @Test
    public void testTheReleasedCheckpointRootsDecodeFieldForField() throws Exception {
        assertMemoryLeak(() -> {
            unpackFixture();
            final ObjList<ReleasedPage> pages = releasedPages(PAGE_KIND_CHECKPOINT_ROOT);
            Assert.assertEquals("the fixture must carry a checkpoint root per boundary", 5, pages.size());

            try (
                    Path checkpointsDir = checkpointsDir();
                    LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(engine.getConfiguration())
            ) {
                for (int p = 0, n = pages.size(); p < n; p++) {
                    final ReleasedPage page = pages.getQuick(p);
                    final byte[] payload = page.payload;
                    final String at = "checkpoint root in segment " + page.segmentId;

                    // formatVersion INT, segmentCount INT, checkpointId LONG, maxTimestamp LONG,
                    // definitionTxn LONG, stateRootRef, functionDirectoryRef, segmentIds LONG[].
                    Assert.assertEquals(at + " [formatVersion]", 1, leInt(payload, 0));
                    final int segmentCount = leInt(payload, 4);
                    final int fixedSize = 2 * Integer.BYTES + 3 * Long.BYTES + 2 * PAGE_REF_BYTES;
                    Assert.assertEquals(
                            at + " [payload length]",
                            fixedSize + segmentCount * Long.BYTES,
                            payload.length
                    );

                    root.of(checkpointsDir, page.ref());
                    Assert.assertEquals(at + " [checkpointId]", leLong(payload, 8), root.getCheckpointId());
                    Assert.assertEquals(at + " [maxTimestamp]", leLong(payload, 16), root.getMaxTimestamp());
                    Assert.assertEquals(at + " [definitionTxn]", leLong(payload, 24), root.getDefinitionTxn());

                    final LiveViewCheckpointPageRef stateRootRef = new LiveViewCheckpointPageRef();
                    root.getStateRootRef(stateRootRef);
                    assertPageRef(at + " [stateRootRef]", payload, 32, stateRootRef);
                    final LiveViewCheckpointPageRef functionDirectoryRef = new LiveViewCheckpointPageRef();
                    root.getFunctionDirectoryRef(functionDirectoryRef);
                    assertPageRef(at + " [functionDirectoryRef]", payload, 52, functionDirectoryRef);

                    Assert.assertEquals(at + " [segmentIdCount]", segmentCount, root.getSegmentIdCount());
                    for (int i = 0; i < segmentCount; i++) {
                        Assert.assertEquals(
                                at + " [segmentId " + i + ']',
                                leLong(payload, fixedSize + i * Long.BYTES),
                                root.getSegmentId(i)
                        );
                    }
                }
                root.detach();
            }
        });
    }

    @Test
    public void testTheReleasedFunctionDirectoriesStillLocateEveryFunction() throws Exception {
        assertMemoryLeak(() -> {
            unpackFixture();
            final ObjList<ReleasedPage> pages = releasedPages(PAGE_KIND_FUNCTION_DIRECTORY);
            Assert.assertEquals("the fixture must carry a function directory per boundary", 5, pages.size());

            try (
                    Path checkpointsDir = checkpointsDir();
                    LiveViewCheckpointFunctionDirectory directory =
                            new LiveViewCheckpointFunctionDirectory(engine.getConfiguration())
            ) {
                for (int p = 0, n = pages.size(); p < n; p++) {
                    final ReleasedPage page = pages.getQuick(p);
                    final byte[] payload = page.payload;
                    final String at = "function directory in segment " + page.segmentId;

                    // formatVersion INT, count INT, then per entry: identityLength INT, the
                    // identity bytes, and the function root reference.
                    Assert.assertEquals(at + " [formatVersion]", 1, leInt(payload, 0));
                    final int count = leInt(payload, 4);
                    Assert.assertEquals(at + " [count]", 2, count);

                    final byte[][] identities = new byte[count][];
                    final int[] refOffsets = new int[count];
                    int offset = 8;
                    for (int i = 0; i < count; i++) {
                        final int identityLength = leInt(payload, offset);
                        offset += Integer.BYTES;
                        identities[i] = Arrays.copyOfRange(payload, offset, offset + identityLength);
                        offset += identityLength;
                        refOffsets[i] = offset;
                        offset += PAGE_REF_BYTES;
                    }
                    Assert.assertEquals(at + " [payload consumed]", payload.length, offset);

                    directory.of(checkpointsDir, page.ref());
                    Assert.assertEquals(at + " [size]", count, directory.size());
                    final LiveViewCheckpointPageRef out = new LiveViewCheckpointPageRef();
                    for (int i = 0; i < count; i++) {
                        directory.getRootRef(i, out);
                        assertPageRef(at + " [rootRef " + i + ']', payload, refOffsets[i], out);

                        // The directory is a sorted array searched by identity bytes, so the
                        // lookup is part of the wire contract rather than a convenience over it.
                        out.clear();
                        Assert.assertTrue(
                                at + ": the directory must find entry " + i + " by its stored identity",
                                directory.find(identities[i], out)
                        );
                        assertPageRef(at + " [find " + i + ']', payload, refOffsets[i], out);
                    }
                }
            }
        });
    }

    @Test
    public void testTheReleasedFunctionRootsDecodeFieldForField() throws Exception {
        assertMemoryLeak(() -> {
            unpackFixture();
            final ObjList<ReleasedPage> pages = releasedPages(PAGE_KIND_FUNCTION_ROOT);
            Assert.assertEquals("the fixture must carry two function roots per boundary", 10, pages.size());

            try (
                    Path checkpointsDir = checkpointsDir();
                    LiveViewCheckpointFunctionRoot functionRoot =
                            new LiveViewCheckpointFunctionRoot(engine.getConfiguration())
            ) {
                for (int p = 0, n = pages.size(); p < n; p++) {
                    final ReleasedPage page = pages.getQuick(p);
                    final byte[] payload = page.payload;
                    final String at = "function root in segment " + page.segmentId;

                    // formatVersion INT, stateFormatVersion INT, identityLength INT,
                    // keySchemaLength INT, segmentCount INT, scalarStateRef, partitionMapRootRef,
                    // then the identity, the key schema and (segmentId, useCount) LONG pairs.
                    Assert.assertEquals(at + " [formatVersion]", 1, leInt(payload, 0));
                    final int stateFormatVersion = leInt(payload, 4);
                    final int identityLength = leInt(payload, 8);
                    final int keySchemaLength = leInt(payload, 12);
                    final int segmentCount = leInt(payload, 16);
                    final int fixedSize = 5 * Integer.BYTES + STATE_REF_BYTES + PAGE_REF_BYTES;
                    Assert.assertEquals(
                            at + " [payload length]",
                            fixedSize + identityLength + keySchemaLength + segmentCount * 16,
                            payload.length
                    );
                    final byte[] identity = Arrays.copyOfRange(payload, fixedSize, fixedSize + identityLength);
                    final byte[] keySchema = Arrays.copyOfRange(
                            payload,
                            fixedSize + identityLength,
                            fixedSize + identityLength + keySchemaLength
                    );

                    functionRoot.of(checkpointsDir, page.ref());
                    Assert.assertEquals(
                            at + " [stateFormatVersion]",
                            stateFormatVersion,
                            functionRoot.getStateFormatVersion()
                    );
                    Assert.assertArrayEquals(at + " [functionIdentity]", identity, functionRoot.getFunctionIdentity());
                    Assert.assertArrayEquals(at + " [keySchema]", keySchema, functionRoot.getKeySchema());

                    // These functions keep their state per partition, so the scalar reference is
                    // the null one: segmentId -1 with every other field zero. Null encoding is
                    // its own compatibility surface - a decoder that read it as a real page
                    // would chase a segment that never existed.
                    final LiveViewCheckpointStatePageRef scalarRef = new LiveViewCheckpointStatePageRef();
                    functionRoot.getScalarStateRef(scalarRef);
                    assertStateRef(at + " [scalarStateRef]", payload, 20, scalarRef);
                    Assert.assertTrue(at + " [scalarStateRef is null]", scalarRef.isNull());

                    final LiveViewCheckpointPageRef mapRef = new LiveViewCheckpointPageRef();
                    functionRoot.getPartitionMapRootRef(mapRef);
                    assertPageRef(at + " [partitionMapRootRef]", payload, 20 + STATE_REF_BYTES, mapRef);
                    assertSegmentUseCounts(
                            at,
                            payload,
                            fixedSize + identityLength + keySchemaLength,
                            segmentCount,
                            functionRoot.getSegmentUseCountSize(),
                            functionRoot::getSegmentId,
                            functionRoot::getSegmentUseCount
                    );
                }
                functionRoot.detach();
            }
        });
    }

    @Test
    public void testTheReleasedSegmentFramingIsTheFramingThisBranchStillWrites() throws Exception {
        assertMemoryLeak(() -> {
            unpackFixture();

            // readReleasedPages() has already checked every segment header and every page CRC on
            // its way through; what is left is to state which shapes the release actually wrote,
            // so the audit's coverage is visible rather than implied. A regenerated fixture that
            // dropped one of these would otherwise turn the case that reads it into a no-op.
            final TreeMap<Integer, Integer> census = new TreeMap<>();
            final ObjList<ReleasedPage> pages = readReleasedPages();
            for (int i = 0, n = pages.size(); i < n; i++) {
                census.merge(pages.getQuick(i).kind, 1, Integer::sum);
            }
            final StringBuilder sink = new StringBuilder();
            for (Map.Entry<Integer, Integer> entry : census.entrySet()) {
                sink.append("0x").append(Integer.toHexString(entry.getKey()))
                        .append('=').append(entry.getValue()).append('\n');
            }
            TestUtils.assertEquals(
                    "0x11=2\n"   // timeline leaf
                            + "0x15=2\n"   // segment directory leaf
                            + "0x16=12\n"  // partition map leaf
                            + "0x18=10\n"  // function root
                            + "0x19=5\n"   // function directory
                            + "0x1a=5\n"   // checkpoint root
                            + "0x1b=5\n",  // anchor root
                    sink
            );
            Assert.assertFalse(
                    "10.0.1 cannot have written a fused window root; a fixture that contains one "
                            + "was not produced by the release",
                    census.containsKey(PAGE_KIND_WINDOW_ROOT)
            );
        });
    }

    @Test
    public void testThisBranchStillProducesTheIdentityBytesTheReleasedDirectoryStores() throws Exception {
        assertMemoryLeak(() -> {
            unpackFixture();
            final ReleasedPage directoryPage = onlyPage(PAGE_KIND_FUNCTION_DIRECTORY, 32);
            final byte[] payload = directoryPage.payload;
            final int count = leInt(payload, 4);
            final byte[][] released = new byte[count][];
            int offset = 8;
            for (int i = 0; i < count; i++) {
                final int identityLength = leInt(payload, offset);
                offset += Integer.BYTES;
                released[i] = Arrays.copyOfRange(payload, offset, offset + identityLength);
                offset += identityLength + PAGE_REF_BYTES;
            }

            // The identity is length-delimited big-endian: magic, formatVersion, outputPosition,
            // then five UTF-8 fields, each preceded by its own length. Reading it back out and
            // re-encoding it through this branch is the encoder half of the audit - the rewrite
            // from ByteBuffer to a hand-rolled writer had to keep the endianness ByteBuffer
            // defaults to, and the '?' a malformed surrogate collapses to.
            for (int i = 0; i < count; i++) {
                final byte[] blob = released[i];
                final String at = "released identity " + i;
                Assert.assertEquals(at + " [magic]", IDENTITY_MAGIC, beInt(blob, 0));
                Assert.assertEquals(at + " [formatVersion]", 1, beInt(blob, 4));
                final int outputPosition = beInt(blob, 8);
                final int[] cursor = {12};
                final String canonicalWindowName = readField(blob, cursor);
                final String factorySignature = readField(blob, cursor);
                final String partitionSignature = readField(blob, cursor);
                final String orderSignature = readField(blob, cursor);
                final String stateCodecIdentity = readField(blob, cursor);
                Assert.assertEquals(at + " [fields consumed]", blob.length, cursor[0]);

                final byte[] reEncoded = new LiveViewCheckpointFunctionIdentity(
                        canonicalWindowName,
                        factorySignature,
                        outputPosition,
                        partitionSignature,
                        orderSignature,
                        stateCodecIdentity
                ).getEncoded();
                Assert.assertArrayEquals(
                        at + ": this branch must re-encode the released identity byte for byte",
                        blob,
                        reEncoded
                );
            }

            // And the producer half. The encoder can be faithful while the compiler feeds it
            // different strings, which breaks a restore just as completely and shows up as a
            // directory miss rather than as corruption.
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                    + "TIMESTAMP(created_at) PARTITION BY HOUR WAL");
            final ObjList<byte[]> compiled = compileIdentities();
            Assert.assertEquals("the released view compiles to two checkpointed functions", count, compiled.size());

            try (
                    Path checkpointsDir = checkpointsDir();
                    LiveViewCheckpointFunctionDirectory directory =
                            new LiveViewCheckpointFunctionDirectory(engine.getConfiguration())
            ) {
                directory.of(checkpointsDir, directoryPage.ref());
                final LiveViewCheckpointPageRef out = new LiveViewCheckpointPageRef();
                for (int i = 0, n = compiled.size(); i < n; i++) {
                    final byte[] identity = compiled.getQuick(i);
                    Assert.assertTrue(
                            "a function compiled by this branch must find the root 10.0.1 wrote for it, "
                                    + "identity=" + describe(identity),
                            directory.find(identity, out)
                    );
                }
            }
        });
    }

    private static void assertPageRef(String at, byte[] payload, int offset, LiveViewCheckpointPageRef ref) {
        Assert.assertEquals(at + " [segmentId]", leLong(payload, offset), ref.getSegmentId());
        Assert.assertEquals(at + " [offset]", leLong(payload, offset + Long.BYTES), ref.getOffset());
        Assert.assertEquals(at + " [length]", leInt(payload, offset + 2 * Long.BYTES), ref.getLength());
    }

    private static void assertSegmentUseCounts(
            String at,
            byte[] payload,
            int offset,
            int segmentCount,
            int decodedCount,
            IntToLongFunction segmentId,
            IntToLongFunction useCount
    ) {
        Assert.assertEquals(at + " [segmentUseCountSize]", segmentCount, decodedCount);
        for (int i = 0; i < segmentCount; i++) {
            Assert.assertEquals(
                    at + " [segmentId " + i + ']',
                    leLong(payload, offset + i * 16),
                    segmentId.applyAsLong(i)
            );
            Assert.assertEquals(
                    at + " [segmentUseCount " + i + ']',
                    leLong(payload, offset + i * 16 + Long.BYTES),
                    useCount.applyAsLong(i)
            );
        }
    }

    private static void assertStateRef(String at, byte[] payload, int offset, LiveViewCheckpointStatePageRef ref) {
        Assert.assertEquals(at + " [segmentId]", leLong(payload, offset), ref.getSegmentId());
        Assert.assertEquals(at + " [offset]", leLong(payload, offset + Long.BYTES), ref.getOffset());
        Assert.assertEquals(at + " [storedLength]", leInt(payload, offset + 16), ref.getStoredLength());
        Assert.assertEquals(at + " [decodedLength]", leInt(payload, offset + 20), ref.getDecodedLength());
        Assert.assertEquals(at + " [pageKind]", leInt(payload, offset + 24), ref.getPageKind());
        Assert.assertEquals(at + " [codec]", leInt(payload, offset + 28), ref.getCodec());
        Assert.assertEquals(at + " [rowCount]", leInt(payload, offset + 32), ref.getRowCount());
        Assert.assertEquals(at + " [flags]", leInt(payload, offset + 36), ref.getFlags());
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

    private static String describe(byte[] identity) {
        final int[] cursor = {12};
        return readField(identity, cursor) + '/' + readField(identity, cursor);
    }

    private static File fixtureDir() {
        return new File(temp.getRoot(), "released-checkpoints");
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

    private static String readField(byte[] bytes, int[] cursor) {
        final int length = beInt(bytes, cursor[0]);
        cursor[0] += Integer.BYTES;
        final String value = new String(bytes, cursor[0], length, StandardCharsets.UTF_8);
        cursor[0] += length;
        return value;
    }

    /**
     * Parses every metadata segment the fixture carries, checking the segment header, its
     * self-checksum and each page's own CRC on the way, and returns the pages in file order.
     * Everything here is written out longhand from the 10.0.x framing rather than through
     * {@code LiveViewCheckpointMetaSegmentReader}, which is the point: the reader under audit
     * cannot be the witness for its own framing.
     */
    private static ObjList<ReleasedPage> readReleasedPages() throws IOException {
        final File metaDir = new File(fixtureDir(), "meta");
        final String[] names = metaDir.list();
        Assert.assertNotNull("the fixture must carry a meta/ directory", names);
        Arrays.sort(names);
        final ObjList<ReleasedPage> pages = new ObjList<>();
        for (int f = 0; f < names.length; f++) {
            final String name = names[f];
            if (!name.startsWith("m.")) {
                continue;
            }
            final byte[] bytes = Files.readAllBytes(new File(metaDir, name).toPath());
            // magic INT, formatVersion INT, segmentId LONG, pageCount INT, headerCrc INT.
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
                // crc INT, payloadLength INT, pageKind INT, payload.
                final int payloadLength = leInt(bytes, offset + 4);
                final int kind = leInt(bytes, offset + 8);
                Assert.assertEquals(
                        name + " page " + i + " [crc]",
                        crc32(bytes, offset + 4, PAGE_HEADER_SIZE - 4 + payloadLength),
                        leInt(bytes, offset)
                );
                pages.add(new ReleasedPage(
                        segmentId,
                        offset,
                        kind,
                        Arrays.copyOfRange(bytes, offset + PAGE_HEADER_SIZE, offset + PAGE_HEADER_SIZE + payloadLength)
                ));
                offset += PAGE_HEADER_SIZE + payloadLength;
            }
            Assert.assertEquals(name + ": the segment must end on its last page", bytes.length, offset);
        }
        Assert.assertTrue("the fixture must carry metadata segments", pages.size() > 0);
        return pages;
    }

    private static String segmentFileName(long segmentId) {
        final StringBuilder sink = new StringBuilder("m.");
        final String digits = Long.toString(segmentId);
        for (int i = digits.length(); i < 16; i++) {
            sink.append('0');
        }
        return sink.append(digits).toString();
    }

    /**
     * Copies the fixture's whole {@code _checkpoints} tree out of the zip, under the temporary
     * folder rather than under the database root: the cases here read the released bytes
     * directly and never register the view, so nothing has to persuade the engine that a second
     * database root exists.
     */
    private static void unpackFixture() throws IOException {
        final File target = fixtureDir();
        TestUtils.removeTestPath(target.getAbsolutePath());
        final byte[] buffer = new byte[1024 * 1024];
        try (InputStream is = LiveViewCheckpointWireFormatTest.class.getResourceAsStream(FIXTURE_RESOURCE)) {
            Assert.assertNotNull("missing fixture resource " + FIXTURE_RESOURCE, is);
            try (ZipInputStream zip = new ZipInputStream(is)) {
                ZipEntry entry;
                while ((entry = zip.getNextEntry()) != null) {
                    final int at = entry.getName().indexOf("/_checkpoints/");
                    if (!entry.isDirectory() && at > -1) {
                        final File dest = new File(target, entry.getName().substring(at + "/_checkpoints/".length()));
                        final File parent = dest.getParentFile();
                        Assert.assertTrue("cannot create " + parent, parent.isDirectory() || parent.mkdirs());
                        try (OutputStream os = new FileOutputStream(dest)) {
                            int read;
                            while ((read = zip.read(buffer)) > 0) {
                                os.write(buffer, 0, read);
                            }
                        }
                    }
                    zip.closeEntry();
                }
            }
        }
        Assert.assertTrue("the fixture must carry a _checkpoints tree", new File(target, "meta").isDirectory());
    }

    /**
     * Flips one bit at {@code pageOffset + fieldOffset} in the segment the page lives in and
     * asserts the decoder rejects the page rather than decoding the altered bytes.
     */
    private void assertCorruptionRejected(
            ReleasedPage page,
            int fieldOffset,
            String field,
            String expectedMessage
    ) throws IOException {
        final File corruptDir = new File(temp.getRoot(), "corrupt-checkpoints");
        TestUtils.removeTestPath(corruptDir.getAbsolutePath());
        final File metaDir = new File(corruptDir, "meta");
        Assert.assertTrue("cannot create " + metaDir, metaDir.mkdirs());
        final String name = segmentFileName(page.segmentId);
        final byte[] bytes = Files.readAllBytes(new File(new File(fixtureDir(), "meta"), name).toPath());
        bytes[(int) page.offset + fieldOffset] ^= 1;
        Files.write(new File(metaDir, name).toPath(), bytes);

        try (
                Path dir = new Path().of(corruptDir.getAbsolutePath());
                LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(engine.getConfiguration())
        ) {
            try {
                root.of(dir, page.ref());
                Assert.fail("a flipped " + field + " must not decode");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
            } finally {
                root.detach();
            }
        }
        TestUtils.removeTestPath(corruptDir.getAbsolutePath());
    }

    private Path checkpointsDir() {
        return new Path().of(fixtureDir().getAbsolutePath());
    }

    /**
     * Compiles the released view's SELECT on this branch and returns the encoded identity of
     * every checkpointed window function it produces, in factory order.
     */
    private ObjList<byte[]> compileIdentities() throws Exception {
        final ObjList<byte[]> identities = new ObjList<>();
        sqlExecutionContext.setLiveViewCompile(true);
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = select(compiler, RELEASED_VIEW_SQL, sqlExecutionContext)
        ) {
            RecordCursorFactory root = factory;
            while (root instanceof QueryProgress) {
                root = root.getBaseFactory();
            }
            Assert.assertTrue(
                    "the released view must compile to a window factory",
                    root instanceof WindowRecordCursorFactory
            );
            final ObjList<WindowFunction> functions = ((WindowRecordCursorFactory) root).getWindowFunctions();
            for (int i = 0, n = functions.size(); i < n; i++) {
                final LiveViewCheckpointFunctionIdentity identity = functions.getQuick(i).checkpointFunctionIdentity();
                Assert.assertNotNull("every function of the released view is checkpointed", identity);
                identities.add(identity.getEncoded());
            }
        } finally {
            sqlExecutionContext.setLiveViewCompile(false);
        }
        return identities;
    }

    private ReleasedPage onlyPage(int kind, long segmentId) throws IOException {
        final ObjList<ReleasedPage> pages = releasedPages(kind);
        for (int i = 0, n = pages.size(); i < n; i++) {
            if (pages.getQuick(i).segmentId == segmentId) {
                return pages.getQuick(i);
            }
        }
        throw new AssertionError("no 0x" + Integer.toHexString(kind) + " page in segment " + segmentId);
    }

    private ObjList<ReleasedPage> releasedPages(int kind) throws IOException {
        final ObjList<ReleasedPage> all = readReleasedPages();
        final ObjList<ReleasedPage> selected = new ObjList<>();
        for (int i = 0, n = all.size(); i < n; i++) {
            if (all.getQuick(i).kind == kind) {
                selected.add(all.getQuick(i));
            }
        }
        Assert.assertTrue("the fixture carries no 0x" + Integer.toHexString(kind) + " page", selected.size() > 0);
        return selected;
    }

    /**
     * One page of a released metadata segment, with its framing already validated and its
     * payload lifted out.
     */
    private static final class ReleasedPage {
        final int kind;
        final long offset;
        final byte[] payload;
        final long segmentId;

        ReleasedPage(long segmentId, long offset, int kind, byte[] payload) {
            this.segmentId = segmentId;
            this.offset = offset;
            this.kind = kind;
            this.payload = payload;
        }

        LiveViewCheckpointPageRef ref() {
            return new LiveViewCheckpointPageRef()
                    .of(segmentId, offset, payload.length + PAGE_HEADER_SIZE);
        }
    }
}
