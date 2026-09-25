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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionRootBuilder;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMutationArena;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapEntry;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapReader;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapWriter;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.cairo.lv.LiveViewSnapshotKeyCodec;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Pins the raw metadata-segment bytes a partition map is persisted as. The keys are the
 * encodings {@link LiveViewSnapshotKeyCodec#writeKey} produces for the shapes a view keys
 * on: ASCII and non-ASCII STRING values, a null and an empty STRING, and a multi-column
 * key mixing fixed-width columns with a STRING. The expectations were captured from the
 * build that still carried partition keys as heap arrays, before they moved to native
 * memory, so any change to how a key is staged, ordered, decoded or written back shows up
 * here as a byte difference rather than as a lookup that silently misses.
 * <p>
 * Each case also reads its map back through the reader, so the bytes are checked both as
 * written and as a lookup and an iteration decode them.
 */
public class LiveViewCheckpointPartitionMapGoldenBytesTest extends AbstractCairoTest {

    private static final String LV_DIR = "lv_partition_map_golden";
    // A single leaf holding the six STRING keys of SMALL_LEAF_VALUES, no state pages: the
    // segment header, the leaf page header and every entry's length, key and scalar bytes.
    // Keys sort unsigned byte by byte, so the null STRING's -1 length sorts last.
    private static final String SMALL_LEAF_SEGMENT_HEX = ""
            // segment header: magic, format version, segment id 41, page count 1, header CRC
            + "534d564c" + "01000000" + "2900000000000000" + "01000000" + "a62b21ce"
            // leaf page header: CRC, payload length 148, page kind 0x16
            + "8183c14e" + "94000000" + "16000000"
            // payload: format version 1, entry count 6
            + "01000000" + "06000000"
            // entries: key length, scalar length, reference count, key, scalar
            + "04000000" + "04000000" + "00000000" + "00000000" + "00000001" // ""
            + "06000000" + "04000000" + "00000000" + "010000006100" + "00000005" // "a"
            + "06000000" + "04000000" + "00000000" + "010000006200" + "00000000" // "b"
            + "08000000" + "04000000" + "00000000" + "020000007167ac4e" + "00000004" // "東京"
            + "10000000" + "04000000" + "00000000" + "060000007a00fc007200690063006800" + "00000003" // "zürich"
            + "04000000" + "04000000" + "00000000" + "ffffffff" + "00000002"; // null
    private static final String[] SMALL_LEAF_VALUES = {"b", "", null, "zürich", "東京", "a"};
    private static final String STRING_MAP_FIRST_DIGEST = "77123800bf613273305905c28029b7cda1cff1f7021565896a7d12569ee41a5a";
    private static final String STRING_MAP_SECOND_DIGEST = "6b94575683b4fdd0120a00db13516a208022f1d53a00e83df47f90542b4b563d";
    // STRING keys ordered neither by value nor by encoding, so the build has to sort them.
    private static final String[] STRING_VALUES = {
            "alpha", "", "Zürich", "zürich", "東京", "東京都", "\uD83D\uDE00", null,
            "\u00ff", "\u0100", "\uffff", "a", "b", "alphabet", "Alpha", "the quick brown fox jumps over the lazy dog"
    };
    private static final String TUPLE_ROOT_FIRST_DIGEST = "7152036cb36077ead429158969abb30b11434f9b9fb9af631bd3a2ce3113dbd8";
    private static final String TUPLE_ROOT_SECOND_DIGEST = "291c8288c47725cc41f45e0f32a4cbdc2afce09ac7b12ea503937f3721e44f0e";

    @Before
    public void setUp() {
        super.setUp();
        try (Path path = new Path()) {
            checkpointsDir(path).concat(LiveViewCheckpointLayout.META_DIR_NAME).slash();
            configuration.getFilesFacade().mkdirs(path, configuration.getMkDirMode());
        }
    }

    @Test
    public void testMultiColumnKeyFunctionRootBytes() throws Exception {
        assertMemoryLeak(() -> {
            final ArrayColumnTypes types = new ArrayColumnTypes()
                    .add(ColumnType.INT)
                    .add(ColumnType.STRING)
                    .add(ColumnType.LONG)
                    .add(ColumnType.DOUBLE)
                    .add(ColumnType.BOOLEAN)
                    .add(ColumnType.CHAR)
                    .add(ColumnType.SHORT)
                    .add(ColumnType.BYTE);
            final byte[][] keys = encodeKeys(types, new Record[]{
                    new KeyRecord(1, "x", 1L, 1.5, true, 'a', (short) 1, (byte) 1),
                    new KeyRecord(-1, "x", 1L, 1.5, true, 'a', (short) 1, (byte) 1),
                    new KeyRecord(Integer.MIN_VALUE, null, Long.MIN_VALUE, Double.NaN, false, '\u0000', (short) -1, (byte) -128),
                    new KeyRecord(0, "πρ", Long.MAX_VALUE, -0.0, true, '\uffff', Short.MAX_VALUE, (byte) 127),
                    new KeyRecord(256, "", 0L, Double.MIN_VALUE, false, 'Z', (short) 0, (byte) 0),
                    new KeyRecord(65_536, "東京", -1L, Double.NEGATIVE_INFINITY, true, 'é', Short.MIN_VALUE, (byte) 7),
                    new KeyRecord(1, "x", 1L, 1.5, true, 'a', (short) 1, (byte) 2),
                    new KeyRecord(1, "xx", 1L, 1.5, true, 'a', (short) 1, (byte) 1)
            });
            final byte[] keySchema = {1, 2, 3, 4};
            final byte[] identity = {'g', 'o', 'l', 'd'};
            final LiveViewCheckpointPageRef first = new LiveViewCheckpointPageRef();
            final LiveViewCheckpointPageRef second = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointFunctionRootBuilder builder = new LiveViewCheckpointFunctionRootBuilder(configuration);
                 Path dir = new Path()) {
                builder.of(checkpointsDir(dir), new LiveViewCheckpointPageRef(), identity, 1, keySchema);
                for (int i = 0; i < keys.length; i++) {
                    LiveViewCheckpointTestKeys.putPartition(builder, keys[i], scalar(i), refs(i, i % 3));
                }
                builder.build(31, first);

                builder.of(checkpointsDir(dir), first, identity, 1, keySchema);
                LiveViewCheckpointTestKeys.putPartition(builder, keys[3], scalar(103), refs(103, 2));
                LiveViewCheckpointTestKeys.putPartition(builder, keys[0], scalar(100), refs(100, 0));
                LiveViewCheckpointTestKeys.removePartition(builder, keys[5]);
                LiveViewCheckpointTestKeys.removePartition(builder, keys[2]);
                LiveViewCheckpointTestKeys.putPartition(builder, keys[6], scalar(6), refs(6, 0));
                builder.build(32, second);
            }
            assertSegmentDigest(31, TUPLE_ROOT_FIRST_DIGEST);
            assertSegmentDigest(32, TUPLE_ROOT_SECOND_DIGEST);
        });
    }

    @Test
    public void testSmallStringLeafBytes() throws Exception {
        assertMemoryLeak(() -> {
            final ArrayColumnTypes types = new ArrayColumnTypes().add(ColumnType.STRING);
            final byte[][] keys = encodeStringKeys(types, SMALL_LEAF_VALUES);
            final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();
            try (LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena();
                 LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                for (int i = 0; i < keys.length; i++) {
                    LiveViewCheckpointTestKeys.put(arena, keys[i], scalar(i));
                }
                writer.apply(new LiveViewCheckpointPageRef(), arena, 41, root);
            }
            final String actual = hex(readSegment(41));
            Assert.assertEquals("small STRING leaf segment bytes", SMALL_LEAF_SEGMENT_HEX, actual);
            assertLookups(root, keys, null);
        });
    }

    @Test
    public void testStringKeyMapBytesAcrossCopyOnWrite() throws Exception {
        assertMemoryLeak(() -> {
            final ArrayColumnTypes types = new ArrayColumnTypes().add(ColumnType.STRING);
            final byte[][] keys = encodeStringKeys(types, STRING_VALUES);
            final LiveViewCheckpointPageRef first = new LiveViewCheckpointPageRef();
            final LiveViewCheckpointPageRef second = new LiveViewCheckpointPageRef();
            try (Path dir = new Path()) {
                // Narrow nodes, so the map has internal pages and a copy-on-write descent.
                try (LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena();
                     LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration, 4, 4)) {
                    writer.of(checkpointsDir(dir));
                    for (int i = 0; i < keys.length; i++) {
                        LiveViewCheckpointTestKeys.put(arena, keys[i], scalar(i), refs(i, 1));
                    }
                    writer.apply(new LiveViewCheckpointPageRef(), arena, 21, first);
                }
                final int[] secondScalars = new int[keys.length];
                for (int i = 0; i < keys.length; i++) {
                    secondScalars[i] = i;
                }
                try (LiveViewCheckpointMutationArena arena = new LiveViewCheckpointMutationArena();
                     LiveViewCheckpointPartitionMapWriter writer = new LiveViewCheckpointPartitionMapWriter(configuration, 4, 4)) {
                    writer.of(checkpointsDir(dir));
                    LiveViewCheckpointTestKeys.put(arena, keys[4], scalar(204), refs(204, 1));
                    secondScalars[4] = 204;
                    LiveViewCheckpointTestKeys.remove(arena, keys[7]);
                    secondScalars[7] = -1;
                    LiveViewCheckpointTestKeys.remove(arena, keys[11]);
                    secondScalars[11] = -1;
                    LiveViewCheckpointTestKeys.put(arena, keys[15], scalar(215), refs(215, 1));
                    secondScalars[15] = 215;
                    // Equal to what the first generation holds: a no-op one layer down.
                    LiveViewCheckpointTestKeys.put(arena, keys[0], scalar(0), refs(0, 1));
                    writer.apply(first, arena, 22, second);
                }
                assertSegmentDigest(21, STRING_MAP_FIRST_DIGEST);
                assertSegmentDigest(22, STRING_MAP_SECOND_DIGEST);
                assertLookups(second, keys, secondScalars);
            }
        });
    }

    private static Path checkpointsDir(Path path) {
        return path.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
    }

    private static byte[][] encodeKeys(ArrayColumnTypes types, Record[] records) {
        final byte[][] keys = new byte[records.length][];
        try (MemoryCARW mem = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            for (int i = 0; i < records.length; i++) {
                mem.jumpTo(0);
                LiveViewSnapshotKeyCodec.writeKey(mem, records[i], types, 0);
                final byte[] key = new byte[(int) mem.getAppendOffset()];
                for (int b = 0; b < key.length; b++) {
                    key[b] = mem.getByte(b);
                }
                keys[i] = key;
            }
        }
        return keys;
    }

    private static byte[][] encodeStringKeys(ArrayColumnTypes types, String[] values) {
        final Record[] records = new Record[values.length];
        for (int i = 0; i < values.length; i++) {
            records[i] = new KeyRecord((Object) values[i]);
        }
        return encodeKeys(types, records);
    }

    private static String hex(byte[] bytes) {
        final StringBuilder sink = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sink.append(Character.forDigit((b >>> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
        }
        return sink.toString();
    }

    private static byte[] readSegment(long segmentId) throws IOException {
        try (Path dir = new Path(); Path file = new Path()) {
            LiveViewCheckpointLayout.metaSegmentPath(file, checkpointsDir(dir), segmentId);
            return Files.readAllBytes(Paths.get(file.toString()));
        }
    }

    private static LiveViewCheckpointStatePageRef[] refs(int id, int count) {
        final LiveViewCheckpointStatePageRef[] refs = new LiveViewCheckpointStatePageRef[count];
        for (int r = 0; r < count; r++) {
            refs[r] = new LiveViewCheckpointStatePageRef().of(id + 1, id * 64L + r * 8L, 8 + r, 16 + r, 0x31, 0, 1 + r, r);
        }
        return refs;
    }

    private static byte[] scalar(int value) {
        return new byte[]{(byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value};
    }

    private static int scalarOf(LiveViewCheckpointPartitionMapEntry entry) {
        final byte[] value = entry.getScalarState();
        Assert.assertEquals(Integer.BYTES, value.length);
        return (value[0] & 0xff) << 24 | (value[1] & 0xff) << 16 | (value[2] & 0xff) << 8 | value[3] & 0xff;
    }

    private static String sha256(byte[] bytes) throws NoSuchAlgorithmException {
        return hex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    // Looks every key up and walks the map: a key with a non-negative expected scalar must
    // be found with it, and one with -1 must be absent. A null array expects key i to hold
    // scalar i.
    private void assertLookups(LiveViewCheckpointPageRef root, byte[][] keys, int[] expectedScalars) {
        try (LiveViewCheckpointPartitionMapReader reader = new LiveViewCheckpointPartitionMapReader(configuration);
             LiveViewCheckpointPartitionMapEntry entry = new LiveViewCheckpointPartitionMapEntry();
             Path dir = new Path()) {
            reader.of(checkpointsDir(dir));
            int liveCount = 0;
            for (int i = 0; i < keys.length; i++) {
                final int expected = expectedScalars == null ? i : expectedScalars[i];
                final boolean isFound = LiveViewCheckpointTestKeys.find(reader, root, keys[i], entry);
                Assert.assertEquals("key " + i, expected >= 0, isFound);
                if (isFound) {
                    Assert.assertEquals("key " + i, expected, scalarOf(entry));
                    Assert.assertArrayEquals("key " + i, keys[i], entry.copyKeyForTest());
                    liveCount++;
                }
            }
            Assert.assertEquals(liveCount, reader.size(root));
            final int[] visited = {0};
            reader.iterateAll(root, visitedEntry -> visited[0]++);
            Assert.assertEquals(liveCount, visited[0]);
        }
    }

    private void assertSegmentDigest(long segmentId, String expectedDigest) throws Exception {
        final byte[] bytes = readSegment(segmentId);
        final String actualDigest = sha256(bytes);
        if (!expectedDigest.equals(actualDigest)) {
            Assert.fail("segment " + segmentId + " bytes changed [expectedSha256=" + expectedDigest
                    + ", actualSha256=" + actualDigest + ", length=" + bytes.length + ", hex=" + hex(bytes) + ']');
        }
    }

    private static final class KeyRecord implements Record {
        private final Object[] values;

        private KeyRecord(Object... values) {
            this.values = values;
        }

        @Override
        public boolean getBool(int col) {
            return (Boolean) values[col];
        }

        @Override
        public byte getByte(int col) {
            return (Byte) values[col];
        }

        @Override
        public char getChar(int col) {
            return (Character) values[col];
        }

        @Override
        public double getDouble(int col) {
            return (Double) values[col];
        }

        @Override
        public int getInt(int col) {
            return (Integer) values[col];
        }

        @Override
        public long getLong(int col) {
            return (Long) values[col];
        }

        @Override
        public short getShort(int col) {
            return (Short) values[col];
        }

        @Override
        public CharSequence getStrA(int col) {
            return (CharSequence) values[col];
        }
    }
}
