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
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.lv.LiveViewSnapshotKeyCodec;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit-test coverage for {@link LiveViewSnapshotKeyCodec}. The codec dispatches
 * every fixed-width primitive (BYTE/BOOLEAN/SHORT/CHAR/INT/SYMBOL/FLOAT/LONG/
 * TIMESTAMP/DATE/DOUBLE), IPv4, the four GEOHASH variants (BYTE/SHORT/INT/LONG)
 * and STRING, so that a window function's partition key and the rank function's
 * chain-prefix can be snapshotted regardless of which projected source column
 * types appear.
 * <p>
 * The tests round-trip through a real {@code Map} so the byte layout matches
 * end-to-end with the codec's read and write halves, rather than relying on
 * test-only synthetic Records. Two multi-column tests cover the whole dispatch:
 * {@link #testKeyRoundTripMixed} exercises the key half ({@code writeKey} plus
 * {@code readKey} into a {@link MapKey}, including STRING) and
 * {@link #testValueSlotsRoundTripMixed} the value half ({@code writeKey} plus
 * {@code readValueSlots} into a {@link MapValue}, every fixed-width arm - the
 * value-slot reader has no STRING case). The per-type {@code testKeyRoundTrip*}
 * tests additionally pin the individual IPv4 / GEOHASH arms in isolation.
 */
public class LiveViewSnapshotKeyCodecTest extends AbstractCairoTest {

    private static final long CODEC_BUF_PAGE_SIZE = 4096;
    private static final int CODEC_BUF_MAX_PAGES = 4;
    // Wire-format byte counts for the testKeyRoundTripMixed key row, pinned as literals. Asserting
    // them against byteSizeOf() instead would compare the codec with itself: byteSizeOfType and the
    // read/write offset advances are separate switches, but a coordinated edit to both would pass.
    // 8 LONG + 1 BYTE + 1 BOOLEAN + 2 SHORT + 2 CHAR + 4 INT + 4 SYMBOL + 4 IPv4 + 4 FLOAT
    //   + 1 GEOBYTE + 2 GEOSHORT + 4 GEOINT + 8 GEOLONG + 8 TIMESTAMP + 8 DATE + 8 DOUBLE
    private static final int MIXED_FIXED_BYTES = 69;
    // A null STRING is a bare 4-byte length of -1: no character payload follows.
    private static final int MIXED_NULL_KEY_BYTES = MIXED_FIXED_BYTES + 4;
    // 69 + STRING "live-view-key": 4-byte length prefix + 13 chars x 2 bytes.
    private static final int MIXED_KEY_BYTES = MIXED_FIXED_BYTES + 4 + 26;

    @Test
    public void testIsAllTypesFixedWidth() {
        // The value-slot reader (readValueSlots) has no STRING case - MapValue
        // exposes no STRING setter - so callers snapshotting value slots (rank's
        // chain-prefix) must gate on isAllTypesFixedWidth, which rejects the
        // STRING exception isAllTypesSupported admits for partition keys.
        ArrayColumnTypes fixedOnly = new ArrayColumnTypes();
        fixedOnly.add(ColumnType.LONG);
        fixedOnly.add(ColumnType.TIMESTAMP);
        fixedOnly.add(ColumnType.DOUBLE);
        Assert.assertTrue(LiveViewSnapshotKeyCodec.isAllTypesFixedWidth(fixedOnly));

        ArrayColumnTypes withString = new ArrayColumnTypes();
        withString.add(ColumnType.LONG);
        withString.add(ColumnType.STRING);
        Assert.assertTrue(LiveViewSnapshotKeyCodec.isAllTypesSupported(withString));
        Assert.assertFalse(LiveViewSnapshotKeyCodec.isAllTypesFixedWidth(withString));
    }

    @Test
    public void testIsAllTypesSupportedNewTypes() {
        ArrayColumnTypes supported = new ArrayColumnTypes();
        supported.add(ColumnType.IPv4);
        supported.add(ColumnType.getGeoHashTypeWithBits(5));   // GEOBYTE
        supported.add(ColumnType.getGeoHashTypeWithBits(10));  // GEOSHORT
        supported.add(ColumnType.getGeoHashTypeWithBits(20));  // GEOINT
        supported.add(ColumnType.getGeoHashTypeWithBits(40));  // GEOLONG
        Assert.assertTrue(LiveViewSnapshotKeyCodec.isAllTypesSupported(supported));

        // STRING is admitted as a variable-width exception so SYMBOL-partitioned
        // LVs can ride STRING keys end-to-end (LiveViewWindow.build rewrites
        // SYMBOL partition columns to STRING in the anchor map's key types).
        ArrayColumnTypes stringOnly = new ArrayColumnTypes();
        stringOnly.add(ColumnType.STRING);
        Assert.assertTrue(LiveViewSnapshotKeyCodec.isAllTypesSupported(stringOnly));

        ArrayColumnTypes uuidOnly = new ArrayColumnTypes();
        uuidOnly.add(ColumnType.UUID);
        Assert.assertFalse(LiveViewSnapshotKeyCodec.isAllTypesSupported(uuidOnly));
    }

    @Test
    public void testKeyRoundTripGeoByte() throws Exception {
        assertSingleColumnKeyRoundTrip(ColumnType.getGeoHashTypeWithBits(5), (byte) 0x0A, MapKey::putByte, MapKey::putByte);
    }

    @Test
    public void testKeyRoundTripGeoInt() throws Exception {
        assertSingleColumnKeyRoundTrip(ColumnType.getGeoHashTypeWithBits(20), 0xCAFE_BABE, MapKey::putInt, MapKey::putInt);
    }

    @Test
    public void testKeyRoundTripGeoLong() throws Exception {
        assertSingleColumnKeyRoundTrip(ColumnType.getGeoHashTypeWithBits(40), 0x1234_5678_9ABC_DEF0L, MapKey::putLong, MapKey::putLong);
    }

    @Test
    public void testKeyRoundTripGeoShort() throws Exception {
        assertSingleColumnKeyRoundTrip(ColumnType.getGeoHashTypeWithBits(10), (short) 0x1234, MapKey::putShort, MapKey::putShort);
    }

    @Test
    public void testKeyRoundTripIPv4() throws Exception {
        // Codec encodes IPv4 as 4 raw bytes via record.getIPv4(); the Map stores
        // the same bytes whether the slot is INT or IPv4-typed, so the round-trip
        // verifies the dispatch handler ends in putInt without truncation.
        assertSingleColumnKeyRoundTrip(ColumnType.IPv4, 0x7F00_0001, MapKey::putInt, MapKey::putInt);
    }

    @Test
    public void testKeyRoundTripMixed() throws Exception {
        // Every fixed-width key arm plus STRING in one composite key, round-tripped
        // writeKey -> buf -> readKey. Pins the key half of the dispatch (readKey
        // pushes into a MapKey) for the arms the per-type testKeyRoundTrip* tests do
        // not reach - BOOLEAN/CHAR/FLOAT/DATE/DOUBLE/STRING on the read side, plus
        // the INT/SYMBOL/LONG/TIMESTAMP/DATE write getters.
        assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = mixedKeyTypes();

            ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            valueTypes.add(ColumnType.LONG);

            try (Map src = MapFactory.createOrderedMap(configuration, keyTypes, valueTypes);
                 Map dst = MapFactory.createOrderedMap(configuration, keyTypes, valueTypes);
                 MemoryCARW buf = Vm.getCARWInstance(CODEC_BUF_PAGE_SIZE, CODEC_BUF_MAX_PAGES, MemoryTag.NATIVE_DEFAULT)) {
                // Build the source key, then serialise it from the map's MapRecord.
                // Key columns follow the value columns in the record layout, so
                // startIndex is the value column count.
                MapKey srcKey = src.withKey();
                putMixedKey(srcKey, 0x1111_2222_3333_4444L, "live-view-key");
                srcKey.createValue().putLong(0, 0xDEADL);

                MapRecordCursor srcCursor = src.getCursor();
                MapRecord srcRecord = src.getRecord();
                Assert.assertTrue(srcCursor.hasNext());
                LiveViewSnapshotKeyCodec.writeKey(buf, srcRecord, keyTypes, valueTypes.getColumnCount());
                Assert.assertFalse(srcCursor.hasNext());
                final long written = buf.getAppendOffset();
                Assert.assertEquals("mixed key wire size", MIXED_KEY_BYTES, written);

                // Restore the key bytes into a fresh destination entry; readKey must
                // consume exactly the bytes writeKey produced.
                MapKey restored = dst.withKey();
                long consumed = LiveViewSnapshotKeyCodec.readKey(restored, buf, 0, keyTypes);
                Assert.assertEquals("readKey must consume exactly what writeKey wrote", written, consumed);
                restored.createValue().putLong(0, 0xDEADL);

                // An independently-built probe key must land on the restored entry:
                // an equal key proves every arm round-tripped byte for byte.
                MapKey probe = dst.withKey();
                putMixedKey(probe, 0x1111_2222_3333_4444L, "live-view-key");
                Assert.assertFalse(
                        "mixed key must survive the writeKey/readKey round-trip",
                        probe.createValue().isNew()
                );
            }
        });
    }

    @Test
    public void testKeyRoundTripMixedNullSentinels() throws Exception {
        // Every arm carrying its NULL sentinel. The fixed-width arms are bit-transparent, so their
        // sentinels (Double.NaN, Numbers.LONG_NULL, GeoHashes.INT_NULL, ...) are just values the
        // round-trip must not mangle - a sloppy widening cast on any of them shows up here. STRING
        // is the one arm with real NULL control flow: writeKey sinks a bare -1 length and readKey
        // must take the strLen < 0 branch, advancing 4 bytes and no character payload. Nothing else
        // covers that branch, and the wire-size assert below is what pins it.
        assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = mixedKeyTypes();

            ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            valueTypes.add(ColumnType.LONG);

            try (Map src = MapFactory.createOrderedMap(configuration, keyTypes, valueTypes);
                 Map dst = MapFactory.createOrderedMap(configuration, keyTypes, valueTypes);
                 MemoryCARW buf = Vm.getCARWInstance(CODEC_BUF_PAGE_SIZE, CODEC_BUF_MAX_PAGES, MemoryTag.NATIVE_DEFAULT)) {
                MapKey srcKey = src.withKey();
                putMixedNullKey(srcKey);
                srcKey.createValue().putLong(0, 0xDEADL);

                MapRecordCursor srcCursor = src.getCursor();
                MapRecord srcRecord = src.getRecord();
                Assert.assertTrue(srcCursor.hasNext());
                LiveViewSnapshotKeyCodec.writeKey(buf, srcRecord, keyTypes, valueTypes.getColumnCount());
                final long written = buf.getAppendOffset();
                Assert.assertEquals(
                        "a null STRING must serialise as a bare -1 length with no payload",
                        MIXED_NULL_KEY_BYTES,
                        written
                );

                MapKey restored = dst.withKey();
                long consumed = LiveViewSnapshotKeyCodec.readKey(restored, buf, 0, keyTypes);
                Assert.assertEquals("readKey must consume exactly what writeKey wrote", written, consumed);
                restored.createValue().putLong(0, 0xDEADL);

                MapKey probe = dst.withKey();
                putMixedNullKey(probe);
                Assert.assertFalse(
                        "all-null key must survive the writeKey/readKey round-trip",
                        probe.createValue().isNew()
                );
            }
        });
    }

    @Test
    public void testKeyRoundTripMultipleKeysAtAdvancingOffsets() throws Exception {
        // Production never reads a key at offset 0 twice: LiveViewWindow.restore and
        // LiveViewFunctionSnapshot.restore loop over the partition count, feeding each readKey the
        // offset the previous one returned. Every other test in this class reads a single key at a
        // literal 0, so a codec whose returned offset was wrong - but whose absolute reads at 0 were
        // right - would pass them all. Three keys of three different widths (the STRING makes the
        // stride non-uniform) read back-to-back pin the returned offset at each step.
        assertMemoryLeak(() -> {
            ArrayColumnTypes keyTypes = mixedKeyTypes();

            ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            valueTypes.add(ColumnType.LONG);

            // "another-live-view-key" is 21 chars: 4-byte length prefix + 42 bytes of payload.
            final int thirdKeyBytes = MIXED_FIXED_BYTES + 4 + 42;

            try (Map src = MapFactory.createOrderedMap(configuration, keyTypes, valueTypes);
                 Map dst = MapFactory.createOrderedMap(configuration, keyTypes, valueTypes);
                 MemoryCARW buf = Vm.getCARWInstance(CODEC_BUF_PAGE_SIZE, CODEC_BUF_MAX_PAGES, MemoryTag.NATIVE_DEFAULT)) {
                // Three distinct keys, concatenated into one buffer the way a multi-partition
                // window snapshot lays them out.
                appendKey(src, buf, keyTypes, valueTypes, key -> putMixedKey(key, 0x1111_2222_3333_4444L, "live-view-key"));
                appendKey(src, buf, keyTypes, valueTypes, this::putMixedNullKey);
                appendKey(src, buf, keyTypes, valueTypes, key -> putMixedKey(key, 0x5555_6666_7777_8888L, "another-live-view-key"));

                Assert.assertEquals(
                        "concatenated wire size",
                        MIXED_KEY_BYTES + MIXED_NULL_KEY_BYTES + thirdKeyBytes,
                        buf.getAppendOffset()
                );

                long offset = 0;
                offset = LiveViewSnapshotKeyCodec.readKey(dst.withKey(), buf, offset, keyTypes);
                Assert.assertEquals("offset after key 1", MIXED_KEY_BYTES, offset);
                offset = LiveViewSnapshotKeyCodec.readKey(dst.withKey(), buf, offset, keyTypes);
                Assert.assertEquals("offset after key 2", MIXED_KEY_BYTES + MIXED_NULL_KEY_BYTES, offset);
                offset = LiveViewSnapshotKeyCodec.readKey(dst.withKey(), buf, offset, keyTypes);
                Assert.assertEquals("offset after key 3", MIXED_KEY_BYTES + MIXED_NULL_KEY_BYTES + thirdKeyBytes, offset);
                Assert.assertEquals("the three reads must consume the whole buffer", buf.getAppendOffset(), offset);

                // Re-read each key at its own offset and insert it, so a torn read at a non-zero
                // offset surfaces as a key that fails to match its independently-built probe.
                assertKeyAt(dst, buf, keyTypes, 0, key -> putMixedKey(key, 0x1111_2222_3333_4444L, "live-view-key"));
                assertKeyAt(dst, buf, keyTypes, MIXED_KEY_BYTES, this::putMixedNullKey);
                assertKeyAt(
                        dst,
                        buf,
                        keyTypes,
                        MIXED_KEY_BYTES + MIXED_NULL_KEY_BYTES,
                        key -> putMixedKey(key, 0x5555_6666_7777_8888L, "another-live-view-key")
                );
            }
        });
    }

    @Test
    public void testValueSlotsRoundTripMixed() throws Exception {
        assertMemoryLeak(() -> {
            // Every fixed-width dispatch arm as a value slot, so the test catches
            // drift between writeKey (used for both partition keys and value-slot
            // writes) and readValueSlots (the value-slot reader for the rank chain-
            // prefix path). STRING is key-only - readValueSlots has no STRING case -
            // so it is exercised by testKeyRoundTripMixed instead.
            ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            valueTypes.add(ColumnType.LONG);                                // slot 0
            valueTypes.add(ColumnType.BYTE);                                // slot 1
            valueTypes.add(ColumnType.BOOLEAN);                             // slot 2
            valueTypes.add(ColumnType.SHORT);                               // slot 3
            valueTypes.add(ColumnType.CHAR);                                // slot 4
            valueTypes.add(ColumnType.INT);                                 // slot 5
            valueTypes.add(ColumnType.SYMBOL);                              // slot 6 (int id)
            valueTypes.add(ColumnType.IPv4);                                // slot 7
            valueTypes.add(ColumnType.FLOAT);                               // slot 8
            valueTypes.add(ColumnType.getGeoHashTypeWithBits(5));           // slot 9  - GEOBYTE
            valueTypes.add(ColumnType.getGeoHashTypeWithBits(10));          // slot 10 - GEOSHORT
            valueTypes.add(ColumnType.getGeoHashTypeWithBits(20));          // slot 11 - GEOINT
            valueTypes.add(ColumnType.getGeoHashTypeWithBits(40));          // slot 12 - GEOLONG
            valueTypes.add(ColumnType.TIMESTAMP);                           // slot 13
            valueTypes.add(ColumnType.DATE);                                // slot 14
            valueTypes.add(ColumnType.DOUBLE);                              // slot 15

            SingleColumnType keyType = new SingleColumnType(ColumnType.LONG);
            try (Map src = MapFactory.createUnorderedMap(configuration, keyType, valueTypes);
                 Map dst = MapFactory.createUnorderedMap(configuration, keyType, valueTypes);
                 MemoryCARW buf = Vm.getCARWInstance(CODEC_BUF_PAGE_SIZE, CODEC_BUF_MAX_PAGES, MemoryTag.NATIVE_DEFAULT)) {
                final long partitionKey = 42L;
                MapKey srcKey = src.withKey();
                srcKey.putLong(partitionKey);
                MapValue srcValue = srcKey.createValue();
                srcValue.putLong(0, 0x1111_2222_3333_4444L);
                srcValue.putByte(1, (byte) 0x5A);
                srcValue.putBool(2, true);
                srcValue.putShort(3, (short) 0x2B3C);
                srcValue.putChar(4, 'Q');
                srcValue.putInt(5, 0x1234_5678);
                srcValue.putInt(6, 7);                                      // SYMBOL id
                srcValue.putInt(7, 0x7F00_0101);                            // IPv4
                srcValue.putFloat(8, 2.71828f);
                srcValue.putByte(9, (byte) 0x0F);                           // GEOBYTE
                srcValue.putShort(10, (short) 0x1357);                      // GEOSHORT
                srcValue.putInt(11, 0xDEAD_BEEF);                           // GEOINT
                srcValue.putLong(12, 0x0BAD_CAFE_F00D_BA11L);               // GEOLONG
                srcValue.putTimestamp(13, 1_700_000_000_000_000L);
                srcValue.putDate(14, 1_600_000_000_000L);
                srcValue.putDouble(15, 3.14159265358979);

                // Iterate the source map's cursor; serialise value slots [0..n)
                // via writeKey with startIndex=0 - the same dispatch the rank
                // function uses to write its chain-prefix from a MapRecord.
                MapRecordCursor srcCursor = src.getCursor();
                MapRecord srcRecord = src.getRecord();
                Assert.assertTrue(srcCursor.hasNext());
                LiveViewSnapshotKeyCodec.writeKey(buf, srcRecord, valueTypes, 0);
                Assert.assertFalse(srcCursor.hasNext());

                // Restore into a fresh entry in the destination map, then assert
                // every slot reads back to the original value.
                MapKey dstKey = dst.withKey();
                dstKey.putLong(partitionKey);
                MapValue dstValue = dstKey.createValue();
                long consumed = LiveViewSnapshotKeyCodec.readValueSlots(dstValue, 0, buf, 0, valueTypes);
                // Pin the wire size to a literal, and only then cross-check byteSizeOf against it:
                // asserting consumed against byteSizeOf alone compares the codec with itself.
                Assert.assertEquals("value-slot wire size", MIXED_FIXED_BYTES, consumed);
                Assert.assertEquals(MIXED_FIXED_BYTES, LiveViewSnapshotKeyCodec.byteSizeOf(valueTypes));
                Assert.assertEquals(MIXED_FIXED_BYTES, buf.getAppendOffset());

                Assert.assertEquals(0x1111_2222_3333_4444L, dstValue.getLong(0));
                Assert.assertEquals((byte) 0x5A, dstValue.getByte(1));
                Assert.assertTrue(dstValue.getBool(2));
                Assert.assertEquals((short) 0x2B3C, dstValue.getShort(3));
                Assert.assertEquals('Q', dstValue.getChar(4));
                Assert.assertEquals(0x1234_5678, dstValue.getInt(5));
                Assert.assertEquals(7, dstValue.getInt(6));
                Assert.assertEquals(0x7F00_0101, dstValue.getInt(7));
                Assert.assertEquals(2.71828f, dstValue.getFloat(8), 0f);
                Assert.assertEquals((byte) 0x0F, dstValue.getByte(9));
                Assert.assertEquals((short) 0x1357, dstValue.getShort(10));
                Assert.assertEquals(0xDEAD_BEEF, dstValue.getInt(11));
                Assert.assertEquals(0x0BAD_CAFE_F00D_BA11L, dstValue.getLong(12));
                Assert.assertEquals(1_700_000_000_000_000L, dstValue.getTimestamp(13));
                Assert.assertEquals(1_600_000_000_000L, dstValue.getDate(14));
                Assert.assertEquals(3.14159265358979, dstValue.getDouble(15), 1e-15);
            }
        });
    }

    // Builds one key in src, serialises it onto the tail of buf, then drops it, so the next call
    // starts from an empty map and appends the next key's bytes directly after this one's.
    private void appendKey(Map src, MemoryCARW buf, ArrayColumnTypes keyTypes, ArrayColumnTypes valueTypes, KeyPut put) {
        src.clear();
        MapKey key = src.withKey();
        put.apply(key);
        key.createValue().putLong(0, 0xDEADL);

        MapRecordCursor cursor = src.getCursor();
        MapRecord record = src.getRecord();
        Assert.assertTrue(cursor.hasNext());
        LiveViewSnapshotKeyCodec.writeKey(buf, record, keyTypes, valueTypes.getColumnCount());
        Assert.assertFalse(cursor.hasNext());
    }

    // Reads the key stored at the given offset and asserts an independently-built probe key lands
    // on the same entry, i.e. the read at that offset was not torn.
    private void assertKeyAt(Map dst, MemoryCARW buf, ArrayColumnTypes keyTypes, long offset, KeyPut put) {
        MapKey restored = dst.withKey();
        LiveViewSnapshotKeyCodec.readKey(restored, buf, offset, keyTypes);
        restored.createValue().putLong(0, 0xDEADL);

        MapKey probe = dst.withKey();
        put.apply(probe);
        Assert.assertFalse(
                "key read at offset " + offset + " must match its independently-built probe",
                probe.createValue().isNew()
        );
    }

    private void assertSingleColumnKeyRoundTrip(int columnType, byte src, ByteKeyPut srcPut, ByteKeyPut dstPut) throws Exception {
        assertMemoryLeak(() -> roundTripByte(columnType, src, srcPut, dstPut));
    }

    private void assertSingleColumnKeyRoundTrip(int columnType, short src, ShortKeyPut srcPut, ShortKeyPut dstPut) throws Exception {
        assertMemoryLeak(() -> roundTripShort(columnType, src, srcPut, dstPut));
    }

    private void assertSingleColumnKeyRoundTrip(int columnType, int src, IntKeyPut srcPut, IntKeyPut dstPut) throws Exception {
        assertMemoryLeak(() -> roundTripInt(columnType, src, srcPut, dstPut));
    }

    private void assertSingleColumnKeyRoundTrip(int columnType, long src, LongKeyPut srcPut, LongKeyPut dstPut) throws Exception {
        assertMemoryLeak(() -> roundTripLong(columnType, src, srcPut, dstPut));
    }

    // The composite key type list shared by the mixed-key tests: every fixed-width arm the codec
    // dispatches, plus STRING.
    private ArrayColumnTypes mixedKeyTypes() {
        ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        keyTypes.add(ColumnType.LONG);                       // col 0
        keyTypes.add(ColumnType.BYTE);                       // col 1
        keyTypes.add(ColumnType.BOOLEAN);                    // col 2
        keyTypes.add(ColumnType.SHORT);                      // col 3
        keyTypes.add(ColumnType.CHAR);                       // col 4
        keyTypes.add(ColumnType.INT);                        // col 5
        keyTypes.add(ColumnType.SYMBOL);                     // col 6 (int id)
        keyTypes.add(ColumnType.IPv4);                       // col 7
        keyTypes.add(ColumnType.FLOAT);                      // col 8
        keyTypes.add(ColumnType.getGeoHashTypeWithBits(5));  // col 9  - GEOBYTE
        keyTypes.add(ColumnType.getGeoHashTypeWithBits(10)); // col 10 - GEOSHORT
        keyTypes.add(ColumnType.getGeoHashTypeWithBits(20)); // col 11 - GEOINT
        keyTypes.add(ColumnType.getGeoHashTypeWithBits(40)); // col 12 - GEOLONG
        keyTypes.add(ColumnType.TIMESTAMP);                  // col 13
        keyTypes.add(ColumnType.DATE);                       // col 14
        keyTypes.add(ColumnType.DOUBLE);                     // col 15
        keyTypes.add(ColumnType.STRING);                     // col 16
        return keyTypes;
    }

    // Writes the mixed key columns in mixedKeyTypes() order, each with the put the codec's readKey
    // uses on the target side (putLong for TIMESTAMP, putInt for SYMBOL/IPv4/GEOINT, etc.), so the
    // source, restored and probe keys all share one byte layout. The LONG and STRING columns are
    // caller-supplied so multi-key tests can build keys that differ in both value and wire width.
    private void putMixedKey(MapKey key, long longValue, String str) {
        key.putLong(longValue);                  // LONG
        key.putByte((byte) 0x5A);                // BYTE
        key.putBool(true);                       // BOOLEAN
        key.putShort((short) 0x2B3C);            // SHORT
        key.putChar('Q');                        // CHAR
        key.putInt(0x1234_5678);                 // INT
        key.putInt(7);                           // SYMBOL id
        key.putInt(0x7F00_0101);                 // IPv4
        key.putFloat(2.71828f);                  // FLOAT
        key.putByte((byte) 0x0F);                // GEOBYTE
        key.putShort((short) 0x1357);            // GEOSHORT
        key.putInt(0xDEAD_BEEF);                 // GEOINT
        key.putLong(0x0BAD_CAFE_F00D_BA11L);     // GEOLONG
        key.putLong(1_700_000_000_000_000L);     // TIMESTAMP
        key.putDate(1_600_000_000_000L);         // DATE
        key.putDouble(3.14159265358979);         // DOUBLE
        key.putStr(str);                         // STRING
    }

    // The mixedKeyTypes() key with every column at its NULL sentinel. BYTE/BOOLEAN/SHORT have no
    // NULL in QuestDB, so they carry their zero value and are here only to keep the column count
    // aligned with mixedKeyTypes().
    private void putMixedNullKey(MapKey key) {
        key.putLong(Numbers.LONG_NULL);          // LONG
        key.putByte((byte) 0);                   // BYTE - no NULL
        key.putBool(false);                      // BOOLEAN - no NULL
        key.putShort((short) 0);                 // SHORT - no NULL
        key.putChar(Numbers.CHAR_NULL);          // CHAR
        key.putInt(Numbers.INT_NULL);            // INT
        key.putInt(SymbolTable.VALUE_IS_NULL);   // SYMBOL id
        key.putInt(Numbers.IPv4_NULL);           // IPv4
        key.putFloat(Float.NaN);                 // FLOAT
        key.putByte(GeoHashes.BYTE_NULL);        // GEOBYTE
        key.putShort(GeoHashes.SHORT_NULL);      // GEOSHORT
        key.putInt(GeoHashes.INT_NULL);          // GEOINT
        key.putLong(GeoHashes.NULL);             // GEOLONG
        key.putLong(Numbers.LONG_NULL);          // TIMESTAMP
        key.putDate(Numbers.LONG_NULL);          // DATE
        key.putDouble(Double.NaN);               // DOUBLE
        key.putStr(null);                        // STRING
    }

    private void roundTripByte(int columnType, byte src, ByteKeyPut srcPut, ByteKeyPut dstPut) {
        SingleColumnType keyType = new SingleColumnType(columnType);
        ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        valueTypes.add(ColumnType.LONG);
        Map source = MapFactory.createUnorderedMap(configuration, keyType, valueTypes);
        Map target = MapFactory.createUnorderedMap(configuration, keyType, valueTypes);
        MemoryCARW buf = Vm.getCARWInstance(CODEC_BUF_PAGE_SIZE, CODEC_BUF_MAX_PAGES, MemoryTag.NATIVE_DEFAULT);
        try {
            MapKey srcKey = source.withKey();
            srcPut.apply(srcKey, src);
            srcKey.createValue().putLong(0, 0xDEADL);

            MapRecordCursor cursor = source.getCursor();
            MapRecord record = source.getRecord();
            Assert.assertTrue(cursor.hasNext());
            LiveViewSnapshotKeyCodec.writeKey(buf, record, keyType, valueTypes.getColumnCount());

            MapKey target2 = target.withKey();
            long consumed = LiveViewSnapshotKeyCodec.readKey(target2, buf, 0, keyType);
            Assert.assertEquals(Byte.BYTES, consumed);
            MapValue v = target2.createValue();
            v.putLong(0, 0xDEADL);

            MapKey probe = target.withKey();
            dstPut.apply(probe, src);
            MapValue found = probe.createValue();
            Assert.assertFalse("expected key " + src + " to survive round-trip for " + ColumnType.nameOf(columnType), found.isNew());
        } finally {
            Misc.free(source);
            Misc.free(target);
            Misc.free(buf);
        }
    }

    private void roundTripInt(int columnType, int src, IntKeyPut srcPut, IntKeyPut dstPut) {
        SingleColumnType keyType = new SingleColumnType(columnType);
        ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        valueTypes.add(ColumnType.LONG);
        Map source = MapFactory.createUnorderedMap(configuration, keyType, valueTypes);
        Map target = MapFactory.createUnorderedMap(configuration, keyType, valueTypes);
        MemoryCARW buf = Vm.getCARWInstance(CODEC_BUF_PAGE_SIZE, CODEC_BUF_MAX_PAGES, MemoryTag.NATIVE_DEFAULT);
        try {
            MapKey srcKey = source.withKey();
            srcPut.apply(srcKey, src);
            srcKey.createValue().putLong(0, 0xDEADL);

            MapRecordCursor cursor = source.getCursor();
            MapRecord record = source.getRecord();
            Assert.assertTrue(cursor.hasNext());
            LiveViewSnapshotKeyCodec.writeKey(buf, record, keyType, valueTypes.getColumnCount());

            MapKey target2 = target.withKey();
            long consumed = LiveViewSnapshotKeyCodec.readKey(target2, buf, 0, keyType);
            Assert.assertEquals(Integer.BYTES, consumed);
            target2.createValue().putLong(0, 0xDEADL);

            MapKey probe = target.withKey();
            dstPut.apply(probe, src);
            MapValue found = probe.createValue();
            Assert.assertFalse("expected key " + src + " to survive round-trip for " + ColumnType.nameOf(columnType), found.isNew());
        } finally {
            Misc.free(source);
            Misc.free(target);
            Misc.free(buf);
        }
    }

    private void roundTripLong(int columnType, long src, LongKeyPut srcPut, LongKeyPut dstPut) {
        SingleColumnType keyType = new SingleColumnType(columnType);
        ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        valueTypes.add(ColumnType.LONG);
        Map source = MapFactory.createUnorderedMap(configuration, keyType, valueTypes);
        Map target = MapFactory.createUnorderedMap(configuration, keyType, valueTypes);
        MemoryCARW buf = Vm.getCARWInstance(CODEC_BUF_PAGE_SIZE, CODEC_BUF_MAX_PAGES, MemoryTag.NATIVE_DEFAULT);
        try {
            MapKey srcKey = source.withKey();
            srcPut.apply(srcKey, src);
            srcKey.createValue().putLong(0, 0xDEADL);

            MapRecordCursor cursor = source.getCursor();
            MapRecord record = source.getRecord();
            Assert.assertTrue(cursor.hasNext());
            LiveViewSnapshotKeyCodec.writeKey(buf, record, keyType, valueTypes.getColumnCount());

            MapKey target2 = target.withKey();
            long consumed = LiveViewSnapshotKeyCodec.readKey(target2, buf, 0, keyType);
            Assert.assertEquals(Long.BYTES, consumed);
            target2.createValue().putLong(0, 0xDEADL);

            MapKey probe = target.withKey();
            dstPut.apply(probe, src);
            MapValue found = probe.createValue();
            Assert.assertFalse("expected key " + src + " to survive round-trip for " + ColumnType.nameOf(columnType), found.isNew());
        } finally {
            Misc.free(source);
            Misc.free(target);
            Misc.free(buf);
        }
    }

    private void roundTripShort(int columnType, short src, ShortKeyPut srcPut, ShortKeyPut dstPut) {
        SingleColumnType keyType = new SingleColumnType(columnType);
        ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        valueTypes.add(ColumnType.LONG);
        Map source = MapFactory.createUnorderedMap(configuration, keyType, valueTypes);
        Map target = MapFactory.createUnorderedMap(configuration, keyType, valueTypes);
        MemoryCARW buf = Vm.getCARWInstance(CODEC_BUF_PAGE_SIZE, CODEC_BUF_MAX_PAGES, MemoryTag.NATIVE_DEFAULT);
        try {
            MapKey srcKey = source.withKey();
            srcPut.apply(srcKey, src);
            srcKey.createValue().putLong(0, 0xDEADL);

            MapRecordCursor cursor = source.getCursor();
            MapRecord record = source.getRecord();
            Assert.assertTrue(cursor.hasNext());
            LiveViewSnapshotKeyCodec.writeKey(buf, record, keyType, valueTypes.getColumnCount());

            MapKey target2 = target.withKey();
            long consumed = LiveViewSnapshotKeyCodec.readKey(target2, buf, 0, keyType);
            Assert.assertEquals(Short.BYTES, consumed);
            target2.createValue().putLong(0, 0xDEADL);

            MapKey probe = target.withKey();
            dstPut.apply(probe, src);
            MapValue found = probe.createValue();
            Assert.assertFalse("expected key " + src + " to survive round-trip for " + ColumnType.nameOf(columnType), found.isNew());
        } finally {
            Misc.free(source);
            Misc.free(target);
            Misc.free(buf);
        }
    }

    @FunctionalInterface
    private interface ByteKeyPut {
        void apply(MapKey key, byte value);
    }

    @FunctionalInterface
    private interface IntKeyPut {
        void apply(MapKey key, int value);
    }

    @FunctionalInterface
    private interface KeyPut {
        void apply(MapKey key);
    }

    @FunctionalInterface
    private interface LongKeyPut {
        void apply(MapKey key, long value);
    }

    @FunctionalInterface
    private interface ShortKeyPut {
        void apply(MapKey key, short value);
    }
}
