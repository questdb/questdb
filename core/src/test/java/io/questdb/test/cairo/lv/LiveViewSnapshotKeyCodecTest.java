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
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.lv.LiveViewSnapshotKeyCodec;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
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

            ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            valueTypes.add(ColumnType.LONG);

            try (Map src = MapFactory.createOrderedMap(configuration, keyTypes, valueTypes);
                 Map dst = MapFactory.createOrderedMap(configuration, keyTypes, valueTypes);
                 MemoryCARW buf = Vm.getCARWInstance(CODEC_BUF_PAGE_SIZE, CODEC_BUF_MAX_PAGES, MemoryTag.NATIVE_DEFAULT)) {
                // Build the source key, then serialise it from the map's MapRecord.
                // Key columns follow the value columns in the record layout, so
                // startIndex is the value column count.
                MapKey srcKey = src.withKey();
                putMixedKey(srcKey);
                srcKey.createValue().putLong(0, 0xDEADL);

                MapRecordCursor srcCursor = src.getCursor();
                MapRecord srcRecord = src.getRecord();
                Assert.assertTrue(srcCursor.hasNext());
                LiveViewSnapshotKeyCodec.writeKey(buf, srcRecord, keyTypes, valueTypes.getColumnCount());
                Assert.assertFalse(srcCursor.hasNext());
                final long written = buf.getAppendOffset();

                // Restore the key bytes into a fresh destination entry; readKey must
                // consume exactly the bytes writeKey produced.
                MapKey restored = dst.withKey();
                long consumed = LiveViewSnapshotKeyCodec.readKey(restored, buf, 0, keyTypes);
                Assert.assertEquals("readKey must consume exactly what writeKey wrote", written, consumed);
                restored.createValue().putLong(0, 0xDEADL);

                // An independently-built probe key must land on the restored entry:
                // an equal key proves every arm round-tripped byte for byte.
                MapKey probe = dst.withKey();
                putMixedKey(probe);
                Assert.assertFalse(
                        "mixed key must survive the writeKey/readKey round-trip",
                        probe.createValue().isNew()
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
                Assert.assertEquals(LiveViewSnapshotKeyCodec.byteSizeOf(valueTypes), consumed);

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

    // Writes the testKeyRoundTripMixed key columns in keyTypes order, each with the
    // put the codec's readKey uses on the target side (putLong for TIMESTAMP,
    // putInt for SYMBOL/IPv4/GEOINT, etc.), so the source, restored and probe keys
    // all share one byte layout.
    private void putMixedKey(MapKey key) {
        key.putLong(0x1111_2222_3333_4444L);     // LONG
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
        key.putStr("live-view-key");             // STRING
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
    private interface LongKeyPut {
        void apply(MapKey key, long value);
    }

    @FunctionalInterface
    private interface ShortKeyPut {
        void apply(MapKey key, short value);
    }
}
