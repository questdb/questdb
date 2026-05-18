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
 * Unit-test coverage for {@link LiveViewSnapshotKeyCodec}. Phase 2a shipped the
 * fixed-width primitive dispatch (BYTE/BOOLEAN/SHORT/CHAR/INT/SYMBOL/FLOAT/
 * LONG/TIMESTAMP/DATE/DOUBLE); Phase 2b.1 extends the dispatch to cover IPv4
 * and the four GEOHASH variants (BYTE/SHORT/INT/LONG) so that the rank
 * function's chain-prefix can be snapshotted regardless of which projected
 * source column types appear.
 * <p>
 * The tests round-trip through a real {@code Map} so the byte layout matches
 * end-to-end with the codec's read and write halves, rather than relying on
 * test-only synthetic Records.
 */
public class LiveViewSnapshotKeyCodecTest extends AbstractCairoTest {

    private static final long CODEC_BUF_PAGE_SIZE = 4096;
    private static final int CODEC_BUF_MAX_PAGES = 4;

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
    public void testValueSlotsRoundTripMixed() throws Exception {
        assertMemoryLeak(() -> {
            // Mix every newly-supported dispatch arm with a couple of existing
            // ones so the test catches drift between writeKey (used for both
            // partition keys and value-slot writes) and readValueSlots (the
            // new value-slot reader added for the rank chain-prefix path).
            ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            valueTypes.add(ColumnType.LONG);                                // slot 0 - existing
            valueTypes.add(ColumnType.IPv4);                                // slot 1 - new
            valueTypes.add(ColumnType.getGeoHashTypeWithBits(5));           // slot 2 - GEOBYTE
            valueTypes.add(ColumnType.getGeoHashTypeWithBits(10));          // slot 3 - GEOSHORT
            valueTypes.add(ColumnType.getGeoHashTypeWithBits(20));          // slot 4 - GEOINT
            valueTypes.add(ColumnType.getGeoHashTypeWithBits(40));          // slot 5 - GEOLONG
            valueTypes.add(ColumnType.DOUBLE);                              // slot 6 - existing

            SingleColumnType keyType = new SingleColumnType(ColumnType.LONG);
            try (Map src = MapFactory.createUnorderedMap(configuration, keyType, valueTypes);
                 Map dst = MapFactory.createUnorderedMap(configuration, keyType, valueTypes);
                 MemoryCARW buf = Vm.getCARWInstance(CODEC_BUF_PAGE_SIZE, CODEC_BUF_MAX_PAGES, MemoryTag.NATIVE_DEFAULT)) {
                final long partitionKey = 42L;
                MapKey srcKey = src.withKey();
                srcKey.putLong(partitionKey);
                MapValue srcValue = srcKey.createValue();
                srcValue.putLong(0, 0x1111_2222_3333_4444L);
                srcValue.putInt(1, 0x7F00_0101);                            // IPv4
                srcValue.putByte(2, (byte) 0x0F);                           // GEOBYTE
                srcValue.putShort(3, (short) 0x1357);                       // GEOSHORT
                srcValue.putInt(4, 0xDEAD_BEEF);                            // GEOINT
                srcValue.putLong(5, 0x0BAD_CAFE_F00D_BA11L);                // GEOLONG
                srcValue.putDouble(6, 3.14159265358979);

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
                Assert.assertEquals(0x7F00_0101, dstValue.getInt(1));
                Assert.assertEquals((byte) 0x0F, dstValue.getByte(2));
                Assert.assertEquals((short) 0x1357, dstValue.getShort(3));
                Assert.assertEquals(0xDEAD_BEEF, dstValue.getInt(4));
                Assert.assertEquals(0x0BAD_CAFE_F00D_BA11L, dstValue.getLong(5));
                Assert.assertEquals(3.14159265358979, dstValue.getDouble(6), 1e-15);
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
