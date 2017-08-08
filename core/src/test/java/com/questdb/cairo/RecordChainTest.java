package com.questdb.cairo;

import com.questdb.misc.Rnd;
import com.questdb.ql.Record;
import com.questdb.ql.impl.CollectionRecordMetadata;
import com.questdb.ql.impl.RecordColumnMetadataImpl;
import com.questdb.std.BinarySequence;
import com.questdb.std.LongList;
import com.questdb.store.ColumnType;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class RecordChainTest {
    public static final long SIZE_4M = 4 * 1024 * 1024L;
    private static CollectionRecordMetadata metadata;

    @Test
    public void testClear() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Record record = new R();
            final int N = 10000;
            try (RecordChain chain = new RecordChain(metadata, SIZE_4M)) {

                Assert.assertFalse(chain.hasNext());
                populateChain(chain, record, N);
                chain.toTop();
                Assert.assertTrue(chain.hasNext());
                chain.clear();
                chain.toTop();
                Assert.assertFalse(chain.hasNext());
            }
        });
    }

    @Test
    public void testPseudoRandomAccess() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10000;
            try (RecordChain chain = new RecordChain(metadata, SIZE_4M)) {
                Record record = new R();
                LongList rows = new LongList();
                long o = -1L;
                for (int i = 0; i < N; i++) {
                    o = chain.putRecord(record, o);
                    rows.add(o);
                }

                Assert.assertEquals(N, rows.size());

                Record expected = new R();

                for (int i = 0, n = rows.size(); i < n; i++) {
                    long row = rows.getQuick(i);
                    Record actual = chain.recordAt(row);
                    Assert.assertEquals(row, actual.getRowId());
                    assertSame(expected, actual);
                }

                Record expected2 = new R();
                Record rec2 = chain.newRecord();

                for (int i = 0, n = rows.size(); i < n; i++) {
                    long row = rows.getQuick(i);
                    chain.recordAt(rec2, row);
                    Assert.assertEquals(row, rec2.getRowId());
                    assertSame(expected2, rec2);
                }

                Record expected3 = new R();
                Record rec3 = chain.getRecord();

                for (int i = 0, n = rows.size(); i < n; i++) {
                    long row = rows.getQuick(i);
                    chain.recordAt(rec3, row);
                    Assert.assertEquals(row, rec3.getRowId());
                    assertSame(expected3, rec3);
                }
            }
        });
    }

    @Test
    public void testReuseWithClear() throws Exception {
        testChainReuseWithClearFunction(RecordChain::clear);
    }

    @Test
    public void testReuseWithClose() throws Exception {
        testChainReuseWithClearFunction(RecordChain::close);
    }

    @Test
    public void testReuseWithReleaseCursor() throws Exception {
        testChainReuseWithClearFunction(RecordChain::releaseCursor);
    }

    @Test
    public void testWriteAndRead() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    final int N = 100000;
                    Record record = new R();

//        try (RecordList records = new RecordList(metadata, 1024 * 1024 * 1024)) {
//            long o = -1L;
//            long t = 0;
//            for (int i = -N; i < N; i++) {
//                if (i == 0) {
//                    t = System.nanoTime();
//                }
//                o = records.append(record, o);
//            }
//            System.out.println(System.nanoTime() - t);
//        }

                    try (RecordChain chain = new RecordChain(metadata, 4 * 1024 * 1024L)) {
                        long o = -1L;
                        long t = 0;
                        for (int i = -N; i < N; i++) {
                            if (i == 0) {
                                t = System.nanoTime();
                            }
                            o = chain.putRecord(record, o);
                        }
                        System.out.println("RecordChain append time: " + (System.nanoTime() - t));
                        assertChain(chain, new R(), N * 2);
                        assertChain(chain, new R(), N * 2);
                    }
                }
        );
    }

    @Test
    public void testWrongColumnType() throws Exception {
        CollectionRecordMetadata metadata = new CollectionRecordMetadata();
        metadata.add(new RecordColumnMetadataImpl("int", 199));
        R r = new R();
        TestUtils.assertMemoryLeak(() -> {
            try (RecordChain chain = new RecordChain(metadata, 1024)) {
                Assert.assertNull(chain.getStorageFacade());
                chain.putRecord(r, -1);
                Assert.fail();
            } catch (CairoException ignored) {
            }
        });
    }

    private static void populateChain(RecordChain chain, Record record, int count) {
        long o = -1L;
        for (int i = 0; i < count; i++) {
            o = chain.putRecord(record, o);
        }
    }

    private void assertChain(RecordChain chain, Record r2, long expectedCount) {
        long count = 0L;
        chain.toTop();
        while (chain.hasNext()) {
            assertSame(r2, chain.next());
            count++;
        }
        //G182.60-P-063903
        Assert.assertEquals(expectedCount, count);

    }

    private void assertSame(Record expected, Record actual) {
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            switch (metadata.getColumnQuick(i).getType()) {
                case ColumnType.INT:
                    Assert.assertEquals(expected.getInt(i), actual.getInt(i));
                    break;
                case ColumnType.DOUBLE:
                    Assert.assertEquals(expected.getDouble(i), actual.getDouble(i), 0.000000001D);
                    break;
                case ColumnType.LONG:
                    Assert.assertEquals(expected.getLong(i), actual.getLong(i));
                    break;
                case ColumnType.DATE:
                    Assert.assertEquals(expected.getDate(i), actual.getDate(i));
                    break;
                case ColumnType.BOOLEAN:
                    Assert.assertEquals(expected.getBool(i), actual.getBool(i));
                    break;
                case ColumnType.BYTE:
                    Assert.assertEquals(expected.get(i), actual.get(i));
                    break;
                case ColumnType.SHORT:
                    Assert.assertEquals(expected.getShort(i), actual.getShort(i));
                    break;
                case ColumnType.SYMBOL:
                    TestUtils.assertEquals(expected.getSym(i), actual.getSym(i));
                    break;
                case ColumnType.FLOAT:
                    Assert.assertEquals(expected.getFloat(i), actual.getFloat(i), 0.00000001f);
                    break;
                case ColumnType.STRING:
                    CharSequence e = expected.getFlyweightStr(i);
                    CharSequence cs1 = actual.getFlyweightStr(i);
                    CharSequence cs2 = actual.getFlyweightStrB(i);
                    TestUtils.assertEquals(e, cs1);
                    Assert.assertFalse(cs1 != null && cs1 == cs2);
                    TestUtils.assertEquals(e, cs2);
                    if (cs1 == null) {
                        Assert.assertEquals(0, actual.getStrLen(i));
                    } else {
                        Assert.assertEquals(cs1.length(), actual.getStrLen(i));
                    }
                    break;
                case ColumnType.BINARY:
                    BinarySequence bs = expected.getBin2(i);
                    BinarySequence actBs = actual.getBin2(i);
                    if (bs == null) {
                        Assert.assertNull(actBs);
                    } else {
                        Assert.assertEquals(bs.length(), actBs.length());
                        Assert.assertEquals(bs.length(), actual.getBinLen(i));
                        for (long l = 0, z = bs.length(); l < z; l++) {
                            Assert.assertTrue(bs.byteAt(l) == actBs.byteAt(l));
                        }
                    }
                    break;
                default:
                    throw CairoException.instance(0).put("Record chain does not support: ").put(ColumnType.nameOf(metadata.getColumnQuick(i).getType()));

            }
        }
    }

    private void testChainReuseWithClearFunction(ClearFunc clear) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Record record = new R();
            Record expected = new R();
            final int N = 10000;
            try (RecordChain chain = new RecordChain(metadata, 4 * 1024 * 1024L)) {

                populateChain(chain, record, N);
                assertChain(chain, expected, N);

                clear.clear(chain);

                populateChain(chain, record, N);
                assertChain(chain, expected, N);
            }
        });
    }

    @FunctionalInterface
    private interface ClearFunc {
        void clear(RecordChain chain);
    }

    private static class R implements Record {
        final Rnd rnd = new Rnd();
        final ArrayBinarySequence abs = new ArrayBinarySequence().of(new byte[1024]);

        @Override
        public byte get(int col) {
            return rnd.nextByte();
        }

        @Override
        public BinarySequence getBin2(int col) {
            if (rnd.nextPositiveInt() % 32 == 0) {
                return null;
            }
            for (int i = 0, n = abs.array.length; i < n; i++) {
                abs.array[i] = rnd.nextByte();
            }
            return abs;
        }

        @Override
        public boolean getBool(int col) {
            return rnd.nextBoolean();
        }

        @Override
        public long getDate(int col) {
            return rnd.nextPositiveLong();
        }

        @Override
        public double getDouble(int col) {
            return rnd.nextDouble();
        }

        @Override
        public float getFloat(int col) {
            return rnd.nextFloat();
        }

        @Override
        public CharSequence getFlyweightStr(int col) {
            return rnd.nextInt() % 16 == 0 ? null : rnd.nextChars(15);
        }

        @Override
        public CharSequence getFlyweightStrB(int col) {
            return rnd.nextInt() % 16 == 0 ? null : rnd.nextChars(15);
        }

        @Override
        public int getInt(int col) {
            return rnd.nextInt();
        }

        @Override
        public long getLong(int col) {
            return rnd.nextLong();
        }

        @Override
        public long getRowId() {
            return -1;
        }

        @Override
        public short getShort(int col) {
            return rnd.nextShort();
        }

        @Override
        public int getStrLen(int col) {
            return 15;
        }

        @Override
        public CharSequence getSym(int col) {
            return rnd.nextChars(10);
        }
    }

    public static class ArrayBinarySequence implements BinarySequence {
        private byte[] array;

        @Override
        public byte byteAt(long index) {
            return array[(int) index];
        }

        @Override
        public long length() {
            return array.length;
        }

        ArrayBinarySequence of(byte[] array) {
            this.array = array;
            return this;
        }
    }

    static {
        metadata = new CollectionRecordMetadata();
        metadata.add(new RecordColumnMetadataImpl("int", ColumnType.INT));
        metadata.add(new RecordColumnMetadataImpl("long", ColumnType.LONG));
        metadata.add(new RecordColumnMetadataImpl("short", ColumnType.SHORT));
        metadata.add(new RecordColumnMetadataImpl("double", ColumnType.DOUBLE));
        metadata.add(new RecordColumnMetadataImpl("float", ColumnType.FLOAT));
        metadata.add(new RecordColumnMetadataImpl("str", ColumnType.STRING));
        metadata.add(new RecordColumnMetadataImpl("byte", ColumnType.BYTE));
        metadata.add(new RecordColumnMetadataImpl("date", ColumnType.DATE));
        metadata.add(new RecordColumnMetadataImpl("bool", ColumnType.BOOLEAN));
        metadata.add(new RecordColumnMetadataImpl("str2", ColumnType.STRING));
        metadata.add(new RecordColumnMetadataImpl("sym", ColumnType.SYMBOL));
        metadata.add(new RecordColumnMetadataImpl("bin", ColumnType.BINARY));
    }
}