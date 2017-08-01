package com.questdb.cairo;

import com.questdb.misc.Rnd;
import com.questdb.ql.Record;
import com.questdb.ql.impl.CollectionRecordMetadata;
import com.questdb.ql.impl.RecordColumnMetadataImpl;
import com.questdb.std.DirectInputStream;
import com.questdb.store.ColumnType;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.OutputStream;

public class RecordChainTest {
    private static CollectionRecordMetadata metadata;

    @Test
    public void testClear() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Record record = new R();
            final int N = 10000;
            try (RecordChain chain = new RecordChain(metadata, 4 * 1024 * 1024L)) {

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
                    // todo
                    break;
                default:
                    throw CairoException.instance(0).put("Record chain does not support: ").put(ColumnType.nameOf(metadata.getColumnQuick(i).getType()));

            }
        }
    }

    private void testChainReuseWithClearFunction(ClearFunc clear) {
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

        @Override
        public byte get(int col) {
            return rnd.nextByte();
        }

        @Override
        public void getBin(int col, OutputStream s) {
        }

        @Override
        public DirectInputStream getBin(int col) {
            return null;
        }

        @Override
        public long getBinLen(int col) {
            return 0;
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
    }
}