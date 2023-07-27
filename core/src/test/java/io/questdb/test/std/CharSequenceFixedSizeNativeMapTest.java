package io.questdb.test.std;

import io.questdb.std.CharSequenceFixedSizeNativeMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.BitSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class CharSequenceFixedSizeNativeMapTest {

    private static final int VALUE_PAYLOAD_SIZE_BYTES = 65;

    @Test
    public void testConcurrent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int totalCount = 1000;
            int readerCount = 32;
            int writerCount = 4;
            int maxLongValues = VALUE_PAYLOAD_SIZE_BYTES / 8;

            CountDownLatch readerLatch = new CountDownLatch(readerCount);
            CountDownLatch writerLatch = new CountDownLatch(writerCount);
            AtomicInteger nonMonotonicReads = new AtomicInteger();
            AtomicInteger corruptedValues = new AtomicInteger();
            try (CharSequenceFixedSizeNativeMap map = new CharSequenceFixedSizeNativeMap(VALUE_PAYLOAD_SIZE_BYTES)) {
                Runnable[] readers = new Runnable[readerCount];
                for (int i = 0; i < readerCount; i++) {
                    Runnable getterTask = () -> {
                        BitSet bitSet = new BitSet();
                        bitSet.set(0, totalCount);
                        // keep reading random keys until all keys are read
                        while (!bitSet.isEmpty()) {
                            int candidate = ThreadLocalRandom.current().nextInt(totalCount);
                            long ptr = map.get(String.valueOf(candidate));
                            if (ptr != 0) {
                                for (int k = 0; k < maxLongValues; k += 8) {
                                    if (Unsafe.getUnsafe().getLong(ptr + k) != k) {
                                        // well, this is unexpected value for this key
                                        corruptedValues.incrementAndGet();
                                    }
                                }
                                bitSet.clear(candidate);
                                map.releaseReader();
                            } else if (!bitSet.get(candidate)) {
                                // this value was already set before, but now it's not.
                                // given there are no removals than that's clearly a violation
                                nonMonotonicReads.incrementAndGet();
                            }
                        }
                        readerLatch.countDown();
                    };
                    readers[i] = getterTask;
                }

                Runnable[] writers = new Runnable[writerCount];
                for (int i = 0; i < writerCount; i++) {
                    Runnable putterTask = () -> {
                        long ptr = Unsafe.malloc(VALUE_PAYLOAD_SIZE_BYTES, MemoryTag.NATIVE_DEFAULT);
                        try {
                            BitSet bitSet = new BitSet();
                            bitSet.set(0, totalCount);

                            // keep writing to random keys until all keys are written
                            while (!bitSet.isEmpty()) {
                                int candidate = ThreadLocalRandom.current().nextInt(totalCount);
                                for (int k = 0; k < maxLongValues; k += 8) {
                                    Unsafe.getUnsafe().putLong(ptr + k, k);
                                }
                                map.put(String.valueOf(candidate), ptr, VALUE_PAYLOAD_SIZE_BYTES);
                                bitSet.clear(candidate);
                            }
                        } finally {
                            Unsafe.free(ptr, VALUE_PAYLOAD_SIZE_BYTES, MemoryTag.NATIVE_DEFAULT);
                            writerLatch.countDown();
                        }
                    };
                    writers[i] = putterTask;
                }

                for (int i = 0; i < readerCount; i++) {
                    new Thread(readers[i]).start();
                }
                for (int i = 0; i < writerCount; i++) {
                    new Thread(writers[i]).start();
                }

                readerLatch.await();
                Assert.assertEquals(0, nonMonotonicReads.get());
                Assert.assertEquals(0, corruptedValues.get());
                writerLatch.await();
            }
        });
    }

    @Test
    public void testLocking() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String key1 = "key1";
            try (CharSequenceFixedSizeNativeMap map = new CharSequenceFixedSizeNativeMap(VALUE_PAYLOAD_SIZE_BYTES)) {
                long ptr = Unsafe.malloc(VALUE_PAYLOAD_SIZE_BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    map.put(key1, ptr, VALUE_PAYLOAD_SIZE_BYTES);
                    Assert.assertEquals(0, map.get("missing"));

                    // this should not block because the previous get() returned 0
                    map.put(key1, ptr, VALUE_PAYLOAD_SIZE_BYTES);

                    // now get() and existing key. this should acquire a read lock
                    Assert.assertNotEquals(0, map.get(key1));

                    map.releaseReader();
                    // now we should be able to write
                    map.put(key1, ptr, VALUE_PAYLOAD_SIZE_BYTES);
                } finally {
                    Unsafe.free(ptr, VALUE_PAYLOAD_SIZE_BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testPutGetRemoveGet() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CharSequenceFixedSizeNativeMap map = new CharSequenceFixedSizeNativeMap(VALUE_PAYLOAD_SIZE_BYTES)) {
                long ptr = Unsafe.malloc(VALUE_PAYLOAD_SIZE_BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < VALUE_PAYLOAD_SIZE_BYTES; i++) {
                        Unsafe.getUnsafe().putByte(ptr + i, (byte) i);
                    }
                    map.put("key1", ptr, VALUE_PAYLOAD_SIZE_BYTES);
                    long valPtr = map.get("key1");
                    try {
                        Assert.assertNotEquals(0, valPtr);
                        for (int i = 0; i < VALUE_PAYLOAD_SIZE_BYTES; i++) {
                            Assert.assertEquals((byte) i, Unsafe.getUnsafe().getByte(valPtr + i));
                        }
                    } finally {
                        map.releaseReader();
                    }
                    map.remove("key1");
                    Assert.assertEquals(0, map.get("key1"));
                } finally {
                    Unsafe.free(ptr, VALUE_PAYLOAD_SIZE_BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testSmoke() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String keySequence1 = "seq1";
            String keySequence2 = "seq2";
            String keyConst1 = "const1";
            String keyConst2 = "const2";
            String keyMissing = "missing";
            String keyToRewrite = "rewrite";
            try (CharSequenceFixedSizeNativeMap map = new CharSequenceFixedSizeNativeMap(VALUE_PAYLOAD_SIZE_BYTES)) {
                long ptr = Unsafe.malloc(VALUE_PAYLOAD_SIZE_BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < VALUE_PAYLOAD_SIZE_BYTES; i++) {
                        Unsafe.getUnsafe().putByte(ptr + i, (byte) i);
                    }
                    map.put(keySequence1, ptr, VALUE_PAYLOAD_SIZE_BYTES);
                    map.put(keySequence2, ptr, VALUE_PAYLOAD_SIZE_BYTES);
                    map.put(keyToRewrite, ptr, VALUE_PAYLOAD_SIZE_BYTES);

                    for (int i = 0; i < VALUE_PAYLOAD_SIZE_BYTES; i++) {
                        Unsafe.getUnsafe().putByte(ptr + i, (byte) 42);
                    }
                    map.put(keyConst1, ptr, VALUE_PAYLOAD_SIZE_BYTES);
                    map.put(keyConst2, ptr, VALUE_PAYLOAD_SIZE_BYTES);
                    map.put(keyToRewrite, ptr, VALUE_PAYLOAD_SIZE_BYTES);
                } finally {
                    Unsafe.free(ptr, VALUE_PAYLOAD_SIZE_BYTES, MemoryTag.NATIVE_DEFAULT);
                }

                Assert.assertEquals(0, map.get(keyMissing));

                ptr = map.get(keySequence1);
                Assert.assertNotEquals(0, ptr);
                try {
                    for (int i = 0; i < VALUE_PAYLOAD_SIZE_BYTES; i++) {
                        Assert.assertEquals((byte) i, Unsafe.getUnsafe().getByte(ptr + i));
                    }
                } finally {
                    map.releaseReader();
                }

                ptr = map.get(keySequence2);
                Assert.assertNotEquals(0, ptr);
                try {
                    for (int i = 0; i < VALUE_PAYLOAD_SIZE_BYTES; i++) {
                        Assert.assertEquals((byte) i, Unsafe.getUnsafe().getByte(ptr + i));
                    }
                } finally {
                    map.releaseReader();
                }

                ptr = map.get(keyConst1);
                Assert.assertNotEquals(0, ptr);
                try {
                    for (int i = 0; i < VALUE_PAYLOAD_SIZE_BYTES; i++) {
                        Assert.assertEquals((byte) 42, Unsafe.getUnsafe().getByte(ptr + i));
                    }
                } finally {
                    map.releaseReader();
                }

                ptr = map.get(keyConst2);
                Assert.assertNotEquals(0, ptr);
                try {
                    for (int i = 0; i < VALUE_PAYLOAD_SIZE_BYTES; i++) {
                        Assert.assertEquals((byte) 42, Unsafe.getUnsafe().getByte(ptr + i));
                    }
                } finally {
                    map.releaseReader();
                }

                ptr = map.get(keyToRewrite);
                Assert.assertNotEquals(0, ptr);
                try {
                    for (int i = 0; i < VALUE_PAYLOAD_SIZE_BYTES; i++) {
                        Assert.assertEquals((byte) 42, Unsafe.getUnsafe().getByte(ptr + i));
                    }
                } finally {
                    map.releaseReader();
                }
            }
        });
    }

    @Test
    public void testTombstoneCompaction() throws Exception {
        int baseCount = 10000;
        TestUtils.assertMemoryLeak(() -> {
            try (CharSequenceFixedSizeNativeMap map = new CharSequenceFixedSizeNativeMap(VALUE_PAYLOAD_SIZE_BYTES)) {
                long ptr = Unsafe.malloc(VALUE_PAYLOAD_SIZE_BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    int lowerBound = 0;
                    int upperBound = baseCount / 2;
                    insertEntries(map, lowerBound, upperBound, ptr);
                    Assert.assertEquals(upperBound, map.size());
                    Assert.assertEquals(lowerBound, map.tombstonesSize());
                    int expectedCapacity = Numbers.ceilPow2((int) (upperBound / CharSequenceFixedSizeNativeMap.LOAD_FACTOR));
                    Assert.assertEquals(expectedCapacity, map.capacity());

                    removeEntries(map, lowerBound, upperBound);
                    assertNoEntries(map, lowerBound, upperBound);
                    Assert.assertEquals(lowerBound, map.size());
                    Assert.assertEquals(upperBound, map.tombstonesSize());

                    lowerBound = baseCount / 4;
                    upperBound = baseCount * 2;
                    insertEntries(map, lowerBound, upperBound, ptr);
                    assertNoEntries(map, 0, lowerBound);
                    assertEntriesExistsAndValid(map, lowerBound, upperBound);

                    int expectedSize = upperBound - lowerBound;
                    Assert.assertEquals(expectedSize, map.size());
                    expectedCapacity = Numbers.ceilPow2((int) (expectedSize / CharSequenceFixedSizeNativeMap.LOAD_FACTOR));
                    Assert.assertEquals(expectedCapacity, map.capacity());
                } finally {
                    Unsafe.free(ptr, VALUE_PAYLOAD_SIZE_BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    private static void assertEntriesExistsAndValid(CharSequenceFixedSizeNativeMap map, int fromInc, int toExc) {
        for (int i = fromInc; i < toExc; i++) {
            long valPtr = map.get(String.valueOf(i));
            Assert.assertNotEquals(0, valPtr);
            try {
                for (int j = 0; j < VALUE_PAYLOAD_SIZE_BYTES; j++) {
                    Assert.assertEquals((byte) i, Unsafe.getUnsafe().getByte(valPtr + j));
                }
            } finally {
                map.releaseReader();
            }
        }

    }

    private static void assertNoEntries(CharSequenceFixedSizeNativeMap map, int fromInc, int toExc) {
        for (int i = fromInc; i < toExc; i++) {
            Assert.assertEquals(0, map.get(String.valueOf(i)));
        }
    }

    private static void insertEntries(CharSequenceFixedSizeNativeMap map, int fromInc, int toExc, long bufferPtr) {
        for (int i = fromInc; i < toExc; i++) {
            for (int j = 0; j < VALUE_PAYLOAD_SIZE_BYTES; j++) {
                Unsafe.getUnsafe().putByte(bufferPtr + j, (byte) i);
            }
            map.put(String.valueOf(i), bufferPtr, VALUE_PAYLOAD_SIZE_BYTES);
        }
    }

    private static void removeEntries(CharSequenceFixedSizeNativeMap map, int fromInc, int toExc) {
        for (int i = fromInc; i < toExc; i++) {
            map.remove(String.valueOf(i));
        }
    }
}