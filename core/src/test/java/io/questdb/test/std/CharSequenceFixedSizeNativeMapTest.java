package io.questdb.test.std;

import io.questdb.std.CharSequenceFixedSizeNativeMap;
import io.questdb.std.MemoryTag;
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

    // wip
    @Test
    public void testConcurrent() throws Exception {
        int totalCount = 1000;
        int readerCount = 20;
        int writerCount = 4;

        CountDownLatch readerLatch = new CountDownLatch(readerCount);
        AtomicInteger nonMonotonicReads = new AtomicInteger();
        try (CharSequenceFixedSizeNativeMap map = new CharSequenceFixedSizeNativeMap(VALUE_PAYLOAD_SIZE_BYTES)) {
            Runnable[] readers = new Runnable[readerCount];
            for (int i = 0; i < readerCount; i++) {
                Runnable getterTask = () -> {
                    BitSet bitSet = new BitSet();
                    bitSet.set(0, totalCount);
                    while (!bitSet.isEmpty()) {
                        int candidate = ThreadLocalRandom.current().nextInt(totalCount);
                        long ptr = map.get(String.valueOf(candidate));
                        if (ptr != 0) {
                            bitSet.clear(candidate);
                            map.releaseReader();
                        } else if (!bitSet.get(candidate)) {
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
                    for (int j = 0; j < totalCount; j++) {
                        long ptr = Unsafe.malloc(VALUE_PAYLOAD_SIZE_BYTES, MemoryTag.NATIVE_DEFAULT);
                        try {
                            for (int k = 0; k < VALUE_PAYLOAD_SIZE_BYTES; k++) {
                                Unsafe.getUnsafe().putByte(ptr + k, (byte) k);
                            }
                            map.put(String.valueOf(j), ptr, VALUE_PAYLOAD_SIZE_BYTES);
                        } finally {
                            Unsafe.free(ptr, VALUE_PAYLOAD_SIZE_BYTES, MemoryTag.NATIVE_DEFAULT);
                        }
                    }
                };
                writers[i] = putterTask;
            }
        }
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
                for (int i = 0; i < VALUE_PAYLOAD_SIZE_BYTES; i++) {
                    Assert.assertEquals((byte) i, Unsafe.getUnsafe().getByte(ptr + i));
                }

                ptr = map.get(keySequence2);
                Assert.assertNotEquals(0, ptr);
                for (int i = 0; i < VALUE_PAYLOAD_SIZE_BYTES; i++) {
                    Assert.assertEquals((byte) i, Unsafe.getUnsafe().getByte(ptr + i));
                }

                ptr = map.get(keyConst1);
                Assert.assertNotEquals(0, ptr);
                for (int i = 0; i < VALUE_PAYLOAD_SIZE_BYTES; i++) {
                    Assert.assertEquals((byte) 42, Unsafe.getUnsafe().getByte(ptr + i));
                }

                ptr = map.get(keyConst2);
                Assert.assertNotEquals(0, ptr);
                for (int i = 0; i < VALUE_PAYLOAD_SIZE_BYTES; i++) {
                    Assert.assertEquals((byte) 42, Unsafe.getUnsafe().getByte(ptr + i));
                }

                ptr = map.get(keyToRewrite);
                Assert.assertNotEquals(0, ptr);
                for (int i = 0; i < VALUE_PAYLOAD_SIZE_BYTES; i++) {
                    Assert.assertEquals((byte) 42, Unsafe.getUnsafe().getByte(ptr + i));
                }
            }
        });
    }
}