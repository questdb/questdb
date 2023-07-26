package io.questdb.std;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// this might not necessarily be the best possible data structure as open addressing with linear probing
// forces us to use a low load factor. this means we are wasting a lot of memory for unused value slots
public final class CharSequenceFixedSizeNativeMap implements QuietCloseable {
    private static final int DEFAULT_CAPACITY = 16;
    private static final double LOAD_FACTOR = 0.5; // todo: externalize me
    private static final int MEMORY_TAG = MemoryTag.NATIVE_DEFAULT; // todo: externalize me
    private static final String TOMBSTONE = "";
    private static final double TOMBSTONE_RATIO_THRESHOLD = 0.1; // todo: externalize me
    private final ReadWriteLock lock = new ReentrantReadWriteLock(false);
    private final int valueSizeBytes;
    private String[] keys;
    private int mask;
    private int resizeThreshold;
    private int tombstonesSize;
    private int totalSize; // includes tombstones
    private long valuesPtr;

    public CharSequenceFixedSizeNativeMap(int valueSizeBytes) {
        this.keys = new String[DEFAULT_CAPACITY];
        this.valueSizeBytes = valueSizeBytes;
        this.valuesPtr = Unsafe.malloc((long) DEFAULT_CAPACITY * valueSizeBytes, MEMORY_TAG);
        this.mask = DEFAULT_CAPACITY - 1;
        this.totalSize = 0;
        this.resizeThreshold = (int) (DEFAULT_CAPACITY * LOAD_FACTOR);
        this.tombstonesSize = 0;
    }

    @Override
    public void close() {
        valuesPtr = Unsafe.free(valuesPtr, (long) keys.length * valueSizeBytes, MEMORY_TAG);
    }

    public long get(CharSequence key) {
        assert key != null;
        assert key.length() > 0;

        int hash = Hash.spread(Chars.hashCode(key));
        lock.readLock().lock();
        int index = hash & mask;

        String currentKey = keys[index];
        while (currentKey != null) {
            if (Chars.equals(key, currentKey)) {
                // it's a match. the caller is now expected to release the read lock
                return valuesPtr + (long) index * valueSizeBytes;
            }
            index = (index + 1) & mask;
            currentKey = keys[index];
        }
        // no match, release the read lock and return null pointer
        lock.readLock().unlock();
        return 0;
    }

    public void put(String key, long valuePtr, int valueLen) {
        assert key != null;
        assert key.length() > 0;
        assert valuePtr != 0;
        assert valueLen == valueSizeBytes;

        int hash = Hash.spread(Chars.hashCode(key));
        lock.writeLock().lock();
        try {
            put0(key, hash, valuePtr);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void releaseReader() {
        assert !lock.writeLock().tryLock(); // sanity check
        lock.readLock().unlock();
    }

    public void remove(CharSequence key) {
        assert key != null;

        int hash = Hash.spread(Chars.hashCode(key));
        lock.writeLock().lock();
        try {
            int index = hash & mask;

            String currentKey = keys[index];
            while (currentKey != null) {
                if (Chars.equals(key, keys[index])) {
                    keys[index] = TOMBSTONE;
                    tombstonesSize++;
                    return;
                }
                index = (index + 1) & mask;
                currentKey = keys[index];
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static boolean notTombstone(String key) {
        // intentionally using reference equality for tombstone check
        return key != TOMBSTONE;
    }

    private void put0(String key, int keyHash, long valuePtr) {
        int index = keyHash & mask;
        while (keys[index] != null) {
            if (Chars.equals(key, keys[index])) {
                Unsafe.getUnsafe().copyMemory(valuePtr, valuesPtr + (long) index * valueSizeBytes, valueSizeBytes);
            }
            index = (index + 1) & mask;
        }

        keys[index] = key;
        Unsafe.getUnsafe().copyMemory(valuePtr, valuesPtr + (long) index * valueSizeBytes, valueSizeBytes);
        totalSize++;
        resizeIfNeeded();
    }

    private void resizeIfNeeded() {
        // can only be called while holding write lock

        if (totalSize < resizeThreshold) {
            return;
        }

        int newCapacity;
        if (tombstonesSize > totalSize * TOMBSTONE_RATIO_THRESHOLD) {
            // too many tombstones, let's just compact first, without resizing
            newCapacity = keys.length;
        } else {
            newCapacity = keys.length * 2;
        }

        String[] oldKeys = keys;
        long oldValuesPtr = valuesPtr;

        keys = new String[newCapacity];
        valuesPtr = Unsafe.malloc((long) newCapacity * valueSizeBytes, MEMORY_TAG);
        mask = newCapacity - 1;
        resizeThreshold = (int) (newCapacity * LOAD_FACTOR);
        for (int i = 0, n = oldKeys.length; i < n; i++) {
            String key = oldKeys[i];
            if ((key != null) && notTombstone(key)) {
                int hash = Hash.spread(Chars.hashCode(key));
                long valuePtr = oldValuesPtr + (long) i * valueSizeBytes;
                put0(key, hash, valuePtr);
            }
        }
        Unsafe.free(oldValuesPtr, (long) oldKeys.length * valueSizeBytes, MEMORY_TAG);
    }
}
