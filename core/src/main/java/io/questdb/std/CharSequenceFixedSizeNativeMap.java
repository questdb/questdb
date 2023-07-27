package io.questdb.std;

import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Specialized map for storing CharSequence keys and fixed-size values in native memory.
 * <p>
 * The implementation is thread-safe and it supports concurrent reads and writes. There is no limit on the number of
 * concurrent readers, but there can only be one writer at a time.
 * <p>
 * Reading and writing cannot be performed concurrently  and the writer will block until all readers have released
 * their read locks and a reader will block until the writer has finished updating the map.
 */
public final class CharSequenceFixedSizeNativeMap implements QuietCloseable {
    public static final double LOAD_FACTOR = 0.5; // todo: externalize me ?
    private static final int DEFAULT_CAPACITY = 16;
    private static final int MEMORY_TAG = MemoryTag.NATIVE_DEFAULT; // todo: externalize me?
    private static final String TOMBSTONE = "";
    private static final double TOMBSTONE_RATIO_THRESHOLD = 0.1; // todo: externalize me?
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

    @TestOnly
    public int capacity() {
        return keys.length;
    }

    /**
     * Close the map and release all memory associated with it.
     * <p>
     * No other method can be called after this method has been called. Calling any method after close() will result
     * in undefined behaviour.
     */
    @Override
    public void close() {
        valuesPtr = Unsafe.free(valuesPtr, (long) keys.length * valueSizeBytes, MEMORY_TAG);
    }

    /**
     * Get pointer to value by key or 0 if key is not found.
     * <p>
     * When a non-null pointer is returned, the caller must release the read lock by calling {@link #releaseReader()}
     * Pointer is guaranteed to be valid until the read lock is released. Upon release, the pointer cannot be used anymore.
     * <p>
     * The caller must not modify the contents of the value, as it may be shared with other threads.
     *
     * @param key key
     * @return pointer to value or 0 if key is not found
     */
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

    /**
     * Create new key-value pair or update existing one.
     * <p>
     * The caller might be blocked if another thread is updating or reading the map.
     *
     * @param key      key
     * @param valuePtr pointer to value
     * @param valueLen value length in bytes, must match value size specified in constructor
     */
    public void put(CharSequence key, long valuePtr, int valueLen) {
        assert key != null;
        assert key.length() > 0;
        assert valuePtr != 0;
        assert valueLen == valueSizeBytes;

        String immutableKey = Chars.toString(key);

        int hash = Hash.spread(Chars.hashCode(immutableKey));
        lock.writeLock().lock();
        try {
            put0(immutableKey, hash, valuePtr);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Release read lock acquired by {@link #get(CharSequence)}. The map is in read-only mode until the lock is released.
     * You must not call this method if you did not call {@link #get(CharSequence)} first or if it returned 0.
     */
    public void releaseReader() {
        assert !lock.writeLock().tryLock(); // sanity check
        lock.readLock().unlock();
    }

    /**
     * Remove key-value pair from map.
     * <p
     * When the key is not found, the method does nothing.
     *
     * @param key key
     */
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

    @TestOnly
    public int size() {
        return totalSize - tombstonesSize;
    }

    @TestOnly
    public int tombstonesSize() {
        return tombstonesSize;
    }

    private static boolean isTombstone(String key) {
        // intentionally using reference equality for tombstone check
        return key == TOMBSTONE;
    }

    private void put0(String key, int keyHash, long valuePtr) {
        // can only be called while holding write lock

        int index = keyHash & mask;
        String currentKey = keys[index];
        boolean tombstone = isTombstone(currentKey);
        while (currentKey != null && !tombstone) {
            if (Chars.equals(key, currentKey)) {
                Unsafe.getUnsafe().copyMemory(valuePtr, valuesPtr + (long) index * valueSizeBytes, valueSizeBytes);
                return;
            }
            index = (index + 1) & mask;
            currentKey = keys[index];
            tombstone = isTombstone(currentKey);
        }

        if (tombstone) {
            tombstonesSize--;
        } else {
            totalSize++;
        }

        keys[index] = key;
        Unsafe.getUnsafe().copyMemory(valuePtr, valuesPtr + (long) index * valueSizeBytes, valueSizeBytes);
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

        assert Numbers.isPow2(newCapacity);
        mask = newCapacity - 1;
        resizeThreshold = (int) (newCapacity * LOAD_FACTOR);
        tombstonesSize = 0;
        totalSize = 0; // will be incremented by put0
        for (int i = 0, n = oldKeys.length; i < n; i++) {
            String key = oldKeys[i];
            if ((key != null) && !isTombstone(key)) {
                int hash = Hash.spread(Chars.hashCode(key));
                long valuePtr = oldValuesPtr + (long) i * valueSizeBytes;
                put0(key, hash, valuePtr);
            }
        }
        Unsafe.free(oldValuesPtr, (long) oldKeys.length * valueSizeBytes, MEMORY_TAG);
    }
}
