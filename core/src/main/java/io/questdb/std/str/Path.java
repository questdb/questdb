/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.std.str;

import io.questdb.std.ThreadLocal;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * Builder class that allows JNI layer access CharSequence without copying memory. It is typically used
 * to create file system paths for files and directories and passing them to {@link Files} static methods, those
 * that accept @link {@link LPSZ} as input.
 * <p>
 * Instances of this class can be re-cycled for creating many different paths and
 * must be closed when no longer required.
 * </p>
 */
public class Path extends AbstractCharSink implements Closeable, LPSZ {
    public static final ThreadLocal<Path> PATH = new ThreadLocal<>(Path::new);
    public static final ThreadLocal<Path> PATH2 = new ThreadLocal<>(Path::new);
    public static final Closeable THREAD_LOCAL_CLEANER = Path::clearThreadLocals;
    private static final byte NULL = (byte) 0;
    private static final int OVERHEAD = 4;
    private int capacity;
    private long headPtr;
    private int len;
    private long tailPtr;

    public Path() {
        this(255);
    }

    public Path(int capacity) {
        assert capacity > 0;
        this.capacity = capacity;
        headPtr = tailPtr = Unsafe.malloc(capacity, MemoryTag.NATIVE_PATH);
    }

    public static void clearThreadLocals() {
        Misc.free(PATH);
        Misc.free(PATH2);
    }

    public static Path getThreadLocal(CharSequence root) {
        return PATH.get().of(root);
    }

    /**
     * Creates path from another instance of Path. The assumption is that
     * the source path is already UTF8 encoded and does not require re-encoding.
     *
     * @param root path
     * @return copy of root path
     */
    public static Path getThreadLocal(Path root) {
        return PATH.get().of(root);
    }

    public static Path getThreadLocal2(CharSequence root) {
        return PATH2.get().of(root);
    }

    public Path $() {
        if (tailPtr == headPtr || Unsafe.getUnsafe().getByte(tailPtr) != NULL) {
            long requiredSize = tailPtr - headPtr;
            if (requiredSize >= capacity) {
                extend((int) (requiredSize + 16));
            }
            Unsafe.getUnsafe().putByte(tailPtr, NULL);
        }
        return this;
    }

    public void $at(int index) {
        Unsafe.getUnsafe().putByte(headPtr + index, NULL);
    }

    @Override
    public long address() {
        return headPtr;
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public char charAt(int index) {
        return (char) Unsafe.getUnsafe().getByte(headPtr + index);
    }

    @Override
    public void close() {
        if (headPtr != NULL) {
            Unsafe.free(headPtr, capacity, MemoryTag.NATIVE_PATH);
            headPtr = tailPtr = NULL;
            len = 0;
        }
    }

    public Path concat(CharSequence str) {
        return concat(str, 0, str.length());
    }

    public Path concat(long pUtf8NameZ) {
        ensureSeparator();
        long p = pUtf8NameZ;
        while (true) {
            if (len + OVERHEAD >= capacity) {
                extend(len * 2 + OVERHEAD);
            }
            byte b = Unsafe.getUnsafe().getByte(p++);
            if (b == NULL) {
                break;
            }
            Unsafe.getUnsafe().putByte(tailPtr++, (byte) (b == '/' && Os.isWindows() ? '\\' : b));
            len++;
        }
        return this;
    }

    public Path concat(CharSequence str, int from, int to) {
        ensureSeparator();
        encodeUtf8(str, from, to);
        return this;
    }

    @Override
    public void flush() {
        $();
    }

    @Override
    public final int length() {
        return len;
    }

    public Path of(CharSequence str) {
        checkClosed();
        if (str == this) {
            len = str.length();
            tailPtr = headPtr + len;
            return this;
        } else {
            tailPtr = headPtr;
            len = 0;
            return concat(str);
        }
    }

    public Path of(Path other) {
        return of((LPSZ) other);
    }

    public Path of(LPSZ other) {
        // This is different from of(CharSequence str) because
        // another Path is already UTF8 encoded and cannot be treated as CharSequence.
        // Copy binary array representation instead of trying to UTF8 encode it
        int len = other.length();
        if (headPtr == NULL) {
            headPtr = Unsafe.malloc(len, MemoryTag.NATIVE_PATH);
            capacity = len;
        } else if (capacity < len) {
            extend(len);
        }

        if (len > 0) {
            Unsafe.getUnsafe().copyMemory(other.address(), headPtr, len);
        }
        this.len = len;
        tailPtr = headPtr + len;
        return this;
    }

    public Path of(CharSequence str, int from, int to) {
        checkClosed();
        tailPtr = headPtr;
        len = 0;
        return concat(str, from, to);
    }

    public Path parent() {
        if (len > 0) {
            int idx = len - 1;
            byte last = Unsafe.getUnsafe().getByte(headPtr + idx);
            if (last == Files.SEPARATOR || last == NULL) {
                if (idx < 2) {
                    return this;
                }
                idx--;
            }
            while (idx > 0 && (char) Unsafe.getUnsafe().getByte(headPtr + idx) != Files.SEPARATOR) {
                idx--;
            }
            len = idx;
            tailPtr = headPtr + len;
        }
        return this;
    }

    public void put(int index, char c) {
        Unsafe.getUnsafe().putByte(headPtr + index, (byte) c);
    }

    @Override
    public Path put(CharSequence str) {
        int l = str.length();
        if (l + len >= capacity) {
            extend(l + len);
        }
        Chars.asciiStrCpy(str, l, tailPtr);
        tailPtr += l;
        len += l;
        return this;
    }

    @Override
    public CharSink put(CharSequence cs, int lo, int hi) {
        int l = hi - lo;
        if (l + len >= capacity) {
            extend(l + len);
        }
        Chars.asciiStrCpy(cs, lo, l, tailPtr);
        tailPtr += l;
        len += l;
        return this;
    }

    @Override
    public Path put(char c) {
        assert c != NULL;
        if (len >= capacity) {
            extend(16 + len);
        }
        Unsafe.getUnsafe().putByte(tailPtr++, (byte) c);
        len++;
        return this;
    }

    @Override
    public Path put(int value) {
        super.put(value);
        return this;
    }

    @Override
    public Path put(long value) {
        super.put(value);
        return this;
    }

    @Override
    public CharSink put(char[] chars, int start, int len) {
        if (len + this.len >= capacity) {
            extend(len);
        }
        Chars.asciiCopyTo(chars, start, len, tailPtr);
        tailPtr += len;
        return this;
    }

    @Override
    public void putUtf8Special(char c) {
        if (c == '/' && Os.isWindows()) {
            put('\\');
        } else {
            put(c);
        }
    }

    public Path seekNull() {
        int count = 0;
        while (count < capacity) {
            if (Unsafe.getUnsafe().getByte(headPtr + count) == NULL) {
                len = count;
                tailPtr = headPtr + len;
                break;
            }
            count++;
        }
        return this;
    }

    public Path slash() {
        ensureSeparator();
        return this;
    }

    public Path slash$() {
        ensureSeparator();
        return $();
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        throw new UnsupportedOperationException();
    }

    public void toSink(CharSink sink) {
        Chars.utf8Decode(headPtr, tailPtr, sink);
    }

    @Override
    @NotNull
    public String toString() {
        if (headPtr != NULL) {
            final CharSink b = Misc.getThreadLocalBuilder();
            toSink(b);
            return b.toString();
        }
        return "";
    }

    public Path trimTo(int len) {
        this.len = len;
        tailPtr = headPtr + len;
        return this;
    }

    private void checkClosed() {
        if (headPtr == NULL) {
            headPtr = tailPtr = Unsafe.malloc(capacity, MemoryTag.NATIVE_PATH);
            len = 0;
        }
    }

    protected final void ensureSeparator() {
        if (tailPtr > headPtr && Unsafe.getUnsafe().getByte(tailPtr - 1) != Files.SEPARATOR) {
            Unsafe.getUnsafe().putByte(tailPtr++, (byte) Files.SEPARATOR);
            len++;
        }
    }

    void extend(int newCapacity) {
        assert newCapacity > capacity;
        headPtr = Unsafe.realloc(headPtr, capacity, newCapacity, MemoryTag.NATIVE_PATH);
        tailPtr = headPtr + len;
        capacity = newCapacity;
    }
}
