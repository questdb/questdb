/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.TableToken;
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
    private final static ThreadLocal<StringSink> tlBuilder = new ThreadLocal<>(StringSink::new);
    private int capacity;
    private long headPtr;
    private long tailPtr;

    public Path() {
        this(255);
    }

    public Path(int capacity) {
        assert capacity > 0;
        this.capacity = capacity;
        headPtr = tailPtr = Unsafe.malloc(capacity + 1, MemoryTag.NATIVE_PATH);
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

    public static Path getThreadLocal2(Path root) {
        return PATH2.get().of(root);
    }

    public static Path getThreadLocal2(CharSequence root) {
        return PATH2.get().of(root);
    }

    public Path $() {
        if (tailPtr == headPtr || Unsafe.getUnsafe().getByte(tailPtr) != NULL) {
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
        if (headPtr != 0L) {
            Unsafe.free(headPtr, capacity + 1, MemoryTag.NATIVE_PATH);
            headPtr = tailPtr = 0L;
        }
    }

    public Path concat(CharSequence str) {
        return concat(str, 0, str.length());
    }

    public Path concat(TableToken token) {
        return concat(token.getDirName());
    }

    public Path concat(long pUtf8NameZ) {
        ensureSeparator();
        long p = pUtf8NameZ;
        while (true) {
            byte b = Unsafe.getUnsafe().getByte(p++);
            if (b == NULL) {
                break;
            }

            int requiredCapacity = length();
            if (requiredCapacity + OVERHEAD >= capacity) {
                extend(requiredCapacity * 2 + OVERHEAD);
            }
            Unsafe.getUnsafe().putByte(tailPtr++, (byte) (b == '/' && Os.isWindows() ? '\\' : b));
        }
        return this;
    }

    public Path concat(CharSequence str, int from, int to) {
        ensureSeparator();
        encodeUtf8(str, from, to);
        return this;
    }

    public void extend(int newCapacity) {
        assert newCapacity > capacity;
        int len = length();
        headPtr = Unsafe.realloc(headPtr, capacity + 1, newCapacity + 1, MemoryTag.NATIVE_PATH);
        tailPtr = headPtr + len;
        capacity = newCapacity;
    }

    @Override
    public void flush() {
        $();
    }

    @Override
    public final int length() {
        return (int) (tailPtr - headPtr);
    }

    public Path of(CharSequence str) {
        checkClosed();
        if (str == this) {
            tailPtr = headPtr + str.length();
            return this;
        } else {
            tailPtr = headPtr;
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
        if (headPtr == 0L) {
            headPtr = Unsafe.malloc(len + 1, MemoryTag.NATIVE_PATH);
            capacity = len;
        } else if (capacity < len) {
            extend(len);
        }

        if (len > 0) {
            Unsafe.getUnsafe().copyMemory(other.address(), headPtr, len);
        }
        tailPtr = headPtr + len;
        return this;
    }

    public Path of(CharSequence str, int from, int to) {
        checkClosed();
        tailPtr = headPtr;
        return concat(str, from, to);
    }

    public Path parent() {
        if (tailPtr > headPtr) {
            long p = tailPtr - 1;
            byte last = Unsafe.getUnsafe().getByte(p);
            if (last == Files.SEPARATOR || last == NULL) {
                if (p < headPtr + 2) {
                    return this;
                }
                p--;
            }
            while (p > headPtr && (char) Unsafe.getUnsafe().getByte(p) != Files.SEPARATOR) {
                p--;
            }
            tailPtr = p;
        }
        return this;
    }

    public Path prefix(Path prefix, int prefixLen) {
        if (prefix != null) {
            if (prefixLen > 0) {
                int thisLen = length();
                checkExtend(thisLen + prefixLen);
                Vect.memmove(headPtr + prefixLen, headPtr, thisLen);
                Vect.memcpy(headPtr, prefix.address(), prefixLen);
                tailPtr += prefixLen;
            }
        }
        return this;
    }

    public void put(int index, char c) {
        Unsafe.getUnsafe().putByte(headPtr + index, (byte) c);
    }

    @Override
    public Path put(CharSequence str) {
        int l = str.length();
        checkExtend(l);
        Chars.asciiStrCpy(str, l, tailPtr);
        tailPtr += l;
        return this;
    }

    @Override
    public CharSink put(CharSequence cs, int lo, int hi) {
        int l = hi - lo;
        int requiredCapacity = length() + l;
        if (requiredCapacity > capacity) {
            extend(requiredCapacity);
        }
        Chars.asciiStrCpy(cs, lo, l, tailPtr);
        tailPtr += l;
        return this;
    }

    @Override
    public Path put(char c) {
        assert c != NULL;
        int requiredCapacity = length() + 1;
        if (requiredCapacity >= capacity) {
            extend(requiredCapacity + 15);
        }
        Unsafe.getUnsafe().putByte(tailPtr++, (byte) c);
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
        checkExtend(len);
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

    public Path seekZ() {
        int count = 0;
        while (count < capacity) {
            if (Unsafe.getUnsafe().getByte(headPtr + count) == NULL) {
                tailPtr = headPtr + count;
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
        Chars.utf8toUtf16(headPtr, tailPtr, sink);
    }

    @Override
    @NotNull
    public String toString() {
        if (headPtr != 0L) {
            // Don't use Misc.getThreadLocalBuilder() to convert Path to String.
            // This leads difficulties in debugging / running tests when FilesFacade tracks open files
            // when this method called implicitly
            final StringSink b = tlBuilder.get();
            b.clear();
            toSink(b);
            return b.toString();
        }
        return "";
    }

    public Path trimTo(int len) {
        tailPtr = headPtr + len;
        return this;
    }

    // allocates given buffer at path tail and sets it to 0
    public void zeroPad(int len) {
        checkExtend(len);
        Vect.memset(tailPtr, len, 0);
    }

    private void checkClosed() {
        if (headPtr == 0L) {
            headPtr = tailPtr = Unsafe.malloc(capacity + 1, MemoryTag.NATIVE_PATH);
        }
    }

    private void checkExtend(int len) {
        int requiredCapacity = length() + len;
        if (requiredCapacity > capacity) {
            extend(requiredCapacity);
        }
    }

    protected final void ensureSeparator() {
        if (tailPtr > headPtr && Unsafe.getUnsafe().getByte(tailPtr - 1) != Files.SEPARATOR) {
            Unsafe.getUnsafe().putByte(tailPtr++, (byte) Files.SEPARATOR);
        }
    }
}
