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
    private static final int OVERHEAD = 4;
    private int capacity;
    private int len;
    private long ptr;
    private long wptr;

    public Path() {
        this(255);
    }

    public Path(int capacity) {
        this.capacity = capacity;
        this.ptr = this.wptr = Unsafe.malloc(capacity + 1, MemoryTag.NATIVE_PATH);
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
        if (wptr == ptr || Unsafe.getUnsafe().getByte(wptr - 1) != (byte) 0) {
            if (1 + (wptr - ptr) >= capacity) {
                extend((int) (16 + (wptr - ptr)));
            }
            Unsafe.getUnsafe().putByte(wptr++, (byte) 0);
        }
        return this;
    }

    public void $at(int index) {
        Unsafe.getUnsafe().putByte(ptr + index, (byte) 0);
    }

    @Override
    public long address() {
        return ptr;
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public char charAt(int index) {
        return (char) Unsafe.getUnsafe().getByte(ptr + index);
    }

    /**
     * Removes trailing zero from path to allow reuse of path as parent.
     *
     * @return instance of this
     */
    public Path chop$() {
        trimTo(length());
        return this;
    }

    @Override
    public void close() {
        if (ptr != 0) {
            Unsafe.free(ptr, capacity + 1, MemoryTag.NATIVE_PATH);
            ptr = 0;
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
            if (b == 0) {
                break;
            }

            Unsafe.getUnsafe().putByte(wptr, (byte) (b == '/' && Os.type == Os.WINDOWS ? '\\' : b));
            wptr++;
            len++;
        }

        return this;
    }

    public Path concat(CharSequence str, int from, int to) {
        ensureSeparator();
        copy(str, from, to);
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
            this.len = str.length();
            this.wptr = ptr + len;
            return this;
        } else {
            this.wptr = ptr;
            this.len = 0;
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
        if (this.ptr == 0) {
            this.ptr = Unsafe.malloc(len + 1, MemoryTag.NATIVE_PATH);
            this.capacity = len;
        } else if (this.capacity < len) {
            extend(len);
        }

        if (len > 0) {
            Unsafe.getUnsafe().copyMemory(other.address(), this.ptr, len);
        }
        this.len = len;
        this.wptr = this.ptr + this.len;
        return this;
    }

    public Path of(CharSequence str, int from, int to) {
        checkClosed();
        this.wptr = ptr;
        this.len = 0;
        return concat(str, from, to);
    }

    public Path parent() {
        if (len > 0) {
            int idx = len - 1;
            char last = (char) Unsafe.getUnsafe().getByte(ptr + idx);
            if (last == Files.SEPARATOR || last == '\0') {
                if (idx < 2) {
                    return this;
                }
                idx--;
            }
            while (idx > 0 && (char) Unsafe.getUnsafe().getByte(ptr + idx) != Files.SEPARATOR) {
                idx--;
            }
            len = idx;
            wptr = ptr + len;
        }
        return this;
    }

    public void put(int index, char c) {
        Unsafe.getUnsafe().putByte(ptr + index, (byte) c);
    }

    @Override
    public Path put(CharSequence str) {
        int l = str.length();
        if (l + len >= capacity) {
            extend(l + len);
        }
        Chars.asciiStrCpy(str, l, wptr);
        wptr += l;
        len += l;
        return this;
    }

    @Override
    public CharSink put(CharSequence cs, int lo, int hi) {
        int l = hi - lo;
        if (l + len >= capacity) {
            extend(l + len);
        }
        Chars.asciiStrCpy(cs, lo, l, wptr);
        wptr += l;
        len += l;
        return this;
    }

    @Override
    public Path put(char c) {
        if (1 + len >= capacity) {
            extend(16 + len);
        }
        Unsafe.getUnsafe().putByte(wptr++, (byte) c);
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
        Chars.asciiCopyTo(chars, start, len, wptr);
        wptr += len;
        return this;
    }

    @Override
    public void putUtf8Special(char c) {
        if (c == '/' && Os.type == Os.WINDOWS) {
            put('\\');
        } else {
            put(c);
        }
    }

    public Path seekZ() {
        int count = 0;
        while (count < capacity + 1) {
            if (Unsafe.getUnsafe().getByte(ptr + count) == 0) {
                len = count;
                wptr = ptr + len;
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
        if (Unsafe.getUnsafe().getByte(wptr - 1) == 0) {
            Chars.utf8Decode(ptr, wptr - 1, sink);
        } else {
            Chars.utf8Decode(ptr, wptr, sink);
        }
    }

    @Override
    @NotNull
    public String toString() {
        if (ptr != 0) {
            final CharSink b = Misc.getThreadLocalBuilder();
            toSink(b);
            return b.toString();
        }
        return "";
    }

    public Path trimTo(int len) {
        this.len = len;
        wptr = ptr + len;
        return this;
    }

    private void checkClosed() {
        if (ptr == 0) {
            this.ptr = this.wptr = Unsafe.malloc(capacity + 1, MemoryTag.NATIVE_PATH);
        }
    }

    private void copy(CharSequence str, int from, int to) {
        encodeUtf8(str, from, to);
    }

    private boolean missingTrailingSeparator() {
        return len > 0 && Unsafe.getUnsafe().getByte(wptr - 1) != Files.SEPARATOR;
    }

    protected final void ensureSeparator() {
        if (missingTrailingSeparator()) {
            Unsafe.getUnsafe().putByte(wptr, (byte) Files.SEPARATOR);
            wptr++;
            len++;
        }
    }

    void extend(int len) {
        long p = Unsafe.realloc(ptr, this.capacity + 1, len + 1, MemoryTag.NATIVE_PATH);
        long d = wptr - ptr;
        this.ptr = p;
        this.wptr = p + d;
        this.capacity = len;
    }
}
