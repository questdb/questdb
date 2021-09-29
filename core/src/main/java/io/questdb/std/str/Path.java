/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
    public static final Closeable CLEANER = Path::clearThreadLocals;
    private static final int OVERHEAD = 4;
    private long ptr;
    private long wptr;
    private int capacity;
    private int len;

    public Path() {
        this(255);
    }

    public Path(int capacity) {
        this.capacity = capacity;
        this.ptr = this.wptr = Unsafe.malloc(capacity + 1, MemoryTag.NATIVE_DEFAULT);
    }

    public static void clearThreadLocals() {
        Misc.free(PATH.get());
        PATH.remove();

        Misc.free(PATH2.get());
        PATH2.remove();
    }

    public static Path getThreadLocal(CharSequence root) {
        return PATH.get().of(root);
    }

    public static Path getThreadLocal2(CharSequence root) {
        return PATH2.get().of(root);
    }

    public Path $() {
        if (1 + (wptr - ptr) >= capacity) {
            extend((int) (16 + (wptr - ptr)));
        }
        Unsafe.getUnsafe().putByte(wptr++, (byte) 0);
        return this;
    }

    public Path slash$() {
        ensureSeparator();
        return $();
    }

    @Override
    public long address() {
        return ptr;
    }

    /**
     * Removes trailing zero from path to allow reuse of path as parent.
     *
     * @return instance of this
     */
    public Path chop$() {
        trimTo(this.length());
        return this;
    }

    @Override
    public void close() {
        if (ptr != 0) {
            Unsafe.free(ptr, capacity + 1, MemoryTag.NATIVE_DEFAULT);
            ptr = 0;
        }
    }

    public Path concat(CharSequence str) {
        return concat(str, 0, str.length());
    }

    public Path concat(long lpsz) {

        ensureSeparator();

        long p = lpsz;
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
    public CharSink put(char[] chars, int start, int len) {
        if (len + this.len >= capacity) {
            extend(len);
        }
        Chars.asciiCopyTo(chars, start, len, wptr);
        wptr += len;
        return this;
    }

    @Override
    public final int length() {
        return len;
    }

    @Override
    public char charAt(int index) {
        return (char) Unsafe.getUnsafe().getByte(ptr + index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        throw new UnsupportedOperationException();
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

    private void checkClosed() {
        if (ptr == 0) {
            this.ptr = this.wptr = Unsafe.malloc(capacity + 1, MemoryTag.NATIVE_DEFAULT);
        }
    }

    public Path of(CharSequence str, int from, int to) {
        checkClosed();
        this.wptr = ptr;
        this.len = 0;
        return concat(str, from, to);
    }

    @Override
    public Path put(long value) {
        super.put(value);
        return this;
    }

    @Override
    public Path put(int value) {
        super.put(value);
        return this;
    }

    @Override
    protected void putUtf8Special(char c) {
        if (c == '/' && Os.type == Os.WINDOWS) {
            put('\\');
        } else {
            put(c);
        }
    }

    public Path slash() {
        ensureSeparator();
        return this;
    }

    @Override
    @NotNull
    public String toString() {
        return ptr == 0 ? "" : AbstractCharSequence.getString(this);
    }

    public Path trimTo(int len) {
        this.len = len;
        wptr = ptr + len;
        return this;
    }

    private void copy(CharSequence str, int from, int to) {
        encodeUtf8(str, from, to);
    }

    protected final void ensureSeparator() {
        if (missingTrailingSeparator()) {
            Unsafe.getUnsafe().putByte(wptr, (byte) Files.SEPARATOR);
            wptr++;
            this.len++;
        }
    }

    private void extend(int len) {
        long p = Unsafe.realloc(ptr, this.capacity + 1, len + 1, MemoryTag.NATIVE_DEFAULT);
        long d = wptr - ptr;
        this.ptr = p;
        this.wptr = p + d;
        this.capacity = len;
    }

    private boolean missingTrailingSeparator() {
        return len > 0 && Unsafe.getUnsafe().getByte(wptr - 1) != Files.SEPARATOR;
    }
}
