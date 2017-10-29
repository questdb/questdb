package com.questdb.std.str;

import com.questdb.misc.Unsafe;
import com.questdb.std.Mutable;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

public class DirectCharSink extends AbstractCharSink implements CharSequence, Closeable, Mutable, DirectBytes {
    private long ptr;
    private int capacity;
    private long lo;
    private long hi;

    public DirectCharSink(int capacity) {
        ptr = Unsafe.malloc(capacity);
        this.capacity = capacity;
        this.lo = ptr;
        this.hi = ptr + capacity;
    }

    @Override
    public long address() {
        return ptr;
    }

    @Override
    public int byteLength() {
        return (int) (lo - ptr);
    }

    @Override
    public void clear() {
        lo = ptr;
    }

    @Override
    public void close() {
        Unsafe.free(ptr, capacity);
    }

    @Override
    public int length() {
        return (int) (lo - ptr) / 2;
    }

    @Override
    public char charAt(int index) {
        return Unsafe.getUnsafe().getChar(ptr + index * 2);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSink put(CharSequence cs) {
        int l = cs.length();
        int l2 = l * 2;

        if (lo + l2 >= hi) {
            resize((int) Math.max(capacity * 2, (lo - ptr + l2) * 2));
        }

        for (int i = 0; i < l; i++) {
            Unsafe.getUnsafe().putChar(lo + i * 2, cs.charAt(i));
        }
        this.lo += l2;
        return this;
    }

    @Override
    public CharSink put(char c) {
        if (lo == hi) {
            resize(this.capacity * 2);
        }
        Unsafe.getUnsafe().putChar(lo, c);
        lo += 2;
        return this;
    }

    @NotNull
    @Override
    public String toString() {
        return AbstractCharSequence.getString(this);
    }

    private void resize(int cap) {
        long temp = Unsafe.malloc(cap);
        int len = (int) (lo - ptr);
        Unsafe.getUnsafe().copyMemory(ptr, temp, len);
        Unsafe.free(ptr, capacity);

        this.ptr = temp;
        this.capacity = cap;
        this.lo = ptr + len;
        this.hi = ptr + cap;
    }
}
