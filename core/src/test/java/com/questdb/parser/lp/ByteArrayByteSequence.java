package com.questdb.parser.lp;

import com.questdb.std.str.AbstractCharSequence;
import com.questdb.std.str.ByteSequence;

class ByteArrayByteSequence extends AbstractCharSequence implements ByteSequence {

    private final byte[] array;
    private int top = 0;
    private int len;

    public ByteArrayByteSequence(byte[] array) {
        this.array = array;
        this.len = array.length;
    }

    @Override
    public byte byteAt(int index) {
        return array[top + index];
    }

    @Override
    public int length() {
        return len;
    }

    @Override
    public char charAt(int index) {
        return (char) byteAt(index);
    }

    ByteArrayByteSequence limit(int top, int len) {
        this.top = top;
        this.len = len;
        return this;
    }
}
