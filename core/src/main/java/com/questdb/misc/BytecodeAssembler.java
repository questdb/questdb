/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.misc;

import com.questdb.std.Mutable;
import com.questdb.std.str.CharSink;
import com.questdb.txt.sink.AbstractCharSink;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BytecodeAssembler implements Mutable {

    public static final int iload_2 = 28;
    public static final int aload_0 = 42;
    public static final int ineg = 0x74;
    public static final int aload_1 = 43;
    public static final int aload_2 = 44;
    public static final int istore_2 = 61;
    public static final int ifne = 154;
    public static final int ireturn = 172;
    public static final int return_ = 177;
    public static final int getfield = 180;
    public static final int putfield = 181;
    public static final int invokevirtual = 182;
    public static final int invokestatic = 184;
    public static final int invokeinterface = 185;
    public static final int i2l = 0x85;
    public static final int i2f = 0x86;
    public static final int i2d = 0x87;
    public static final int l2i = 0x88;
    public static final int l2f = 0x89;
    public static final int l2d = 0x8A;
    public static final int f2i = 0x8B;
    public static final int f2l = 0x8C;
    public static final int f2d = 0x8D;
    public static final int d2i = 0x8E;
    public static final int d2l = 0x8F;
    public static final int d2f = 0x90;
    public static final int i2b = 0x91;
    public static final int i2s = 0x93;
    private static final int iconst_0 = 3;
    private static final int bipush = 16;
    private static final int sipush = 17;
    private static final int invokespecial = 183;
    private static final int O_POOL_COUNT = 8;

    private final Utf8Appender utf8Appender = new Utf8Appender();
    private ByteBuffer buf;
    private int poolCount;
    private int objectClassIndex;
    private int defaultConstructorNameIndex;
    private int defaultConstructorDescIndex;
    private int defaultConstructorMethodIndex;
    private int codeAttributeIndex;
    private int methodCut1;
    private int methodCut2;

    public BytecodeAssembler() {
        this.buf = ByteBuffer.allocate(1024).order(ByteOrder.BIG_ENDIAN);
        this.poolCount = 1;
    }

    @Override
    public void clear() {
        this.buf.clear();
        this.poolCount = 1;
    }

    public void defineClass(int flags, int thisClassIndex) {
        // access flags
        putShort(flags);
        // this class index
        putShort(thisClassIndex);
        // super class
        putShort(objectClassIndex);
    }

    public void defineDefaultConstructor() {
        // constructor method entry
        startMethod(1, defaultConstructorNameIndex, defaultConstructorDescIndex, 1, 1);
        // code
        put(aload_0);
        put(invokespecial);
        putShort(defaultConstructorMethodIndex);
        put(return_);
        endMethodCode();
        // exceptions
        putShort(0);
        // attribute count
        putShort(0);
        endMethod();
    }

    public void defineField(int flags, int nameIndex, int typeIndex) {
        putShort(flags);
        putShort(nameIndex);
        putShort(typeIndex);
        // attribute count
        putShort(0);
    }

    public void dump(String path) {
        try (FileOutputStream fos = new FileOutputStream(path)) {
            int p = buf.position();
            int l = buf.limit();
            buf.flip();
            fos.getChannel().write(buf);
            buf.limit(l);
            buf.position(p);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void endMethod() {
        putInt(methodCut1, position() - methodCut1 - 4);
    }

    public void endMethodCode() {
        putInt(methodCut2, position() - methodCut2 - 4);
    }

    public void finishPool() {
        putShort(O_POOL_COUNT, poolCount);
    }

    public byte get(int pos) {
        return buf.get(pos);
    }

    public void invokeInterface(int interfaceIndex, int argCount) {
        put(invokeinterface);
        putShort(interfaceIndex);
        put(argCount + 1);
        put(0);
    }

    public void invokeStatic(int index) {
        put(invokestatic);
        putShort(index);
    }

    public void invokeVirtual(int index) {
        put(invokevirtual);
        putShort(index);
    }

    @SuppressWarnings("unchecked")
    public <T> Class<T> loadClass(Class<?> host) {
        byte b[] = new byte[position()];
        System.arraycopy(buf.array(), 0, b, 0, b.length);
        return (Class<T>) Unsafe.getUnsafe().defineAnonymousClass(host, b, null);
    }

    public int poolClass(int classIndex) {
        put(0x07);
        putShort(classIndex);
        return poolCount++;
    }

    public int poolClass(Class clazz) {
        String name = clazz.getName();
        put(0x01);
        int n;
        putShort(n = name.length());
        for (int i = 0; i < n; i++) {
            char c = name.charAt(i);
            switch (c) {
                case '.':
                    put('/');
                    break;
                default:
                    put(c);
                    break;
            }
        }
        return poolClass(this.poolCount++);
    }

    public int poolField(int classIndex, int nameAndTypeIndex) {
        return poolRef(0x09, classIndex, nameAndTypeIndex);
    }

    public int poolInterfaceMethod(int classIndex, int nameAndTypeIndex) {
        return poolRef(0x0B, classIndex, nameAndTypeIndex);
    }

    public int poolMethod(int classIndex, int nameAndTypeIndex) {
        return poolRef(0x0A, classIndex, nameAndTypeIndex);
    }

    public int poolNameAndType(int nameIndex, int typeIndex) {
        return poolRef(0x0C, nameIndex, typeIndex);
    }

    public Utf8Appender poolUtf8() {
        put(0x01);
        utf8Appender.lenpos = position();
        utf8Appender.utf8len = 0;
        putShort(0);
        return utf8Appender;
    }

    public int poolUtf8(CharSequence cs) {
        put(0x01);
        int n;
        putShort(n = cs.length());
        for (int i = 0; i < n; i++) {
            put(cs.charAt(i));
        }
        return this.poolCount++;
    }

    public int position() {
        return buf.position();
    }

    public void put(int b) {
        if (buf.remaining() == 0) {
            resize();
        }
        buf.put((byte) b);
    }

    public void putConstant(int v) {
        if (v > -1 && v < 6) {
            put(iconst_0 + v);
        } else if (v < 0) {
            put(sipush);
            putShort(v);
        } else if (v < 128) {
            put(bipush);
            put(v);
        } else {
            put(sipush);
            putShort(v);
        }
    }

    public void putShort(int v) {
        putShort((short) v);
    }

    public void putShort(int pos, int v) {
        buf.putShort(pos, (short) v);
    }

    public void putStackMapAppendInt(int stackMapTableIndex, int position) {
        putShort(stackMapTableIndex);
        int lenPos = position();
        // length - we will come back here
        putInt(0);
        // number of entries
        putShort(1);
        // frame type APPEND
        put(0xFC);
        // offset delta - points at branch target
        putShort(position);
        // type: int
        put(0x01);
        // fix attribute length
        putInt(lenPos, position() - lenPos - 4);
    }

    public void setupPool() {
        // magic
        putInt(0xCAFEBABE);
        // version
        putInt(0x33);
        // skip pool count, write later when we know the value
        putShort(0);

        // add standard stuff
        objectClassIndex = poolClass(Object.class);
        defaultConstructorMethodIndex = poolMethod(objectClassIndex, poolNameAndType(
                defaultConstructorNameIndex = poolUtf8("<init>"),
                defaultConstructorDescIndex = poolUtf8("()V"))
        );
        codeAttributeIndex = poolUtf8("Code");
    }

    public void startMethod(int flags, int nameIndex, int descriptorIndex, int maxStack, int maxLocal) {
        // access flags
        putShort(flags);
        // name index
        putShort(nameIndex);
        // descriptor index
        putShort(descriptorIndex);
        // attribute count
        putShort(1);

        // code
        putShort(codeAttributeIndex);

        // come back to this later
        this.methodCut1 = position();
        // attribute len
        putInt(0);
        // max stack
        putShort(maxStack);
        // max locals
        putShort(maxLocal);

        // code len
        this.methodCut2 = position();
        putInt(0);

    }

    private int poolRef(int op, int name, int type) {
        put(op);
        putShort(name);
        putShort(type);
        return poolCount++;
    }

    private void putInt(int pos, int v) {
        buf.putInt(pos, v);
    }

    private void putInt(int v) {
        if (buf.remaining() < 4) {
            resize();
        }
        buf.putInt(v);
    }

    private void putShort(short v) {
        if (buf.remaining() < 2) {
            resize();
        }
        buf.putShort(v);
    }

    private void resize() {
        ByteBuffer b = ByteBuffer.allocate(buf.capacity() * 2).order(ByteOrder.BIG_ENDIAN);
        System.arraycopy(buf.array(), 0, b.array(), 0, buf.capacity());
        b.position(buf.position());
        buf = b;
    }

    public class Utf8Appender extends AbstractCharSink implements CharSink {
        private int utf8len = 0;
        private int lenpos;

        public int $() {
            putShort(lenpos, utf8len);
            return poolCount++;
        }

        @Override
        public void flush() throws IOException {

        }

        @Override
        public Utf8Appender put(CharSequence cs) {
            int n = cs.length();
            for (int i = 0; i < n; i++) {
                BytecodeAssembler.this.put(cs.charAt(i));
            }
            utf8len += n;
            return this;
        }

        @Override
        public Utf8Appender put(char c) {
            BytecodeAssembler.this.put(c);
            utf8len++;
            return this;
        }

        @Override
        public Utf8Appender put(int value) {
            super.put(value);
            return this;
        }
    }
}
