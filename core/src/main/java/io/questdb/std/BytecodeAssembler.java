/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.std;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.bytes.Bytes;
import io.questdb.std.ex.BytecodeException;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import org.jetbrains.annotations.Nullable;

import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BytecodeAssembler {

    private static final int ACC_PRIVATE = 0x02;
    private static final int ACC_PUBLIC = 0x01;
    private static final Log LOG = LogFactory.getLog(BytecodeAssembler.class);
    private static final int O_POOL_COUNT = 8;
    private static final int aload = 0x19;
    private static final int aload_0 = 0x2a;
    private static final int aload_1 = 0x2b;
    private static final int aload_2 = 0x2c;
    private static final int aload_3 = 0x2d;
    private static final int bipush = 0x10;
    private static final int iconst_0 = 3;
    private static final int iconst_m1 = 2;
    private static final int iinc = 0x84;
    private static final int iload = 0x15;
    private static final int iload_0 = 0x1a;
    private static final int iload_1 = 0x1b;
    private static final int iload_2 = 0x1c;
    private static final int iload_3 = 0x1d;
    private static final int invokespecial = 183;
    private static final int istore = 0x36;
    private static final int istore_0 = 0x3b;
    private static final int istore_1 = 0x3c;
    private static final int istore_2 = 0x3d;
    private static final int istore_3 = 0x3e;
    private static final int lload = 0x16;
    private static final int lload_0 = 0x1e;
    private static final int lload_1 = 0x1f;
    private static final int lload_2 = 0x20;
    private static final int lload_3 = 0x21;
    private static final int lstore = 0x37;
    private static final int lstore_0 = 0x3f;
    private static final int lstore_1 = 0x40;
    private static final int lstore_2 = 0x41;
    private static final int lstore_3 = 0x42;
    private static final int new_ = 0xbb;
    private static final int sipush = 0x11;
    private final ObjIntHashMap<Class<?>> classCache = new ObjIntHashMap<>();
    private final Utf8Appender utf8Appender = new Utf8Appender();
    private final CharSequenceIntHashMap utf8Cache = new CharSequenceIntHashMap();
    private ByteBuffer buf;
    private int codeAttributeIndex;
    private int codeAttributeStart;
    private int codeStart;
    private int defaultConstructorDescIndex;
    private int defaultConstructorMethodIndex;
    private int defaultConstructorNameIndex;
    private int defaultConstructorSigIndex;
    private Class<?> host;
    private int objectClassIndex;
    private int poolCount;
    private int stackMapTableCut;

    public BytecodeAssembler() {
        this.buf = ByteBuffer.allocate(1024 * 1024).order(ByteOrder.BIG_ENDIAN);
        this.poolCount = 1;
    }

    public void aload(int value) {
        optimisedIO(aload_0, aload_1, aload_2, aload_3, aload, value);
    }

    public void append_frame(int itemCount, int offset) {
        putByte(0xfc + itemCount - 1);
        putShort(offset);
    }

    @SuppressWarnings("unused")
    public void athrow() {
        putByte(0xbf);
    }

    @SuppressWarnings("unused")
    public void d2f() {
        putShort(0x90);
    }

    @SuppressWarnings("unused")
    public void d2i() {
        putShort(0x8E);
    }

    @SuppressWarnings("unused")
    public void d2l() {
        putShort(0x8F);
    }

    public void dcmpg() {
        putByte(0x98);
    }

    public void defineClass(int thisClassIndex) {
        defineClass(thisClassIndex, objectClassIndex);
    }

    public void defineClass(int thisClassIndex, int superclassIndex) {
        // access flags
        putShort(ACC_PUBLIC);
        // this class index
        putShort(thisClassIndex);
        // super class
        putShort(superclassIndex);
    }

    public void defineDefaultConstructor() {
        defineDefaultConstructor(defaultConstructorMethodIndex);
    }

    public void defineDefaultConstructor(int superIndex) {
        // constructor method entry
        startMethod(defaultConstructorNameIndex, defaultConstructorDescIndex, 1, 1);
        // code
        aload(0);
        putByte(invokespecial);
        putShort(superIndex);
        return_();
        endMethodCode();
        // exceptions
        putShort(0);
        // attribute count
        putShort(0);
        endMethod();
    }

    public void defineField(int nameIndex, int typeIndex) {
        putShort(ACC_PRIVATE);
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

    public void dup() {
        putByte(0x59);
    }

    @SuppressWarnings("unused")
    public void dup2() {
        putByte(0x5c);
    }

    public void dup_x2() {
        putByte(0x5b);
    }

    public void endMethod() {
        putInt(codeAttributeStart - 4, position() - codeAttributeStart);
    }

    public void endMethodCode() {
        int len = position() - codeStart;
        if (len > 64 * 1024) {
            LOG.error().$("Too much input to generate ").$(host.getName()).$(". Bytecode is too long").$();
            throw BytecodeException.INSTANCE;
        }
        putInt(codeStart - 4, position() - codeStart);
    }

    public void endStackMapTables() {
        putInt(stackMapTableCut, position() - stackMapTableCut - 4);
    }

    public void f2d() {
        putShort(0x8D);
    }

    @SuppressWarnings("unused")
    public void f2i() {
        putShort(0x8B);
    }

    @SuppressWarnings("unused")
    public void f2l() {
        putShort(0x8C);
    }

    public void fieldCount(int count) {
        putShort(count);
    }

    public void finishPool() {
        putShort(O_POOL_COUNT, poolCount);
    }

    public void full_frame(int offset) {
        putByte(0xff);
        putShort(offset);
    }

    public int getCodeStart() {
        return codeStart;
    }

    public int getDefaultConstructorDescIndex() {
        return defaultConstructorDescIndex;
    }

    public int getDefaultConstructorNameIndex() {
        return defaultConstructorNameIndex;
    }

    public int getDefaultConstructorSigIndex() {
        return defaultConstructorSigIndex;
    }

    public int getObjectInitMethodIndex() {
        return defaultConstructorMethodIndex;
    }

    public int getPoolCount() {
        return poolCount;
    }

    public void getStatic(int index) {
        putByte(178);
        putShort(index);
    }

    public void getfield(int index) {
        putByte(0xb4);
        putShort(index);
    }

    public int goto_() {
        return genericGoto(0xa7);
    }

    public void i2b() {
        putShort(0x91);
    }

    public void i2d() {
        putShort(0x87);
    }

    public void i2f() {
        putShort(0x86);
    }

    public void i2l() {
        putShort(0x85);
    }

    public void i2s() {
        putShort(0x93);
    }

    public void iadd() {
        putByte(0x60);
    }

    public void iconst(int v) {
        if (v == -1) {
            putByte(iconst_m1);
        } else if (v > -1 && v < 6) {
            putByte(iconst_0 + v);
        } else if (v < 0 && v >= Short.MIN_VALUE) {
            putByte(sipush);
            putShort(v);
        } else if (v < 128 && v >= Short.MIN_VALUE) {
            putByte(bipush);
            putByte(v);
        } else {
            putByte(sipush);
            assert v >= Short.MIN_VALUE && v <= Short.MAX_VALUE;
            putShort(v);
        }
    }

    public int if_icmpge() {
        return genericGoto(0xa2);
    }

    public int if_icmpne() {
        return genericGoto(0xa0);
    }

    @SuppressWarnings("unused")
    public int ifle() {
        return genericGoto(0x9e);
    }

    public int iflt() {
        return genericGoto(0x9b);
    }

    public int ifne() {
        return genericGoto(0x9a);
    }

    public void iinc(int index, int inc) {
        putByte(iinc);
        putByte(index);
        putByte(inc);
    }

    public void iload(int value) {
        optimisedIO(iload_0, iload_1, iload_2, iload_3, iload, value);
    }

    public void ineg() {
        putByte(0x74);
    }

    public void init(Class<?> host) {
        this.host = host;
        this.buf.clear();
        this.poolCount = 1;
        this.utf8Cache.clear();
        this.classCache.clear();
    }

    public void interfaceCount(int count) {
        putShort(count);
    }

    // The count operand of the invokeinterface instruction records a measure of the number of
    // argument values, where an argument value of type long or type double contributes two
    // units to the count value and an argument of any other type contributes one unit. This
    // information can also be derived from the descriptor of the selected method. The redundancy is
    // historical
    public void invokeInterface(int interfaceIndex, int argCount) {
        putByte(185);
        putShort(interfaceIndex);
        putByte(argCount + 1);
        putByte(0);
    }

    public void invokeInterface(int interfaceIndex) {
        invokeInterface(interfaceIndex, 1);
    }

    public void invokeStatic(int index) {
        putByte(184);
        putShort(index);
    }

    public void invokeVirtual(int index) {
        putByte(182);
        putShort(index);
    }

    public void invokespecial(int index) {
        putByte(invokespecial);
        putShort(index);
    }

    public void irem() {
        putByte(0x70);
    }

    public void ireturn() {
        putByte(0xac);
    }

    public void istore(int value) {
        optimisedIO(istore_0, istore_1, istore_2, istore_3, istore, value);
    }

    public void isub() {
        putByte(0x64);
    }

    public void l2d() {
        putShort(0x8A);
    }

    public void l2f() {
        putShort(0x89);
    }

    public void l2i() {
        putShort(0x88);
    }

    public void lcmp() {
        putByte(0x94);
    }

    public void lconst_0() {
        putByte(0x09);
    }

    public void ldc(int index) {
        if (index < 256) {
            putByte(0x12);
            putByte(index);
        } else {
            ldc_w(index);
        }
    }

    public void ldc2_w(int index) {
        putByte(0x14);
        putShort(index);
    }

    public void ldc_w(int index) {
        putByte(0x13);
        putShort(index);
    }

    public void lload(int value) {
        optimisedIO(lload_0, lload_1, lload_2, lload_3, lload, value);
    }

    public void lmul() {
        putByte(0x69);
    }

    public <T> Class<T> loadClass() {
        Class<T> x = loadClass(host);
        assert x != null;
        return x;
    }

    public void lreturn() {
        putByte(0xad);
    }

    public void lstore(int value) {
        optimisedIO(lstore_0, lstore_1, lstore_2, lstore_3, lstore, value);
    }

    public void methodCount(int count) {
        putShort(count);
    }

    public <T> T newInstance() {
        Class<T> x = loadClass(host);
        assert x != null;
        try {
            return x.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            LOG.critical().$("could not create an instance of ").$(host.getName()).$(", cause: ").$(e).$();
            throw BytecodeException.INSTANCE;
        }
    }

    public void new_(int classIndex) {
        putByte(new_);
        putShort(classIndex);
    }

    public int poolClass(int classIndex) {
        putByte(0x07);
        putShort(classIndex);
        return poolCount++;
    }

    public int poolClass(Class<?> clazz) {
        int index = classCache.keyIndex(clazz);
        if (index > -1) {
            String name = clazz.getName();
            putByte(0x01);
            int n;
            putShort(n = name.length());
            for (int i = 0; i < n; i++) {
                char c = name.charAt(i);
                if (c == '.') {
                    putByte('/');
                } else {
                    putByte(c);
                }
            }
            int result = poolClass(this.poolCount++);
            classCache.putAt(index, clazz, result);
            return result;
        }
        return classCache.valueAt(index);
    }

    public int poolDoubleConst(double value) {
        putByte(0x06);
        putDouble(value);
        int index = poolCount;
        poolCount += 2;
        return index;
    }

    public int poolField(int classIndex, int nameAndTypeIndex) {
        return poolRef(0x09, classIndex, nameAndTypeIndex);
    }

    public void poolIntConst(int value) {
        putByte(0x03);
        putInt(value);
        poolCount += 1;
    }

    public int poolInterfaceMethod(Class<?> clazz, String name, String sig) {
        return poolInterfaceMethod(poolClass(clazz), poolNameAndType(poolUtf8(name), poolUtf8(sig)));
    }

    public int poolInterfaceMethod(int classIndex, String name, String sig) {
        return poolInterfaceMethod(classIndex, poolNameAndType(poolUtf8(name), poolUtf8(sig)));
    }

    public int poolInterfaceMethod(int classIndex, int nameAndTypeIndex) {
        return poolRef(0x0B, classIndex, nameAndTypeIndex);
    }

    public int poolLongConst(long value) {
        putByte(0x05);
        putLong(value);
        int index = poolCount;
        poolCount += 2;
        return index;
    }

    public int poolMethod(int classIndex, int nameAndTypeIndex) {
        return poolRef(0x0A, classIndex, nameAndTypeIndex);
    }

    public int poolMethod(int classIndex, CharSequence methodName, CharSequence signature) {
        return poolMethod(classIndex, poolNameAndType(poolUtf8(methodName), poolUtf8(signature)));
    }

    public int poolMethod(Class<?> clazz, CharSequence methodName, CharSequence signature) {
        return poolMethod(poolClass(clazz), poolNameAndType(poolUtf8(methodName), poolUtf8(signature)));
    }

    public int poolNameAndType(int nameIndex, int typeIndex) {
        return poolRef(0x0C, nameIndex, typeIndex);
    }

    public int poolStringConst(int utf8Index) {
        putByte(0x8);
        putShort(utf8Index);
        return poolCount++;
    }

    public Utf8Appender poolUtf8() {
        putByte(0x01);
        utf8Appender.lenpos = position();
        utf8Appender.utf8len = 0;
        putShort(0);
        return utf8Appender;
    }

    public int poolUtf8(CharSequence cs) {
        int index = utf8Cache.keyIndex(cs);
        if (index > -1) {
            putByte(0x01);
            int n = cs.length();
            int pos = buf.position();
            putShort(0);
            for (int i = 0; i < n; ) {
                final char c = cs.charAt(i++);
                if (c < 128) {
                    putByte(c);
                } else {
                    if (c < 2048) {
                        putByte((char) (192 | c >> 6));
                        putByte((char) (128 | c & 63));
                    } else if (Character.isSurrogate(c)) {
                        i = encodeSurrogate(c, cs, i, n);
                    } else {
                        putByte((char) (224 | c >> 12));
                        putByte((char) (128 | c >> 6 & 63));
                        putByte((char) (128 | c & 63));
                    }
                }
            }
            buf.putShort(pos, (short) (buf.position() - pos - 2));
            utf8Cache.putAt(index, cs, poolCount);
            return this.poolCount++;
        }

        return utf8Cache.valueAt(index);
    }

    public void pop() {
        putByte(0x57);
    }

    public int position() {
        return buf.position();
    }

    public void putByte(int b) {
        if (buf.remaining() == 0) {
            resize();
        }
        buf.put((byte) b);
    }

    public void putDouble(double value) {
        if (buf.remaining() < 4) {
            resize();
        }
        buf.putDouble(value);
    }

    public void putITEM_Integer() {
        putByte(0x01);
    }

    public void putITEM_Long() {
        putByte(0x04);
    }

    public void putITEM_Object(int classIndex) {
        putByte(0x07);
        putShort(classIndex);
    }

    public void putITEM_Top() {
        putByte(0);
    }

    public void putLong(long value) {
        if (buf.remaining() < 4) {
            resize();
        }
        buf.putLong(value);
    }

    public void putShort(int v) {
        putShort((short) v);
    }

    public void putShort(int pos, int v) {
        buf.putShort(pos, (short) v);
    }

    public void putfield(int index) {
        putByte(181);
        putShort(index);
    }

    public void return_() {
        putByte(0xb1);
    }

    public void same_frame(int offset) {
        if (offset < 64) {
            putByte(offset);
        } else {
            putByte(251);
            putShort(offset);
        }
    }

    public void setJmp(int branch, int target) {
        putShort(branch, target - branch + 1);
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
        defaultConstructorMethodIndex = poolMethod(objectClassIndex, defaultConstructorSigIndex = poolNameAndType(
                        defaultConstructorNameIndex = poolUtf8("<init>"),
                        defaultConstructorDescIndex = poolUtf8("()V")
                )
        );
        codeAttributeIndex = poolUtf8("Code");
    }

    public void startMethod(int nameIndex, int descriptorIndex, int maxStack, int maxLocal) {
        // access flags
        putShort(ACC_PUBLIC);
        // name index
        putShort(nameIndex);
        // descriptor index
        putShort(descriptorIndex);
        // attribute count
        putShort(1);

        // code
        putShort(codeAttributeIndex);

        // attribute len
        putInt(0);
        // come back to this later
        this.codeAttributeStart = position();
        // max stack
        putShort(maxStack);
        // max locals
        putShort(maxLocal);

        // code len
        putInt(0);
        this.codeStart = position();
    }

    public void startStackMapTables(int attributeNameIndex, int frameCount) {
        putShort(attributeNameIndex);
        this.stackMapTableCut = position();
        // length - we will come back here
        putInt(0);
        // number of entries
        putShort(frameCount);
    }

    private int encodeSurrogate(char c, CharSequence in, int pos, int hi) {
        int dword;
        if (Character.isHighSurrogate(c)) {
            if (hi - pos < 1) {
                putByte('?');
                return pos;
            } else {
                char c2 = in.charAt(pos++);
                if (Character.isLowSurrogate(c2)) {
                    dword = Character.toCodePoint(c, c2);
                } else {
                    putByte('?');
                    return pos;
                }
            }
        } else if (Character.isLowSurrogate(c)) {
            putByte('?');
            return pos;
        } else {
            dword = c;
        }
        putByte((char) (240 | dword >> 18));
        putByte((char) (128 | dword >> 12 & 63));
        putByte((char) (128 | dword >> 6 & 63));
        putByte((char) (128 | dword & 63));

        return pos;
    }

    private int genericGoto(int cmd) {
        putByte(cmd);
        int pos = position();
        putShort(0);
        return pos;
    }

    @SuppressWarnings("unchecked")
    @Nullable
    private <T> Class<T> loadClass(Class<?> host) {
        byte[] b = new byte[position()];
        System.arraycopy(buf.array(), 0, b, 0, b.length);
        return (Class<T>) Unsafe.defineAnonymousClass(host, b);
    }

    private void optimisedIO(int code0, int code1, int code2, int code3, int code, int value) {
        switch (value) {
            case 0:
                putByte(code0);
                break;
            case 1:
                putByte(code1);
                break;
            case 2:
                putByte(code2);
                break;
            case 3:
                putByte(code3);
                break;
            default:
                putByte(code);
                putByte(value);
                break;
        }
    }

    private int poolRef(int op, int name, int type) {
        putByte(op);
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

    public class Utf8Appender implements Utf8Sink {
        private int lenpos;
        private int utf8len = 0;

        public int $() {
            putShort(lenpos, utf8len);
            return poolCount++;
        }

        @Override
        public Utf8Sink put(@Nullable Utf8Sequence us) {
            if (us != null) {
                int size = us.size();
                for (int i = 0; i < size; i++) {
                    BytecodeAssembler.this.putByte(us.byteAt(i));
                }
                utf8len += size;
            }
            return this;
        }

        @Override
        public Utf8Appender put(byte b) {
            BytecodeAssembler.this.putByte(b);
            utf8len++;
            return this;
        }

        @Override
        public Utf8Appender put(int value) {
            Utf8Sink.super.put(value);
            return this;
        }

        @Override
        public Utf8Appender put(@Nullable CharSequence cs) {
            Utf8Sink.super.put(cs);
            return this;
        }

        @Override
        public Utf8Appender putAscii(char c) {
            Utf8Sink.super.putAscii(c);
            return this;
        }

        @Override
        public Utf8Appender putAscii(@Nullable CharSequence cs) {
            if (cs != null) {
                int len = cs.length();
                for (int i = 0; i < len; i++) {
                    BytecodeAssembler.this.putByte(cs.charAt(i));
                }
                utf8len += len;
            }
            return this;
        }

        @Override
        public Utf8Appender putNonAscii(long lo, long hi) {
            Bytes.checkedLoHiSize(lo, hi, BytecodeAssembler.this.position());
            for (long p = lo; p < hi; p++) {
                BytecodeAssembler.this.putByte(Unsafe.getUnsafe().getByte(p));
            }
            return this;
        }
    }
}
