/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.std;

import com.questdb.std.str.AbstractCharSequence;
import com.questdb.std.str.AbstractCharSink;
import com.questdb.std.str.CharSink;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;

public class Lexer2 implements ImmutableIterator<CharSequence> {
    public static final LenComparator COMPARATOR = new LenComparator();
    private static final CharSequenceHashSet whitespace = new CharSequenceHashSet();
    private final IntObjHashMap<ObjList<CharSequence>> symbols = new IntObjHashMap<>();
    private final CharSequence flyweightSequence = new InternalFloatingSequence();
    private final ObjectPool<FloatingSequence> csPool = new ObjectPool<>(FloatingSequence::new, 64);
    private final NameAssemblerImpl nameAssembler = new NameAssemblerImpl();
    private CharSequence next = null;
    private int _lo;
    private int _hi;
    private int _pos;
    private int _len;
    private CharSequence content;
    private CharSequence unparsed;
    private CharSequence last;

    public Lexer2() {
        for (int i = 0, n = whitespace.size(); i < n; i++) {
            defineSymbol(whitespace.get(i).toString());
        }
    }

    public final void defineSymbol(String token) {
        char c0 = token.charAt(0);
        ObjList<CharSequence> l;
        int index = symbols.keyIndex(c0);
        if (index > -1) {
            l = new ObjList<>();
            symbols.putAt(index, c0, l);
        } else {
            l = symbols.valueAt(index);
        }
        l.add(token);
        l.sort(COMPARATOR);
    }

    public CharSequence getContent() {
        return content;
    }

    public void setContent(CharSequence cs) {
        setContent(cs, 0, cs == null ? 0 : cs.length());
    }

    public NameAssembler getNameAssembler() {
        return nameAssembler.next();
    }

    @Override
    public boolean hasNext() {
        boolean n = next != null || unparsed != null || (content != null && _pos < _len);
        if (!n && last != null) {
            last = null;
        }
        return n;
    }

    @Override
    public CharSequence next() {

        if (unparsed != null) {
            CharSequence result = unparsed;
            unparsed = null;
            return last = result;
        }

        this._lo = this._hi;

        if (next != null) {
            CharSequence result = next;
            next = null;
            return last = result;
        }

        this._lo = this._hi = _pos;

        char term = 0;

        while (hasNext()) {
            char c = content.charAt(_pos++);
            CharSequence token;
            switch (term) {
                case 1:
                    if ((token = token(c)) != null) {
                        return last = token;
                    } else {
                        _hi++;
                    }
                    break;
                case 0:
                    switch (c) {
                        case '\'':
                            term = '\'';
                            break;
                        case '"':
                            term = '"';
                            break;
                        case '`':
                            term = '`';
                            break;
                        default:
                            if ((token = token(c)) != null) {
                                return last = token;
                            } else {
                                _hi++;
                            }
                            term = 1;
                            break;
                    }
                    break;
                case '\'':
                    switch (c) {
                        case '\'':
                            _hi += 2;
                            return last = flyweightSequence;
                        default:
                            _hi++;
                            break;
                    }
                    break;
                case '"':
                    switch (c) {
                        case '"':
                            _hi += 2;
                            return last = flyweightSequence;
                        default:
                            _hi++;
                            break;
                    }
                    break;
                case '`':
                    switch (c) {
                        case '`':
                            _hi += 2;
                            return last = flyweightSequence;
                        default:
                            _hi++;
                            break;
                    }
                    break;
                default:
                    break;
            }
        }
        return last = flyweightSequence;
    }

    public CharSequence optionTok() {
        int blockCount = 0;
        boolean lineComment = false;
        while (hasNext()) {
            CharSequence cs = next();

            if (lineComment) {
                if (Chars.equals(cs, '\n') || Chars.equals(cs, '\r')) {
                    lineComment = false;
                }
                continue;
            }

            if (Chars.equals("--", cs)) {
                lineComment = true;
                continue;
            }

            if (Chars.equals("/*", cs)) {
                blockCount++;
                continue;
            }

            if (Chars.equals("*/", cs) && blockCount > 0) {
                blockCount--;
                continue;
            }

            if (blockCount == 0 && whitespace.excludes(cs)) {
                return cs;
            }
        }
        return null;
    }

    public int position() {
        return _lo;
    }

    public void setContent(CharSequence cs, int lo, int hi) {
        this.csPool.clear();
        this.content = cs;
        this._pos = lo;
        this._len = hi;
        this.next = null;
        this.unparsed = null;
    }

    public CharSequence toImmutable(CharSequence value) {
        if (value instanceof InternalFloatingSequence) {
            FloatingSequence that = csPool.next();
            that.lo = _lo;
            that.hi = _hi;
            assert that.lo < that.hi;
            return that;
        }

        if (value instanceof String) {
            return value;
        }

        throw new RuntimeException("!!!");
    }

    public CharSequence toImmutable(CharSequence value, int lo, int hi) {
        if (value instanceof InternalFloatingSequence) {
            FloatingSequence that = csPool.next();
            that.lo = _lo + lo;
            that.hi = _lo + hi;
            assert that.lo < that.hi;
            return that;
        }

        if (value instanceof FloatingSequence) {
            FloatingSequence that = csPool.next();
            that.lo = ((FloatingSequence) value).lo + lo;
            that.hi = ((FloatingSequence) value).lo + hi;
            assert that.lo < that.hi;
            return that;
        }

        if (value instanceof NameAssemblerImpl.NameAssemblerCharSequence) {
            NameAssemblerImpl.NameAssemblerCharSequence that = nameAssembler.csPool.next();
            that.lo = ((NameAssemblerImpl.NameAssemblerCharSequence) value).lo + lo;
            that.hi = ((NameAssemblerImpl.NameAssemblerCharSequence) value).lo + hi;
            assert that.lo < that.hi;
            return that;
        }

        throw new RuntimeException("!!!");
    }

    public void unparse() {
        unparsed = last;
    }

    public CharSequence unquote(CharSequence value) {
        if (Chars.isQuoted(value)) {
            return toImmutable(value, 1, value.length() - 1);
        }
        return toImmutable(value);
    }

    private static CharSequence findToken0(char c, CharSequence content, int _pos, int _len, IntObjHashMap<ObjList<CharSequence>> symbols) {
        final int index = symbols.keyIndex(c);
        return index > -1 ? null : findToken00(content, _pos, _len, symbols, index);
    }

    @Nullable
    private static CharSequence findToken00(CharSequence content, int _pos, int _len, IntObjHashMap<ObjList<CharSequence>> symbols, int index) {
        final ObjList<CharSequence> l = symbols.valueAt(index);
        for (int i = 0, sz = l.size(); i < sz; i++) {
            CharSequence txt = l.getQuick(i);
            int n = txt.length();
            boolean match = (n - 2) < (_len - _pos);
            if (match) {
                for (int k = 1; k < n; k++) {
                    if (content.charAt(_pos + (k - 1)) != txt.charAt(k)) {
                        match = false;
                        break;
                    }
                }
            }

            if (match) {
                return txt;
            }
        }
        return null;
    }

    private CharSequence token(char c) {
        CharSequence t = findToken0(c, content, _pos, _len, symbols);
        if (t != null) {
            _pos = _pos + t.length() - 1;
            if (_lo == _hi) {
                return t;
            } else {
                next = t;
            }
            return flyweightSequence;
        } else {
            return null;
        }
    }

    public interface NameAssembler extends CharSink {
        void setSize(int size);

        int size();

        CharSequence toImmutable();
    }

    private static class LenComparator implements Comparator<CharSequence> {
        @Override
        public int compare(CharSequence o1, CharSequence o2) {
            return o2.length() - o1.length();
        }
    }

    public class InternalFloatingSequence extends AbstractCharSequence {
        @Override
        public int length() {
            return _hi - _lo;
        }

        @Override
        public char charAt(int index) {
            return content.charAt(_lo + index);
        }
    }

    public class FloatingSequence extends AbstractCharSequence implements Mutable {
        int lo;
        int hi;

        @Override
        public void clear() {
        }

        @Override
        public int length() {
            return hi - lo;
        }

        @Override
        public char charAt(int index) {
            return content.charAt(lo + index);
        }
    }

    private class NameAssemblerImpl extends AbstractCharSink implements NameAssembler, Mutable {
        private final ObjectPool<NameAssemblerCharSequence> csPool = new ObjectPool<>(NameAssemblerCharSequence::new, 64);
        private int capacity = 1024;
        private char[] chars = new char[capacity];
        private int size = 0;
        private NameAssemblerCharSequence next = null;

        public NameAssembler next() {
            this.next = csPool.next();
            this.next.lo = size;
            return this;
        }

        @Override
        public CharSink put(CharSequence cs) {
            assert cs != null;
            return put(cs, 0, cs.length());
        }

        @Override
        public CharSink put(CharSequence cs, int start, int end) {
            for (int i = start; i < end; i++) {
                put(cs.charAt(i));
            }
            return this;
        }

        @Override
        public CharSink put(char c) {
            if (size < capacity) {
                Unsafe.arrayPut(chars, size++, c);
            } else {
                resizeAndPut(c);
            }
            return this;
        }

        public void setSize(int size) {
            this.size = size;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public CharSequence toImmutable() {
            next.hi = size;
            return next;
        }

        private void resizeAndPut(char c) {
            char[] next = new char[capacity * 2];
            System.arraycopy(chars, 0, next, 0, capacity);
            chars = next;
            capacity *= 2;
            Unsafe.arrayPut(chars, size++, c);
        }

        private class NameAssemblerCharSequence extends AbstractCharSequence implements Mutable {
            int lo;
            int hi;

            @Override
            public void clear() {
            }

            @Override
            public int length() {
                return hi - lo;
            }

            @Override
            public char charAt(int index) {
                return Unsafe.arrayGet(chars, lo + index);
            }
        }

        @Override
        public void clear() {
            csPool.clear();
            size = 0;
            next = null;
        }
    }


    static {
        whitespace.add(" ");
        whitespace.add("\t");
        whitespace.add("\n");
        whitespace.add("\r");
    }
}