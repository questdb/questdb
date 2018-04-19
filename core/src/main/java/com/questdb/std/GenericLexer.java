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

public class GenericLexer implements ImmutableIterator<CharSequence> {
    public static final LenComparator COMPARATOR = new LenComparator();
    public static final CharSequenceHashSet WHITESPACE = new CharSequenceHashSet();
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

    public GenericLexer() {
        for (int i = 0, n = WHITESPACE.size(); i < n; i++) {
            defineSymbol(WHITESPACE.get(i).toString());
        }
    }

    public static CharSequence immutableOf(CharSequence value) {
        if (value instanceof InternalFloatingSequence) {
            GenericLexer lexer = ((InternalFloatingSequence) value).getParent();
            FloatingSequence that = lexer.csPool.next();
            that.lo = lexer._lo;
            that.hi = lexer._hi;
            assert that.lo < that.hi;
            return that;
        }

        if (value instanceof String) {
            return value;
        }

        throw new RuntimeException("!!!");
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

    public NameAssembler getNameAssembler() {
        return nameAssembler.next();
    }

    public int getPosition() {
        return _pos;
    }

    public void goToPosition(int position) {
        assert position < this._len;
        this._pos = position;
        next = null;
        unparsed = null;
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

    public int lastTokenPosition() {
        return _lo;
    }

    public void of(CharSequence cs) {
        of(cs, 0, cs == null ? 0 : cs.length());
    }

    public void of(CharSequence cs, int lo, int hi) {
        this.csPool.clear();
        this.content = cs;
        this._pos = lo;
        this._len = hi;
        this.next = null;
        this.unparsed = null;
    }

    public void unparse() {
        unparsed = last;
    }

    public CharSequence unquote(CharSequence value) {
        if (Chars.isQuoted(value)) {
            return value.subSequence(1, value.length() - 1);
        }
        return immutableOf(value);
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
        int length();

        CharSequence toImmutable();

        void trimTo(int size);
    }

    private static class LenComparator implements Comparator<CharSequence> {
        @Override
        public int compare(CharSequence o1, CharSequence o2) {
            return o2.length() - o1.length();
        }
    }

    private static class NameAssemblerImpl extends AbstractCharSink implements NameAssembler, Mutable {
        private final ObjectPool<NameAssemblerCharSequence> csPool = new ObjectPool<>(NameAssemblerCharSequence::new, 64);
        private int capacity = 1024;
        private char[] chars = new char[capacity];
        private int size = 0;
        private NameAssemblerCharSequence next = null;

        @Override
        public int length() {
            return size;
        }

        @Override
        public CharSequence toImmutable() {
            next.hi = size;
            return next;
        }

        public void trimTo(int size) {
            this.size = size;
        }

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
            public CharSequence subSequence(int start, int end) {
                NameAssemblerCharSequence that = csPool.next();
                that.lo = lo + start;
                that.hi = lo + end;
                assert that.lo < that.hi;
                return that;
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

    public class InternalFloatingSequence extends AbstractCharSequence {

        GenericLexer getParent() {
            return GenericLexer.this;
        }

        @Override
        public int length() {
            return _hi - _lo;
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            FloatingSequence next = csPool.next();
            next.lo = _lo + start;
            next.hi = _lo + end;
            assert next.lo < next.hi;
            return next;
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

        @Override
        public CharSequence subSequence(int start, int end) {
            FloatingSequence that = csPool.next();
            that.lo = lo + start;
            that.hi = lo + end;
            assert that.lo < that.hi;
            return that;
        }
    }

    static {
        WHITESPACE.add(" ");
        WHITESPACE.add("\t");
        WHITESPACE.add("\n");
        WHITESPACE.add("\r");
    }
}