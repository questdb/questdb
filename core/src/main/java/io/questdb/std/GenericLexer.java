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

package io.questdb.std;

import io.questdb.griffin.SqlException;
import io.questdb.std.str.AbstractCharSequence;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;

public class GenericLexer implements ImmutableIterator<CharSequence> {
    public static final LenComparator COMPARATOR = new LenComparator();
    public static final CharSequenceHashSet WHITESPACE = new CharSequenceHashSet();
    public static final IntHashSet WHITESPACE_CH = new IntHashSet();
    private final IntObjHashMap<ObjList<CharSequence>> symbols = new IntObjHashMap<>();
    private final CharSequence flyweightSequence = new InternalFloatingSequence();
    private final ObjectPool<FloatingSequence> csPool;
    private CharSequence next = null;
    private int _lo;
    private int _hi;
    private int _pos;
    private int _len;
    private CharSequence content;
    private CharSequence unparsed;
    private CharSequence last;
    private int _start;

    public GenericLexer(int poolCapacity) {
        this.csPool = new ObjectPool<>(FloatingSequence::new, poolCapacity);
        for (int i = 0, n = WHITESPACE.size(); i < n; i++) {
            defineSymbol(Chars.toString(WHITESPACE.get(i)));
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
        return value;
    }

    public static CharSequence assertNoDots(CharSequence value, int position) throws SqlException {
        int len = value.length();
        if (len == 1 && value.charAt(0) == '.') {
            throw SqlException.position(position).put("'.' is an invalid table name");
        }
        for (int i = 0; i < len; i++) {
            char c = value.charAt(i);
            if ((c == '.' && i < len - 1 && value.charAt(i + 1) == '.')) {
                throw SqlException.position(position + i).put('\'').put(c).put("' is not allowed");
            }
        }

        return value;
    }

    public static CharSequence assertNoDotsAndSlashes(CharSequence value, int position) throws SqlException {
        int len = value.length();
        if (len == 1 && value.charAt(0) == '.') {
            throw SqlException.position(position).put("'.' is an invalid table name");
        }
        for (int i = 0; i < len; i++) {
            char c = value.charAt(i);
            if (c == '/' || c == '\\' || (c == '.' && i < len - 1 && value.charAt(i + 1) == '.')) {
                throw SqlException.position(position + i).put('\'').put(c).put("' is not allowed");
            }
        }

        return value;
    }

    public static CharSequence unquote(CharSequence value) {
        if (Chars.isQuoted(value)) {
            return value.subSequence(1, value.length() - 1);
        }
        return immutableOf(value);
    }

    public final void defineSymbol(String token) {
        char c0 = token.charAt(0);
        ObjList<CharSequence> l;
        int index = symbols.keyIndex(c0);
        if (index > -1) {
            l = new ObjList<>();
            symbols.putAt(index, c0, l);
        } else {
            l = symbols.valueAtQuick(index);
        }
        l.add(token);
        l.sort(COMPARATOR);
    }

    public CharSequence getContent() {
        return content;
    }

    public CharSequence peek() {
        return next;
    }

    public int getPosition() {
        return _pos;
    }

    public int getTokenHi() {
        return _hi;
    }

    public CharSequence getUnparsed() {
        return unparsed == null ? null : immutableOf(unparsed);
    }

    public void goToPosition(int position, CharSequence unparsed) {
        assert position <= this._len;
        this._pos = position;
        next = null;
        this.unparsed = unparsed;
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
                            break;
                    }
                    break;
                case '\'':
                    if (c == '\'') {
                        _hi += 2;
                        return last = flyweightSequence;
                    } else {
                        _hi++;
                    }
                    break;
                case '"':
                    if (c == '"') {
                        _hi += 2;
                        return last = flyweightSequence;
                    } else {
                        _hi++;
                    }
                    break;
                case '`':
                    if (c == '`') {
                        _hi += 2;
                        return last = flyweightSequence;
                    } else {
                        _hi++;
                    }
                    break;
                default:
                    break;
            }
        }
        return last = flyweightSequence;
    }

    public CharSequence immutableBetween(int lo, int hi) {
        FloatingSequence that = csPool.next();
        that.lo = lo;
        that.hi = hi;
        assert that.lo < that.hi;
        return that;
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
        this._start = lo;
        this._pos = lo;
        this._len = hi;
        this.next = null;
        this.unparsed = null;
        this.last = null;
    }

    public void restart() {
        this.csPool.clear();
        this._pos = this._start;
        this.csPool.clear();
        this.next = null;
        this.unparsed = null;
        this.last = null;
    }

    public void unparse() {
        unparsed = last;
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
            }
            next = t;
            return flyweightSequence;
        } else {
            return null;
        }
    }

    public static class LenComparator implements Comparator<CharSequence> {
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

        @Override
        public CharSequence subSequence(int start, int end) {
            FloatingSequence next = csPool.next();
            next.lo = _lo + start;
            next.hi = _lo + end;
            assert next.lo < next.hi;
            return next;
        }

        GenericLexer getParent() {
            return GenericLexer.this;
        }
    }

    public class FloatingSequence extends AbstractCharSequence implements Mutable {
        int lo;
        int hi;

        @Override
        public void clear() {
        }

        public int getHi() {
            return hi;
        }

        public void setHi(int hi) {
            this.hi = hi;
        }

        public void setLo(int lo) {
            this.lo = lo;
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

        WHITESPACE_CH.add(' ');
        WHITESPACE_CH.add('\t');
        WHITESPACE_CH.add('\n');
        WHITESPACE_CH.add('\r');
    }
}