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

package io.questdb.std;

import io.questdb.griffin.SqlException;
import io.questdb.std.str.AbstractCharSequence;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.Comparator;

public class GenericLexer implements ImmutableIterator<CharSequence> {
    public static final LenComparator COMPARATOR = new LenComparator();
    public static final CharSequenceHashSet WHITESPACE = new CharSequenceHashSet();
    public static final IntHashSet WHITESPACE_CH = new IntHashSet();
    private final IntObjHashMap<ObjList<CharSequence>> symbols = new IntObjHashMap<>();
    private final CharSequence flyweightSequence = new InternalFloatingSequence();
    private final ObjectPool<FloatingSequence> csPool;
    private final ObjectPool<FloatingSequencePair> csPairPool;
    private final ArrayDeque<CharSequence> unparsed = new ArrayDeque<>();
    private final IntStack unparsedPosition = new IntStack();
    private final ArrayDeque<CharSequence> parkedUnparsed = new ArrayDeque<>();
    private final IntStack parkedPosition = new IntStack();
    private CharSequence next = null;
    private int _lo;
    private int _hi;
    private int _pos;
    private int _len;
    private CharSequence content;
    private CharSequence last;
    private int _start;

    public GenericLexer(int poolCapacity) {
        csPool = new ObjectPool<>(FloatingSequence::new, poolCapacity);
        csPairPool = new ObjectPool<>(FloatingSequencePair::new, poolCapacity);
        for (int i = 0, n = WHITESPACE.size(); i < n; i++) {
            defineSymbol(Chars.toString(WHITESPACE.get(i)));
        }
    }

    public CharSequence immutablePairOf(CharSequence value0, CharSequence value1) {
        return immutablePairOf(value0, FloatingSequencePair.NO_SEPARATOR, value1);
    }

    public CharSequence immutablePairOf(CharSequence value0, char separator, CharSequence value1) {
        if (!(value0 instanceof FloatingSequence && value1 instanceof InternalFloatingSequence)) {
            throw new UnsupportedOperationException("only pairs of floating sequences are allowed");
        }
        FloatingSequencePair seqPair = csPairPool.next();
        seqPair.cs0 = (FloatingSequence) value0;
        seqPair.cs1 = (FloatingSequence) immutableOf(value1);
        seqPair.sep = separator;
        return seqPair;
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

    public void stash() {
        int count = 0;
        while (unparsed.size() > 0) {
            parkedUnparsed.push(unparsed.pop());
            parkedPosition.push(unparsedPosition.pop());
            parkedPosition.push(unparsedPosition.pop());
            count++;
        }
        parkedPosition.push(getPosition());
        parkedPosition.push(count);
        // clear next because we create a new parsing context
        next = null;
    }

    public void unstash() {
        int count = parkedPosition.pop();
        _pos = parkedPosition.pop();

        unparsed.clear();
        unparsedPosition.clear();
        while (count > 0) {
            unparsed.push(parkedUnparsed.pop());
            unparsedPosition.push(parkedPosition.pop()); // last
            unparsedPosition.push(parkedPosition.pop()); // pos
            count--;
        }
        // clear next because we create a new parsing context
        next = null;
    }

    public boolean hasUnparsed() {
        return unparsed.size() > 0;
    }

    public void goToPosition(int position) {
        assert position <= this._len;
        this._pos = position;
    }

    @Override
    public boolean hasNext() {
        boolean n = next != null || hasUnparsed() || (content != null && _pos < _len);
        if (!n && last != null) {
            last = null;
        }
        return n;
    }

    @Override
    public CharSequence next() {

        if (unparsed.size() > 0) {

            this._lo  = unparsedPosition.pollLast();
            this._pos = unparsedPosition.pollLast();

            return last = unparsed.pollLast();
        }

        this._lo = this._hi;

        if (next != null) {
            CharSequence result = next;
            next = null;
            return last = result;
        }

        this._lo = this._hi = _pos;

        char term = 0;
        int openTermIdx = -1;
        while (hasNext()) {
            char c = content.charAt(_pos++);
            CharSequence token;
            switch (term) {
                case 0:
                    switch (c) {
                        case '\'':
                            term = '\'';
                            openTermIdx = _pos - 1;
                            break;
                        case '"':
                            term = '"';
                            openTermIdx = _pos - 1;
                            break;
                        case '`':
                            term = '`';
                            openTermIdx = _pos - 1;
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
        if (openTermIdx != -1) { // dangling terms
            if (_len == 1) {
                _hi += 1; // emit term
            } else {
                if (openTermIdx == _lo) { // term is at the start
                    _hi = _lo + 1; // emit term
                    _pos = _hi; // rewind pos
                } else if (openTermIdx == _len - 1) { // term is at the end, high is right on term
                    FloatingSequence termFs = csPool.next();
                    termFs.lo = _hi;
                    termFs.hi = _hi + 1;
                    next = termFs; // emit term next
                } else { // term is somewhere in between
                    _hi = openTermIdx; // emit whatever comes before term
                    _pos = openTermIdx; // rewind pos
                }
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
        this.csPairPool.clear();
        this.content = cs;
        this._start = lo;
        this._pos = lo;
        this._len = hi;
        this.next = null;
        this.unparsed.clear();
        this.unparsedPosition.clear();
        this.last = null;
    }

    public void restart() {
        this.csPool.clear();
        this.csPairPool.clear();
        this._pos = this._start;
        this.next = null;
        this.unparsed.clear();
        this.unparsedPosition.clear();
        this.last = null;
        this.parkedPosition.clear();
        this.parkedUnparsed.clear();
    }

    public void unparseLast() {
        if (last != null) {
            unparsed.push(immutableOf(last));
            unparsedPosition.push(lastTokenPosition());
            unparsedPosition.push(getPosition());
        }
    }

    public void unparse(CharSequence what, int last, int pos) {
        unparsed.push(what);
        unparsedPosition.push(last);
        unparsedPosition.push(pos);
    }

    public void backTo(int position, CharSequence lastSeen) {
        if (position < 0 || position > _len) {
            throw new IndexOutOfBoundsException();
        }
        _pos = position;
        last = lastSeen;
        next = null;
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

        public int getLo() {
            return lo;
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

    public static class FloatingSequencePair extends AbstractCharSequence implements Mutable {
        public static final char NO_SEPARATOR = (char) 0;

        FloatingSequence cs0;
        FloatingSequence cs1;
        char sep = NO_SEPARATOR;

        @Override
        public int length() {
            return cs0.length() + cs1.length() + (sep != NO_SEPARATOR ? 1 : 0);
        }

        @Override
        public char charAt(int index) {
            int cs0Len = cs0.length();
            if (index < cs0Len) {
                return cs0.charAt(index);
            }
            if (sep == NO_SEPARATOR) {
                return cs1.charAt(index - cs0Len);
            }
            return index == cs0Len ? sep : cs1.charAt(index - cs0Len - 1);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            throw new UnsupportedOperationException();
        }

        @NotNull
        @Override
        public String toString() {
            final CharSink b = Misc.getThreadLocalBuilder();
            b.put(cs0);
            if (sep != NO_SEPARATOR) {
                b.put(sep);
            }
            b.put(cs1);
            return b.toString();
        }

        @Override
        public void clear() {
            // no-op
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