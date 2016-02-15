/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.ql.parser;

import com.nfsdb.std.AbstractCharSequence;
import com.nfsdb.std.AbstractImmutableIterator;
import com.nfsdb.std.CharSequenceHashSet;
import com.nfsdb.std.IntObjHashMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

class Lexer extends AbstractImmutableIterator<CharSequence> {
    private static final CharSequenceHashSet whitespace = new CharSequenceHashSet();
    private final IntObjHashMap<List<CharSequence>> symbols = new IntObjHashMap<>();
    private final CharSequence floatingSequence = new FloatingSequence();
    private final LenComparator comparator = new LenComparator();
    private CharSequence next = null;
    private int _lo;
    private int _hi;
    private int _pos;
    private int _len;
    private CharSequence content;
    private CharSequence unparsed;
    private CharSequence last;

    public Lexer() {
        for (int i = 0, n = whitespace.size(); i < n; i++) {
            defineSymbol(whitespace.get(i).toString());
        }
    }

    public final void defineSymbol(String token) {
        char c0 = token.charAt(0);
        List<CharSequence> l = symbols.get(c0);
        if (l == null) {
            l = new ArrayList<>();
            symbols.put(c0, l);
        }
        l.add(token);
        Collections.sort(l, comparator);
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

        this._lo = this._hi = _pos;

        if (next != null) {
            CharSequence result = next;
            next = null;
            return last = result;
        }

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
                            return last = floatingSequence;
                        default:
                            _hi++;
                            break;
                    }
                    break;
                case '"':
                    switch (c) {
                        case '"':
                            _hi += 2;
                            return last = floatingSequence;
                        default:
                            _hi++;
                            break;
                    }
                    break;
                case '`':
                    switch (c) {
                        case '`':
                            _hi += 2;
                            return last = floatingSequence;
                        default:
                            _hi++;
                    }
                    break;
                default:
                    break;
            }
        }
        return last = floatingSequence;
    }

    public CharSequence optionTok() {
        while (hasNext()) {
            CharSequence cs = next();
            if (!whitespace.contains(cs)) {
                return cs;
            }
        }
        return null;
    }

    public int position() {
        return _lo;
    }

    public void setContent(CharSequence cs) {
        this.content = cs;
        this._pos = 0;
        this._len = cs == null ? 0 : cs.length();
    }

    public void unparse() {
        unparsed = last;
    }

    @SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
    private CharSequence getSymbol(char c) {

        List<CharSequence> l = symbols.get(c);
        if (l == null) {
            return null;
        }

        for (int i = 0, sz = l.size(); i < sz; i++) {
            CharSequence txt = l.get(i);
            boolean match = (txt.length() - 2) < (_len - _pos);
            if (match) {
                for (int k = 1; k < txt.length(); k++) {
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
        CharSequence t = getSymbol(c);
        if (t != null) {
            _pos = _pos + t.length() - 1;
            if (_lo == _hi) {
                return t;
            } else {
                next = t;
            }
            return floatingSequence;
        } else {
            return null;
        }
    }

    @SuppressFBWarnings({"SE_COMPARATOR_SHOULD_BE_SERIALIZABLE"})
    private static class LenComparator implements Comparator<CharSequence> {
        @Override
        public int compare(CharSequence o1, CharSequence o2) {
            return o2.length() - o1.length();
        }
    }

    private class FloatingSequence extends AbstractCharSequence {
        @Override
        public int length() {
            return _hi - _lo;
        }

        @Override
        public char charAt(int index) {
            return content.charAt(_lo + index);
        }
    }

    static {
        whitespace.add(" ");
        whitespace.add("\t");
        whitespace.add("\n");
        whitespace.add("\r");
    }
}