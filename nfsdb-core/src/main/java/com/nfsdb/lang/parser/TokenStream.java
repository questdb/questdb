/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.lang.parser;

import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.collections.IntObjHashMap;
import org.jetbrains.annotations.NotNull;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class TokenStream extends AbstractImmutableIterator<CharSequence> {
    private final IntObjHashMap<List<Token>> symbols = new IntObjHashMap<>();
    private final CharSequence floatingSequence = new FloatingSequence();
    private CharSequence next = null;
    private int _lo;
    private int _hi;
    private int _pos;
    private int _len;
    private CharSequence content;

    public void defineSymbol(String text) {
        defineSymbol(new Token(text));
    }

    public void defineSymbol(Token token) {
        char c0 = token.text.charAt(0);
        List<Token> l = symbols.get(c0);
        if (l == null) {
            l = new ArrayList<>();
            symbols.put(c0, l);
        }
        l.add(token);
        Collections.sort(l, new Comparator<Token>() {
            @Override
            public int compare(Token o1, Token o2) {
                return o2.text.length() - o1.text.length();
            }
        });
    }

    public Token getSymbol(char c) {

        List<Token> l = symbols.get(c);
        if (l == null) {
            return null;
        }

        for (int i = 0, sz = l.size(); i < sz; i++) {
            final Token t = l.get(i);
            CharSequence txt = t.text;
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
                return t;
            }
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        return next != null || (content != null && _pos < _len);
    }

    @Override
    public CharSequence next() {

        if (next != null) {
            CharSequence result = next;
            next = null;
            return result;
        }

        char term = 0;

        this._lo = this._hi = _pos;

        while (hasNext()) {
            char c = content.charAt(_pos++);
            CharSequence token;
            switch (term) {
                case 1:
                    if ((token = token(c)) != null) {
                        return token;
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
                        default:
                            if ((token = token(c)) != null) {
                                return token;
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
                            return floatingSequence;
                        default:
                            _hi++;
                    }
                    break;
                case '"':
                    switch (c) {
                        case '"':
                            return floatingSequence;
                        default:
                            _hi++;
                    }
            }
        }
        return floatingSequence;
    }

    public int position() {
        return _lo;
    }

    public void setContent(CharSequence cs) {
        this.content = cs;
        this._pos = 0;
        this._len = cs == null ? 0 : cs.length();
    }

    private CharSequence token(char c) {
        Token t = getSymbol(c);
        if (t != null) {
            _pos = _pos + t.text.length() - 1;
            if (_lo == _hi) {
                return t.text;
            } else {
                next = t.text;
            }
            return floatingSequence;
        } else {
            return null;
        }
    }

    public class FloatingSequence implements CharSequence {
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
            throw new NotImplementedException();
        }

        @Override
        @NotNull
        public String toString() {
            int l = this.length();
            char data[] = new char[l];
            for (int i = 0; i < l; i++) {
                data[i] = this.charAt(i);
            }
            return new String(data);
        }
    }
}
