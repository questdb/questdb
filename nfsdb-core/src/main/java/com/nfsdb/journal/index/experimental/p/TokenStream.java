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

package com.nfsdb.journal.index.experimental.p;

import com.nfsdb.journal.utils.ByteBuffers;
import gnu.trove.map.hash.TCharObjectHashMap;

import java.nio.ByteBuffer;
import java.util.*;

public class TokenStream implements Iterator<String>, Iterable<String> {
    private final TCharObjectHashMap<List<Token>> symbols = new TCharObjectHashMap<>();
    private final StringBuilder s = new StringBuilder();
    private ByteBuffer buffer;
    private String next = null;

    public void setContent(String s) {
        if (s == null || s.length() == 0 && buffer != null) {
            buffer.limit(0);
            return;
        }

        if (buffer == null || buffer.capacity() < s.length() * 2) {
            buffer = ByteBuffer.allocate(s.length() * 2);
        } else {
            buffer.limit(s.length() * 2);
        }
        buffer.rewind();
        ByteBuffers.putStr(buffer, s);
        buffer.rewind();
    }

    public void defineSymbol(String text) {
        List<Token> l = symbols.get(text.charAt(0));
        if (l == null) {
            l = new ArrayList<>();
            symbols.put(text.charAt(0), l);
        }
        l.add(new Token(text));
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

        int pos = buffer.position();
        for (int i = 0, sz = l.size(); i < sz; i++) {
            final Token t = l.get(i);
            boolean match = t.text.length() < buffer.remaining();
            if (match) {
                for (int k = 1, tsz = t.text.length(); k < tsz; k++) {
                    if (buffer.getChar(pos + 2 * (k - 1)) != t.text.charAt(k)) {
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
        return buffer != null && buffer.hasRemaining();
    }

    @Override
    public String next() {

        if (next != null) {
            String result = next;
            next = null;
            return result;
        }

        s.setLength(0);

        while (hasNext()) {
            char c = buffer.getChar();
            Token t = getSymbol(c);
            if (t != null) {
                buffer.position(buffer.position() + (t.text.length() - 1) * 2);
                if (s.length() == 0) {
                    return t.text;
                } else {
                    next = t.text;
                }
                return s.toString();
            } else {
                s.append(c);
            }
        }
        return s.toString();
    }

    @Override
    public void remove() {

    }

    @Override
    public Iterator<String> iterator() {
        return this;
    }
}
