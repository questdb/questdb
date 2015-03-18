/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb;

import com.nfsdb.lang.parser.TokenStream;
import com.nfsdb.utils.Chars;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

public class TokenStreamTest {
    @Test
    public void testEdgeSymbol() throws Exception {
        TokenStream ts = new TokenStream();
        ts.defineSymbol(" ");
        ts.defineSymbol("+");
        ts.defineSymbol("(");
        ts.defineSymbol(")");
        ts.defineSymbol(",");

        ts.setContent("create journal xyz(a int, b int)");

        Iterator<CharSequence> iterator = ts.iterator();
        Assert.assertTrue(Chars.equals("create", iterator.next()));
        Assert.assertTrue(Chars.equals(" ", iterator.next()));
        Assert.assertTrue(Chars.equals("journal", iterator.next()));
        Assert.assertTrue(Chars.equals(" ", iterator.next()));
        Assert.assertTrue(Chars.equals("xyz", iterator.next()));
        Assert.assertTrue(Chars.equals("(", iterator.next()));
        Assert.assertTrue(Chars.equals("a", iterator.next()));
        Assert.assertTrue(Chars.equals(" ", iterator.next()));
        Assert.assertTrue(Chars.equals("int", iterator.next()));
        Assert.assertTrue(Chars.equals(",", iterator.next()));
        Assert.assertTrue(Chars.equals(" ", iterator.next()));
        Assert.assertTrue(Chars.equals("b", iterator.next()));
        Assert.assertTrue(Chars.equals(" ", iterator.next()));
        Assert.assertTrue(Chars.equals("int", iterator.next()));
        Assert.assertTrue(Chars.equals(")", iterator.next()));
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testNullContent() throws Exception {
        TokenStream ts = new TokenStream();
        ts.defineSymbol(" ");
        ts.setContent(null);
        Assert.assertFalse(ts.iterator().hasNext());
    }

    @Test
    public void testSymbolLookup() throws Exception {
        TokenStream ts = new TokenStream();
        ts.defineSymbol("+");
        ts.defineSymbol("++");
        ts.defineSymbol("*");

        ts.setContent("+*a+b++blah-");

        Iterator<CharSequence> iterator = ts.iterator();
        Assert.assertTrue(Chars.equals("+", iterator.next()));
        Assert.assertTrue(Chars.equals("*", iterator.next()));
        Assert.assertTrue(Chars.equals("a", iterator.next()));
        Assert.assertTrue(Chars.equals("+", iterator.next()));
        Assert.assertTrue(Chars.equals("b", iterator.next()));
        Assert.assertTrue(Chars.equals("++", iterator.next()));
        Assert.assertTrue(Chars.equals("blah-", iterator.next()));
        Assert.assertFalse(iterator.hasNext());
    }
}
