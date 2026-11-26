/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.cutlass;

import io.questdb.cutlass.SelectCacheKey;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

public class SelectCacheKeyTest {

    @Test
    public void testNullScopeKeyIsTheSqlText() {
        final String sql = "SELECT * FROM t";
        // the shared scope keeps the cache keyed by the SQL text alone, as it was before scopes
        Assert.assertSame(sql, SelectCacheKey.of(null, sql, new StringSink()));
        // an empty text included
        Assert.assertSame("", SelectCacheKey.of(null, "", new StringSink()));
    }

    @Test
    public void testScopeAndSqlTextCannotSpellAnotherKey() {
        // the scope is length-prefixed, so moving characters between scope and SQL text changes the key
        final String keyA = SelectCacheKey.of("ab", "c", new StringSink()).toString();
        final String keyB = SelectCacheKey.of("a", "bc", new StringSink()).toString();
        Assert.assertNotEquals(keyA, keyB);
        // a qualified key starts with a NUL, and the scope's length follows it
        Assert.assertEquals('\u0000', keyA.charAt(0));
        Assert.assertEquals('2', keyA.charAt(1));
    }

    @Test
    public void testScopeQualifiesTheKey() {
        final StringSink sinkA = new StringSink();
        final StringSink sinkB = new StringSink();
        final String sql = "SELECT * FROM t";
        final String keyA = SelectCacheKey.of("alice", sql, sinkA).toString();
        final String keyB = SelectCacheKey.of("bob", sql, sinkB).toString();
        Assert.assertNotEquals(keyA, keyB);
        Assert.assertNotEquals(sql, keyA);
        Assert.assertEquals(keyA, SelectCacheKey.of("alice", sql, sinkB).toString());
    }

    @Test
    public void testSqlTextStartingWithNulCannotSpellAnotherKey() {
        final StringSink sink = new StringSink();
        // An HTTP client can send a SQL text that starts with a NUL, the character the other keys
        // start with. Its key in the shared scope spells neither a scoped key nor the key of a
        // text with variables.
        Assert.assertNotEquals(
                SelectCacheKey.of("s", "c", sink).toString(),
                SelectCacheKey.of(null, "\u00001:sc", sink).toString()
        );
        Assert.assertNotEquals(
                SelectCacheKey.of(null, "c", "@a := 1", sink).toString(),
                SelectCacheKey.of(null, "\u0000V7:@a := 1-c", sink).toString()
        );
        Assert.assertNotEquals(
                SelectCacheKey.of("s", "c", "@a := 1", sink).toString(),
                SelectCacheKey.of(null, "\u0000V7:@a := 11:sc", sink).toString()
        );
        // nor the key of another such text
        Assert.assertNotEquals(
                SelectCacheKey.of(null, "\u0000a", sink).toString(),
                SelectCacheKey.of(null, "\u0000\u0000a", sink).toString()
        );
        Assert.assertNotEquals(
                SelectCacheKey.of(null, "\u0000a", sink).toString(),
                SelectCacheKey.of(null, "\u0000b", sink).toString()
        );
        // without variables, the text takes the same key from either method, and a scope qualifies it
        Assert.assertEquals(
                SelectCacheKey.of(null, "\u0000a", sink).toString(),
                SelectCacheKey.of(null, "\u0000a", "", sink).toString()
        );
        Assert.assertNotEquals(
                SelectCacheKey.of("s", "\u0000a", sink).toString(),
                SelectCacheKey.of(null, "\u0000a", sink).toString()
        );
    }

    @Test
    public void testVariablesCannotSpellAnotherKey() {
        final StringSink sink = new StringSink();
        // moving characters between the variables, the scope and the SQL text changes the key
        final String keyA = SelectCacheKey.of("s", "c", "@a := 1", sink).toString();
        final String keyB = SelectCacheKey.of("s", "1c", "@a := ", sink).toString();
        final String keyC = SelectCacheKey.of(null, "c", "@a := 1", sink).toString();
        final String keyD = SelectCacheKey.of(null, "1c", "@a := ", sink).toString();
        Assert.assertNotEquals(keyA, keyB);
        Assert.assertNotEquals(keyC, keyD);
        Assert.assertNotEquals(keyA, keyC);
        // Variables that end with what follows them in another key: only the variables' length
        // tells these apart. Unscoped, "@a := 1-" + "-" + "c" against "@a := 1" + "-" + "-c";
        // scoped, "@a := 11:s" + "1:s" + "c" against "@a := 1" + "1:s" + "1:sc".
        Assert.assertNotEquals(
                SelectCacheKey.of(null, "c", "@a := 1-", sink).toString(),
                SelectCacheKey.of(null, "-c", "@a := 1", sink).toString()
        );
        Assert.assertNotEquals(
                SelectCacheKey.of("s", "c", "@a := 11:s", sink).toString(),
                SelectCacheKey.of("s", "1:sc", "@a := 1", sink).toString()
        );
        // A scope that ends with what follows it in another key: only the scope's length tells
        // these apart, "ab" + "c" against "a" + "bc".
        Assert.assertNotEquals(
                SelectCacheKey.of("ab", "c", "@a := 1", sink).toString(),
                SelectCacheKey.of("a", "bc", "@a := 1", sink).toString()
        );
        // a key with variables starts with a NUL and a 'V'; a scoped key without them starts with
        // a NUL and the scope's length, even when the scope starts with a 'V'
        Assert.assertEquals('\u0000', keyA.charAt(0));
        Assert.assertEquals('V', keyA.charAt(1));
        Assert.assertEquals("\u00002:Vxc", SelectCacheKey.of("Vx", "c", sink).toString());
    }

    @Test
    public void testVariablesQualifyTheKey() {
        final String sql = "SELECT * FROM t WHERE v > @a";
        final String keyA = SelectCacheKey.of(null, sql, "@a := 1", new StringSink()).toString();
        final String keyB = SelectCacheKey.of(null, sql, "@a := 2", new StringSink()).toString();
        Assert.assertNotEquals(keyA, keyB);
        Assert.assertNotEquals(sql, keyA);
        // without variables, the key is the one of the SQL text alone
        Assert.assertSame(sql, SelectCacheKey.of(null, sql, "", new StringSink()));
        Assert.assertEquals(
                SelectCacheKey.of("alice", sql, new StringSink()).toString(),
                SelectCacheKey.of("alice", sql, "", new StringSink()).toString()
        );
        Assert.assertNotEquals(
                SelectCacheKey.of("alice", sql, "@a := 1", new StringSink()).toString(),
                SelectCacheKey.of("bob", sql, "@a := 1", new StringSink()).toString()
        );
    }
}
