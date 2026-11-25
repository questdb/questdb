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
    public void testLegacyPlannerQualifiesTheKey() {
        final String sql = "SELECT * FROM t";
        // a factory one planner compiled never reaches a query the other plans, in any scope
        Assert.assertEquals("\u0001-" + sql, SelectCacheKey.of(null, true, sql, new StringSink()).toString());
        Assert.assertEquals("\u00015:alice" + sql, SelectCacheKey.of("alice", true, sql, new StringSink()).toString());
        Assert.assertEquals("\u00005:alice" + sql, SelectCacheKey.of("alice", false, sql, new StringSink()).toString());
    }

    @Test
    public void testLegacyPlannerSharedScopeCannotSpellAScopedKey() {
        // the shared scope is a '-', which no scope length starts with
        final String shared = SelectCacheKey.of(null, true, "1:aSELECT 1", new StringSink()).toString();
        final String scoped = SelectCacheKey.of("a", true, "SELECT 1", new StringSink()).toString();
        Assert.assertNotEquals(shared, scoped);
    }

    @Test
    public void testNullScopeKeyIsTheSqlText() {
        final String sql = "SELECT * FROM t";
        // the shared scope keeps the cache keyed by the SQL text alone, as it was before scopes
        Assert.assertSame(sql, SelectCacheKey.of(null, false, sql, new StringSink()));
    }

    @Test
    public void testScopeAndSqlTextCannotSpellAnotherKey() {
        // the scope is length-prefixed, so moving characters between scope and SQL text changes the key
        final String keyA = SelectCacheKey.of("ab", false, "c", new StringSink()).toString();
        final String keyB = SelectCacheKey.of("a", false, "bc", new StringSink()).toString();
        Assert.assertNotEquals(keyA, keyB);
        // and no SQL text starts with the NUL that starts a qualified key
        Assert.assertEquals('\u0000', keyA.charAt(0));
    }

    @Test
    public void testScopeQualifiesTheKey() {
        final StringSink sinkA = new StringSink();
        final StringSink sinkB = new StringSink();
        final String sql = "SELECT * FROM t";
        final String keyA = SelectCacheKey.of("alice", false, sql, sinkA).toString();
        final String keyB = SelectCacheKey.of("bob", false, sql, sinkB).toString();
        Assert.assertNotEquals(keyA, keyB);
        Assert.assertNotEquals(sql, keyA);
        Assert.assertEquals(keyA, SelectCacheKey.of("alice", false, sql, sinkB).toString());
    }
}
