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
import org.junit.Assert;
import org.junit.Test;

public class SelectCacheKeyTest {
    private static final String SQL = "SELECT * FROM t";

    @Test
    public void testClearKeysTheSharedScopeOfTheNewPlanner() {
        final SelectCacheKey key = new SelectCacheKey().of("alice", true);
        key.clear();
        Assert.assertSame(SQL, key.key(SQL));
        Assert.assertSame(SQL, key.insertKey(SQL));
    }

    @Test
    public void testCopyOfTakesScopeAndPlanner() {
        final SelectCacheKey key = new SelectCacheKey().of("bob", false);
        key.copyOf(new SelectCacheKey().of("alice", true));
        Assert.assertEquals("\u00015:alice" + SQL, key.key(SQL).toString());
    }

    @Test
    public void testInsertKeyCarriesThePlannerButNoScope() {
        Assert.assertEquals("\u0001-" + SQL, new SelectCacheKey().of("alice", true).insertKey(SQL).toString());
        Assert.assertSame(SQL, new SelectCacheKey().of("alice", false).insertKey(SQL));
    }

    @Test
    public void testLegacyPlannerQualifiesTheKey() {
        // a factory one planner compiled never reaches a query the other plans, in any scope
        Assert.assertEquals("\u0001-" + SQL, new SelectCacheKey().of(null, true).key(SQL).toString());
        Assert.assertEquals("\u00015:alice" + SQL, new SelectCacheKey().of("alice", true).key(SQL).toString());
        Assert.assertEquals("\u00005:alice" + SQL, new SelectCacheKey().of("alice", false).key(SQL).toString());
    }

    @Test
    public void testLegacyPlannerSharedScopeCannotSpellAScopedKey() {
        // the shared scope is a '-', which no scope length starts with
        final String shared = new SelectCacheKey().of(null, true).key("1:aSELECT 1").toString();
        final String scoped = new SelectCacheKey().of("a", true).key("SELECT 1").toString();
        Assert.assertNotEquals(shared, scoped);
    }

    @Test
    public void testNullScopeKeyIsTheSqlText() {
        // the shared scope keeps the cache keyed by the SQL text alone, as it was before scopes
        Assert.assertSame(SQL, new SelectCacheKey().of(null, false).key(SQL));
    }

    @Test
    public void testScopeAndSqlTextCannotSpellAnotherKey() {
        // the scope is length-prefixed, so moving characters between scope and SQL text changes the key
        final String keyA = new SelectCacheKey().of("ab", false).key("c").toString();
        final String keyB = new SelectCacheKey().of("a", false).key("bc").toString();
        Assert.assertNotEquals(keyA, keyB);
        // and no SQL text starts with the NUL that starts a qualified key
        Assert.assertEquals('\u0000', keyA.charAt(0));
    }

    @Test
    public void testScopeQualifiesTheKey() {
        final SelectCacheKey key = new SelectCacheKey();
        final String keyA = key.of("alice", false).key(SQL).toString();
        final String keyB = key.of("bob", false).key(SQL).toString();
        Assert.assertNotEquals(keyA, keyB);
        Assert.assertNotEquals(SQL, keyA);
        Assert.assertEquals(keyA, key.of("alice", false).key(SQL).toString());
    }
}
