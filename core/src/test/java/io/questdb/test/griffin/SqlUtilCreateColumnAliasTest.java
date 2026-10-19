/*******************************************************************************
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

package io.questdb.test.griffin;

import io.questdb.griffin.CharacterStore;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.std.Chars;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the wildcard-dedup helper {@link SqlUtil#createColumnAlias}. Its twin
 * {@link SqlUtil#createExprColumnAlias} is unit-tested in {@code SqlUtilTest}; this class pins the
 * branches where the two diverge: {@code createColumnAlias} never truncates, appends the dedup
 * sequence with no {@code '_'} separator, and re-derives the protective quotes per candidate so a
 * duplicated quote-protected alias strips to a clean display name.
 */
public class SqlUtilCreateColumnAliasTest {

    @Test
    public void testBareOperatorTokenCollidesWithQuotedSibling() {
        // A wildcard passthrough of a source column literally named `in` (nonLiteral == false, so it
        // is not quote-forced) shares a display name with an already-taken protective-quoted "in".
        // The precomputed bareQuotedSibling must force a dedup to a clean in1, not a second bare in.
        Ctx ctx = new Ctx();
        ctx.take("\"in\"");
        Assert.assertEquals("in1", alias(ctx, "in", false).toString());
    }

    @Test
    public void testBareOperatorTokenPassThroughWhenFree() {
        // With no quoted sibling taken, a bare operator-token column surfaces verbatim.
        Ctx ctx = new Ctx();
        Assert.assertEquals("in", alias(ctx, "in", false).toString());
    }

    @Test
    public void testComposedQuoteProtectedDottedRefDedupsClean() {
        // Regression: a composed table.column reference whose column part is a quote-protected dotted
        // alias (t1."a.b") keys the dedup on the bare interior and re-wraps the protective quotes
        // around the suffix ("a.b1"), so a duplicate strips to a clean a.b1 - not "a.b"1, which leaks
        // the quotes into result set metadata.
        Ctx ctx = new Ctx();
        CharSequence first = alias(ctx, "t1.\"a.b\"", false);
        Assert.assertEquals("\"a.b\"", first.toString());
        Assert.assertEquals("a.b", SqlUtil.toColumnName(first));
        ctx.take(first.toString());
        CharSequence second = alias(ctx, "t2.\"a.b\"", false);
        Assert.assertEquals("\"a.b1\"", second.toString());
        Assert.assertTrue(SqlUtil.isQuoteProtectedAlias(second));
        Assert.assertEquals("a.b1", SqlUtil.toColumnName(second));
    }

    @Test
    public void testNumericBaseDedupsToColumnPlaceholder() {
        // A numeric base has no valid identifier content, so a collision falls through to the
        // "column" placeholder rather than appending a suffix to the digits.
        Ctx ctx = new Ctx();
        ctx.take("123");
        Assert.assertEquals("column", alias(ctx, "123", false).toString());
    }

    @Test
    public void testNumericBasePassThroughWhenFree() {
        Ctx ctx = new Ctx();
        Assert.assertEquals("123", alias(ctx, "123", false).toString());
    }

    @Test
    public void testPlainDedupHasNoUnderscoreSeparator() {
        // Wildcard dedup numbers with no '_' separator (foo -> foo1), unlike createExprColumnAlias
        // (foo -> foo_2). Drive several collisions to confirm the sequence numbering.
        Ctx ctx = new Ctx();
        ctx.take("foo");
        Assert.assertEquals("foo1", alias(ctx, "foo", false).toString());
        ctx.take("foo1");
        Assert.assertEquals("foo2", alias(ctx, "foo", false).toString());
    }

    @Test
    public void testPlainPassThroughPreservesIdentity() {
        // A free, unprotected base is returned as-is (the wildcard '*' passthrough relies on identity).
        Ctx ctx = new Ctx();
        CharSequence base = "foo";
        Assert.assertSame(base, SqlUtil.createColumnAlias(ctx.store, base, -1, ctx.taken, ctx.seq, false));
    }

    @Test
    public void testPrefixedLiteralStripsTablePrefix() {
        // A composed table.column base keeps only the column tail.
        Ctx ctx = new Ctx();
        Assert.assertEquals("bar", alias(ctx, "foo.bar", false).toString());
    }

    @Test
    public void testQuoteProtectedDottedDedupStaysProtected() {
        // A quote-protected dotted alias keeps its dot as content, so the dedup suffix lands INSIDE
        // the quotes ("a.b" -> "a.b1") and still strips to a clean display name a.b1 - not "a.b"1,
        // which would leak the quotes.
        Ctx ctx = new Ctx();
        ctx.take("a.b"); // a sibling already surfaced the clean display name a.b
        CharSequence second = alias(ctx, "\"a.b\"", false);
        Assert.assertEquals("\"a.b1\"", second.toString());
        Assert.assertTrue(SqlUtil.isQuoteProtectedAlias(second));
        Assert.assertEquals("a.b1", SqlUtil.toColumnName(second));
    }

    @Test
    public void testQuoteProtectedDottedPassThroughWhenFree() {
        Ctx ctx = new Ctx();
        CharSequence first = alias(ctx, "\"a.b\"", false);
        Assert.assertEquals("\"a.b\"", first.toString());
        Assert.assertEquals("a.b", SqlUtil.toColumnName(first));
    }

    @Test
    public void testQuoteProtectedOperatorTokenDedupShedsQuotes() {
        // Once the dedup suffix turns the operator token into a plain identifier the protective quotes
        // are no longer needed: "in" -> in1 (bare), matching the pivot-through-join dedup (in / in1).
        Ctx ctx = new Ctx();
        ctx.take("in"); // a sibling already surfaced the clean display name in
        CharSequence second = alias(ctx, "\"in\"", false);
        Assert.assertEquals("in1", second.toString());
        Assert.assertFalse(SqlUtil.isQuoteProtectedAlias(second));
        Assert.assertEquals("in1", SqlUtil.toColumnName(second));
    }

    @Test
    public void testQuoteProtectedOperatorTokenPassThroughWhenFree() {
        Ctx ctx = new Ctx();
        CharSequence first = alias(ctx, "\"in\"", false);
        Assert.assertEquals("\"in\"", first.toString());
        Assert.assertEquals("in", SqlUtil.toColumnName(first));
    }

    private static CharSequence alias(Ctx ctx, CharSequence base, boolean nonLiteral) {
        return SqlUtil.createColumnAlias(ctx.store, base, Chars.indexOfLastUnquoted(base, '.'), ctx.taken, ctx.seq, nonLiteral);
    }

    private static final class Ctx {
        final LowerCaseCharSequenceIntHashMap seq = new LowerCaseCharSequenceIntHashMap();
        final CharacterStore store = new CharacterStore(64, 4);
        final LowerCaseCharSequenceObjHashMap<QueryColumn> taken = new LowerCaseCharSequenceObjHashMap<>();

        void take(CharSequence alias) {
            taken.put(alias, null);
        }
    }
}
