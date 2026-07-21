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

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.MillisTimestampDriver;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.InvalidColumnException;
import io.questdb.griffin.CharacterStore;
import io.questdb.griffin.OperatorExpression;
import io.questdb.griffin.OperatorRegistry;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.griffin.engine.functions.constants.Long256Constant;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class SqlUtilTest {

    @Test
    public void testExprColumnAliasAllSpaceBaseYieldsColumn() {
        // A quote-protected base whose content truncates down to only spaces must not surface as
        // a bare space: the space-trim collapses it to nothing and the "column" placeholder takes
        // over (regression for the all-space alias leak - the old !quote-gated trim left " ").
        // The placeholder honours maxLength too, so at maxLength 4 it surfaces the bounded "colu"
        // rather than a fixed 6-char name.
        CharacterStore store = new CharacterStore(64, 4);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(8);
        LowerCaseCharSequenceIntHashMap seqMap = new LowerCaseCharSequenceIntHashMap();
        CharSequence alias = SqlUtil.createExprColumnAlias(store, "  .z", aliasMap, seqMap, 4, true);
        Assert.assertEquals("colu", alias.toString());
        Assert.assertFalse(SqlUtil.isQuoteProtectedAlias(alias));
        // the full "column" placeholder still surfaces at a larger cap (see testExprColumnAliasEmptyBase)
        CharSequence full = SqlUtil.createExprColumnAlias(new CharacterStore(64, 4), "   ",
                new LowerCaseCharSequenceObjHashMap<>(8), new LowerCaseCharSequenceIntHashMap(), 64, true);
        Assert.assertEquals("column", full.toString());
    }

    @Test
    public void testExprColumnAliasBareOperatorTokenAfterQuotedSiblingDedups() {
        // Regression: the createExprColumnAlias early-exit returned a bare operator-token base
        // verbatim when only its RAW form was free, missing the display-name collision with an
        // already-taken protective-quoted "<token>" sibling ("in" and bare in both surface as in
        // via toColumnName). A qualified x."in" (aliased "in") followed by a bare "in" (base in,
        // nonLiteral=false) then produced two columns both displaying `in`, so DISTINCT / GROUP BY /
        // SAMPLE BY threw "duplicate column [name=in]". The early-exit must check the sibling and
        // fall through to the dedup loop, mirroring createColumnAlias.
        CharacterStore store = new CharacterStore(64, 4);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(8);
        LowerCaseCharSequenceIntHashMap seqMap = new LowerCaseCharSequenceIntHashMap();

        // a sibling column already surfaced the protective-quoted "in" (display name in)
        aliasMap.put("\"in\"", null);

        // the bare operator-token literal reference must not reuse the display name in
        CharSequence alias = SqlUtil.createExprColumnAlias(store, "in", aliasMap, seqMap, 64, false);
        Assert.assertEquals("in_2", alias.toString());
        Assert.assertFalse(SqlUtil.isQuoteProtectedAlias(alias));
        Assert.assertEquals("in_2", SqlUtil.toColumnName(alias));

        // an ordinary (non-token) base whose raw form is free still returns verbatim, by identity
        CharSequence plain = "plain";
        Assert.assertSame(plain, SqlUtil.createExprColumnAlias(store, plain, aliasMap, seqMap, 64, false));
    }

    @Test
    public void testExprColumnAliasComposedQuoteProtectedDottedDedupsClean() {
        // Regression: a composed literal reference whose column part is a quote-protected dotted
        // alias (t1."a.b") must alias the bare interior and re-wrap the protective quotes around the
        // dedup suffix ("a.b_2"), not leave them outside a copied "a.b" ("a.b"_2), which
        // isQuoteProtectedAlias no longer recognizes and toColumnName cannot strip - leaking the
        // quotes into result set metadata.
        CharacterStore store = new CharacterStore(64, 4);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(8);
        LowerCaseCharSequenceIntHashMap seqMap = new LowerCaseCharSequenceIntHashMap();

        CharSequence first = SqlUtil.createExprColumnAlias(store, "t1.\"a.b\"", aliasMap, seqMap, 64, false);
        Assert.assertEquals("\"a.b\"", first.toString());
        Assert.assertEquals("a.b", SqlUtil.toColumnName(first));
        aliasMap.put(first.toString(), null);

        CharSequence second = SqlUtil.createExprColumnAlias(store, "t2.\"a.b\"", aliasMap, seqMap, 64, false);
        Assert.assertEquals("\"a.b_2\"", second.toString());
        Assert.assertTrue(SqlUtil.isQuoteProtectedAlias(second));
        Assert.assertEquals("a.b_2", SqlUtil.toColumnName(second));
    }

    @Test
    public void testExprColumnAliasDedupManyCollisions() {
        // Drives the dedup loop through many collisions in a single call. The candidate store
        // entry is rewound (trimTo) and rebuilt each iteration, so the sequence numbering must
        // stay correct - guards the store-reuse optimization against rewind corruption.
        CharacterStore store = new CharacterStore(64, 4);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(8);
        LowerCaseCharSequenceIntHashMap seqMap = new LowerCaseCharSequenceIntHashMap();
        aliasMap.put("same", null);
        aliasMap.put("same_2", null);
        aliasMap.put("same_3", null);
        aliasMap.put("same_4", null);
        // a single call must skip past all four occupied names to same_5
        Assert.assertEquals("same_5", SqlUtil.createExprColumnAlias(store, "same", aliasMap, seqMap, 64, false).toString());
    }

    @Test
    public void testExprColumnAliasDedupOperatorTokenDropsStaleQuotes() {
        // An operator-token alias is quote-protected, but once a dedup suffix makes it a plain
        // identifier the protection is no longer needed: the quotes must be dropped, not left to
        // leak into the result-set column name (regression for the PIVOT operator-token leak).
        CharacterStore store = new CharacterStore(64, 4);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(8);
        LowerCaseCharSequenceIntHashMap seqMap = new LowerCaseCharSequenceIntHashMap();

        CharSequence first = SqlUtil.createExprColumnAlias(store, "in", aliasMap, seqMap, 64, true);
        Assert.assertEquals("\"in\"", first.toString());
        aliasMap.put(first.toString(), null);

        CharSequence second = SqlUtil.createExprColumnAlias(store, "in", aliasMap, seqMap, 64, true);
        Assert.assertEquals("in_2", second.toString());
        Assert.assertFalse(SqlUtil.isQuoteProtectedAlias(second));
        // the visible names carry no leaked quotes
        Assert.assertEquals("in", SqlUtil.toColumnName(first));
        Assert.assertEquals("in_2", SqlUtil.toColumnName(second));
    }

    @Test
    public void testExprColumnAliasDisallowedAlias() {
        OperatorRegistry registry = OperatorExpression.getRegistry();
        CharacterStore store = new CharacterStore(32, 1);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(0);
        LowerCaseCharSequenceIntHashMap seqMap = new LowerCaseCharSequenceIntHashMap();
        for (int i = 0, n = registry.operators.size(); i < n; i++) {
            String token = registry.operators.getQuick(i).getToken();
            Assert.assertEquals(
                    '"' + token + '"',
                    SqlUtil.createExprColumnAlias(store, token, aliasMap, seqMap, 64, true).toString()
            );

            // verify that disallowed aliases look-up is case-insensitive
            token = token.toUpperCase();
            aliasMap.clear();
            seqMap.clear();
            Assert.assertEquals(
                    '"' + token + '"',
                    SqlUtil.createExprColumnAlias(store, token, aliasMap, seqMap, 64, true).toString()
            );
        }
    }

    @Test
    public void testExprColumnAliasDottedTruncatedTrailingSpaceOperatorTokenDedups() {
        // Regression: a quote-protected dotted base ('in .x') whose maxLength truncation drops the
        // discriminating dot leaves a trailing-space slice ('in ') that trims back to the operator
        // token 'in'. The quote decision must key on the TRIMMED content, or the un-suffixed candidate
        // surfaces a bare 'in' that duplicates a sibling value 'in' (both display 'in' via toColumnName),
        // so the projection metadata build throws "Duplicate column [name=in]". This is the quote==true
        // counterpart of testExprColumnAliasOperatorTokenTrailingSpaceDedups (the !quote path the earlier
        // fix covered); deciding emitQuote before the space-trim left this variant open.
        CharacterStore store = new CharacterStore(64, 4);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(8);
        LowerCaseCharSequenceIntHashMap seqMap = new LowerCaseCharSequenceIntHashMap();

        // sibling value 'in' surfaces the protective-quoted "in" (display name in)
        CharSequence first = SqlUtil.createExprColumnAlias(store, "in", aliasMap, seqMap, 6, true);
        Assert.assertEquals("\"in\"", first.toString());
        Assert.assertEquals("in", SqlUtil.toColumnName(first));
        aliasMap.put(first.toString(), null);

        // 'in .x' truncated at maxLength 6 drops the dot; the residual 'in ' trims to the operator
        // token 'in', which must re-quote and dedup - never a second bare 'in' colliding with the sibling.
        CharSequence second = SqlUtil.createExprColumnAlias(store, "in .x", aliasMap, seqMap, 6, true);
        Assert.assertNotEquals("in", SqlUtil.toColumnName(second));
        Assert.assertEquals("i_2", second.toString());
        Assert.assertEquals("i_2", SqlUtil.toColumnName(second));
    }

    @Test
    public void testExprColumnAliasDuplicates() {
        CharacterStore store = new CharacterStore(32, 1);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(8);
        LowerCaseCharSequenceIntHashMap seqMap = new LowerCaseCharSequenceIntHashMap();
        aliasMap.put("same", null);
        Assert.assertEquals(
                "same_2",
                SqlUtil.createExprColumnAlias(store, "same", aliasMap, seqMap, 64, false).toString()
        );
        aliasMap.put("same_2", null);
        Assert.assertEquals(
                "same_3",
                SqlUtil.createExprColumnAlias(store, "same", aliasMap, seqMap, 64, false).toString()
        );
    }

    @Test(timeout = 30000)
    public void testExprColumnAliasEmptyBase() {
        // An empty base is quote-forced (the empty string is a disallowed alias) but has no
        // content left to protect, so the return gate keyed on contentLen was never satisfied
        // and the loop spun forever. It must terminate with a "column" placeholder instead
        // (regression for the PIVOT IN ('') compiler hang).
        CharacterStore store = new CharacterStore(64, 4);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(8);
        LowerCaseCharSequenceIntHashMap seqMap = new LowerCaseCharSequenceIntHashMap();

        CharSequence first = SqlUtil.createExprColumnAlias(store, "", aliasMap, seqMap, 64, true);
        Assert.assertEquals("column", first.toString());
        Assert.assertFalse(SqlUtil.isQuoteProtectedAlias(first));
        aliasMap.put(first.toString(), null);

        // a second empty base dedups cleanly, still no leaked quotes
        CharSequence second = SqlUtil.createExprColumnAlias(store, "", aliasMap, seqMap, 64, true);
        Assert.assertEquals("column_2", second.toString());
    }

    @Test
    public void testExprColumnAliasOperatorTokenTrailingSpaceDedups() {
        // Regression: an operator-token value with trailing whitespace ('in ') is NOT quote-forced
        // (the raw string "in " is not a disallowed alias, so quote == false), but the space-trim
        // reduces its bare content to `in`, an operator token whose display name collides with the
        // protective-quoted "in" of a sibling value 'in'. The un-suffixed bare candidate used to skip
        // the quoted-sibling check and surface a second bare `in`, so both columns displayed as `in`.
        // The dedup must now yield a clean in / in_2. The collision is caught in either alias order.
        LowerCaseCharSequenceIntHashMap seqMap = new LowerCaseCharSequenceIntHashMap();

        // 'in' aliased first -> "in"; 'in ' collides on display name in -> clean in_2
        CharacterStore store = new CharacterStore(64, 4);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(8);
        CharSequence first = SqlUtil.createExprColumnAlias(store, "in", aliasMap, seqMap, 64, true);
        Assert.assertEquals("\"in\"", first.toString());
        aliasMap.put(first.toString(), null);
        CharSequence second = SqlUtil.createExprColumnAlias(store, "in ", aliasMap, seqMap, 64, true);
        Assert.assertEquals("in_2", second.toString());
        Assert.assertFalse(SqlUtil.isQuoteProtectedAlias(second));
        Assert.assertEquals("in_2", SqlUtil.toColumnName(second));

        // reverse order: 'in ' aliased first -> bare in; 'in' -> in_2. Same {in, in_2} display set.
        CharacterStore store2 = new CharacterStore(64, 4);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap2 = new LowerCaseCharSequenceObjHashMap<>(8);
        LowerCaseCharSequenceIntHashMap seqMap2 = new LowerCaseCharSequenceIntHashMap();
        CharSequence firstRev = SqlUtil.createExprColumnAlias(store2, "in ", aliasMap2, seqMap2, 64, true);
        Assert.assertEquals("in", firstRev.toString());
        aliasMap2.put(firstRev.toString(), null);
        CharSequence secondRev = SqlUtil.createExprColumnAlias(store2, "in", aliasMap2, seqMap2, 64, true);
        Assert.assertEquals("in_2", secondRev.toString());
        Assert.assertEquals("in_2", SqlUtil.toColumnName(secondRev));
    }

    @Test
    public void testExprColumnAliasProtectedTrailingSpaceTrimmed() {
        // A quote-protected dotted value with a trailing space ('a.b ') must surface a clean display
        // name with no bare trailing space (an interop hazard over PG / HTTP / CSV), matching the
        // operator-token path ('in ' -> in). The dot survives the trim, so the value stays protected
        // and strips clean; a space-free sibling 'a.b' then dedups it (a.b / a.b_2) rather than
        // leaving two columns differing only by a trailing space.
        CharacterStore store = new CharacterStore(64, 4);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(8);
        LowerCaseCharSequenceIntHashMap seqMap = new LowerCaseCharSequenceIntHashMap();

        // solo dotted value with a trailing space surfaces trimmed, still protected
        CharSequence solo = SqlUtil.createExprColumnAlias(store, "a.b ", aliasMap, seqMap, 64, true);
        Assert.assertEquals("\"a.b\"", solo.toString());
        Assert.assertTrue(SqlUtil.isQuoteProtectedAlias(solo));
        Assert.assertEquals("a.b", SqlUtil.toColumnName(solo));

        // a space-free sibling forces the trailing-space value to dedup instead of colliding
        CharacterStore store2 = new CharacterStore(64, 4);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap2 = new LowerCaseCharSequenceObjHashMap<>(8);
        LowerCaseCharSequenceIntHashMap seqMap2 = new LowerCaseCharSequenceIntHashMap();
        CharSequence first = SqlUtil.createExprColumnAlias(store2, "a.b", aliasMap2, seqMap2, 64, true);
        Assert.assertEquals("\"a.b\"", first.toString());
        aliasMap2.put(first.toString(), null);
        CharSequence second = SqlUtil.createExprColumnAlias(store2, "a.b ", aliasMap2, seqMap2, 64, true);
        Assert.assertEquals("\"a.b_2\"", second.toString());
        Assert.assertEquals("a.b_2", SqlUtil.toColumnName(second));
    }

    @Test
    public void testExprColumnAliasQuotedContentValueDedupsClean() {
        // A PIVOT value whose data is literally "in" displays as in (toColumnName strips the data
        // quotes, the documented quoted-content-value tradeoff). When it collides with an operator-token
        // value 'in', the dedup must yield a clean in_2 - not "in"_2, which left the quotes embedded
        // mid-name where toColumnName could not strip them, leaking into result set metadata and breaking
        // CREATE TABLE AS SELECT (regression, non-deterministic for a dynamic pivot where scan order
        // decides which value collides).
        CharacterStore store = new CharacterStore(64, 4);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(8);
        LowerCaseCharSequenceIntHashMap seqMap = new LowerCaseCharSequenceIntHashMap();

        // value 'in' -> protective "in" (displays in)
        CharSequence first = SqlUtil.createExprColumnAlias(store, "in", aliasMap, seqMap, 64, true);
        Assert.assertEquals("\"in\"", first.toString());
        aliasMap.put(first.toString(), null);

        // value '"in"' (data literally "in") collides on display name in -> clean in_2, no leaked quotes
        CharSequence second = SqlUtil.createExprColumnAlias(store, "\"in\"", aliasMap, seqMap, 64, true);
        Assert.assertEquals("in_2", second.toString());
        Assert.assertFalse(SqlUtil.isQuoteProtectedAlias(second));
        Assert.assertEquals("in_2", SqlUtil.toColumnName(second));
    }

    @Test
    public void testExprColumnAliasQuotedDottedContentValueDedupsClean() {
        // A dotted quoted-content value "a.b" displays as a.b; a second one must dedup to a clean a.b_2,
        // re-protected as a WHOLE ("a.b_2"), not "a.b"_2 with the quotes embedded mid-name.
        CharacterStore store = new CharacterStore(64, 4);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(8);
        LowerCaseCharSequenceIntHashMap seqMap = new LowerCaseCharSequenceIntHashMap();

        CharSequence first = SqlUtil.createExprColumnAlias(store, "\"a.b\"", aliasMap, seqMap, 64, true);
        Assert.assertEquals("\"a.b\"", first.toString());
        Assert.assertEquals("a.b", SqlUtil.toColumnName(first));
        aliasMap.put(first.toString(), null);

        CharSequence second = SqlUtil.createExprColumnAlias(store, "\"a.b\"", aliasMap, seqMap, 64, true);
        Assert.assertEquals("\"a.b_2\"", second.toString());
        Assert.assertTrue(SqlUtil.isQuoteProtectedAlias(second));
        Assert.assertEquals("a.b_2", SqlUtil.toColumnName(second));
    }

    @Test
    public void testExprColumnAliasSeqSizeDigitBoundaryUnderTruncation() {
        // The dedup suffix reserves seqSize chars from a truncated alias, so crossing the 9 -> 10
        // sequence boundary (seqSize 2 -> 3) shrinks the retained content by one. This is the only
        // regime where seqSize affects the output, and it exercises the integer digit-count that
        // replaced Math.log10 (which rounds wrong at exact powers of ten). At maxLength 6 the taken
        // set forces the loop past sequence 9: abcdef, abcd_2..abcd_9 are occupied, so the next
        // candidate must be abc_10 - "abc" (3 chars) + "_10" (3 chars) = 6, the truncated content
        // one char shorter than the abcd_N (seqSize 2) candidates.
        CharacterStore store = new CharacterStore(64, 4);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(16);
        LowerCaseCharSequenceIntHashMap seqMap = new LowerCaseCharSequenceIntHashMap();
        aliasMap.put("abcdef", null);
        for (int i = 2; i <= 9; i++) {
            aliasMap.put("abcd_" + i, null);
        }
        CharSequence alias = SqlUtil.createExprColumnAlias(store, "abcdef", aliasMap, seqMap, 6, false);
        Assert.assertEquals("abc_10", alias.toString());
        Assert.assertEquals(6, alias.length());
    }

    @Test
    public void testExprColumnAliasSimpleCase() {
        CharacterStore store = new CharacterStore(32, 1);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(0);
        LowerCaseCharSequenceIntHashMap seqMap = new LowerCaseCharSequenceIntHashMap();
        Assert.assertEquals(
                "basic",
                SqlUtil.createExprColumnAlias(store, "basic", aliasMap, seqMap, 64, false).toString()
        );
    }

    @Test
    public void testExprColumnAliasTrimEnd() {
        CharacterStore store = new CharacterStore(32, 1);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(0);
        LowerCaseCharSequenceIntHashMap seqMap = new LowerCaseCharSequenceIntHashMap();
        Assert.assertEquals(
                "  space",
                SqlUtil.createExprColumnAlias(store, "  space    ", aliasMap, seqMap, 64, false).toString()
        );
    }

    @Test
    public void testExprColumnAliasTrimmed() {
        CharacterStore store = new CharacterStore(32, 1);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(0);
        LowerCaseCharSequenceIntHashMap seqMap = new LowerCaseCharSequenceIntHashMap();
        Assert.assertEquals(
                "longstr",
                SqlUtil.createExprColumnAlias(store, "longstring", aliasMap, seqMap, 7, false).toString()
        );
    }

    @Test
    public void testExprColumnAliasTruncationDropsStaleQuotes() {
        // The alias is quote-protected because of its dot, but truncation to maxLength cuts the
        // dot off. The bare remainder needs no protection, so the wrapping quotes must not survive
        // into the column name (regression for the long-dotted-alias leak).
        CharacterStore store = new CharacterStore(64, 4);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(8);
        LowerCaseCharSequenceIntHashMap seqMap = new LowerCaseCharSequenceIntHashMap();
        CharSequence alias = SqlUtil.createExprColumnAlias(store, "aaaaaaaa.b", aliasMap, seqMap, 6, true);
        Assert.assertEquals("aaa", alias.toString());
        Assert.assertFalse(SqlUtil.isQuoteProtectedAlias(alias));
        Assert.assertEquals("aaa", SqlUtil.toColumnName(alias));
    }

    @Test
    public void testExprColumnAliasTruncationDropsTrailingSpace() {
        // Truncation cuts the dot off a quote-protected base, so the protective quotes are
        // dropped; the bare remainder must still have its trailing space trimmed. The trim now
        // keys on the final bare-ness (!emitQuote), not on the original quote flag, so the column
        // name never ends in a space (regression: the old !quote gate left "ab ").
        CharacterStore store = new CharacterStore(64, 4);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(8);
        LowerCaseCharSequenceIntHashMap seqMap = new LowerCaseCharSequenceIntHashMap();
        CharSequence alias = SqlUtil.createExprColumnAlias(store, "ab .c", aliasMap, seqMap, 6, true);
        Assert.assertEquals("ab", alias.toString());
        Assert.assertFalse(SqlUtil.isQuoteProtectedAlias(alias));
    }

    @Test
    public void testExprColumnAliasWholeQuotedLiteralDottedStaysProtected() {
        // A whole quote-protected DOTTED base with nonLiteral=false must alias its interior as protected
        // content (displaying a.b), not mis-fire prefixedLiteral and alias only the tail `b`. A duplicate
        // keeps the dedup suffix INSIDE the quotes ("a.b_2") so it strips to a clean a.b_2, never a leaked
        // "a.b"_2. Mirrors createColumnAlias's whole quote-protected handling for the literal path.
        CharacterStore store = new CharacterStore(64, 4);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(8);
        LowerCaseCharSequenceIntHashMap seqMap = new LowerCaseCharSequenceIntHashMap();

        CharSequence first = SqlUtil.createExprColumnAlias(store, "\"a.b\"", aliasMap, seqMap, 64, false);
        Assert.assertEquals("\"a.b\"", first.toString());
        Assert.assertEquals("a.b", SqlUtil.toColumnName(first));
        aliasMap.put(first.toString(), null);

        CharSequence second = SqlUtil.createExprColumnAlias(store, "\"a.b\"", aliasMap, seqMap, 64, false);
        Assert.assertEquals("\"a.b_2\"", second.toString());
        Assert.assertTrue(SqlUtil.isQuoteProtectedAlias(second));
        Assert.assertEquals("a.b_2", SqlUtil.toColumnName(second));
    }

    @Test
    public void testExprColumnAliasWholeQuotedLiteralOperatorTokenDedups() {
        // Regression (createExprColumnAlias vs createColumnAlias asymmetry): a WHOLE quote-protected
        // operator-token base with nonLiteral=false (e.g. a compiler alias surfacing in its protected
        // "in" form) must be keyed on its stripped display name `in`, not returned verbatim. A sibling
        // `in` and a base "in" both display `in`, so the second must dedup to in_2 - not a second bare
        // column also displaying `in` (the early-exit used to return "in" verbatim), and never a leaked
        // "in"_2. The early-exit only checked the raw-token sibling, missing the quote-protected interior
        // its twin createColumnAlias checks. This path is not reachable from SQL today (subquery metadata
        // stores clean names, so an outer reference is the bare `in`), so it is a defensive consistency
        // guard on a zero-tolerance path.
        CharacterStore store = new CharacterStore(64, 4);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(8);
        LowerCaseCharSequenceIntHashMap seqMap = new LowerCaseCharSequenceIntHashMap();

        // no collision: the whole-quoted base surfaces as its protected form (display name in)
        CharSequence fresh = SqlUtil.createExprColumnAlias(store, "\"in\"", aliasMap, seqMap, 64, false);
        Assert.assertEquals("\"in\"", fresh.toString());
        Assert.assertEquals("in", SqlUtil.toColumnName(fresh));

        // sibling `in` already surfaced the display name in
        CharSequence sibling = SqlUtil.createExprColumnAlias(store, "in", aliasMap, seqMap, 64, false);
        Assert.assertEquals("in", sibling.toString());
        aliasMap.put(sibling.toString(), null);

        // the whole-quoted "in" must now dedup against it, cleanly and quote-free
        CharSequence deduped = SqlUtil.createExprColumnAlias(store, "\"in\"", aliasMap, seqMap, 64, false);
        Assert.assertEquals("in_2", deduped.toString());
        Assert.assertFalse(SqlUtil.isQuoteProtectedAlias(deduped));
        Assert.assertEquals("in_2", SqlUtil.toColumnName(deduped));
    }

    @Test
    public void testExprNonLiteral() {
        CharacterStore store = new CharacterStore(32, 1);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(0);
        LowerCaseCharSequenceIntHashMap seqMap = new LowerCaseCharSequenceIntHashMap();
        Assert.assertEquals(
                "\"quoted\"",
                SqlUtil.createExprColumnAlias(store, "\"quoted\"", aliasMap, seqMap, 64, true).toString()
        );
        Assert.assertEquals(
                "\"prefix.nonliteral\"",
                SqlUtil.createExprColumnAlias(store, "prefix.nonliteral", aliasMap, seqMap, 64, true).toString()
        );
        Assert.assertEquals(
                "\"prefix.\"",
                SqlUtil.createExprColumnAlias(store, "prefix.", aliasMap, seqMap, 64, true).toString()
        );
    }

    @Test
    public void testExprPrefixedColumn() {
        CharacterStore store = new CharacterStore(32, 1);
        LowerCaseCharSequenceObjHashMap<QueryColumn> aliasMap = new LowerCaseCharSequenceObjHashMap<>(0);
        LowerCaseCharSequenceIntHashMap seqMap = new LowerCaseCharSequenceIntHashMap();
        Assert.assertEquals(
                "basic",
                SqlUtil.createExprColumnAlias(store, "table.basic", aliasMap, seqMap, 64, false).toString()
        );
        aliasMap.put("basic", null);
        Assert.assertEquals(
                "\"between\"",
                SqlUtil.createExprColumnAlias(store, "table.between", aliasMap, seqMap, 64, false).toString()
        );
        Assert.assertEquals(
                "\"quoted\"",
                SqlUtil.createExprColumnAlias(store, "\"table\".\"quoted\"", aliasMap, seqMap, 64, false).toString()
        );
        Assert.assertEquals(
                "\"quoted.table\"",
                SqlUtil.createExprColumnAlias(store, "\"quoted.table\"", aliasMap, seqMap, 64, false).toString()
        );
        Assert.assertEquals(
                "spaces",
                SqlUtil.createExprColumnAlias(store, "table.spaces   ", aliasMap, seqMap, 64, false).toString()
        );
        Assert.assertEquals(
                "\"quoted spaces   \"",
                SqlUtil.createExprColumnAlias(store, "table.\"quoted spaces   \"", aliasMap, seqMap, 64, false).toString()
        );
        Assert.assertEquals(
                "\"table.\"",
                SqlUtil.createExprColumnAlias(store, "table.", aliasMap, seqMap, 64, false).toString()
        );

        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(
                    "basic_" + (i + 2),
                    SqlUtil.createExprColumnAlias(store, "table.basic", aliasMap, seqMap, 64, false).toString()
            );
            aliasMap.put("basic_" + (i + 2), null);
        }
    }

    @Test
    public void testGetColumnIndexQuietHelperStripsProtectedAliasOnMiss() {
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a.b", ColumnType.INT));
        metadata.add(new TableColumnMetadata("in", ColumnType.INT));
        metadata.add(new TableColumnMetadata("\"f,g\"", ColumnType.INT));

        // protected tokens resolve against the clean stored names
        Assert.assertEquals(0, SqlUtil.getColumnIndexQuiet(metadata, "\"a.b\""));
        Assert.assertEquals(1, SqlUtil.getColumnIndexQuiet(metadata, "\"in\""));
        // unprotected tokens stay verbatim
        Assert.assertEquals(0, SqlUtil.getColumnIndexQuiet(metadata, "a.b"));
        Assert.assertEquals(2, SqlUtil.getColumnIndexQuiet(metadata, "\"f,g\""));
        Assert.assertEquals(-1, SqlUtil.getColumnIndexQuiet(metadata, "\"c.d\""));

        // a verbatim match wins over the stripped retry
        GenericRecordMetadata quoted = new GenericRecordMetadata();
        quoted.add(new TableColumnMetadata("\"a.b\"", ColumnType.INT));
        quoted.add(new TableColumnMetadata("a.b", ColumnType.INT));
        Assert.assertEquals(0, SqlUtil.getColumnIndexQuiet(quoted, "\"a.b\""));
    }

    @Test
    public void testGetColumnIndexQuietInterfaceDefaultIsVerbatim() {
        // Ingestion paths resolve wire-supplied names through the RecordMetadata default;
        // it must never strip compiler-alias quotes, or an ILP field named "in" (quote
        // characters included) would be silently redirected into a column named in.
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("in", ColumnType.INT));
        metadata.add(new TableColumnMetadata("a.b", ColumnType.INT));
        Assert.assertEquals(-1, metadata.getColumnIndexQuiet("\"in\""));
        Assert.assertEquals(-1, metadata.getColumnIndexQuiet("\"a.b\""));
        Assert.assertEquals(0, metadata.getColumnIndexQuiet("in"));
        Assert.assertEquals(1, metadata.getColumnIndexQuiet("a.b"));
    }

    @Test
    public void testGetColumnIndexThrowsOnMiss() {
        // The throwing SqlUtil.getColumnIndex resolves like the quiet variant (verbatim, then the
        // protective-quote strip retry) but raises the shared InvalidColumnException singleton on a
        // miss, matching the old RecordMetadata.getColumnIndex default it replaced.
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a.b", ColumnType.INT));

        Assert.assertEquals(0, SqlUtil.getColumnIndex(metadata, "a.b"));
        Assert.assertEquals(0, SqlUtil.getColumnIndex(metadata, "\"a.b\""));

        try {
            SqlUtil.getColumnIndex(metadata, "nope");
            Assert.fail("expected InvalidColumnException");
        } catch (InvalidColumnException e) {
            // expected - the shared singleton, no message
            Assert.assertSame(InvalidColumnException.INSTANCE, e);
        }
    }

    @Test
    public void testImplicitCastCharAsGeoHash() {
        int bits = 5;
        long hash = SqlUtil.implicitCastCharAsGeoHash('c', ColumnType.getGeoHashTypeWithBits(bits));
        StringSink sink = new StringSink();
        GeoHashes.appendChars(hash, bits / 5, sink);
        TestUtils.assertEquals("c", sink);
    }

    @Test
    public void testImplicitCastCharAsGeoHashInvalidChar() {
        testImplicitCastCharAsGeoHashInvalidChar0('o');
        testImplicitCastCharAsGeoHashInvalidChar0('O');
        testImplicitCastCharAsGeoHashInvalidChar0('l');
        testImplicitCastCharAsGeoHashInvalidChar0('L');
        testImplicitCastCharAsGeoHashInvalidChar0('i');
        testImplicitCastCharAsGeoHashInvalidChar0('I');
        testImplicitCastCharAsGeoHashInvalidChar0('-');
    }

    @Test
    public void testImplicitCastCharAsGeoHashNarrowing() {
        int bits = 6;
        try {
            SqlUtil.implicitCastCharAsGeoHash('c', ColumnType.getGeoHashTypeWithBits(bits));
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: c [CHAR -> GEOHASH(6b)]");
        }
    }

    @Test
    public void testImplicitCastCharAsGeoHashWidening() {
        int bits = 4;
        long hash = SqlUtil.implicitCastCharAsGeoHash('c', ColumnType.getGeoHashTypeWithBits(bits));
        StringSink sink = new StringSink();
        GeoHashes.appendBinary(hash, bits, sink);
        TestUtils.assertEquals("0101", sink);
    }

    @Test
    public void testImplicitCastStrAsIPv4() {
        Assert.assertEquals(0, SqlUtil.implicitCastStrAsIPv4((CharSequence) null));
        Assert.assertEquals(201741578, SqlUtil.implicitCastStrAsIPv4("12.6.85.10"));
        Assert.assertEquals(4738954, SqlUtil.implicitCastStrAsIPv4("0.72.79.138"));

        try {
            SqlUtil.implicitCastStrAsIPv4("77823.23232.23232.33");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("invalid IPv4 format: 77823.23232.23232.33", e.getFlyweightMessage());
        }

        try {
            SqlUtil.implicitCastStrAsIPv4("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("invalid IPv4 format: hello", e.getFlyweightMessage());
        }
    }

    @Test
    public void testImplicitCastStrAsLong256() {
        Assert.assertEquals(Constants.getNullConstant(ColumnType.LONG256), SqlUtil.implicitCastStrAsLong256(null));
        Assert.assertEquals(Constants.getNullConstant(ColumnType.LONG256), SqlUtil.implicitCastStrAsLong256(""));
        int n = 5;
        SOCountDownLatch completed = new SOCountDownLatch(n);
        for (int t = 0; t < n; t++) {
            new Thread(() -> {
                Rnd rnd = new Rnd();
                StringSink sink0 = new StringSink();
                StringSink sink1 = new StringSink();
                for (int i = 0; i < 1000; i++) {
                    sink0.clear();
                    sink1.clear();
                    Long256Constant expected = new Long256Constant(rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
                    expected.getLong256(null, sink0);
                    Long256Function function = SqlUtil.implicitCastStrAsLong256(sink0);
                    function.getLong256(null, sink1);
                    Assert.assertEquals(sink0.toString(), sink1.toString());
                    Assert.assertEquals(expected.getLong256A(null), function.getLong256A(null));
                    Assert.assertEquals(expected.getLong256B(null), function.getLong256B(null));
                }
                completed.countDown();
            }).start();
        }
        Assert.assertTrue(completed.await(TimeUnit.SECONDS.toNanos(2L)));
    }

    @Test
    public void testImplicitCastUtf8StrAsIPv4() {
        Assert.assertEquals(0, SqlUtil.implicitCastStrAsIPv4((Utf8Sequence) null));
        Assert.assertEquals(201741578, SqlUtil.implicitCastStrAsIPv4(new Utf8String("12.6.85.10")));
        Assert.assertEquals(4738954, SqlUtil.implicitCastStrAsIPv4(new Utf8String("0.72.79.138")));

        try {
            SqlUtil.implicitCastStrAsIPv4(new Utf8String("77823.23232.23232.33"));
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("invalid IPv4 format: 77823.23232.23232.33", e.getFlyweightMessage());
        }

        try {
            SqlUtil.implicitCastStrAsIPv4(new Utf8String("hello"));
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("invalid IPv4 format: hello", e.getFlyweightMessage());
        }
    }

    @Test
    public void testIsQuoteProtectedAlias() {
        Assert.assertTrue(SqlUtil.isQuoteProtectedAlias("\"a.b\""));
        Assert.assertTrue(SqlUtil.isQuoteProtectedAlias("\"in\""));
        Assert.assertTrue(SqlUtil.isQuoteProtectedAlias("\".\""));
        Assert.assertFalse(SqlUtil.isQuoteProtectedAlias(null));
        Assert.assertFalse(SqlUtil.isQuoteProtectedAlias(""));
        Assert.assertFalse(SqlUtil.isQuoteProtectedAlias("\"\""));
        Assert.assertFalse(SqlUtil.isQuoteProtectedAlias("a.b"));
        // comma content needs no protection: the quotes are genuine content
        Assert.assertFalse(SqlUtil.isQuoteProtectedAlias("\"f,g\""));
        // the dot is guarded by inner quotes
        Assert.assertFalse(SqlUtil.isQuoteProtectedAlias("\"a\".\"b\""));

        // the ranged variant slices composed "alias.column" references
        String composed = "m.\"k.b\"";
        Assert.assertFalse(SqlUtil.isQuoteProtectedAlias(composed));
        Assert.assertTrue(SqlUtil.isQuoteProtectedAlias(composed, 2, composed.length()));
        Assert.assertFalse(SqlUtil.isQuoteProtectedAlias(composed, 0, composed.length()));
    }

    @Test
    public void testNaNCast() {
        Assert.assertEquals(0, SqlUtil.implicitCastIntAsByte(Numbers.INT_NULL));
        Assert.assertEquals(0, SqlUtil.implicitCastIntAsShort(Numbers.INT_NULL));
        Assert.assertEquals(0, SqlUtil.implicitCastLongAsByte(Numbers.LONG_NULL));
        Assert.assertEquals(Numbers.INT_NULL, SqlUtil.implicitCastLongAsInt(Numbers.LONG_NULL));
        Assert.assertEquals(0, SqlUtil.implicitCastLongAsShort(Numbers.LONG_NULL));
    }

    @Test
    public void testParseMicros() throws SqlException {
        Assert.assertEquals(1, SqlUtil.expectMicros("1000ns", 12));
        Assert.assertEquals(1, SqlUtil.expectMicros("1us", 12));
        Assert.assertEquals(1000, SqlUtil.expectMicros("1ms", 12));
        Assert.assertEquals(2000000, SqlUtil.expectMicros("2s", 12));
        Assert.assertEquals(180000000, SqlUtil.expectMicros("3m", 12));
        Assert.assertEquals(14400000000L, SqlUtil.expectMicros("4h", 12));
        Assert.assertEquals(432000000000L, SqlUtil.expectMicros("5d", 12));
    }

    @Test
    public void testParseMicrosSansQualifier() {
        try {
            SqlUtil.expectMicros("125", 12);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "expected interval qualifier");
        }
    }

    @Test
    public void testParseMicrosTooLongQualifier() {
        try {
            SqlUtil.expectMicros("125usu", 12);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "expected 1/2 letter interval qualifier in 125us");
        }
    }

    @Test
    public void testParseSeconds() throws SqlException {
        Assert.assertEquals(1, SqlUtil.expectSeconds("1s", 12));
        Assert.assertEquals(120, SqlUtil.expectSeconds("2m", 12));
        Assert.assertEquals(10800, SqlUtil.expectSeconds("3h", 12));
        Assert.assertEquals(345600, SqlUtil.expectSeconds("4d", 12));
    }

    @Test
    public void testParseSecondsSansQualifier() {
        try {
            SqlUtil.expectSeconds("125", 12);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "expected interval qualifier");
        }
    }

    @Test
    public void testParseSecondsTooLongQualifier() {
        try {
            SqlUtil.expectSeconds("125us", 12);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "expected single letter interval qualifier in 125us");
        }
    }

    @Test
    public void testParseStrByte() {
        Assert.assertEquals(0, SqlUtil.implicitCastStrAsByte(null));
        Assert.assertEquals(89, SqlUtil.implicitCastStrAsByte("89"));
        Assert.assertEquals(-89, SqlUtil.implicitCastStrAsByte("-89"));

        // overflow
        try {
            SqlUtil.implicitCastStrAsByte("778");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `778` [STRING -> BYTE]", e.getFlyweightMessage());
        }

        // not a number
        try {
            SqlUtil.implicitCastStrAsByte("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [STRING -> BYTE]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseStrChar() {
        Assert.assertEquals(0, SqlUtil.implicitCastStrAsChar(null));
        Assert.assertEquals(0, SqlUtil.implicitCastStrAsChar(""));
        Assert.assertEquals('a', SqlUtil.implicitCastStrAsChar("a"));
        // arabic
        Assert.assertEquals('ع', SqlUtil.implicitCastStrAsChar("ع"));

        try {
            SqlUtil.implicitCastStrAsChar("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [STRING -> CHAR]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseStrDate() {
        Assert.assertEquals(Numbers.LONG_NULL, MillisTimestampDriver.INSTANCE.implicitCast(null));
        Assert.assertEquals("2022-11-20T10:30:55.123Z", Dates.toString(MillisTimestampDriver.INSTANCE.implicitCast("2022-11-20T10:30:55.123Z")));
        Assert.assertEquals("2022-11-20T10:30:55.000Z", Dates.toString(MillisTimestampDriver.INSTANCE.implicitCast("2022-11-20 10:30:55Z")));
        Assert.assertEquals("2022-11-20T00:00:00.000Z", Dates.toString(MillisTimestampDriver.INSTANCE.implicitCast("2022-11-20 Z")));
        Assert.assertEquals("2022-11-20T00:00:00.000Z", Dates.toString(MillisTimestampDriver.INSTANCE.implicitCast("2022-11-20")));
        Assert.assertEquals("2022-11-20T10:30:55.123Z", Dates.toString(MillisTimestampDriver.INSTANCE.implicitCast("2022-11-20 10:30:55.123Z")));
        Assert.assertEquals("1970-01-01T00:00:00.200Z", Dates.toString(MillisTimestampDriver.INSTANCE.implicitCast("200")));
        Assert.assertEquals("1969-12-31T23:59:59.100Z", Dates.toString(MillisTimestampDriver.INSTANCE.implicitCast("-900")));

        // not a number
        try {
            MillisTimestampDriver.INSTANCE.implicitCast("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [STRING -> DATE]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseStrDouble() {
        Assert.assertNotSame(SqlUtil.implicitCastStrAsDouble(null), SqlUtil.implicitCastStrAsDouble(null));
        Assert.assertEquals(9.901E62, SqlUtil.implicitCastStrAsDouble("990.1e60"), 0.001);

        // overflow
        try {
            SqlUtil.implicitCastStrAsDouble("1e450");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `1e450` [STRING -> DOUBLE]", e.getFlyweightMessage());
        }

        // not a number
        try {
            SqlUtil.implicitCastStrAsDouble("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [STRING -> DOUBLE]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseStrFloat() {
        Assert.assertNotSame(SqlUtil.implicitCastStrAsFloat(null), SqlUtil.implicitCastStrAsFloat(null));
        Assert.assertEquals(990.1, SqlUtil.implicitCastStrAsFloat("990.1"), 0.001);
        Assert.assertEquals(-899.23, SqlUtil.implicitCastStrAsFloat("-899.23"), 0.001);

        // overflow
        try {
            SqlUtil.implicitCastStrAsFloat("1e210");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `1e210` [STRING -> FLOAT]", e.getFlyweightMessage());
        }

        // not a number
        try {
            SqlUtil.implicitCastStrAsFloat("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [STRING -> FLOAT]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseStrInt() {
        Assert.assertEquals(Numbers.INT_NULL, SqlUtil.implicitCastStrAsInt(null));
        Assert.assertEquals(22222123, SqlUtil.implicitCastStrAsInt("22222123"));
        Assert.assertEquals(-2222232, SqlUtil.implicitCastStrAsInt("-2222232"));

        // overflow
        try {
            SqlUtil.implicitCastStrAsInt("77823232322323233");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `77823232322323233` [STRING -> INT]", e.getFlyweightMessage());
        }

        // not a number
        try {
            SqlUtil.implicitCastStrAsInt("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [STRING -> INT]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseStrLong() {
        Assert.assertEquals(Numbers.LONG_NULL, SqlUtil.implicitCastStrAsLong(null));
        Assert.assertEquals(222221211212123L, SqlUtil.implicitCastStrAsLong("222221211212123"));
        Assert.assertEquals(222221211212123L, SqlUtil.implicitCastStrAsLong("222221211212123L"));
        Assert.assertEquals(-222221211212123L, SqlUtil.implicitCastStrAsLong("-222221211212123"));
        Assert.assertEquals(-222221211212123L, SqlUtil.implicitCastStrAsLong("-222221211212123L"));

        // overflow
        try {
            SqlUtil.implicitCastStrAsLong("778232323223232389080898083");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `778232323223232389080898083` [STRING -> LONG]", e.getFlyweightMessage());
        }

        // not a number
        try {
            SqlUtil.implicitCastStrAsLong("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [STRING -> LONG]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseStrShort() {
        Assert.assertEquals(0, SqlUtil.implicitCastStrAsShort(null));
        Assert.assertEquals(22222, SqlUtil.implicitCastStrAsShort("22222"));
        Assert.assertEquals(-22222, SqlUtil.implicitCastStrAsShort("-22222"));

        // overflow
        try {
            SqlUtil.implicitCastStrAsShort("77823232323");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `77823232323` [STRING -> SHORT]", e.getFlyweightMessage());
        }

        // not a number
        try {
            SqlUtil.implicitCastStrAsShort("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [STRING -> SHORT]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseStrTimestamp() {
        TimestampDriver timestampDriver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);
        Assert.assertEquals(Numbers.LONG_NULL, timestampDriver.implicitCast(null));
        Assert.assertEquals("2022-11-20T10:30:55.123999Z", Micros.toUSecString(timestampDriver.implicitCast("2022-11-20T10:30:55.123999Z")));
        Assert.assertEquals("2022-11-20T10:30:55.000000Z", Micros.toUSecString(timestampDriver.implicitCast("2022-11-20 10:30:55Z")));
        Assert.assertEquals("2022-11-20T00:00:00.000000Z", Micros.toUSecString(timestampDriver.implicitCast("2022-11-20 Z")));
        Assert.assertEquals("2022-11-20T10:30:55.123000Z", Micros.toUSecString(timestampDriver.implicitCast("2022-11-20 10:30:55.123Z")));
        Assert.assertEquals("1970-01-01T00:00:00.000200Z", Micros.toUSecString(timestampDriver.implicitCast("200")));
        Assert.assertEquals("1969-12-31T23:59:59.999100Z", Micros.toUSecString(timestampDriver.implicitCast("-900")));

        // not a number
        try {
            timestampDriver.implicitCast("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [STRING -> TIMESTAMP]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseStrVarcharAsTimestamp0() {
        TimestampDriver timestampDriver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);
        Assert.assertEquals(Numbers.LONG_NULL, timestampDriver.implicitCastVarchar(null));
        Assert.assertEquals("2022-11-20T10:30:55.123999Z", Micros.toUSecString(timestampDriver.implicitCastVarchar(new Utf8String("2022-11-20T10:30:55.123999Z"))));
        Assert.assertEquals("2022-11-20T10:30:55.000000Z", Micros.toUSecString(timestampDriver.implicitCastVarchar(new Utf8String("2022-11-20 10:30:55Z"))));
        Assert.assertEquals("2022-11-20T00:00:00.000000Z", Micros.toUSecString(timestampDriver.implicitCastVarchar(new Utf8String("2022-11-20 Z"))));
        Assert.assertEquals("2022-11-20T10:30:55.123000Z", Micros.toUSecString(timestampDriver.implicitCastVarchar(new Utf8String("2022-11-20 10:30:55.123Z"))));
        Assert.assertEquals("1970-01-01T00:00:00.000200Z", Micros.toUSecString(timestampDriver.implicitCastVarchar(new Utf8String("200"))));
        Assert.assertEquals("1969-12-31T23:59:59.999100Z", Micros.toUSecString(timestampDriver.implicitCastVarchar(new Utf8String("-900"))));

        // not a number
        try {
            timestampDriver.implicitCastVarchar(new Utf8String("hello"));
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [VARCHAR -> TIMESTAMP]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseVarcharByte() {
        Utf8StringSink sink = new Utf8StringSink();
        Assert.assertEquals(0, SqlUtil.implicitCastVarcharAsByte(null));

        sink.put("89");
        Assert.assertEquals(89, SqlUtil.implicitCastVarcharAsByte(sink));

        sink.clear();
        sink.put("-89");
        Assert.assertEquals(-89, SqlUtil.implicitCastVarcharAsByte(sink));

        // overflow
        try {
            sink.clear();
            sink.put("778");
            SqlUtil.implicitCastVarcharAsByte(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `778` [VARCHAR -> BYTE]", e.getFlyweightMessage());
        }

        // not a number
        try {
            sink.clear();
            sink.put("hello");
            SqlUtil.implicitCastVarcharAsByte(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [VARCHAR -> BYTE]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseVarcharChar() {
        Utf8StringSink sink = new Utf8StringSink();
        Assert.assertEquals(0, SqlUtil.implicitCastVarcharAsChar(null));

        Assert.assertEquals(0, SqlUtil.implicitCastVarcharAsChar(sink));

        sink.clear();
        sink.put("a");
        Assert.assertEquals('a', SqlUtil.implicitCastVarcharAsChar(sink));
        // arabic
        sink.clear();
        sink.put("ع");
        Assert.assertEquals('ع', SqlUtil.implicitCastVarcharAsChar(sink));

        try {
            sink.clear();
            sink.put("hello");
            SqlUtil.implicitCastVarcharAsChar(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [VARCHAR -> CHAR]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseVarcharDate() {
        Assert.assertEquals(Numbers.LONG_NULL, SqlUtil.implicitCastVarcharAsDate(null));
        Assert.assertEquals("2022-11-20T10:30:55.123Z", Dates.toString(SqlUtil.implicitCastVarcharAsDate(new Utf8String("2022-11-20T10:30:55.123Z"))));
        Assert.assertEquals("2022-11-20T10:30:55.000Z", Dates.toString(SqlUtil.implicitCastVarcharAsDate(new Utf8String("2022-11-20 10:30:55Z"))));
        Assert.assertEquals("2022-11-20T00:00:00.000Z", Dates.toString(SqlUtil.implicitCastVarcharAsDate(new Utf8String("2022-11-20 Z"))));
        Assert.assertEquals("2022-11-20T00:00:00.000Z", Dates.toString(SqlUtil.implicitCastVarcharAsDate(new Utf8String("2022-11-20"))));
        Assert.assertEquals("2022-11-20T10:30:55.123Z", Dates.toString(SqlUtil.implicitCastVarcharAsDate(new Utf8String("2022-11-20 10:30:55.123Z"))));
        Assert.assertEquals("1970-01-01T00:00:00.200Z", Dates.toString(SqlUtil.implicitCastVarcharAsDate(new Utf8String("200"))));
        Assert.assertEquals("1969-12-31T23:59:59.100Z", Dates.toString(SqlUtil.implicitCastVarcharAsDate(new Utf8String("-900"))));

        // not a number
        try {
            SqlUtil.implicitCastVarcharAsDate(new Utf8String("hello"));
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [VARCHAR -> DATE]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseVarcharDouble() {
        Utf8StringSink sink = new Utf8StringSink();
        //noinspection SimplifiableAssertion
        Assert.assertFalse(SqlUtil.implicitCastVarcharAsDouble(null) == SqlUtil.implicitCastStrAsDouble(null));
        sink.put("990.1e60");
        Assert.assertEquals(9.901E62, SqlUtil.implicitCastVarcharAsDouble(sink), 0.001);

        // overflow
        try {
            sink.clear();
            sink.put("1e450");
            SqlUtil.implicitCastVarcharAsDouble(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `1e450` [VARCHAR -> DOUBLE]", e.getFlyweightMessage());
        }

        // not a number
        try {
            sink.clear();
            sink.put("hello");
            SqlUtil.implicitCastVarcharAsDouble(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [VARCHAR -> DOUBLE]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseVarcharFloat() {
        Utf8StringSink sink = new Utf8StringSink();
        //noinspection SimplifiableAssertion
        Assert.assertFalse(SqlUtil.implicitCastVarcharAsFloat(null) == SqlUtil.implicitCastStrAsFloat(null));

        sink.put("990.1");
        Assert.assertEquals(990.1, SqlUtil.implicitCastVarcharAsFloat(sink), 0.001);

        sink.clear();
        sink.put("-899.23");
        Assert.assertEquals(-899.23, SqlUtil.implicitCastVarcharAsFloat(sink), 0.001);

        // overflow
        try {
            sink.clear();
            sink.put("1e210");
            SqlUtil.implicitCastVarcharAsFloat(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `1e210` [VARCHAR -> FLOAT]", e.getFlyweightMessage());
        }

        // not a number
        try {
            sink.clear();
            sink.put("hello");
            SqlUtil.implicitCastVarcharAsFloat(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [VARCHAR -> FLOAT]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseVarcharInt() {
        Utf8StringSink sink = new Utf8StringSink();
        Assert.assertEquals(Numbers.INT_NULL, SqlUtil.implicitCastVarcharAsInt(null));

        sink.put("22222123");
        Assert.assertEquals(22222123, SqlUtil.implicitCastVarcharAsInt(sink));

        sink.clear();
        sink.put("-2222232");
        Assert.assertEquals(-2222232, SqlUtil.implicitCastVarcharAsInt(sink));

        // overflow
        try {
            sink.clear();
            sink.put("77823232322323233");
            SqlUtil.implicitCastVarcharAsInt(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `77823232322323233` [VARCHAR -> INT]", e.getFlyweightMessage());
        }

        // not a number
        try {
            sink.clear();
            sink.put("hello");
            SqlUtil.implicitCastVarcharAsInt(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [VARCHAR -> INT]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseVarcharLong() {
        Assert.assertEquals(Numbers.LONG_NULL, SqlUtil.implicitCastStrAsLong(null));
        Utf8StringSink sink = new Utf8StringSink();
        sink.put("222221211212123");
        Assert.assertEquals(222221211212123L, SqlUtil.implicitCastVarcharAsLong(sink));

        sink.clear();
        sink.put("222221211212123L");
        Assert.assertEquals(222221211212123L, SqlUtil.implicitCastVarcharAsLong(sink));

        sink.clear();
        sink.put("-222221211212123");
        Assert.assertEquals(-222221211212123L, SqlUtil.implicitCastVarcharAsLong(sink));

        sink.clear();
        sink.put("-222221211212123L");
        Assert.assertEquals(-222221211212123L, SqlUtil.implicitCastVarcharAsLong(sink));

        // overflow
        try {
            sink.clear();
            sink.put("778232323223232389080898083");
            SqlUtil.implicitCastVarcharAsLong(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `778232323223232389080898083` [VARCHAR -> LONG]", e.getFlyweightMessage());
        }

        // not a number
        try {
            sink.clear();
            sink.put("hello");
            SqlUtil.implicitCastVarcharAsLong(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [VARCHAR -> LONG]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseVarcharShort() {
        Assert.assertEquals(0, SqlUtil.implicitCastVarcharAsShort(null));
        Utf8StringSink sink = new Utf8StringSink();
        sink.put("22222");
        Assert.assertEquals(22222, SqlUtil.implicitCastVarcharAsShort(sink));
        sink.clear();
        sink.put("-22222");
        Assert.assertEquals(-22222, SqlUtil.implicitCastVarcharAsShort(sink));

        // overflow
        try {
            sink.clear();
            sink.put("77823232323");
            SqlUtil.implicitCastVarcharAsShort(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `77823232323` [VARCHAR -> SHORT]", e.getFlyweightMessage());
        }

        // not a number
        try {
            sink.clear();
            sink.put("hello");
            SqlUtil.implicitCastVarcharAsShort(sink);
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: `hello` [VARCHAR -> SHORT]", e.getFlyweightMessage());
        }
    }

    @Test
    public void testProtectColumnAliasEmptyNameRoundTrips() {
        // Regression (review m2): protectColumnAlias must not wrap an empty name. The empty string is a
        // disallowed alias, but wrapping it as "" is not recognized as quote-protected (the interior is
        // empty), so toColumnName could not strip it back and the quotes would leak. An empty name is
        // returned as-is and round-trips; a non-empty operator token / dotted name still wraps and
        // round-trips through toColumnName.
        CharacterStore store = new CharacterStore(64, 4);
        CharSequence empty = SqlUtil.protectColumnAlias(store, "");
        Assert.assertEquals("", empty.toString());
        Assert.assertEquals("", SqlUtil.toColumnName(empty));
        Assert.assertEquals("in", SqlUtil.toColumnName(SqlUtil.protectColumnAlias(store, "in")));
        Assert.assertEquals("a.b", SqlUtil.toColumnName(SqlUtil.protectColumnAlias(store, "a.b")));
        Assert.assertEquals(".", SqlUtil.toColumnName(SqlUtil.protectColumnAlias(store, ".")));
    }

    private void testImplicitCastCharAsGeoHashInvalidChar0(char c) {
        int bits = 5;
        try {
            SqlUtil.implicitCastCharAsGeoHash(c, ColumnType.getGeoHashTypeWithBits(bits));
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: " + c + " [CHAR -> GEOHASH(1c)]");
        }
    }

    static {
        // this is required to initialize calendar indexes ahead of using them
        // otherwise sink can end up having odd characters
        MicrosFormatUtils.init();
    }
}
