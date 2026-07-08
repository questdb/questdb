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

package io.questdb.griffin;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.MillisTimestampDriver;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DoubleArrayParser;
import io.questdb.cairo.arr.VarcharArrayParser;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.InvalidColumnException;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.functions.constants.Long256Constant;
import io.questdb.griffin.engine.functions.constants.Long256NullConstant;
import io.questdb.griffin.engine.functions.date.TimestampFloorFromOffsetUtcFunctionFactory;
import io.questdb.griffin.engine.functions.date.TimestampFloorFunctionFactory;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.ParquetEncoding;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.std.AbstractLowerCaseCharSequenceHashSet;
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Acceptor;
import io.questdb.std.Long256FromCharSequenceDecoder;
import io.questdb.std.Long256Impl;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.CarrierLocal;
import io.questdb.std.Uuid;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.millitime.DateFormatCompiler;
import io.questdb.std.fastdouble.FastFloatParser;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.std.GenericLexer.unquote;
import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;
import static io.questdb.std.datetime.millitime.DateFormatUtils.PG_DATE_MILLI_TIME_Z_FORMAT;
import static io.questdb.std.datetime.millitime.DateFormatUtils.PG_DATE_Z_FORMAT;

public class SqlUtil {
    static final LowerCaseCharSequenceHashSet disallowedAliases = new LowerCaseCharSequenceHashSet();
    private static final DateFormat[] IMPLICIT_CAST_FORMATS;
    private static final int IMPLICIT_CAST_FORMATS_SIZE;
    private static final CarrierLocal<Long256ConstantFactory> LONG256_FACTORY = new CarrierLocal<>(Long256ConstantFactory::new);

    public static void addSelectStar(
            IQueryModel model,
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<ExpressionNode> expressionNodePool
    ) throws SqlException {
        model.addBottomUpColumn(nextColumn(queryColumnPool, expressionNodePool, "*", "*", true, 0));
        model.setArtificialStar(true);
    }

    public static long castPGDates(CharSequence value, int fromColumnType, TimestampDriver driver) {
        final int hi = value.length();
        for (int i = 0; i < IMPLICIT_CAST_FORMATS_SIZE; i++) {
            try {
                return driver.fromDate(IMPLICIT_CAST_FORMATS[i].parse(value, 0, hi, EN_LOCALE));
            } catch (NumericException ignore) {
            }
        }
        throw ImplicitCastException.inconvertibleValue(value, fromColumnType, driver.getTimestampType());
    }

    public static void collectAllTableAndViewNames(
            @NotNull IQueryModel model,
            @NotNull ObjList<CharSequence> outTableNames,
            boolean viewsOnly
    ) {
        IQueryModel m = model;
        do {
            if (!viewsOnly) {
                final ExpressionNode tableNameExpr = m.getTableNameExpr();
                if (tableNameExpr != null && tableNameExpr.type == ExpressionNode.LITERAL) {
                    outTableNames.add(unquote(tableNameExpr.token));
                }
            }

            final ExpressionNode viewNameExpr = m.getOriginatingViewNameExpr();
            if (viewNameExpr != null) {
                outTableNames.add(unquote(viewNameExpr.token));
            }

            final ObjList<IQueryModel> joinModels = m.getJoinModels();
            for (int i = 0, n = joinModels.size(); i < n; i++) {
                final IQueryModel joinModel = joinModels.getQuick(i);
                if (joinModel == m) {
                    continue;
                }
                collectAllTableAndViewNames(joinModel, outTableNames, viewsOnly);
            }

            final IQueryModel unionModel = m.getUnionModel();
            if (unionModel != null) {
                collectAllTableAndViewNames(unionModel, outTableNames, viewsOnly);
            }

            m = m.getNestedModel();
        } while (m != null);
    }

    public static void collectAllTableNames(
            @NotNull IQueryModel model,
            @NotNull LowerCaseCharSequenceHashSet outTableNames,
            @Nullable IntList outTableNamePositions
    ) {
        IQueryModel m = model;
        do {
            final ExpressionNode tableNameExpr = m.getTableNameExpr();
            if (tableNameExpr != null && tableNameExpr.type == ExpressionNode.LITERAL) {
                if (outTableNames.add(unquote(tableNameExpr.token)) && outTableNamePositions != null) {
                    outTableNamePositions.add(tableNameExpr.position);
                }
            }

            final ObjList<IQueryModel> joinModels = m.getJoinModels();
            for (int i = 0, n = joinModels.size(); i < n; i++) {
                final IQueryModel joinModel = joinModels.getQuick(i);
                if (joinModel == m) {
                    continue;
                }
                collectAllTableNames(joinModel, outTableNames, outTableNamePositions);
            }

            final IQueryModel unionModel = m.getUnionModel();
            if (unionModel != null) {
                collectAllTableNames(unionModel, outTableNames, outTableNamePositions);
            }

            m = m.getNestedModel();
        } while (m != null);
    }

    public static void collectTableAndColumnReferences(
            @NotNull CairoEngine engine,
            @NotNull IQueryModel model,
            @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> depMap
    ) {
        IQueryModel m = model;
        do {
            // Process columns in SELECT clause
            final ObjList<QueryColumn> columns = m.getColumns();
            for (int i = 0, n = columns.size(); i < n; i++) {
                final QueryColumn column = columns.getQuick(i);
                if (column != null && column.getAst() != null) {
                    collectColumnReferencesFromExpression(engine, column.getAst(), m, depMap);
                }
            }

            // Process WHERE clause
            final ExpressionNode whereClause = m.getWhereClause();
            if (whereClause != null) {
                collectColumnReferencesFromExpression(engine, whereClause, m, depMap);
            }

            // Process JOIN conditions
            final ObjList<ExpressionNode> joinColumns = m.getJoinColumns();
            collectColumnReferencesFromJoinColumns(engine, joinColumns, m, depMap);

            // Process GROUP BY
            final ObjList<ExpressionNode> groupBy = m.getGroupBy();
            for (int i = 0, n = groupBy.size(); i < n; i++) {
                final ExpressionNode groupByExpr = groupBy.getQuick(i);
                if (groupByExpr != null) {
                    collectColumnReferencesFromExpression(engine, groupByExpr, m, depMap);
                }
            }

            // Process ORDER BY
            final ObjList<ExpressionNode> orderBy = m.getOrderBy();
            for (int i = 0, n = orderBy.size(); i < n; i++) {
                final ExpressionNode orderByExpr = orderBy.getQuick(i);
                if (orderByExpr != null) {
                    collectColumnReferencesFromExpression(engine, orderByExpr, m, depMap);
                }
            }

            // Process tables directly referenced
            final ExpressionNode tableNameExpr = m.getTableNameExpr();
            if (tableNameExpr != null && tableNameExpr.type == ExpressionNode.LITERAL) {
                String tableName = unquote(tableNameExpr.token).toString();
                if (!depMap.contains(tableName)) {
                    depMap.put(tableName, new LowerCaseCharSequenceHashSet());
                }
            }

            // Process views
            final ExpressionNode viewNameExpr = m.getOriginatingViewNameExpr();
            if (viewNameExpr != null) {
                String viewName = unquote(viewNameExpr.token).toString();
                if (!depMap.contains(viewName)) {
                    depMap.put(viewName, new LowerCaseCharSequenceHashSet());
                }
            }

            // Process join models
            final ObjList<IQueryModel> joinModels = m.getJoinModels();
            for (int i = 0, n = joinModels.size(); i < n; i++) {
                final IQueryModel joinModel = joinModels.getQuick(i);
                if (joinModel != m) {
                    collectColumnReferencesFromJoinColumns(engine, joinModel.getJoinColumns(), m, depMap);
                    collectTableAndColumnReferences(engine, joinModel, depMap);
                }
            }

            // Process union models
            final IQueryModel unionModel = m.getUnionModel();
            if (unionModel != null) {
                collectTableAndColumnReferences(engine, unionModel, depMap);
            }

            m = m.getNestedModel();
        } while (m != null);
    }

    /**
     * Creates a unique column alias with O(1) amortized complexity by tracking the next sequence
     * number for each base alias in the provided map.
     *
     * @param store                character store for creating the alias string
     * @param base                 base name for the alias
     * @param indexOfDot           index of the last dot in base, or -1 if none
     * @param aliasToColumnMap     set of existing aliases to check for uniqueness
     * @param nextAliasSequenceMap map tracking next sequence number for each base alias (updated in place)
     * @param nonLiteral           whether this is a non-literal expression
     * @return unique alias
     */
    public static CharSequence createColumnAlias(
            CharacterStore store,
            CharSequence base,
            int indexOfDot,
            AbstractLowerCaseCharSequenceHashSet aliasToColumnMap,
            LowerCaseCharSequenceIntHashMap nextAliasSequenceMap,
            boolean nonLiteral
    ) {
        // containsDisallowed and quoteProtected are consumed only on the dot-free path (indexOfDot == -1);
        // a dotted base uses the ranged isQuoteProtectedAlias(base, indexOfDot + 1, ...) below instead, so
        // gate both on indexOfDot to skip a full-base hash / scan that would be dead work for a dotted base.
        final boolean containsDisallowed = indexOfDot == -1 && disallowedAliases.contains(base);
        final boolean disallowed = nonLiteral && containsDisallowed;

        // A quote-protected base carries its dedup suffix inside the quotes and re-derives
        // whether the quotes are still needed from the final content (see the loop below): a
        // dotted alias stays protected ("a.b" -> "a.b1", stripping to a clean a.b1) while an
        // operator token sheds the quotes once the suffix makes it a plain identifier ("in" ->
        // in1). Putting the suffix after the quotes ("a.b"1) would fail isQuoteProtectedAlias
        // and leak them into the result set metadata.
        final boolean quoteProtected = indexOfDot == -1 && isQuoteProtectedAlias(base);

        // early exit for simple cases: return the base verbatim (preserving its object identity,
        // which the wildcard '*' passthrough and other callers rely on) when it carries no
        // result-set name collision. A base whose toColumnName display name matches an already-taken
        // alias in the OTHER representation must instead run the dedup loop below, which forces a
        // suffix:
        //  - a quote-protected "in" and a bare in both surface as in (check the bare interior);
        //  - a bare operator token (in, *, and, ...) and a protective-quoted "in" both surface as in
        //    (check that "<base>" sibling).
        if (indexOfDot == -1 && !disallowed && aliasToColumnMap.excludes(base)) {
            if (quoteProtected) {
                if (aliasToColumnMap.excludes(base, 1, base.length() - 1)) {
                    return base;
                }
            } else if (!containsDisallowed) {
                return base;
            } else {
                final CharacterStoreEntry siblingEntry = store.newEntry();
                siblingEntry.put('"').put(base).put('"');
                if (aliasToColumnMap.excludes(siblingEntry.toImmutable())) {
                    return base;
                }
            }
        }

        // Resolve the dedup content: the [contentLo, contentHi) slice of base reused for every
        // candidate, or the "column" placeholder (contentLo < 0) for empty/numeric/disallowed
        // bases. A quote-protected base keys on its bare interior so the sequence counter is
        // shared whether or not a given candidate re-emits the protective quotes.
        final int contentLo;
        final int contentHi;
        // True when the content is the stripped interior of a quote-protected alias - a whole
        // "a.b"/"in" base, or the column part of a composed t1."a.b" reference. The loop below then
        // re-wraps the protective quotes around the content AND its dedup suffix, so a duplicate
        // strips to a clean name (t2."a.b" -> "a.b1" -> a.b1) instead of leaking the quotes outside
        // a copied "a.b" ("a.b"1).
        boolean contentProtected = false;
        final CharacterStoreEntry baseEntry = store.newEntry();
        if (indexOfDot == -1) {
            if (disallowed || Numbers.parseIntQuiet(base) != Numbers.INT_NULL) {
                contentLo = -1;
                contentHi = -1;
                baseEntry.put("column");
            } else if (quoteProtected) {
                contentProtected = true;
                contentLo = 1;
                contentHi = base.length() - 1;
                baseEntry.put(base, contentLo, contentHi);
            } else {
                contentLo = 0;
                contentHi = base.length();
                baseEntry.put(base);
            }
        } else if (indexOfDot + 1 == base.length()) {
            contentLo = -1;
            contentHi = -1;
            baseEntry.put("column");
        } else if (isQuoteProtectedAlias(base, indexOfDot + 1, base.length())) {
            // composed "table.column" whose column part is itself quote-protected: key on the bare
            // interior so the per-candidate protection re-wraps it (dedup suffix inside the quotes).
            contentProtected = true;
            contentLo = indexOfDot + 2;
            contentHi = base.length() - 1;
            baseEntry.put(base, contentLo, contentHi);
        } else {
            contentLo = indexOfDot + 1;
            contentHi = base.length();
            baseEntry.put(base, contentLo, contentHi);
        }
        final CharSequence seqKey = baseEntry.toImmutable();

        // Look up the starting sequence for this base alias
        int sequence = nextAliasSequenceMap.get(seqKey);
        if (sequence == -1) {
            sequence = 0;
        }

        // The content slice [contentLo, contentHi) is fixed for the whole loop (createColumnAlias
        // never truncates), so its dot / operator-token classification is loop-invariant - compute it
        // once here rather than re-scanning per dedup candidate. contentHasDot is the "deduped" form
        // (a dotted interior always needs quotes); contentNeedsQuotingWhenFresh adds the operator-token
        // case that a dedup suffix later sheds.
        final boolean contentHasDot = contentLo >= 0 && Chars.indexOfLastUnquoted(base, '.', contentLo, contentHi) > -1;
        final boolean contentNeedsQuotingWhenFresh = contentHasDot
                || (contentLo >= 0 && disallowedAliases.contains(base, contentLo, contentHi));

        // A bare candidate that is itself quoting-worthy (an operator token; a dotted base reduces
        // to its dot-free tail, so it never surfaces dotted here) shares a display name with a
        // protective-quoted "<name>" sibling. Precompute that sibling once so the loop can force a
        // dedup suffix when it is already taken. It lives in its own store region, ahead of the
        // reused candidate entry, so the loop's trimTo never disturbs it.
        CharSequence bareQuotedSibling = null;
        if (!contentProtected && contentNeedsQuotingWhenFresh) {
            final CharacterStoreEntry siblingEntry = store.newEntry();
            siblingEntry.put('"').put(base, contentLo, contentHi).put('"');
            bareQuotedSibling = siblingEntry.toImmutable();
        }

        // Reuse one store entry across dedup candidates: trimTo(entryStart) rewinds it each
        // iteration so rejected candidates do not accumulate in the store. seqKey (and any sibling
        // probe) live in earlier entries and are left untouched.
        final CharacterStoreEntry entry = store.newEntry();
        final int entryStart = entry.length();

        while (true) {
            final boolean deduped = sequence > 0;
            // Re-emit the protective quotes per candidate only while the final content still
            // needs them: a dotted interior always does; an operator token only until a dedup
            // suffix makes it a plain identifier (so "a.b" -> "a.b1", while "in" stays quoted
            // at sequence 0 to collide with the stored "in", then surfaces as bare in1).
            final boolean emitQuote = contentProtected && (deduped ? contentHasDot : contentNeedsQuotingWhenFresh);
            entry.trimTo(entryStart);
            if (emitQuote) {
                entry.put('"');
            }
            if (contentLo >= 0) {
                entry.put(base, contentLo, contentHi);
            } else {
                entry.put("column");
            }
            if (deduped) {
                entry.put(sequence);
            }
            if (emitQuote) {
                entry.put('"');
            }
            sequence++;
            final CharSequence alias = entry.toImmutable();
            // Only the un-suffixed candidate can share a display name with the "<name>" sibling; a
            // suffixed in1 is a distinct display name that has no protected sibling.
            if (isAliasNameAvailable(aliasToColumnMap, alias, emitQuote, deduped ? null : bareQuotedSibling)) {
                // Update the sequence tracker for next time
                nextAliasSequenceMap.put(seqKey, sequence);
                return alias;
            }
        }
    }

    /**
     * Creates a unique column alias for expressions with O(1) amortized complexity by tracking
     * the next sequence number for each base alias in the provided map.
     */
    public static CharSequence createExprColumnAlias(
            CharacterStore store,
            CharSequence base,
            AbstractLowerCaseCharSequenceHashSet aliasToColumnMap,
            LowerCaseCharSequenceIntHashMap nextAliasSequenceMap,
            int maxLength,
            boolean nonLiteral
    ) {
        // We need to wrap disallowed aliases with double quotes to avoid later conflicts.
        // A base that is itself a protective-looking quoted string (a PIVOT value whose data is
        // literally "in" or "a.b", or a literal reference whose alias surfaces in its protected form)
        // displays as its stripped interior - toColumnName strips it - so alias the interior and let the
        // per-candidate protection below re-wrap it only when the final content still needs it. Otherwise
        // a dedup suffix would land outside the quotes ("in"_2), where isQuoteProtectedAlias no longer
        // recognizes it and toColumnName cannot strip it, leaking the quotes into result set metadata and
        // breaking CREATE TABLE AS SELECT; and a bare "in" returned verbatim by the early-exit below would
        // duplicate the display name of an already-taken sibling.
        boolean forceQuote = false;
        if (isQuoteProtectedAlias(base)) {
            // A non-literal base re-derives its protection from the stripped interior's dot / operator
            // token via the quote decision below, so it needs no forced flag. A literal base must force
            // it: its quote rule fires only for a composed table."col", so without the flag a stripped
            // operator token (in) would surface bare and collide, and a stripped dotted interior (a.b)
            // would mis-fire prefixedLiteral and alias just the tail. Mirrors createColumnAlias, which
            // keys a whole quote-protected base on its interior for literal and non-literal callers alike.
            forceQuote = !nonLiteral;
            base = Chars.toString(base, 1, base.length() - 1);
        }
        int baseLen = base.length();
        int indexOfDot = Chars.indexOfLastUnquoted(base, '.');
        // A literal composed reference whose column part is itself quote-protected (t1."a.b"): drop
        // the table prefix and alias the column part's stripped interior, forcing the protective
        // quotes back on per candidate (like a whole quote-protected base). Without this a dedup
        // suffix lands OUTSIDE the copied quotes ("a.b"_2), which toColumnName cannot strip - leaking
        // them into result set metadata and breaking CREATE TABLE AS SELECT.
        if (!forceQuote && !nonLiteral && indexOfDot > -1 && indexOfDot < baseLen - 1
                && isQuoteProtectedAlias(base, indexOfDot + 1, baseLen)) {
            base = Chars.toString(base, indexOfDot + 2, baseLen - 1);
            baseLen = base.length();
            indexOfDot = Chars.indexOfLastUnquoted(base, '.');
            forceQuote = true;
        }
        final boolean prefixedLiteral = !forceQuote && !nonLiteral && indexOfDot > -1 && indexOfDot < baseLen - 1;
        // Hoisted so the non-literal path tests it once (mirrors createColumnAlias's containsDisallowed)
        // rather than at both the quote decision and the early-exit sibling check below.
        final boolean baseDisallowed = disallowedAliases.contains(base);
        boolean quote = forceQuote || (nonLiteral
                ? !Chars.isDoubleQuoted(base) && (indexOfDot > -1 || baseDisallowed)
                : indexOfDot > -1 && disallowedAliases.contains(base, indexOfDot + 1, baseLen));

        // early exit for simple cases. A bare operator-token base (in, and, ...) shares its
        // toColumnName display name with an already-taken protective-quoted "<token>" sibling
        // (both surface as the bare token), so returning it verbatim here would push a duplicate
        // result-set column name to the projection-metadata build. Take the fast path only when
        // that sibling is also free; otherwise fall through to the dedup loop, which suffixes it
        // (in -> in_2) via bareQuotedSibling. Mirrors createColumnAlias's early-exit.
        if (!prefixedLiteral && !quote && aliasToColumnMap.excludes(base)
                && baseLen > 0 && baseLen <= maxLength && base.charAt(baseLen - 1) != ' ') {
            if (!baseDisallowed) {
                return base;
            }
            final CharacterStoreEntry siblingEntry = store.newEntry();
            siblingEntry.put('"').put(base).put('"');
            if (aliasToColumnMap.excludes(siblingEntry.toImmutable())) {
                return base;
            }
        }

        final int start = prefixedLiteral ? indexOfDot + 1 : 0;

        // Track the per-base sequence under a stable key: the protected base, keeping the
        // historical (quote-prefixed when protecting) form so suffix numbering is unchanged.
        // seqKey lives in its own store entry, ahead of the reusable candidate entry below,
        // so rewinding that entry never overwrites it.
        final CharacterStoreEntry keyEntry = store.newEntry();
        if (quote) {
            keyEntry.put('"');
        }
        keyEntry.put(base, start, baseLen);
        final CharSequence seqKey = keyEntry.toImmutable();

        final int truncatedLen = Math.min(baseLen - start + (quote ? 2 : 0), maxLength - (quote ? 1 : 0));

        // Look up the starting sequence for this base alias
        int sequence = nextAliasSequenceMap.get(seqKey);
        if (sequence == -1) {
            sequence = 1;
        }

        // The trailing-space trim below (or truncation) can reduce a !quote base to a bare operator
        // token - e.g. the pivot value 'in ' trims to `in`, which shares a display name with the
        // protective-quoted "in" of a sibling value 'in'. Precompute that "<token>" sibling once
        // (mirroring createColumnAlias) so the un-suffixed bare candidate is rejected and deduped
        // (in / in_2) instead of silently producing a duplicate result-set column. It lives in its
        // own store region, ahead of the reused candidate entry below, so the loop's trimTo never
        // disturbs it. Only the !quote path needs it: a genuine operator-token base has quote == true
        // and emits quoted first, so its bare siblings are already covered by the interior check.
        CharSequence bareQuotedSibling = null;
        if (!quote) {
            // seq == 1 content extent: len == min(truncatedLen, maxLength) == truncatedLen (!quote),
            // then the same trailing-space trim the loop applies to a bare candidate.
            int firstContentLen = truncatedLen;
            if (firstContentLen > 0 && base.charAt(start + firstContentLen - 1) == ' ') {
                final int lastNonSpace = Chars.lastIndexOfDifferent(base, start, start + firstContentLen, ' ') - start;
                firstContentLen = lastNonSpace >= 0 ? lastNonSpace + 1 : 0;
            }
            if (firstContentLen > 0 && disallowedAliases.contains(base, start, start + firstContentLen)) {
                final CharacterStoreEntry siblingEntry = store.newEntry();
                siblingEntry.put('"').put(base, start, start + firstContentLen).put('"');
                bareQuotedSibling = siblingEntry.toImmutable();
            }
        }

        // Reuse one store entry across dedup candidates: trimTo(entryStart) rewinds it each
        // iteration so rejected candidates do not accumulate in the store.
        final CharacterStoreEntry entry = store.newEntry();
        final int entryStart = entry.length();

        // Cache the first unquoted dot once: as a dedup suffix shrinks the content under truncation,
        // whether a dot still falls inside the content reduces to a position compare (below), so no
        // per-candidate re-scan is needed. Mirrors Chars.indexOfLastUnquoted's double-quote handling.
        // indexOfDot already proved whether base carries an unquoted dot at all, and a prefixedLiteral's
        // content starts past the last one, so both cases leave firstContentDot at -1 without a scan.
        int firstContentDot = -1;
        if (indexOfDot > -1 && !prefixedLiteral) {
            boolean inContentQuotes = false;
            for (int i = start; i < baseLen; i++) {
                final char c = base.charAt(i);
                if (c == '"') {
                    inContentQuotes = !inContentQuotes;
                } else if (c == '.' && !inContentQuotes) {
                    firstContentDot = i;
                    break;
                }
            }
        }

        int seqSize = 0;
        while (true) {
            if (sequence > 1) {
                // decimal digit count of `sequence` plus 1 for the '_' separator (an integer count,
                // not a libm log10)
                seqSize = 1;
                for (int s = sequence; s > 0; s /= 10) {
                    seqSize++;
                }
            }
            final int len = Math.min(truncatedLen, maxLength - seqSize - (quote ? 1 : 0));
            int contentLen = Math.max(0, len - (quote ? 2 : 0));

            // Trim trailing spaces FIRST, so a display name never ends in a bare space (matching the
            // non-dotted early-exit check and the operator-token path 'in ' -> in) AND so the quote
            // decision below keys on the final trimmed content. A quote-protected candidate carries
            // the dot that forced the quotes, and trailing spaces sit after it, so the trim never
            // reaches the dot - the content stays protected and strips to a clean name (the pivot
            // value 'FNCL 2.5 ' -> the column FNCL 2.5, not a bare 'FNCL 2.5 ' with a leaked trailing
            // space that is an interop hazard over PG / HTTP / CSV). A sibling value 'FNCL 2.5' then
            // dedups it (FNCL 2.5 / FNCL 2.5_2) via the collision check below, instead of two columns
            // differing only by a space. An all-space slice trims to nothing and falls through to the
            // "column" placeholder.
            if (contentLen > 0 && base.charAt(start + contentLen - 1) == ' ') {
                final int lastNonSpace = Chars.lastIndexOfDifferent(base, start, start + contentLen, ' ') - start;
                contentLen = lastNonSpace >= 0 ? lastNonSpace + 1 : 0;
            }

            // Decide whether the protective quotes wrap this candidate AFTER the trim above, so the
            // operator-token check keys on the FINAL trimmed content. A base such as 'in .x' truncated
            // past its dot leaves 'in ', whose trailing space trims back to the operator token 'in';
            // that must re-quote ("in") and then dedup against a sibling "in", not surface as a bare
            // 'in' duplicating the sibling's display name (deciding emitQuote on the pre-trim 'in '
            // slice, which is not a disallowed alias, would wrongly leave it bare). Emit the quotes
            // only when the content still needs them: truncation may have dropped the discriminating
            // dot, and a dedup suffix turns an operator token into a plain identifier; in both cases
            // the bare name is unambiguous, so keeping the quotes would only leak them into result-set
            // metadata (see toColumnName). contentHasDot is trim-invariant (a dot is never a trailing
            // space); only the operator-token lookup, reached for the un-suffixed candidate, changes.
            final boolean deduped = sequence > 1;
            final boolean contentHasDot = firstContentDot >= 0 && firstContentDot < start + contentLen;
            final boolean emitQuote = quote && contentLen > 0
                    && (contentHasDot || (!deduped && disallowedAliases.contains(base, start, start + contentLen)));

            // No usable content survived: an empty base, truncation plus the quote reservation,
            // or an all-space slice. Emit a "column" placeholder (matching createColumnAlias) so
            // the loop terminates with a valid, unquoted name instead of spinning forever - the
            // old return gate keyed on len, which still counted the quote budget, so it always
            // terminated.
            final boolean emptyContent = contentLen == 0;

            entry.trimTo(entryStart);
            if (emitQuote) {
                entry.put('"');
            }
            if (emptyContent) {
                // Keep the "column" placeholder within the configured cap (maxLength >= 4): reserve
                // room for the dedup suffix (seqSize) and truncate, so an empty/all-space value at a
                // small maxLength surfaces a bounded name rather than a fixed 6-char one (6 ==
                // "column".length()). Keep at least one placeholder char so a pathological collision
                // count at the minimum cap never collapses the content to empty and leaks a bare
                // "_<seq>" name.
                entry.put("column", 0, Math.max(1, Math.min(6, maxLength - seqSize)));
            } else {
                entry.put(base, start, start + contentLen);
            }
            if (sequence > 1) {
                entry.put('_');
                entry.put(sequence);
            }
            if (emitQuote) {
                entry.put('"');
            }
            final CharSequence alias = entry.toImmutable();
            // A quote-protected candidate ("in") and an already-taken bare identifier (in) reduce to
            // the same toColumnName display name, so they collide in the projection metadata; reject
            // the candidate in that case so it gets a dedup suffix. The reverse also collides: a bare
            // un-suffixed candidate that trimmed/truncated down to an operator token shares a display
            // name with a stored "<token>" - bareQuotedSibling (non-null only then) forces its dedup.
            if (isAliasNameAvailable(aliasToColumnMap, alias, emitQuote, deduped ? null : bareQuotedSibling)) {
                // Update the sequence tracker for next time
                nextAliasSequenceMap.put(seqKey, sequence + 1);
                return alias;
            }
            sequence++;
        }
    }

    public static long expectMicros(CharSequence tok, int position) throws SqlException {
        final int len = tok.length();
        final int k = findEndOfDigitsPos(tok, len, position);

        try {
            long interval = Numbers.parseLong(tok, 0, k);
            int nChars = len - k;
            if (nChars > 2) {
                throw SqlException.$(position + k, "expected 1/2 letter interval qualifier in ").put(tok);
            }
            TimestampDriver driver = MicrosTimestampDriver.INSTANCE;

            switch (tok.charAt(k)) {
                case 's':
                    if (nChars == 1) {
                        // seconds
                        return driver.fromSeconds(interval);
                    }
                    break;
                case 'm':
                    if (nChars == 1) {
                        // minutes
                        return driver.fromMinutes((int) interval);
                    } else {
                        if (tok.charAt(k + 1) == 's') {
                            // millis
                            return driver.fromMillis((int) interval);
                        }
                    }
                    break;
                case 'h':
                    if (nChars == 1) {
                        // hours
                        return driver.fromHours((int) interval);
                    }
                    break;
                case 'd':
                    if (nChars == 1) {
                        // days
                        return driver.fromDays((int) interval);
                    }
                    break;
                case 'u':
                    if (nChars == 2 && tok.charAt(k + 1) == 's') {
                        return driver.fromMicros(interval);
                    }
                    break;
                case 'n':
                    if (nChars == 2 && tok.charAt(k + 1) == 's') {
                        return driver.fromNanos(interval);
                    }
                    break;
                default:
                    break;
            }
        } catch (NumericException ex) {
            // Ignored
        }

        throw SqlException.$(position + len, "invalid interval qualifier ").put(tok);
    }

    public static long expectSeconds(CharSequence tok, int position) throws SqlException {
        final int len = tok.length();
        final int k = findEndOfDigitsPos(tok, len, position);

        try {
            long interval = Numbers.parseLong(tok, 0, k);
            int nChars = len - k;
            if (nChars > 1) {
                throw SqlException.$(position + k, "expected single letter interval qualifier in ").put(tok);
            }

            switch (tok.charAt(k)) {
                case 's': // seconds
                    return interval;
                case 'm': // minutes
                    return interval * Micros.MINUTE_SECONDS;
                case 'h': // hours
                    return interval * Micros.HOUR_SECONDS;
                case 'd': // days
                    return interval * Micros.DAY_SECONDS;
                default:
                    break;
            }
        } catch (NumericException ex) {
            // Ignored
        }

        throw SqlException.$(position + len, "invalid interval qualifier ").put(tok);
    }

    /**
     * Fetches next non-whitespace token that's not part of single or multiline comment.
     *
     * @param lexer input lexer
     * @return with next valid token or null if end of input is reached .
     */
    public static CharSequence fetchNext(GenericLexer lexer) throws SqlException {
        return fetchNext(lexer, false);
    }

    /**
     * Fetches next non-whitespace token that's not part of single or multiline comment.
     *
     * @param lexer        The input lexer containing the token stream to process
     * @param includeHints If true, hint block markers (/*+) are treated as valid tokens and returned;
     *                     if false, hint blocks are treated as comments and skipped
     * @return The next meaningful token as a CharSequence, or null if the end of input is reached
     * @throws SqlException If a parsing error occurs while processing the token stream
     * @see #fetchNextHintToken(GenericLexer) For handling tokens within hint blocks
     */
    public static CharSequence fetchNext(GenericLexer lexer, boolean includeHints) throws SqlException {
        int blockCount = 0;
        boolean lineComment = false;
        while (lexer.hasNext()) {
            CharSequence cs = lexer.next();

            if (lineComment) {
                if (Chars.equals(cs, '\n') || Chars.equals(cs, '\r')) {
                    lineComment = false;
                } else {
                    // Check if token contains a newline (can happen with unbalanced quotes in comments)
                    for (int i = 0, n = cs.length(); i < n; i++) {
                        char c = cs.charAt(i);
                        if (c == '\n' || c == '\r') {
                            // Found newline inside token - reposition lexer to after newline
                            int newPos = lexer.lastTokenPosition() + i + 1;
                            // Skip \r\n sequence
                            if (c == '\r' && i + 1 < n && cs.charAt(i + 1) == '\n') {
                                newPos++;
                            }
                            lexer.backTo(newPos, null);
                            lineComment = false;
                            break;
                        }
                    }
                }
                continue;
            }

            if (Chars.equals("--", cs)) {
                lineComment = true;
                continue;
            }

            if (Chars.equals("/*", cs)) {
                blockCount++;
                continue;
            }

            if (Chars.equals("/*+", cs) && (!includeHints || blockCount > 0)) {
                blockCount++;
                continue;
            }

            if (Chars.equals("*/", cs) && blockCount > 0) {
                blockCount--;
                continue;
            }

            if (blockCount == 0 && GenericLexer.WHITESPACE.excludes(cs)) {
                // unclosed quote check
                if (cs.length() == 1 && cs.charAt(0) == '"') {
                    throw SqlException.$(lexer.lastTokenPosition(), "unclosed quotation mark");
                }
                return cs;
            }
        }
        return null;
    }

    /**
     * Fetches the next non-whitespace, non-comment hint token from the lexer.
     * <p>
     * This method should only be called after entering a hint block. Specifically,
     * a previous call to {@link #fetchNext(GenericLexer, boolean)} must have returned
     * a hint start token (<code>/*+</code>) before this method can be used.
     * <p>
     * The method processes the input stream, skipping over any nested comments and whitespace,
     * and returns the next meaningful hint token. This allows for clean parsing of hint
     * content without manual handling of comments and formatting characters.
     * <p>
     * When the end of the hint block is reached, the method returns null, indicating
     * no more hint tokens are available for processing.
     * <p>
     * If a hint contains unbalanced quotes, the method will NOT throw an exception, instead
     * it will consume all tokens until the end of the hint block is reached and then return null indicating
     * the end of the hint block.
     *
     * @param lexer The input lexer containing the token stream to process
     * @return The next meaningful hint token, or null if the end of the hint block is reached
     * @see #fetchNext(GenericLexer, boolean) For entering the hint block initially
     */
    public static CharSequence fetchNextHintToken(GenericLexer lexer) {
        int blockCount = 0;
        boolean lineComment = false;
        boolean inError = false;
        while (lexer.hasNext()) {
            CharSequence cs = lexer.next();

            if (lineComment) {
                if (Chars.equals(cs, '\n') || Chars.equals(cs, '\r')) {
                    lineComment = false;
                } else {
                    // Check if token contains a newline (can happen with unbalanced quotes in comments)
                    for (int i = 0, n = cs.length(); i < n; i++) {
                        char c = cs.charAt(i);
                        if (c == '\n' || c == '\r') {
                            // Found newline inside token - reposition lexer to after newline
                            int newPos = lexer.lastTokenPosition() + i + 1;
                            // Skip \r\n sequence
                            if (c == '\r' && i + 1 < n && cs.charAt(i + 1) == '\n') {
                                newPos++;
                            }
                            lexer.backTo(newPos, null);
                            lineComment = false;
                            break;
                        }
                    }
                }
                continue;
            }

            if (Chars.equals("--", cs)) {
                lineComment = true;
                continue;
            }

            if (Chars.equals("/*", cs)) {
                blockCount++;
                continue;
            }

            if (Chars.equals("/*+", cs)) {
                // nested hints are treated as regular comments
                blockCount++;
                continue;
            }

            // end of hints or a nested comment
            if (Chars.equals("*/", cs)) {
                if (blockCount > 0) {
                    blockCount--;
                    continue;
                }
                return null;
            }

            if (!inError && blockCount == 0 && GenericLexer.WHITESPACE.excludes(cs)) {
                // unclosed quote check
                if (cs.length() == 1 && cs.charAt(0) == '"') {
                    inError = true;
                } else {
                    return cs;
                }
            }
        }
        return null;
    }

    public static RecordCursorFactory generateFactory(SqlCompiler compiler, ExecutionModel model, SqlExecutionContext executionContext) throws SqlException {
        final IQueryModel queryModel = model.getQueryModel();
        assert queryModel != null;
        return compiler.generateSelectWithRetries(queryModel, null, executionContext, false);
    }

    /**
     * Resolves a compiler-side column reference like {@link #getColumnIndexQuiet(RecordMetadata, CharSequence)},
     * throwing {@link InvalidColumnException} when the column does not exist.
     */
    public static int getColumnIndex(RecordMetadata metadata, CharSequence columnName) {
        final int index = getColumnIndexQuiet(metadata, columnName);
        if (index > -1) {
            return index;
        }
        throw InvalidColumnException.INSTANCE;
    }

    /**
     * Resolves a compiler-side column reference against the given metadata. The lookup runs
     * verbatim first; on a miss, if the name carries protective double quotes added by
     * {@link #createExprColumnAlias} (or by the parser for dotted user aliases), it retries
     * with the quotes stripped, because projection metadata stores such names unquoted
     * (see {@link #toColumnName(CharSequence)}). Ingestion and storage paths must NOT use
     * this method: wire-supplied names are not compiler aliases, and stripping them would
     * redirect quoted names into unrelated columns. {@link RecordMetadata#getColumnIndexQuiet(CharSequence)}
     * stays verbatim for those paths.
     */
    public static int getColumnIndexQuiet(RecordMetadata metadata, CharSequence columnName) {
        final int index = metadata.getColumnIndexQuiet(columnName);
        if (index > -1) {
            return index;
        }
        // Classify the miss with a single interior scan: >= 0 dotted interior, -1 operator token,
        // MIN_VALUE not a compiler-protected alias (stays verbatim - the ingestion/storage contract).
        final int interiorDot = quoteProtectedInteriorDot(columnName, 0, columnName.length());
        if (interiorDot == Integer.MIN_VALUE) {
            return index;
        }
        // Retry with the protective quotes stripped: flat projection metadata stores the clean name.
        // Dot-splitting metadata (join metadata, or a PriorityMetadata wrapping it - see
        // RecordMetadata.splitsOnDot) is the exception: its ranged lookup splits on an unquoted dot
        // (a.b -> table a, column b), which is not the verbatim match this strip assumes. So for a
        // DOTTED interior against such metadata, skip the retry and report a miss: a real dotted join
        // column is stored re-quoted and was already matched verbatim above, so a split match here
        // could only bind the stripped name to an unrelated column. The operator-token interior
        // ("in") carries no dot and is looked up bare, so it stays verbatim and is left to the retry.
        if (interiorDot > -1 && metadata.splitsOnDot()) {
            return -1;
        }
        return metadata.getColumnIndexQuiet(columnName, 1, columnName.length() - 1);
    }

    /**
     * Extracts the interval/stride expression from a timestamp_floor or
     * timestamp_floor_utc function call, handling 2/3/5-param overloads.
     */
    public static ExpressionNode getTimestampFloorInterval(ExpressionNode ast) {
        if (ast.paramCount == 3 || ast.paramCount == 5) {
            return ast.args.getQuick(ast.paramCount - 1);
        }
        return ast.lhs;
    }

    /**
     * Extracts the timestamp column expression from a timestamp_floor or
     * timestamp_floor_utc function call, handling 2/3/5-param overloads.
     */
    public static ExpressionNode getTimestampFloorTimestampArg(ExpressionNode ast) {
        if (ast.paramCount == 3 || ast.paramCount == 5) {
            return ast.args.getQuick(ast.paramCount - 2);
        }
        return ast.rhs;
    }

    public static byte implicitCastAsByte(long value, int fromType) {
        if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
            return (byte) value;
        }
        throw ImplicitCastException.inconvertibleValue(value, fromType, ColumnType.BYTE);
    }

    public static char implicitCastAsChar(long value, int fromType) {
        if (value >= 0 && value <= 9) {
            return (char) (value + '0');
        }
        throw ImplicitCastException.inconvertibleValue(value, fromType, ColumnType.CHAR);
    }

    public static float implicitCastAsFloat(double value, int fromType) {
        if ((value >= Float.MIN_VALUE && value <= Float.MAX_VALUE) || Numbers.isNull(value)) {
            return (float) value;
        }
        throw ImplicitCastException.inconvertibleValue(value, fromType, ColumnType.FLOAT);
    }

    public static int implicitCastAsInt(long value, int fromType) {
        if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
            return (int) value;
        }

        throw ImplicitCastException.inconvertibleValue(value, fromType, ColumnType.INT);
    }

    public static short implicitCastAsShort(long value, int fromType) {
        if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
            return (short) value;
        }
        throw ImplicitCastException.inconvertibleValue(value, fromType, ColumnType.SHORT);
    }

    // used by bytecode assembler
    @SuppressWarnings("unused")
    public static byte implicitCastCharAsByte(char value, int toType) {
        return implicitCastCharAsType(value, toType);
    }

    @SuppressWarnings("unused")
    // used by copier bytecode assembler
    public static byte implicitCastCharAsGeoHash(char value, int toType) {
        int v;
        // '0' .. '9' and 'A-Z', excl 'A', 'I', 'L', 'O'
        if ((value >= '0' && value <= '9') || ((v = value | 32) > 'a' && v <= 'z' && v != 'i' && v != 'l' && v != 'o')) {
            int toBits = ColumnType.getGeoHashBits(toType);
            if (toBits < 5) {
                // widening
                return (byte) GeoHashes.widen(GeoHashes.encodeChar(value), 5, toBits);
            }

            if (toBits == 5) {
                return GeoHashes.encodeChar(value);
            }
        }
        throw ImplicitCastException.inconvertibleValue(value, ColumnType.CHAR, toType);
    }

    public static byte implicitCastCharAsType(char value, int toType) {
        byte v = (byte) (value - '0');
        if (v > -1 && v < 10) {
            return v;
        }
        throw ImplicitCastException.inconvertibleValue(value, ColumnType.CHAR, toType);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static byte implicitCastDoubleAsByte(double value) {
        if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
            return (byte) value;
        }

        if (Numbers.isNull(value)) {
            return 0;
        }

        throw ImplicitCastException.inconvertibleValue(value, ColumnType.FLOAT, ColumnType.BYTE);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static float implicitCastDoubleAsFloat(double value) {
        final double d = Math.abs(value);
        if ((d >= Float.MIN_VALUE && d <= Float.MAX_VALUE) || (Numbers.isNull(value) || d == 0.0)) {
            return (float) value;
        }

        throw ImplicitCastException.inconvertibleValue(value, ColumnType.DOUBLE, ColumnType.FLOAT);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static int implicitCastDoubleAsInt(double value) {
        if (Numbers.isNull(value)) {
            return Numbers.INT_NULL;
        }
        return implicitCastAsInt((long) value, ColumnType.LONG);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static long implicitCastDoubleAsLong(double value) {
        if (value > Long.MIN_VALUE && value <= Long.MAX_VALUE) {
            return (long) value;
        }

        if (Numbers.isNull(value)) {
            return Numbers.LONG_NULL;
        }

        throw ImplicitCastException.inconvertibleValue(value, ColumnType.DOUBLE, ColumnType.LONG);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static short implicitCastDoubleAsShort(double value) {
        if (Numbers.isNull(value)) {
            return 0;
        }
        return implicitCastAsShort((long) value, ColumnType.LONG);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static byte implicitCastFloatAsByte(float value) {
        if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
            return (byte) value;
        }

        if (Numbers.isNull(value)) {
            return 0;
        }

        throw ImplicitCastException.inconvertibleValue(value, ColumnType.FLOAT, ColumnType.BYTE);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static double implicitCastFloatAsDouble(float value) {
        if (Numbers.isNull(value)) {
            return Double.NaN;
        }

        return value;
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static int implicitCastFloatAsInt(float value) {
        if (value > Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
            return (int) value;
        }

        if (Numbers.isNull(value)) {
            return Numbers.INT_NULL;
        }

        throw ImplicitCastException.inconvertibleValue(value, ColumnType.FLOAT, ColumnType.INT);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static long implicitCastFloatAsLong(float value) {
        if (value > Long.MIN_VALUE && value <= Long.MAX_VALUE) {
            return (long) value;
        }

        if (Numbers.isNull(value)) {
            return Numbers.LONG_NULL;
        }

        throw ImplicitCastException.inconvertibleValue(value, ColumnType.FLOAT, ColumnType.LONG);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static short implicitCastFloatAsShort(float value) {
        if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
            return (short) value;
        }

        if (Numbers.isNull(value)) {
            return 0;
        }

        throw ImplicitCastException.inconvertibleValue(value, ColumnType.FLOAT, ColumnType.SHORT);
    }

    public static long implicitCastGeoHashAsGeoHash(long value, int fromType, int toType) {
        final int fromBits = ColumnType.getGeoHashBits(fromType);
        final int toBits = ColumnType.getGeoHashBits(toType);
        assert fromBits >= toBits;
        return GeoHashes.widen(value, fromBits, toBits);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static byte implicitCastIntAsByte(int value) {
        if (value != Numbers.INT_NULL) {
            return implicitCastAsByte(value, ColumnType.INT);
        }
        return 0;
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static double implicitCastIntAsDouble(int value) {
        if (value == Numbers.INT_NULL) {
            return Double.NaN;
        } else {
            return value;
        }
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static float implicitCastIntAsFloat(int value) {
        if (value == Numbers.INT_NULL) {
            return Float.NaN;
        } else {
            return value;
        }
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static long implicitCastIntAsLong(int value) {
        if (value == Numbers.INT_NULL) {
            return Long.MIN_VALUE;
        } else {
            return value;
        }
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static short implicitCastIntAsShort(int value) {
        if (value != Numbers.INT_NULL) {
            return implicitCastAsShort(value, ColumnType.INT);
        }
        return 0;
    }

    public static boolean implicitCastLong256AsStr(Long256 long256, CharSink<?> sink) {
        if (Long256Impl.isNull(long256)) {
            return false;
        }
        Numbers.appendLong256(long256, sink);
        return true;
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static byte implicitCastLongAsByte(long value) {
        if (value != Numbers.LONG_NULL) {
            return implicitCastAsByte(value, ColumnType.LONG);
        }
        return 0;
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static double implicitCastLongAsDouble(long value) {
        if (value == Numbers.LONG_NULL) {
            return Double.NaN;
        } else {
            return (double) value;
        }
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static float implicitCastLongAsFloat(long value) {
        if (value == Numbers.LONG_NULL) {
            return Float.NaN;
        } else {
            return (float) value;
        }
    }


    @SuppressWarnings("unused")
    // used by the row copier
    public static int implicitCastLongAsInt(long value) {
        if (value != Numbers.LONG_NULL) {
            return implicitCastAsInt(value, ColumnType.LONG);
        }
        return Numbers.INT_NULL;
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static short implicitCastLongAsShort(long value) {
        if (value != Numbers.LONG_NULL) {
            return implicitCastAsShort(value, ColumnType.LONG);
        }
        return 0;
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static byte implicitCastShortAsByte(short value) {
        return implicitCastAsByte(value, ColumnType.SHORT);
    }

    public static byte implicitCastStrAsByte(CharSequence value) {
        if (value != null) {
            try {
                int res = Numbers.parseInt(value);
                if (res >= Byte.MIN_VALUE && res <= Byte.MAX_VALUE) {
                    return (byte) res;
                }
            } catch (NumericException ignore) {
            }
            throw ImplicitCastException.inconvertibleValue(value, ColumnType.STRING, ColumnType.BYTE);
        }
        return 0;
    }

    public static char implicitCastStrAsChar(CharSequence value) {
        if (value == null || value.isEmpty()) {
            return 0;
        }

        if (value.length() == 1) {
            return value.charAt(0);
        }

        throw ImplicitCastException.inconvertibleValue(value, ColumnType.STRING, ColumnType.CHAR);
    }

    @SuppressWarnings("unused")
    public static long implicitCastStrAsDate(CharSequence value) {
        return MillisTimestampDriver.INSTANCE.implicitCast(value, ColumnType.STRING);
    }

    public static double implicitCastStrAsDouble(CharSequence value) {
        if (value != null) {
            try {
                return Numbers.parseDouble(value);
            } catch (NumericException e) {
                throw ImplicitCastException.inconvertibleValue(value, ColumnType.STRING, ColumnType.DOUBLE);
            }
        }
        return Double.NaN;
    }

    public static float implicitCastStrAsFloat(CharSequence value) {
        if (value != null) {
            try {
                return FastFloatParser.parseFloat(value, 0, value.length(), true);
            } catch (NumericException ignored) {
                throw ImplicitCastException.inconvertibleValue(value, ColumnType.STRING, ColumnType.FLOAT);
            }
        }
        return Float.NaN;
    }

    public static int implicitCastStrAsIPv4(CharSequence value) {
        if (value != null) {
            try {
                return Numbers.parseIPv4(value);
            } catch (NumericException exception) {
                throw ImplicitCastException.instance().put("invalid IPv4 format: ").put(value);
            }
        }
        return Numbers.IPv4_NULL;
    }

    public static int implicitCastStrAsIPv4(Utf8Sequence value) {
        if (value != null) {
            try {
                return Numbers.parseIPv4(value);
            } catch (NumericException exception) {
                throw ImplicitCastException.instance().put("invalid IPv4 format: ").put(value);
            }
        }
        return Numbers.IPv4_NULL;
    }

    public static int implicitCastStrAsInt(CharSequence value) {
        if (value != null) {
            try {
                return Numbers.parseInt(value);
            } catch (NumericException e) {
                throw ImplicitCastException.inconvertibleValue(value, ColumnType.STRING, ColumnType.INT);
            }
        }
        return Numbers.INT_NULL;
    }

    public static long implicitCastStrAsLong(CharSequence value) {
        if (value != null) {
            try {
                return Numbers.parseLong(value);
            } catch (NumericException e) {
                throw ImplicitCastException.inconvertibleValue(value, ColumnType.STRING, ColumnType.LONG);
            }
        }
        return Numbers.LONG_NULL;
    }

    public static Long256Constant implicitCastStrAsLong256(CharSequence value) {
        if (value == null || value.isEmpty()) {
            return Long256NullConstant.INSTANCE;
        }
        int start = 0;
        int end = value.length();
        if (end > 2 && value.charAt(start) == '0' && (value.charAt(start + 1) | 32) == 'x') {
            start += 2;
        }
        Long256ConstantFactory factory = LONG256_FACTORY.get();
        Long256FromCharSequenceDecoder.decode(value, start, end, factory);
        return factory.pop();
    }

    public static void implicitCastStrAsLong256(CharSequence value, Long256Acceptor long256Acceptor) {
        if (value != null) {
            Long256FromCharSequenceDecoder.decode(value, 0, value.length(), long256Acceptor);
        } else {
            long256Acceptor.setAll(
                    Long256Impl.NULL_LONG256.getLong0(),
                    Long256Impl.NULL_LONG256.getLong1(),
                    Long256Impl.NULL_LONG256.getLong2(),
                    Long256Impl.NULL_LONG256.getLong3()
            );
        }
    }

    public static short implicitCastStrAsShort(@Nullable CharSequence value) {
        try {
            return value != null ? Numbers.parseShort(value) : 0;
        } catch (NumericException ignore) {
            throw ImplicitCastException.inconvertibleValue(value, ColumnType.STRING, ColumnType.SHORT);
        }
    }

    public static void implicitCastStrAsUuid(CharSequence str, Uuid uuid) {
        if (str == null || str.isEmpty()) {
            uuid.ofNull();
            return;
        }
        try {
            uuid.of(str);
        } catch (NumericException e) {
            throw ImplicitCastException.inconvertibleValue(str, ColumnType.STRING, ColumnType.UUID);
        }
    }

    public static void implicitCastStrAsUuid(Utf8Sequence str, Uuid uuid) {
        if (str == null || str.size() == 0) {
            uuid.ofNull();
            return;
        }
        try {
            uuid.of(str);
        } catch (NumericException e) {
            throw ImplicitCastException.inconvertibleValue(str, ColumnType.STRING, ColumnType.UUID);
        }
    }

    public static ArrayView implicitCastStringAsDoubleArray(CharSequence value, DoubleArrayParser parser, int expectedType) {
        try {
            // the parser will handle the weak dimensionality case (-1)
            parser.of(value, ColumnType.decodeWeakArrayDimensionality(expectedType));
        } catch (IllegalArgumentException e) {
            throw ImplicitCastException.inconvertibleValue(value, ColumnType.STRING, expectedType);
        }
        return parser;
    }

    public static ArrayView implicitCastStringAsVarcharArray(CharSequence value, VarcharArrayParser parser, int expectedType) {
        try {
            parser.of(value, ColumnType.decodeWeakArrayDimensionality(expectedType));
        } catch (IllegalArgumentException e) {
            throw ImplicitCastException.inconvertibleValue(value, ColumnType.STRING, expectedType);
        }
        return parser;
    }

    public static boolean implicitCastUuidAsStr(long lo, long hi, CharSink<?> sink) {
        if (Uuid.isNull(lo, hi)) {
            return false;
        }
        Numbers.appendUuid(lo, hi, sink);
        return true;
    }

    public static byte implicitCastVarcharAsByte(Utf8Sequence value) {
        if (value != null) {
            try {
                int res = Numbers.parseInt(value);
                if (res >= Byte.MIN_VALUE && res <= Byte.MAX_VALUE) {
                    return (byte) res;
                }
            } catch (NumericException ignore) {
            }
            throw ImplicitCastException.inconvertibleValue(value, ColumnType.VARCHAR, ColumnType.BYTE);
        }
        return 0;
    }

    public static char implicitCastVarcharAsChar(Utf8Sequence value) {
        if (value == null || value.size() == 0) {
            return 0;
        }

        int encodedResult = Utf8s.utf8CharDecode(value);
        short consumedBytes = Numbers.decodeLowShort(encodedResult);
        if (consumedBytes == value.size()) {
            return (char) Numbers.decodeHighShort(encodedResult);
        }
        throw ImplicitCastException.inconvertibleValue(value, ColumnType.VARCHAR, ColumnType.CHAR);
    }

    public static long implicitCastVarcharAsDate(Utf8Sequence value) {
        return MillisTimestampDriver.INSTANCE.implicitCastVarchar(value);
    }

    public static double implicitCastVarcharAsDouble(Utf8Sequence value) {
        if (value != null) {
            try {
                return Numbers.parseDouble(value.asAsciiCharSequence());
            } catch (NumericException e) {
                throw ImplicitCastException.inconvertibleValue(value, ColumnType.VARCHAR, ColumnType.DOUBLE);
            }
        }
        return Double.NaN;
    }

    public static ArrayView implicitCastVarcharAsDoubleArray(Utf8Sequence value, DoubleArrayParser parser, int expectedType) {
        try {
            // the parser will handle the weak dimensionality case (-1)
            parser.of(value.asAsciiCharSequence(), ColumnType.decodeWeakArrayDimensionality(expectedType));
        } catch (IllegalArgumentException e) {
            throw ImplicitCastException.inconvertibleValue(value, ColumnType.VARCHAR, expectedType);
        }
        return parser;
    }

    public static float implicitCastVarcharAsFloat(Utf8Sequence value) {
        if (value != null) {
            try {
                CharSequence ascii = value.asAsciiCharSequence();
                return FastFloatParser.parseFloat(ascii, 0, ascii.length(), true);
            } catch (NumericException ignored) {
                throw ImplicitCastException.inconvertibleValue(value, ColumnType.VARCHAR, ColumnType.FLOAT);
            }
        }
        return Float.NaN;
    }

    public static int implicitCastVarcharAsInt(Utf8Sequence value) {
        if (value != null) {
            try {
                return Numbers.parseInt(value);
            } catch (NumericException e) {
                throw ImplicitCastException.inconvertibleValue(value, ColumnType.VARCHAR, ColumnType.INT);
            }
        }
        return Numbers.INT_NULL;
    }

    public static long implicitCastVarcharAsLong(Utf8Sequence value) {
        if (value != null) {
            try {
                return Numbers.parseLong(value);
            } catch (NumericException e) {
                throw ImplicitCastException.inconvertibleValue(value, ColumnType.VARCHAR, ColumnType.LONG);
            }
        }
        return Numbers.LONG_NULL;
    }

    public static short implicitCastVarcharAsShort(@Nullable Utf8Sequence value) {
        try {
            return value != null ? Numbers.parseShort(value) : 0;
        } catch (NumericException ignore) {
            throw ImplicitCastException.inconvertibleValue(value, ColumnType.VARCHAR, ColumnType.SHORT);
        }
    }

    public static boolean isNotPlainSelectModel(IQueryModel model) {
        return model.getTableName() != null
                || model.getGroupBy().size() > 0
                || model.getJoinModels().size() > 1
                || model.getLatestByType() != IQueryModel.LATEST_BY_NONE
                || model.getUnionModel() != null;
    }

    public static boolean isParallelismSupported(ObjList<Function> functions) {
        for (int i = 0, n = functions.size(); i < n; i++) {
            if (!functions.getQuick(i).supportsParallelism()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true if the model stands for a SELECT ... FROM tab; or a SELECT ... FROM tab WHERE ...; query.
     * We're aiming for potential page frame support with this check.
     */
    public static boolean isPlainSelect(IQueryModel model) {
        while (model != null) {
            if (model.getSelectModelType() != IQueryModel.SELECT_MODEL_NONE
                    || model.getGroupBy().size() > 0
                    || model.getJoinModels().size() > 1
                    || model.getLatestByType() != IQueryModel.LATEST_BY_NONE
                    || model.getUnionModel() != null) {
                return false;
            }
            model = model.getNestedModel();
        }
        return true;
    }

    /**
     * Returns true when the alias carries protective double quotes added by
     * {@link #createExprColumnAlias(CharacterStore, CharSequence, AbstractLowerCaseCharSequenceHashSet, LowerCaseCharSequenceIntHashMap, int, boolean)}
     * (or by the parser for user aliases), i.e. the quoted content contains a dot or collides
     * with an operator token, so the compiler could not process the bare name. A double-quoted
     * alias whose content needs no protection (e.g. a string alias '"f,g"') is not considered
     * protected: there the quotes are genuine content.
     */
    public static boolean isQuoteProtectedAlias(CharSequence alias) {
        return alias != null && isQuoteProtectedAlias(alias, 0, alias.length());
    }

    /**
     * Ranged variant of {@link #isQuoteProtectedAlias(CharSequence)} testing the
     * {@code [lo, hi)} slice of the given sequence.
     */
    public static boolean isQuoteProtectedAlias(CharSequence alias, int lo, int hi) {
        return quoteProtectedInteriorDot(alias, lo, hi) != Integer.MIN_VALUE;
    }

    /**
     * Returns true if the given expression node is a timestamp_floor or
     * timestamp_floor_utc function call.
     */
    public static boolean isTimestampFloorFunction(ExpressionNode ast) {
        return ast.type == ExpressionNode.FUNCTION
                && (Chars.equalsIgnoreCase(TimestampFloorFunctionFactory.NAME, ast.token)
                || Chars.equalsIgnoreCase(TimestampFloorFromOffsetUtcFunctionFactory.NAME, ast.token));
    }

    public static ExpressionNode nextExpr(ObjectPool<ExpressionNode> pool, int exprNodeType, CharSequence token, int position) {
        return pool.next().of(exprNodeType, token, 0, position);
    }

    public static int parseArrayDimensionality(GenericLexer lexer, int columnType, int typeTagPosition) throws SqlException {
        if (ColumnType.tagOf(columnType) == ColumnType.ARRAY) {
            throw SqlException.position(typeTagPosition).put("the system supports type-safe arrays, e.g. `type[]`. Supported types are: DOUBLE. More types incoming.");
        }
        boolean hasNumericDimensionality = false;
        int dimensionalityFirstPos = -1;
        int dim = 0;
        do {
            CharSequence tok = fetchNext(lexer);
            if (Chars.equalsNc(tok, '[')) {
                // Check for whitespace before '[' in array type declaration
                int openBracketPosition = lexer.lastTokenPosition();
                if (openBracketPosition > 0 && Character.isWhitespace(lexer.getContent().charAt(openBracketPosition - 1))) {
                    throw SqlException.position(openBracketPosition)
                            .put("array type requires no whitespace between type and brackets");
                }

                // could be a start of array type
                tok = fetchNext(lexer);

                if (Chars.equalsNc(tok, ']')) {
                    dim++;
                } else {
                    // check if someone is trying to specify numeric dimensionality, e.g. double[1]
                    try {
                        Numbers.parseInt(tok);
                        hasNumericDimensionality = true;
                        if (dimensionalityFirstPos == -1) {
                            dimensionalityFirstPos = lexer.lastTokenPosition();
                        }
                        continue;
                    } catch (NumericException ignore) {
                        // never mind
                    }

                    // we are looking at something like `type[something` right now, lets consume the rest of the
                    // lexer until we hit one of the following: `]`, `,` or `)` to get the complete picture of
                    // what the user provide. We will show what we see and offer what we expect to see.

                    // we will fail here regardless, so we do not care about the state of the parser
                    int stopPos;
                    do {
                        int p = lexer.lastTokenPosition();
                        tok = fetchNext(lexer);
                        if (tok == null || Chars.equals(tok, ']') || Chars.equals(tok, ',') || Chars.equals(tok, ')')) {
                            if (!Chars.equalsNc(tok, ']')) {
                                stopPos = p;
                            } else {
                                stopPos = lexer.lastTokenPosition();
                            }
                            break;
                        }
                    } while (true);

                    SqlException e = SqlException.position(openBracketPosition)
                            .put("syntax error at column type definition, expected array type: '")
                            .put(ColumnType.nameOf(columnType));

                    // add dimensionality we found so far
                    for (int i = 0, n = dim + 1; i < n; i++) {
                        e.put("[]");
                    }
                    e.put("...', but found: '")
                            .put(lexer.getContent(), typeTagPosition, stopPos)
                            .put('\'');

                    throw e;
                }
            } else {
                lexer.unparseLast();
                break;
            }
        } while (true);

        if (hasNumericDimensionality) {
            throw SqlException.$(dimensionalityFirstPos, "arrays do not have a fixed size, remove the number");
        }
        return dim;
    }

    /**
     * Parses the content of a PARQUET(...) clause and returns the packed config int.
     * Syntax: PARQUET( (encoding [, compression[(level)]] [, BLOOM_FILTER]) | BLOOM_FILTER )
     * The opening PARQUET keyword must have been consumed already; this method consumes from '(' through ')'.
     */
    public static int parseParquetConfig(GenericLexer lexer, int columnType) throws SqlException {
        CharSequence tok = fetchNext(lexer);
        if (tok == null) {
            throw SqlException.position(lexer.getPosition()).put("'(' expected");
        }
        if (!Chars.equals(tok, '(')) {
            throw SqlException.position(lexer.lastTokenPosition()).put("'(' expected");
        }

        int encoding = 0;
        int packedCompression = 0;
        int packedLevel = 0;
        boolean bloomFilter = false;

        tok = fetchNext(lexer);
        if (tok == null) {
            throw SqlException.position(lexer.getPosition()).put("encoding name or BLOOM_FILTER expected");
        }

        // PARQUET(BLOOM_FILTER) shorthand
        if (SqlKeywords.isBloomFilterKeyword(tok)) {
            bloomFilter = true;
            tok = fetchNext(lexer);
            if (tok == null || !Chars.equals(tok, ')')) {
                throw SqlException.position(lexer.lastTokenPosition()).put("')' expected");
            }
            return TableUtils.packParquetConfig(encoding, packedCompression, packedLevel, bloomFilter);
        }

        int encodingPos = lexer.lastTokenPosition();
        encoding = ParquetEncoding.getEncoding(tok);
        if (encoding < 0) {
            SqlException e = SqlException.$(encodingPos, "invalid parquet encoding '").put(tok).put("', supported values: ");
            ParquetEncoding.addValidEncodingNamesForType(e, columnType);
            throw e;
        }
        if (encoding != ParquetEncoding.ENCODING_DEFAULT && !ParquetEncoding.isValidForColumnType(encoding, columnType)) {
            SqlException e = SqlException.$(encodingPos, "encoding '").put(tok).put("' is not valid for column type ").put(ColumnType.nameOf(columnType))
                    .put(", supported encodings for this type: ");
            ParquetEncoding.addValidEncodingNamesForType(e, columnType);
            throw e;
        }

        tok = fetchNext(lexer);
        if (tok == null) {
            throw SqlException.position(lexer.getPosition()).put("',' or ')' expected");
        }

        if (Chars.equals(tok, ',')) {
            tok = fetchNext(lexer);
            if (tok == null) {
                throw SqlException.position(lexer.getPosition()).put("compression codec name or BLOOM_FILTER expected");
            }

            // PARQUET(encoding, BLOOM_FILTER)
            if (SqlKeywords.isBloomFilterKeyword(tok)) {
                bloomFilter = true;
                tok = fetchNext(lexer);
                if (tok == null || !Chars.equals(tok, ')')) {
                    throw SqlException.position(lexer.lastTokenPosition()).put("')' expected");
                }
                return TableUtils.packParquetConfig(encoding, packedCompression, packedLevel, bloomFilter);
            }

            int codecPos = lexer.lastTokenPosition();
            int compression = ParquetCompression.getCompressionCodec(tok);
            if (compression < 0) {
                SqlException e = SqlException.$(codecPos, "invalid parquet compression codec '").put(tok).put("', supported values: ");
                ParquetCompression.addCodecNamesToException(e);
                throw e;
            }
            packedCompression = compression + 1;

            tok = fetchNext(lexer);
            if (tok != null && Chars.equals(tok, '(')) {
                tok = fetchNext(lexer);
                if (tok == null) {
                    throw SqlException.position(lexer.getPosition()).put("compression level expected");
                }
                int levelPos = lexer.lastTokenPosition();
                try {
                    int level = Numbers.parseInt(tok);
                    ParquetCompression.validateCompressionLevel(compression, level, levelPos);
                    packedLevel = level + 1;
                } catch (NumericException e) {
                    throw SqlException.$(levelPos, "compression level must be a number");
                }
                tok = fetchNext(lexer);
                if (tok == null || !Chars.equals(tok, ')')) {
                    throw SqlException.position(lexer.lastTokenPosition()).put("')' expected");
                }
                tok = fetchNext(lexer);
            }

            // PARQUET(encoding, compression[(level)], BLOOM_FILTER)
            if (tok != null && Chars.equals(tok, ',')) {
                tok = fetchNext(lexer);
                if (tok == null || !SqlKeywords.isBloomFilterKeyword(tok)) {
                    throw SqlException.position(lexer.lastTokenPosition()).put("BLOOM_FILTER expected");
                }
                bloomFilter = true;
                tok = fetchNext(lexer);
            }
        }

        if (tok == null || !Chars.equals(tok, ')')) {
            throw SqlException.position(lexer.lastTokenPosition()).put("')' expected");
        }

        if (encoding == 0 && packedCompression == 0 && !bloomFilter) {
            return 0;
        }
        return TableUtils.packParquetConfig(encoding, packedCompression, packedLevel, bloomFilter);
    }

    /**
     * Inverse of {@link #toColumnName(CharSequence)}: takes a clean, unquoted display name and
     * returns the compiler-internal alias, wrapping it in protective double quotes only when the
     * bare name could not be processed verbatim - it carries an unquoted dot (which downstream
     * {@code table.column} resolution would mis-split) or collides with an operator token. Names
     * that need no protection are returned unchanged, so the result always round-trips back to
     * {@code name} through {@link #toColumnName(CharSequence)}.
     * <p>
     * Mirrors the {@code nonLiteral} quoting decision of
     * {@link #createExprColumnAlias(CharacterStore, CharSequence, AbstractLowerCaseCharSequenceHashSet, LowerCaseCharSequenceIntHashMap, int, boolean)}.
     * Use it where an alias is assembled from already-stripped parts (e.g. the PIVOT rewrite composes
     * {@code value_aggregate}) and must be re-protected as a WHOLE: leaving a component's quotes
     * embedded mid-name would defeat {@link #isQuoteProtectedAlias(CharSequence)} / {@link #toColumnName(CharSequence)}
     * and leak the quotes into result set metadata.
     */
    public static CharSequence protectColumnAlias(CharacterStore store, CharSequence name) {
        // An empty name has no interior to protect: wrapping it as "" is not recognized as a
        // quote-protected alias (quoteProtectedInteriorDot needs a >= 1 char interior), so toColumnName
        // could not strip it back and the quotes would leak. Only a non-empty name that carries an
        // unquoted dot or collides with an operator token needs the protective quotes.
        if (name.length() > 0 && (Chars.indexOfLastUnquoted(name, '.') > -1 || disallowedAliases.contains(name))) {
            final CharacterStoreEntry entry = store.newEntry();
            entry.put('"').put(name).put('"');
            return entry.toImmutable();
        }
        return name;
    }

    /**
     * Classifies the {@code [lo, hi)} slice as a compiler-protected alias and, when it is, locates
     * the unquoted dot in its interior. This is the shared primitive behind
     * {@link #isQuoteProtectedAlias(CharSequence, int, int)}; callers that also need to know whether
     * the interior is dotted (the {@code splitsOnDot} strip-retry guard in
     * {@link #getColumnIndexQuiet(RecordMetadata, CharSequence)} and
     * {@link io.questdb.griffin.engine.join.JoinRecordMetadata#getColumnIndexQuiet(CharSequence, int, int)})
     * use it so one scan answers both questions instead of re-deriving the dot separately. The alias
     * is protected when the slice carries outer double quotes and its interior has an unquoted dot or
     * collides with an operator token. Returns:
     * <ul>
     *     <li>{@code >= 0} - the index of the interior's last unquoted dot (protected via a dot);</li>
     *     <li>{@code -1} - protected via an operator token, no interior dot;</li>
     *     <li>{@link Integer#MIN_VALUE} - not protected (quotes are genuine content, or not quote-shaped).</li>
     * </ul>
     */
    public static int quoteProtectedInteriorDot(CharSequence alias, int lo, int hi) {
        if (hi - lo < 3 || alias.charAt(lo) != '"' || alias.charAt(hi - 1) != '"') {
            return Integer.MIN_VALUE;
        }
        final int dot = Chars.indexOfLastUnquoted(alias, '.', lo + 1, hi - 1);
        if (dot > -1) {
            return dot;
        }
        return disallowedAliases.contains(alias, lo + 1, hi - 1) ? -1 : Integer.MIN_VALUE;
    }

    /**
     * Converts a projection alias to the user-visible result set column name by removing
     * the protective double quotes {@link #createExprColumnAlias(CharacterStore, CharSequence, AbstractLowerCaseCharSequenceHashSet, LowerCaseCharSequenceIntHashMap, int, boolean)}
     * wraps around aliases the compiler cannot process verbatim. The quotes are
     * compiler-internal syntax, not part of the name, and must not leak into result set
     * metadata. Aliases whose quotes are genuine content (see {@link #isQuoteProtectedAlias(CharSequence)})
     * are returned as-is.
     */
    public static String toColumnName(CharSequence alias) {
        if (isQuoteProtectedAlias(alias)) {
            return Chars.toString(alias, 1, alias.length() - 1);
        }
        return Chars.toString(alias);
    }

    public static int toPersistedType(@NotNull CharSequence tok, int tokPosition) throws SqlException {
        final int columnType = ColumnType.typeOf(tok);
        if (columnType == -1) {
            throw SqlException.$(tokPosition, "unsupported column type: ").put(tok);
        }
        if (ColumnType.isPersisted(ColumnType.tagOf(columnType))) {
            return columnType;
        }
        throw SqlException.$(tokPosition, "non-persisted type: ").put(tok);
    }

    // tableName and columnName have to be string objects,
    // they will be used in the view definition
    private static void addDependency(@NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> depMap, String tableName, String columnName) {
        LowerCaseCharSequenceHashSet columns = depMap.get(tableName);
        if (columns == null) {
            columns = new LowerCaseCharSequenceHashSet();
            depMap.put(tableName, columns);
        }
        columns.add(columnName);
    }

    private static void collectColumnReferencesFromExpression(
            @NotNull CairoEngine engine,
            @NotNull ExpressionNode expr,
            @NotNull IQueryModel model,
            @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> depMap
    ) {
        // A sub-query embedded in an expression (e.g. WHERE col IN (SELECT ... FROM t),
        // EXISTS (SELECT ... FROM t), or a scalar sub-select) carries its own table and
        // column references, so descend into them here.
        if (expr.queryModel != null) {
            collectTableAndColumnReferences(engine, expr.queryModel, depMap);
        }

        // Handle column literals (e.g., table.column or column)
        if (expr.type == ExpressionNode.LITERAL) {
            CharSequence token = expr.token;
            if (token != null) {
                int dot = Chars.indexOfLastUnquoted(token, '.');
                if (dot > -1) {
                    // This is a qualified column reference: table.column
                    String tableName = unquote(token.subSequence(0, dot)).toString();
                    String columnName = unquote(token.subSequence(dot + 1, token.length())).toString();
                    if (engine.getTableTokenIfExists(tableName) != null) {
                        addDependency(depMap, tableName, columnName);
                    }
                } else {
                    final IQueryModel nestedModel = model.getNestedModel();
                    final CharSequence tableName = nestedModel != null ? nestedModel.getTableName() : model.getTableName();
                    if (tableName != null && engine.getTableTokenIfExists(tableName) != null) {
                        addDependency(depMap, tableName.toString(), expr.token.toString());
                    }
                }
            }
        }

        // Recursively process function arguments and operators
        for (int i = 0, n = expr.args.size(); i < n; i++) {
            collectColumnReferencesFromExpression(engine, expr.args.getQuick(i), model, depMap);
        }

        // Process left and right hand sides for operators
        if (expr.lhs != null) {
            collectColumnReferencesFromExpression(engine, expr.lhs, model, depMap);
        }
        if (expr.rhs != null) {
            collectColumnReferencesFromExpression(engine, expr.rhs, model, depMap);
        }
    }

    private static void collectColumnReferencesFromJoinColumns(
            @NotNull CairoEngine engine,
            @NotNull ObjList<ExpressionNode> joinColumns,
            @NotNull IQueryModel model,
            @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> depMap
    ) {
        for (int i = 0, n = joinColumns.size(); i < n; i++) {
            final ExpressionNode joinColumn = joinColumns.getQuick(i);
            if (joinColumn != null) {
                collectColumnReferencesFromExpression(engine, joinColumn, model, depMap);
            }
        }
    }

    private static int findEndOfDigitsPos(CharSequence tok, int tokLen, int tokPosition) throws SqlException {
        int k = -1;
        // look for end of digits
        for (int i = 0; i < tokLen; i++) {
            char c = tok.charAt(i);
            if (c < '0' || c > '9') {
                k = i;
                break;
            }
        }

        if (k == -1) {
            throw SqlException.$(tokPosition + tokLen, "expected interval qualifier in ").put(tok);
        }
        return k;
    }

    /**
     * Tests whether {@code alias} (a freshly generated candidate, {@code quoted} when it carries the
     * compiler's protective double quotes) is free of a result-set name collision in the already
     * assigned {@code takenAliases}. The set holds raw aliases, but two aliases that reduce to the
     * same {@link #toColumnName} display name still collide when the projection metadata is built:
     * a protective-quoted {@code "in"} and a bare {@code in} both surface as {@code in}. So a
     * candidate is available only when NEITHER representation of its display name is taken:
     * <ul>
     *     <li>a quoted {@code "<name>"} candidate must also find the bare {@code <name>} free;</li>
     *     <li>a bare {@code <name>} candidate whose value is itself quoting-worthy must also find the
     *     protective-quoted sibling free. {@code bareQuotedSibling} is that {@code "<name>"} form,
     *     precomputed by the caller (null when the candidate has no protected sibling, e.g. an
     *     ordinary identifier or a suffixed {@code in1}).</li>
     * </ul>
     */
    private static boolean isAliasNameAvailable(
            AbstractLowerCaseCharSequenceHashSet takenAliases,
            CharSequence alias,
            boolean quoted,
            @Nullable CharSequence bareQuotedSibling
    ) {
        if (!takenAliases.excludes(alias)) {
            return false;
        }
        if (quoted) {
            return takenAliases.excludes(alias, 1, alias.length() - 1);
        }
        return bareQuotedSibling == null || takenAliases.excludes(bareQuotedSibling);
    }

    static QueryColumn nextColumn(
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<ExpressionNode> sqlNodePool,
            CharSequence alias,
            CharSequence column,
            boolean includeIntoWildcard,
            int position
    ) {
        return queryColumnPool.next().of(alias, nextLiteral(sqlNodePool, column, position), includeIntoWildcard);
    }

    static ExpressionNode nextConstant(ObjectPool<ExpressionNode> pool, CharSequence token, int position) {
        return nextExpr(pool, ExpressionNode.CONSTANT, token, position);
    }

    static ExpressionNode nextLiteral(ObjectPool<ExpressionNode> pool, CharSequence token, int position) {
        return nextExpr(pool, ExpressionNode.LITERAL, token, position);
    }

    private static class Long256ConstantFactory implements Long256Acceptor {
        private Long256Constant long256;

        @Override
        public void setAll(long l0, long l1, long l2, long l3) {
            assert long256 == null;
            long256 = new Long256Constant(l0, l1, l2, l3);
        }

        Long256Constant pop() {
            Long256Constant v = long256;
            long256 = null;
            return v;
        }
    }

    static {
        // note: it's safe to take any registry (new or old) because we don't use precedence here
        OperatorRegistry registry = OperatorExpression.getRegistry();
        for (int i = 0, n = registry.operators.size(); i < n; i++) {
            SqlUtil.disallowedAliases.add(registry.operators.getQuick(i).operator.token);
        }
        SqlUtil.disallowedAliases.add("");

        final DateFormatCompiler milliCompiler = new DateFormatCompiler();
        final DateFormat pgDateTimeFormat = milliCompiler.compile("y-MM-dd HH:mm:ssz");

        // we are using "millis" compiler deliberately because clients encode millis into strings
        IMPLICIT_CAST_FORMATS = new DateFormat[]{
                PG_DATE_Z_FORMAT,
                PG_DATE_MILLI_TIME_Z_FORMAT,
                pgDateTimeFormat
        };

        IMPLICIT_CAST_FORMATS_SIZE = IMPLICIT_CAST_FORMATS.length;
    }
}
