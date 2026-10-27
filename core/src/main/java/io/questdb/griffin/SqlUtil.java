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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.MillisTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DoubleArrayParser;
import io.questdb.cairo.arr.VarcharArrayParser;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.InvalidColumnException;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
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
import io.questdb.std.FiberLocal;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Acceptor;
import io.questdb.std.Long256FromCharSequenceDecoder;
import io.questdb.std.Long256Impl;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.Uuid;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.millitime.DateFormatCompiler;
import io.questdb.std.fastdouble.FastFloatParser;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;

import static io.questdb.std.GenericLexer.unquote;
import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;
import static io.questdb.std.datetime.millitime.DateFormatUtils.PG_DATE_MILLI_TIME_Z_FORMAT;
import static io.questdb.std.datetime.millitime.DateFormatUtils.PG_DATE_Z_FORMAT;

public class SqlUtil {
    static final LowerCaseCharSequenceHashSet disallowedAliases = new LowerCaseCharSequenceHashSet();
    private static final DateFormat[] IMPLICIT_CAST_FORMATS;
    private static final int IMPLICIT_CAST_FORMATS_SIZE;
    private static final FiberLocal<StringSink> IMPLICIT_CAST_VARCHAR_SINK = new FiberLocal<>(StringSink::new);
    private static final FiberLocal<Long256ConstantFactory> LONG256_FACTORY = new FiberLocal<>(Long256ConstantFactory::new);

    public static boolean containsWithin(ExpressionNode node, ArrayDeque<ExpressionNode> stack) {
        stack.clear();
        if (node != null) {
            stack.push(node);
        }
        try {
            while (!stack.isEmpty()) {
                ExpressionNode next = stack.pop();
                if (next.token != null && SqlKeywords.isWithinKeyword(next.token)) {
                    return true;
                }
                if (next.lhs != null) {
                    stack.push(next.lhs);
                }
                if (next.rhs != null) {
                    stack.push(next.rhs);
                }
                for (int i = 0, n = next.args.size(); i < n; i++) {
                    stack.push(next.args.getQuick(i));
                }
            }
            return false;
        } finally {
            stack.clear();
        }
    }

    public static ExpressionNode getEqualsOrColumn(ExpressionNode node, ArrayDeque<ExpressionNode> stack) {
        if (node == null || node.paramCount != 2 || node.token == null || !SqlKeywords.isOrKeyword(node.token)) {
            return null;
        }
        ExpressionNode column = null;
        stack.clear();
        stack.push(node);
        try {
            while (!stack.isEmpty()) {
                ExpressionNode leaf = stack.pop();
                if (leaf.paramCount == 2 && SqlKeywords.isOrKeyword(leaf.token)) {
                    stack.push(leaf.lhs);
                    stack.push(leaf.rhs);
                    continue;
                }
                ExpressionNode key = getEqualsKey(leaf);
                if (key == null || (column != null && !Chars.equalsIgnoreCase(column.token, key.token))) {
                    return null;
                }
                column = key;
            }
            return column;
        } finally {
            stack.clear();
        }
    }

    public static void rewriteEqualsOrToIn(ExpressionNode node, ArrayDeque<ExpressionNode> stack) {
        assert node.args.size() == 0;
        ExpressionNode column = null;
        stack.clear();
        stack.push(node);
        try {
            while (!stack.isEmpty()) {
                ExpressionNode leaf = stack.pop();
                if (leaf.paramCount == 2 && SqlKeywords.isOrKeyword(leaf.token)) {
                    stack.push(leaf.lhs);
                    stack.push(leaf.rhs);
                    continue;
                }
                column = getEqualsKey(leaf);
                assert column != null;
                node.args.add(column == leaf.lhs ? leaf.rhs : leaf.lhs);
            }
        } finally {
            stack.clear();
        }
        node.args.add(column);
        node.paramCount = node.args.size();
        node.type = ExpressionNode.FUNCTION;
        node.token = "in";
        node.lhs = null;
        node.rhs = null;
    }

    public static void addSelectStar(
            IQueryModel model,
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<ExpressionNode> expressionNodePool
    ) throws SqlException {
        model.addBottomUpColumn(nextColumn(queryColumnPool, expressionNodePool, "*", "*", true, 0));
        model.setArtificialStar(true);
    }

    public static long castPGDates(CharSequence value, int fromColumnType, TimestampDriver driver) {
        try {
            return driver.fromDate(parsePGDate(value));
        } catch (NumericException ignore) {
            throw ImplicitCastException.inconvertibleValue(value, fromColumnType, driver.getTimestampType());
        }
    }

    public static long castPGDates(Utf8Sequence value, int fromColumnType, TimestampDriver driver) {
        try {
            // The common case needs neither an ASCII scan nor UTF-16 decoding.
            return driver.fromDate(parsePGDate(value.asAsciiCharSequence()));
        } catch (NumericException ignore) {
        }
        // A failed parse may still be a valid localized time-zone name.
        // Avoid decoding when the false ASCII hint was merely conservative.
        if (!Utf8s.isAscii(value)) {
            final StringSink utf16Sink = IMPLICIT_CAST_VARCHAR_SINK.get();
            utf16Sink.clear();
            if (Utf8s.utf8ToUtf16(value, utf16Sink)) {
                try {
                    return driver.fromDate(parsePGDate(utf16Sink));
                } catch (NumericException ignore) {
                }
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

    /**
     * Collects into {@code depMap} the objects the parsed SELECT of a view names, and the tables the
     * view reads every column of. Every table and view the SELECT reads, and every object a
     * table-name function in it names, becomes a key of the map. A table the view reads every
     * column of also gets the column {@code *}: the view reads every column the table has,
     * including one it gains after the view is created. That is a table a wildcard in the SELECT
     * list of the view selects from, such as {@code x} in {@code SELECT * FROM x} or in
     * {@code SELECT t.* FROM x t}, or one the wildcard selects from through a sub-query, a CTE or a
     * view whose own wildcard selects every column of it, such as {@code x} in
     * {@code SELECT * FROM (SELECT * FROM x)}. It is also a table a wildcard selects from in a
     * SELECT that compares or groups whole rows, which reads every column the wildcard selects
     * whichever columns the query above it keeps: a SELECT DISTINCT, a SELECT that groups, and a
     * branch of a set operation other than UNION ALL, such as {@code x} in
     * {@code SELECT a FROM (SELECT DISTINCT * FROM x)}. The aggregates of a WINDOW JOIN group no rows
     * of their SELECT, so a SELECT with a WINDOW JOIN is not one that groups.
     * <p>
     * Any other wildcard under a SELECT list that names the columns it keeps records no {@code *}
     * here: the view reads only the columns that list names, such as {@code a} in
     * {@code SELECT a FROM (SELECT * FROM x)}. Reading a table through the view takes no permission
     * on the columns the map lists for it, so a {@code *} there would let a reader that overrides a
     * variable the view declares {@code OVERRIDABLE} read a column of the table the view does not.
     * A list that names none of the columns of the sub-query under it keeps all of them, which only
     * the optimised model tells, see
     * {@link #collectTableAndColumnReferences(CairoEngine, IQueryModel, LowerCaseCharSequenceObjHashMap, ViewWildcardSources)}.
     * <p>
     * This records no other column. The parsed model has not resolved its column references, so it
     * cannot tell which table {@code t.a}, or {@code a} in a join, reads. The columns the view reads
     * come from its optimised model instead, see
     * {@link #collectTableColumnReads(CairoEngine, IQueryModel, LowerCaseCharSequenceObjHashMap, ViewWildcardSources)}.
     *
     * @param engine the engine, which resolves the table names
     * @param model  the parsed SELECT of the view, before optimisation
     * @param depMap the dependency map of the view
     */
    public static void collectTableAndColumnReferences(
            @NotNull CairoEngine engine,
            @NotNull IQueryModel model,
            @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> depMap
    ) {
        collectTableAndColumnReferences(engine, model, depMap, null);
    }

    /**
     * Collects into {@code depMap} what {@link #collectTableAndColumnReferences(CairoEngine, IQueryModel, LowerCaseCharSequenceObjHashMap)}
     * does, and into {@code wildcardSources} the tables a wildcard selects through a sub-query, a CTE
     * or a view whose SELECT list names the columns it keeps. Such a wildcard may still read every
     * column of its tables: the optimiser keeps every column of a sub-query whose columns the query
     * above it does not name, such as the one in {@code SELECT count() FROM (SELECT * FROM x)}, and of
     * the left side of a LATERAL join. Only the optimised model tells, see
     * {@link #collectTableColumnReads(CairoEngine, IQueryModel, LowerCaseCharSequenceObjHashMap, ViewWildcardSources)}.
     *
     * @param engine          the engine, which resolves the table names
     * @param model           the parsed SELECT of the view, before optimisation
     * @param depMap          the dependency map of the view
     * @param wildcardSources the tables whose {@code *} the optimised model decides, which this
     *                        clears first, or null to collect none
     */
    public static void collectTableAndColumnReferences(
            @NotNull CairoEngine engine,
            @NotNull IQueryModel model,
            @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> depMap,
            @Nullable ViewWildcardSources wildcardSources
    ) {
        collectObjectReferences(engine, model, depMap);
        if (wildcardSources != null) {
            wildcardSources.clear();
        }
        collectWildcardReferences(engine, model, depMap, true, wildcardSources, 0);
    }

    /**
     * Collects into {@code depMap} the columns the plan of a view's optimised SELECT reads from each
     * table through the view. Reading a table through the view takes no permission on the columns the
     * map lists for that table, see {@code AbstractPartitionFrameCursorFactory.authorizeSelect()}, so
     * the map has to list every column the view reads, and only those: a reader that overrides a
     * variable the view declares {@code OVERRIDABLE} could otherwise read a column the view does not.
     * <p>
     * Optimisation resolves the column references of the SELECT. Every model that scans a table lists
     * the columns the scan provides as its top-down columns, which are the columns
     * {@code SqlCodeGenerator} reads, whichever alias or join the SELECT names them through. The scan
     * also reads the timestamp of the table when the plan needs one the SELECT does not name, for a
     * temporal join or for SAMPLE BY over a sub-query. This adds that timestamp where the code
     * generator requires it, see {@code SqlCodeGenerator.buildQueryMetadata()}.
     * <p>
     * A sub-query in an expression, such as the one in {@code WHERE k IN (SELECT k FROM y)}, reads its
     * tables as the reader rather than through the view, so this lists none of the columns it reads.
     * Listed, they would let a reader read them through a scan of the same table that does go through
     * the view whenever the sub-query itself does not run, as when an OR whose other operand is true
     * skips it. The parsed SELECT records the tables of the sub-query as dependencies, see
     * {@link #collectTableAndColumnReferences(CairoEngine, IQueryModel, LowerCaseCharSequenceObjHashMap)}.
     * <p>
     * This also adds {@code *} for the tables in {@code wildcardSources} whose wildcard the optimised
     * model keeps whole: the optimiser keeps every column of the sub-query the wildcard sits in, so the
     * view reads every column of the table, including one the table gains after the view is created.
     * For that, the scans of the table under the wildcard have to read every column the optimiser
     * enumerated for them, not every column the table has by the time this runs: a column an ALTER TABLE
     * adds while the view compiles is not in the plan built before it, and the {@code *} covers it once
     * the plan is built again. A scan of the table elsewhere in the plan can have enumerated the columns
     * after the ALTER TABLE, so the check leaves it out.
     *
     * @param engine          the engine, which resolves the table names
     * @param model           the SELECT of the view, after optimisation
     * @param depMap          the dependency map of the view
     * @param wildcardSources the tables {@link #collectTableAndColumnReferences(CairoEngine, IQueryModel, LowerCaseCharSequenceObjHashMap, ViewWildcardSources)}
     *                        collected from the parsed SELECT, or null
     */
    public static void collectTableColumnReads(
            @NotNull CairoEngine engine,
            @NotNull IQueryModel model,
            @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> depMap,
            @Nullable ViewWildcardSources wildcardSources
    ) {
        collectTableColumnReads(engine, model, depMap, wildcardSources, false);
        if (wildcardSources != null) {
            collectKeptWildcardReferences(engine, model, depMap, wildcardSources);
        }
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
     * Returns the validated percentile at the given logical index of a one-dimensional
     * percentile list. The list may be a slice or sub-array of a larger array, so this
     * method reads through the list's stride and offset rather than its backing buffer.
     *
     * @param percentiles   the percentile list, checked by {@link #getPercentileCount}
     * @param index         the logical index, below {@link #getPercentileCount}
     * @param percentilePos the position of the percentile argument in the SQL text
     * @return the percentile
     */
    public static double getPercentile(ArrayView percentiles, int index, int percentilePos) {
        return validatePercentile(percentiles.getDouble(index * percentiles.getStride(0)), percentilePos);
    }

    /**
     * Returns the number of percentiles in a percentile list. A NULL list fails, as a NULL
     * percentile does in {@link #validatePercentile}. The list must be one-dimensional: the
     * result array of the percentile functions is always one-dimensional, so flattening a
     * deeper list would silently lose its shape.
     *
     * @param percentiles   the percentile list
     * @param percentilePos the position of the percentile argument in the SQL text
     * @return the number of percentiles
     */
    public static int getPercentileCount(ArrayView percentiles, int percentilePos) {
        if (percentiles.isNull()) {
            throw CairoException.nonCritical().position(percentilePos).put("percentile array must not be NULL");
        }
        if (percentiles.getDimCount() != 1) {
            throw CairoException.nonCritical().position(percentilePos).put("array is not one-dimensional");
        }
        return percentiles.getDimLen(0);
    }

    /**
     * Returns the index of the percentile_disc() value in a sorted list of {@code size} values.
     * The SQL standard defines the value as the first one whose cumulative distribution reaches
     * the percentile; the k-th value's cumulative distribution is {@code k / size}, which this
     * method computes in double precision, as cume_dist() does. Rounding
     * {@code percentile * size} up alone gets it wrong when the product rounds past a whole
     * number: {@code 0.07 * 100} is 7.000000000000001, yet the 7th of 100 values already has
     * the cumulative distribution {@code 7 / 100 == 0.07}.
     *
     * @param percentile the percentile, within [0, 1]
     * @param size       the number of values, at least 1
     * @return the 0-based index of the percentile_disc() value
     */
    public static long getPercentileDiscIndex(double percentile, long size) {
        // the product carries one rounding error, which moves its ceiling by one at most
        long k = Math.max(1, (long) Math.ceil(percentile * size));
        while (k > 1 && (double) (k - 1) / size >= percentile) {
            k--;
        }
        while (k < size && (double) k / size < percentile) {
            k++;
        }
        return k - 1;
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

    /**
     * Interpolates between two adjacent sorted values the way percentile_cont() does:
     * {@code lo + (hi - lo) * fraction}. The difference {@code hi - lo} overflows to infinity
     * when {@code lo} and {@code hi} are huge and of opposite signs, although the result lies
     * between them. Halving both values and doubling the result is exact at that magnitude, so
     * the method then runs the same arithmetic on the halves.
     *
     * @param lo       the lower value, finite
     * @param hi       the upper value, finite and not less than {@code lo}
     * @param fraction the position between the two values, within [0, 1]
     * @return the interpolated value, within [lo, hi]
     */
    public static double interpolatePercentile(double lo, double hi, double fraction) {
        final double delta = hi - lo;
        if (Numbers.isFinite(delta)) {
            return lo + delta * fraction;
        }
        final double halfLo = lo * 0.5;
        final double result = (halfLo + (hi * 0.5 - halfLo) * fraction) * 2;
        // keeps rounding at the edge of the double range from pushing the result past hi
        return Math.max(lo, Math.min(hi, result));
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

    /**
     * Checks that a percentile lies within [0, 1], as the SQL standard requires of
     * percentile_disc() and percentile_cont(), and returns it. NULL, NaN, infinities and
     * negative values fail; negative zero passes as zero.
     *
     * @param percentile    the percentile
     * @param percentilePos the position of the percentile argument in the SQL text
     * @return the percentile
     */
    public static double validatePercentile(double percentile, int percentilePos) {
        // Numbers.isNull() is true for NaN and both infinities
        if (Numbers.isNull(percentile) || percentile < 0 || percentile > 1) {
            throw CairoException.nonCritical().position(percentilePos)
                    .put("invalid percentile [expected=range(0.0, 1.0), actual=").put(percentile).put(']');
        }
        return percentile;
    }

    // the view definition keeps the names after the model they come from is gone, so they are stored as strings
    private static void addDependency(
            @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> depMap,
            @NotNull CharSequence tableName,
            @NotNull CharSequence columnName
    ) {
        addTableDependency(depMap, tableName).add(Chars.toString(columnName));
    }

    private static LowerCaseCharSequenceHashSet addTableDependency(
            @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> depMap,
            @NotNull CharSequence tableName
    ) {
        LowerCaseCharSequenceHashSet columns = depMap.get(tableName);
        if (columns == null) {
            columns = new LowerCaseCharSequenceHashSet();
            depMap.put(Chars.toString(tableName), columns);
        }
        return columns;
    }

    // Collects the objects an expression names: the ones a sub-query in it reads, such as t in
    // WHERE col IN (SELECT ... FROM t), EXISTS (SELECT ... FROM t) or a scalar sub-select, and the
    // ones a table-name function in it names.
    private static void collectExpressionReferences(
            @NotNull CairoEngine engine,
            @Nullable ExpressionNode expr,
            @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> depMap
    ) {
        if (expr == null) {
            return;
        }
        if (expr.queryModel != null) {
            collectObjectReferences(engine, expr.queryModel, depMap);
        }
        collectTableNameFunctionReference(engine, expr, depMap);
        for (int i = 0, n = expr.args.size(); i < n; i++) {
            collectExpressionReferences(engine, expr.args.getQuick(i), depMap);
        }
        collectExpressionReferences(engine, expr.lhs, depMap);
        collectExpressionReferences(engine, expr.rhs, depMap);
    }

    // Adds "*" for the tables of wildcardSources whose wildcard the optimised model keeps whole. The
    // holder of a table is a model of the parsed FROM clause that holds a sub-query, a CTE or a view the
    // wildcard selects the table through. Optimisation points the holder at the model that provides the
    // columns of the sub-query, also when it then takes the holder itself out of the model. That model
    // keeps every column when the query above it names none of them, and so gives it no top-down columns,
    // and when the optimiser shares it, as it does the left side of a LATERAL join, see
    // QueryModel.allowsColumnsChange(). The plan then reads every column of the table, and one the table
    // gains later too, as the wildcard expands to it.
    private static void collectKeptWildcardReferences(
            @NotNull CairoEngine engine,
            @NotNull IQueryModel model,
            @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> depMap,
            @NotNull ViewWildcardSources wildcardSources
    ) {
        for (int i = 0, n = wildcardSources.tableNames.size(); i < n; i++) {
            final IQueryModel sourceModel = wildcardSources.holders.getQuick(i).getNestedModel();
            // a model the optimised model no longer reaches has no top-down columns, kept or not
            if (sourceModel == null
                    || (sourceModel.getTopDownColumns().size() > 0 && sourceModel.allowsColumnsChange())
                    || !isReachable(model, sourceModel)) {
                continue;
            }
            final TableToken tableToken = engine.getTableTokenIfExists(wildcardSources.tableNames.getQuick(i));
            if (tableToken == null) {
                continue;
            }
            final LowerCaseCharSequenceHashSet columns = depMap.get(tableToken.getTableName());
            if (columns == null || columns.contains("*")) {
                continue;
            }
            // The scan of a table whose wildcard the plan keeps whole reads every column of it. One that
            // does not has met a rewrite this does not recognise, and the map keeps only its columns. Every
            // column is every one the optimiser enumerated for the scans under the holder: one that an ALTER
            // TABLE adds while the view compiles is in the metadata of the table, but not in the plan built
            // before it, and a scan of the table elsewhere in the plan may already list it.
            if (wildcardSources.isEveryScannedColumnRead(sourceModel, tableToken.getTableName(), columns)) {
                columns.add("*");
            }
        }
    }

    // Collects the objects a parsed model names: the tables and views it reads, the ones the sub-queries
    // in it read, a SHOW statement's object, and the objects the table-name functions in it name.
    private static void collectObjectReferences(
            @NotNull CairoEngine engine,
            @NotNull IQueryModel model,
            @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> depMap
    ) {
        IQueryModel m = model;
        do {
            // sub-queries and table-name functions in the clauses
            final ObjList<QueryColumn> columns = m.getColumns();
            for (int i = 0, n = columns.size(); i < n; i++) {
                final QueryColumn column = columns.getQuick(i);
                if (column != null) {
                    collectExpressionReferences(engine, column.getAst(), depMap);
                }
            }
            collectExpressionReferences(engine, m.getWhereClause(), depMap);
            collectExpressionReferences(engine, m.getJoinCriteria(), depMap);
            final ObjList<ExpressionNode> groupBy = m.getGroupBy();
            for (int i = 0, n = groupBy.size(); i < n; i++) {
                collectExpressionReferences(engine, groupBy.getQuick(i), depMap);
            }
            final ObjList<ExpressionNode> orderBy = m.getOrderBy();
            for (int i = 0, n = orderBy.size(); i < n; i++) {
                collectExpressionReferences(engine, orderBy.getQuick(i), depMap);
            }

            // Tables directly referenced, a SHOW statement's object included, and the tables that
            // table-name functions in the FROM clause name
            final ExpressionNode tableNameExpr = m.getTableNameExpr();
            if (tableNameExpr != null) {
                if (tableNameExpr.type == ExpressionNode.LITERAL) {
                    addTableDependency(depMap, unquote(tableNameExpr.token));
                } else {
                    collectTableNameFunctionReference(engine, tableNameExpr, depMap);
                }
            }

            final ExpressionNode viewNameExpr = m.getOriginatingViewNameExpr();
            if (viewNameExpr != null) {
                addTableDependency(depMap, unquote(viewNameExpr.token));
            }

            final ObjList<IQueryModel> joinModels = m.getJoinModels();
            for (int i = 0, n = joinModels.size(); i < n; i++) {
                final IQueryModel joinModel = joinModels.getQuick(i);
                if (joinModel != m) {
                    collectObjectReferences(engine, joinModel, depMap);
                }
            }

            final IQueryModel unionModel = m.getUnionModel();
            if (unionModel != null) {
                collectObjectReferences(engine, unionModel, depMap);
            }

            m = m.getNestedModel();
        } while (m != null);
    }

    // Collects the columns the source of an optimised model reads: the scan of its table, or what its
    // nested model reads. isTimestampRequired tells whether the plan requires a timestamp of the source,
    // as the flag SqlCodeGenerator pushes for it does.
    private static void collectSourceColumnReads(
            @NotNull CairoEngine engine,
            @NotNull IQueryModel model,
            @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> depMap,
            @Nullable ViewWildcardSources wildcardSources,
            boolean isTimestampRequired
    ) {
        final ExpressionNode tableNameExpr = model.getTableNameExpr();
        if (tableNameExpr != null) {
            // a table-name function or a SHOW statement reads no table data
            if ((tableNameExpr.type == ExpressionNode.LITERAL || tableNameExpr.type == ExpressionNode.CONSTANT)
                    && model.getSelectModelType() != IQueryModel.SELECT_MODEL_SHOW) {
                collectTableScanColumns(engine, model, tableNameExpr.token, depMap, wildcardSources, isTimestampRequired);
            }
            return;
        }
        final boolean isNestedTimestampRequired;
        if (model.getSampleBy() != null) {
            // SqlCodeGenerator.generateSampleBy(): SAMPLE BY requires the timestamp of its source,
            // unless its own TIMESTAMP() clause names one
            isNestedTimestampRequired = model.getTimestamp() == null;
        } else if (model.getSelectModelType() == IQueryModel.SELECT_MODEL_CHOOSE && model.hasExplicitTimestamp()) {
            // SqlCodeGenerator.generateSelectChoose(): a TIMESTAMP() clause names the timestamp
            // instead of the source
            isNestedTimestampRequired = false;
        } else {
            isNestedTimestampRequired = isTimestampRequired;
        }
        collectTableColumnReads(engine, model.getNestedModel(), depMap, wildcardSources, isNestedTimestampRequired);
    }

    private static void collectTableColumnReads(
            @NotNull CairoEngine engine,
            @Nullable IQueryModel model,
            @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> depMap,
            @Nullable ViewWildcardSources wildcardSources,
            boolean isTimestampRequired
    ) {
        if (model == null) {
            return;
        }
        // The walk skips the sub-queries of expressions: they read their tables as the reader, and the
        // columns they read must not cover a scan that goes through the view.
        final ObjList<IQueryModel> joinModels = model.getJoinModels();
        final int joinCount = joinModels.size();
        if (joinCount > 1) {
            // SqlCodeGenerator.generateJoins(): the first model in join order provides a timestamp when
            // a join after it needs one, and every other model when its own join needs one
            final IntList orderedJoinModels = model.getOrderedJoinModels();
            final boolean isOrdered = orderedJoinModels.size() == joinCount;
            boolean isAnyJoinTemporal = false;
            for (int i = 1; i < joinCount; i++) {
                final IQueryModel joinModel = joinModels.getQuick(isOrdered ? orderedJoinModels.getQuick(i) : i);
                if (SqlCodeGenerator.joinsRequiringTimestamp[joinModel.getJoinType()]) {
                    isAnyJoinTemporal = true;
                    break;
                }
            }
            for (int i = 0; i < joinCount; i++) {
                final IQueryModel joinModel = joinModels.getQuick(isOrdered ? orderedJoinModels.getQuick(i) : i);
                final boolean isJoinTimestampRequired = i == 0
                        ? isAnyJoinTemporal
                        : SqlCodeGenerator.joinsRequiringTimestamp[joinModel.getJoinType()];
                if (joinModel == model) {
                    collectSourceColumnReads(engine, model, depMap, wildcardSources, isJoinTimestampRequired);
                } else {
                    collectTableColumnReads(engine, joinModel, depMap, wildcardSources, isJoinTimestampRequired);
                }
            }
        } else {
            // SqlCodeGenerator.generateQuery0(): a model that orders its rows requires no timestamp
            // of its source
            collectSourceColumnReads(engine, model, depMap, wildcardSources, isTimestampRequired && model.getOrderBy().size() == 0);
        }
        // every branch of a set operation compiles under the requirement of the first one
        collectTableColumnReads(engine, model.getUnionModel(), depMap, wildcardSources, isTimestampRequired);
    }

    // A table-name function, such as table_columns('t'), reads the metadata of the object its constant
    // argument names. The object is recorded as a dependency of the view with no columns: a reader of the
    // view may read its metadata through the view, see SqlExecutionContext.TableFunctionView.isDependency(),
    // but none of its data. The argument is recorded as the view's definition writes it, so a caller that
    // later overrides an OVERRIDABLE variable of the view does not change it.
    private static void collectTableNameFunctionReference(
            @NotNull CairoEngine engine,
            @NotNull ExpressionNode node,
            @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> depMap
    ) {
        if (node.type == ExpressionNode.FUNCTION
                && node.paramCount == 1
                && node.rhs != null
                && node.rhs.type == ExpressionNode.CONSTANT
                && engine.getFunctionFactoryCache().isTableNameFunction(node.token)) {
            addTableDependency(depMap, unquote(node.rhs.token));
        }
    }

    // Collects the columns the scan of a table model reads: its top-down columns, which
    // SqlCodeGenerator.buildQueryMetadata() reads, and the timestamp it adds when the plan requires one.
    // Records the scan in wildcardSources, when given, for collectKeptWildcardReferences().
    private static void collectTableScanColumns(
            @NotNull CairoEngine engine,
            @NotNull IQueryModel model,
            @NotNull CharSequence tableName,
            @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> depMap,
            @Nullable ViewWildcardSources wildcardSources,
            boolean isTimestampRequired
    ) {
        CharSequence name = tableName;
        if (Chars.startsWith(name, IQueryModel.NO_ROWID_MARKER)) {
            name = name.subSequence(IQueryModel.NO_ROWID_MARKER.length(), name.length());
        }
        final TableToken tableToken = engine.getTableTokenIfExists(unquote(name));
        if (tableToken == null) {
            return;
        }
        final LowerCaseCharSequenceHashSet columns = addTableDependency(depMap, tableToken.getTableName());
        final ObjList<QueryColumn> topDownColumns = model.getTopDownColumns();
        for (int i = 0, n = topDownColumns.size(); i < n; i++) {
            columns.add(Chars.toString(topDownColumns.getQuick(i).getName()));
        }
        if (wildcardSources != null) {
            wildcardSources.addScan(tableToken.getTableName(), model);
        }
        if (!isTimestampRequired && !SqlCodeGenerator.joinsRequiringTimestamp[model.getJoinType()]) {
            return;
        }
        // SqlCodeGenerator.getTimestampIndex(): the scan reads the column a TIMESTAMP() clause names,
        // which is the designated timestamp when the query names none
        final ExpressionNode timestamp = model.getTimestamp();
        if (timestamp == null) {
            return;
        }
        if (timestamp.isTimestampOrderInherited) {
            try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
                final int timestampIndex = metadata.getTimestampIndex();
                if (timestampIndex > -1) {
                    columns.add(Chars.toString(metadata.getColumnName(timestampIndex)));
                }
            }
            return;
        }
        CharSequence timestampName = timestamp.token;
        final int dot = Chars.indexOfLastUnquoted(timestampName, '.');
        if (dot > -1) {
            timestampName = timestampName.subSequence(dot + 1, timestampName.length());
        }
        columns.add(Chars.toString(unquote(timestampName)));
    }

    // Adds "*" for the tables a parsed SELECT reads every column of, see collectTableAndColumnReferences().
    // A wildcard reads every column it selects when the query above its SELECT keeps every column of it,
    // isEveryColumnKept: the SELECT of the view, every branch of a set operation it is made of, and a
    // sub-query, a CTE or a view a wildcard of such a SELECT selects from. A wildcard also reads every
    // column it selects when its SELECT compares or groups whole rows, whichever columns the query above
    // keeps: the optimiser keeps every column of a SELECT DISTINCT, of a SELECT that groups, which groups
    // by every column it selects, and of every branch of a set operation that has an operation other than
    // UNION ALL, which compares the rows of the branches. The aggregates of a SELECT with a WINDOW JOIN do
    // not group it: each aggregates the rows of the right side that the window of one row matches, and
    // the optimiser keeps only the columns of such a SELECT that the query above it names, so the
    // optimised model decides on its wildcards, as on any other. A wildcard selects every column of the
    // sources in the FROM clause of its SELECT, and a qualified one, such as t.*, every column of the one
    // it names.
    // The parser keeps the SELECT list in a model of its own, whose nested model holds the FROM clause:
    // the table or sub-query read first, and the models joined to it. The walk visits every source of a
    // FROM clause once, and records nothing in a sub-query of an expression, whose scans the reader
    // authorizes. The source of a PIVOT is a FROM clause with no SELECT list, so no wildcard selects
    // from it, and the PIVOT reads only the columns it groups and aggregates.
    // A wildcard under a SELECT list that names its columns may still read every column it selects, as
    // the optimiser keeps every column of a sub-query whose columns the query above it names none of. Such
    // a wildcard adds its tables to wildcardSources instead, see collectKeptWildcardReferences(), under
    // the holders of the sub-queries that keep every column of the wildcard once the optimiser keeps every
    // column of any of them: the ones from holderLo up on the holder stack.
    private static void collectWildcardReferences(
            @NotNull CairoEngine engine,
            @NotNull IQueryModel model,
            @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> depMap,
            boolean isEveryColumnKept,
            @Nullable ViewWildcardSources wildcardSources,
            int holderLo
    ) {
        boolean isEveryRowCompared = false;
        for (IQueryModel branch = model; branch.getUnionModel() != null; branch = branch.getUnionModel()) {
            if (branch.getSetOperationType() != IQueryModel.SET_OPERATION_UNION_ALL) {
                isEveryRowCompared = true;
                break;
            }
        }
        final FunctionFactoryCache functionFactoryCache = engine.getFunctionFactoryCache();
        for (IQueryModel branch = model; branch != null; branch = branch.getUnionModel()) {
            // a model with no SELECT list, such as the source of a PIVOT, is a FROM clause itself
            final IQueryModel fromModel = branch.getColumns().size() > 0 && branch.getNestedModel() != null
                    ? branch.getNestedModel()
                    : branch;
            final boolean isEveryColumnRead = isEveryColumnKept
                    || isEveryRowCompared
                    || branch.isDistinct()
                    || fromModel.getGroupBy().size() > 0
                    || (hasGroupByFunction(functionFactoryCache, branch.getColumns()) && !hasWindowJoin(fromModel));
            final ObjList<IQueryModel> sources = fromModel.getJoinModels();
            for (int i = 0, n = sources.size(); i < n; i++) {
                final IQueryModel source = sources.getQuick(i);
                final boolean isSelected = isSelectedByWildcard(branch, source);
                final boolean isSourceRead = isEveryColumnRead && isSelected;
                final IQueryModel sourceModel = source.getNestedModel();
                if (sourceModel != null) {
                    // the columns of a sub-query, a CTE or a view are the ones its own SELECT selects
                    if (wildcardSources == null) {
                        collectWildcardReferences(engine, sourceModel, depMap, isSourceRead, null, 0);
                    } else {
                        // Keeping every column of the branch keeps every column of a source its wildcard
                        // selects, so such a source adds its holder to the ones of the branch. Any other
                        // source starts holders of its own.
                        final ObjList<IQueryModel> holderStack = wildcardSources.holderStack;
                        final int sourceHolderLo = isSelected ? holderLo : holderStack.size();
                        holderStack.add(source);
                        collectWildcardReferences(engine, sourceModel, depMap, isSourceRead, wildcardSources, sourceHolderLo);
                        holderStack.popLast();
                    }
                    continue;
                }
                // a table-valued function has no table to select from
                final ExpressionNode tableNameExpr = source.getTableNameExpr();
                if (isSelected && tableNameExpr != null && tableNameExpr.type == ExpressionNode.LITERAL) {
                    final CharSequence tableName = unquote(tableNameExpr.token);
                    if (engine.getTableTokenIfExists(tableName) != null) {
                        if (isEveryColumnRead) {
                            addDependency(depMap, tableName, "*");
                        } else if (wildcardSources != null) {
                            wildcardSources.add(holderLo, tableName);
                        }
                    }
                }
            }
        }
    }

    private static ExpressionNode getEqualsKey(ExpressionNode node) {
        if (node.paramCount != 2 || !Chars.equals(node.token, "=") || node.lhs == null || node.rhs == null) {
            return null;
        }
        ExpressionNode key = node.lhs.type == ExpressionNode.LITERAL ? node.lhs : node.rhs;
        ExpressionNode value = key == node.lhs ? node.rhs : node.lhs;
        if (key.type != ExpressionNode.LITERAL || value.type != ExpressionNode.CONSTANT
                || !(SqlKeywords.isNullKeyword(value.token) || Chars.isQuoted(value.token))) {
            return null;
        }
        return key;
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

    // Whether a SELECT list calls an aggregate function, which makes the SELECT group its rows by every
    // other column it selects, unless the SELECT has a WINDOW JOIN, see hasWindowJoin().
    private static boolean hasGroupByFunction(
            @NotNull FunctionFactoryCache functionFactoryCache,
            @NotNull ObjList<QueryColumn> columns
    ) {
        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn column = columns.getQuick(i);
            if (column != null && hasGroupByFunction(functionFactoryCache, column.getAst())) {
                return true;
            }
        }
        return false;
    }

    // A window function aggregates no rows of its SELECT, and an aggregate in a sub-query of an expression
    // groups the rows of that sub-query only.
    private static boolean hasGroupByFunction(
            @NotNull FunctionFactoryCache functionFactoryCache,
            @Nullable ExpressionNode node
    ) {
        if (node == null || node.queryModel != null) {
            return false;
        }
        if (node.type == ExpressionNode.FUNCTION && node.windowExpression == null && functionFactoryCache.isGroupBy(node.token)) {
            return true;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (hasGroupByFunction(functionFactoryCache, node.args.getQuick(i))) {
                return true;
            }
        }
        return hasGroupByFunction(functionFactoryCache, node.lhs) || hasGroupByFunction(functionFactoryCache, node.rhs);
    }

    // Whether a FROM clause has a WINDOW JOIN. Its aggregates leave the rows of their SELECT ungrouped:
    // each aggregates the rows of the right side that the window of one row of the left side matches.
    private static boolean hasWindowJoin(@NotNull IQueryModel fromModel) {
        final ObjList<IQueryModel> joinModels = fromModel.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            if (joinModels.getQuick(i).getJoinType() == IQueryModel.JOIN_WINDOW) {
                return true;
            }
        }
        return false;
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

    // Whether the qualifier of a qualified name, such as t in t.*, names the model: by its alias, or by
    // its table name when it has none, as SqlOptimiser resolves the qualifier.
    private static boolean isModelAlias(@NotNull IQueryModel model, @NotNull CharSequence qualifiedName, int dot) {
        final ExpressionNode aliasExpr = model.getAlias() != null ? model.getAlias() : model.getTableNameExpr();
        return aliasExpr != null && Chars.equalsIgnoreCase(unquote(aliasExpr.token), unquote(qualifiedName.subSequence(0, dot)));
    }

    // Whether the optimised model reaches target. A model of the parsed SELECT can drop out of it, when
    // optimisation replaces the models around it.
    private static boolean isReachable(@Nullable IQueryModel root, @NotNull IQueryModel target) {
        if (root == null) {
            return false;
        }
        if (root == target) {
            return true;
        }
        final ObjList<IQueryModel> joinModels = root.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            if (isReachable(joinModels.getQuick(i), target)) {
                return true;
            }
        }
        return isReachable(root.getNestedModel(), target) || isReachable(root.getUnionModel(), target);
    }

    // Whether a wildcard of the SELECT list selects every column of a source of its FROM clause: an
    // unqualified one selects every column of every source, and a qualified one, such as t.*, every
    // column of the source it names.
    private static boolean isSelectedByWildcard(@NotNull IQueryModel select, @NotNull IQueryModel source) {
        final ObjList<QueryColumn> columns = select.getColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn column = columns.getQuick(i);
            final ExpressionNode ast = column != null ? column.getAst() : null;
            if (ast != null && ast.isWildcard()) {
                final int dot = Chars.indexOfLastUnquoted(ast.token, '.');
                if (dot == -1 || isModelAlias(source, ast.token, dot)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Parses the value against every implicit-cast format, in order. Signals a failed
     * parse with the flyweight {@link NumericException} rather than an
     * {@link ImplicitCastException}: callers retry a failed parse against a decoded
     * value, and a discarded {@code ImplicitCastException} would cost a stack fill and
     * a message allocation on every row.
     */
    private static long parsePGDate(CharSequence value) throws NumericException {
        final int hi = value.length();
        for (int i = 0; i < IMPLICIT_CAST_FORMATS_SIZE; i++) {
            try {
                return IMPLICIT_CAST_FORMATS[i].parse(value, 0, hi, EN_LOCALE);
            } catch (NumericException ignore) {
            }
        }
        throw NumericException.instance();
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

    /**
     * The tables a wildcard of a view's SELECT selects through sub-queries, CTEs or views whose SELECT
     * lists name the columns they keep, each with a holder: a model of the parsed FROM clause that holds
     * one of those sub-queries. The view reads every column of the table when the optimiser keeps every
     * column of the sub-query a holder holds.
     * {@link #collectTableAndColumnReferences(CairoEngine, IQueryModel, LowerCaseCharSequenceObjHashMap, ViewWildcardSources)}
     * fills this from the parsed SELECT, and
     * {@link #collectTableColumnReads(CairoEngine, IQueryModel, LowerCaseCharSequenceObjHashMap, ViewWildcardSources)}
     * reads it once optimisation has rewritten the SELECT, which keeps the holders, and records the
     * scans of the optimised SELECT in it, which tell the columns of each table the plan is built on.
     */
    public static final class ViewWildcardSources implements Mutable {
        // the holders of the sub-queries the walk of the parsed SELECT is in, outermost first
        private final ObjList<IQueryModel> holderStack = new ObjList<>();
        private final ObjList<IQueryModel> holders = new ObjList<>();
        // the tables the models in scans scan, at the same index
        private final ObjList<String> scanTableNames = new ObjList<>();
        // the models of the optimised SELECT that scan a table
        private final ObjList<IQueryModel> scans = new ObjList<>();
        private final ObjList<String> tableNames = new ObjList<>();

        @Override
        public void clear() {
            holderStack.clear();
            holders.clear();
            scanTableNames.clear();
            scans.clear();
            tableNames.clear();
        }

        // Adds the table under every holder from holderLo up the holder stack: the optimiser keeping every
        // column of the sub-query any of them holds keeps every column the wildcard selects from it.
        private void add(int holderLo, CharSequence tableName) {
            final String name = Chars.toString(tableName);
            for (int i = holderLo, n = holderStack.size(); i < n; i++) {
                holders.add(holderStack.getQuick(i));
                tableNames.add(name);
            }
        }

        private void addScan(String tableName, IQueryModel scan) {
            scanTableNames.add(tableName);
            scans.add(scan);
        }

        // Whether the plan scans the table under sourceModel, and the plan reads every column the optimiser
        // enumerated for those scans from the metadata of the table, the columns the wildcard sourceModel
        // keeps expands to. The optimiser enumerates the columns of each scan of the table separately, so a
        // scan elsewhere in the plan can list a column that an ALTER TABLE added after the optimiser
        // enumerated the scans under sourceModel, and that the plan built on them does not read.
        private boolean isEveryScannedColumnRead(
                IQueryModel sourceModel,
                String tableName,
                LowerCaseCharSequenceHashSet readColumns
        ) {
            boolean isScanned = false;
            for (int i = 0, n = scans.size(); i < n; i++) {
                final IQueryModel scan = scans.getQuick(i);
                if (Chars.equalsIgnoreCase(scanTableNames.getQuick(i), tableName) && isReachable(sourceModel, scan)) {
                    final ObjList<CharSequence> columnNames = scan.getWildcardColumnNames();
                    for (int j = 0, m = columnNames.size(); j < m; j++) {
                        if (readColumns.excludes(columnNames.getQuick(j))) {
                            return false;
                        }
                    }
                    isScanned = true;
                }
            }
            return isScanned;
        }
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
