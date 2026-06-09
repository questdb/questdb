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

package io.questdb.test.griffin.fuzz.clauses;

import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.griffin.fuzz.FuzzColumn;
import io.questdb.test.griffin.fuzz.FuzzNames;
import io.questdb.test.griffin.fuzz.FuzzSource;
import io.questdb.test.griffin.fuzz.FuzzTable;
import io.questdb.test.griffin.fuzz.GeneratedQuery;
import io.questdb.test.griffin.fuzz.PredicateGenerator;
import io.questdb.test.griffin.fuzz.expr.BindContext;
import io.questdb.test.griffin.fuzz.expr.ExpressionGenerator;
import io.questdb.test.griffin.fuzz.expr.FuzzExpr;
import io.questdb.test.griffin.fuzz.types.ColumnKind;

/**
 * ASOF / LT / SPLICE join across two WAL tables. Both tables are aliased
 * (table aliases are mandatory when columns collide, and our factory
 * always produces a shared {@code sym} and {@code ts}). Projection is a
 * mix of left-side and right-side expressions so the join path is
 * exercised beyond a single "ts+sym" shape.
 * <p>
 * The ON clause is optional on all three temporal joins. When present it
 * carries one or two equality key pairs drawn from join-eligible columns
 * (anything {@link ColumnKind#isJoinKey()} accepts, not just symbols),
 * excluding the designated timestamp since the temporal join already
 * matches on it implicitly. A pair is emitted only when the two column
 * types are join-compatible -- identical concrete type, or both members
 * of the SYMBOL/STRING/VARCHAR family -- so the generator never produces
 * a "join column type mismatch". Keys are written either as the shorthand
 * {@code ON (name[, name])} (shared column names) or the explicit
 * {@code ON a.x = b.y [AND a.p = b.q]} form, exercising both parser paths.
 * ASOF and LT additionally accept an optional {@code TOLERANCE}; SPLICE
 * does not.
 */
public final class TemporalJoinClause {

    private static final String[] JOIN_KINDS = {"ASOF JOIN", "LT JOIN", "SPLICE JOIN"};
    private static final String[] TOLERANCES = {"1s", "30s", "5m", "1h"};

    private TemporalJoinClause() {
    }

    public static GeneratedQuery generate(Rnd rnd, FuzzSource leftSrc, FuzzSource rightSrc, BindContext ctx) {
        FuzzTable left = leftSrc.getTable();
        FuzzTable right = rightSrc.getTable();
        String joinKind = JOIN_KINDS[rnd.nextInt(JOIN_KINDS.length)];
        boolean splice = "SPLICE JOIN".equals(joinKind);

        String leftAlias = "a";
        String rightAlias = "b";

        ExpressionGenerator leftGen = new ExpressionGenerator(rnd, left.getColumns(), leftAlias, 2);
        ExpressionGenerator rightGen = new ExpressionGenerator(rnd, right.getColumns(), rightAlias, 2);

        StringSink sql = new StringSink();
        sql.put(leftSrc.getPrefixSql());
        sql.put(rightSrc.getPrefixSql());
        sql.put("SELECT ");
        int n = 2 + rnd.nextInt(4);
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                sql.put(", ");
            }
            FuzzExpr e = rnd.nextBoolean() ? leftGen.generateAnyKind() : rightGen.generateAnyKind();
            e.appendSql(sql, ctx);
            sql.put(" AS e").put(i);
        }

        sql.put(" FROM ").put(leftSrc.getFromSqlWithGarble(rnd)).put(' ').put(leftAlias);
        sql.put(' ').put(joinKind).put(' ').put(rightSrc.getFromSqlWithGarble(rnd)).put(' ').put(rightAlias);

        // ON is optional on all temporal joins; when present it carries one
        // or two equality key pairs over join-eligible columns of any type.
        appendOnClause(sql, rnd, left, right, leftAlias, rightAlias);

        // TOLERANCE applies to ASOF/LT only.
        if (!splice && rnd.nextInt(3) == 0) {
            sql.put(" TOLERANCE ").put(TOLERANCES[rnd.nextInt(TOLERANCES.length)]);
        }

        if (rnd.nextBoolean()) {
            // WHERE can touch either side; use left.
            String pred = new PredicateGenerator(rnd, 1).generate(left.getColumns(), leftAlias, ctx);
            sql.put(" WHERE ").put(pred);
        }

        if (rnd.nextBoolean()) {
            int picks = 1 + rnd.nextInt(Math.min(2, n));
            sql.put(" ORDER BY ");
            for (int i = 0; i < picks; i++) {
                if (i > 0) {
                    sql.put(", ");
                }
                int idx = rnd.nextInt(n);
                if (rnd.nextBoolean()) {
                    sql.put('e').put(idx);
                } else {
                    sql.put(idx + 1);
                }
                if (rnd.nextBoolean()) {
                    sql.put(rnd.nextBoolean() ? " ASC" : " DESC");
                }
            }
        }

        // LIMIT over a parallel ASOF / LT / SPLICE join can pick a different
        // valid subset across runs. Emit it half the time and tell the runner
        // which queries are safe to compare row-for-row.
        boolean hasLimit = rnd.nextBoolean();
        if (hasLimit) {
            sql.put(" LIMIT ").put(1 + rnd.nextInt(50));
        }
        return new GeneratedQuery(sql.toString(), !hasLimit);
    }

    private static void appendEquality(
            StringSink sql,
            Rnd rnd,
            ObjList<String> pairLeft,
            ObjList<String> pairRight,
            int idx,
            String leftAlias,
            String rightAlias
    ) {
        sql.put(leftAlias).put('.').put(FuzzNames.column(rnd, pairLeft.getQuick(idx)));
        sql.put(" = ");
        sql.put(rightAlias).put('.').put(FuzzNames.column(rnd, pairRight.getQuick(idx)));
    }

    private static void appendOnClause(
            StringSink sql,
            Rnd rnd,
            FuzzTable left,
            FuzzTable right,
            String leftAlias,
            String rightAlias
    ) {
        // 0 -> no ON clause, 1 -> one key pair, 2 -> two key pairs.
        int arity = rnd.nextInt(3);
        if (arity == 0) {
            return;
        }

        ObjList<FuzzColumn> leftKeys = joinKeyColumns(left);
        ObjList<FuzzColumn> rightKeys = joinKeyColumns(right);

        // Shorthand ON (name) resolves the same name on both sides; explicit
        // ON a.x = b.y can pair any two compatible columns regardless of name.
        // Build both candidate sets. The shared 'sym' SYMBOL guarantees at
        // least one shared name and one compatible pair, so neither set is
        // empty in practice and at least one key can always be emitted.
        ObjList<String> sharedNames = sharedCompatibleNames(leftKeys, rightKeys);
        ObjList<String> pairLeft = new ObjList<>();
        ObjList<String> pairRight = new ObjList<>();
        for (int i = 0, m = leftKeys.size(); i < m; i++) {
            FuzzColumn lc = leftKeys.getQuick(i);
            for (int j = 0, n = rightKeys.size(); j < n; j++) {
                FuzzColumn rc = rightKeys.getQuick(j);
                if (keysCompatible(lc, rc)) {
                    pairLeft.add(lc.getName());
                    pairRight.add(rc.getName());
                }
            }
        }
        if (pairLeft.size() == 0) {
            return; // defensive: sym/sym guarantees a pair, so unreachable in practice
        }

        // Draw the coin unconditionally so the literal and bind generation
        // passes consume the same rnd ops; fall back to explicit form when no
        // shared name is available for the shorthand.
        boolean useShorthand = rnd.nextBoolean() && sharedNames.size() > 0;
        sql.put(" ON ");
        if (useShorthand) {
            int size = sharedNames.size();
            int keys = Math.min(arity, size);
            int i1 = rnd.nextInt(size);
            sql.put('(').put(FuzzNames.column(rnd, sharedNames.getQuick(i1)));
            if (keys == 2) {
                int i2 = rnd.nextInt(size - 1);
                if (i2 >= i1) {
                    i2++;
                }
                sql.put(", ").put(FuzzNames.column(rnd, sharedNames.getQuick(i2)));
            }
            sql.put(')');
        } else {
            int size = pairLeft.size();
            int keys = Math.min(arity, size);
            int p1 = rnd.nextInt(size);
            appendEquality(sql, rnd, pairLeft, pairRight, p1, leftAlias, rightAlias);
            if (keys == 2) {
                int p2 = rnd.nextInt(size - 1);
                if (p2 >= p1) {
                    p2++;
                }
                sql.put(" AND ");
                appendEquality(sql, rnd, pairLeft, pairRight, p2, leftAlias, rightAlias);
            }
        }
    }

    private static ObjList<FuzzColumn> joinKeyColumns(FuzzTable table) {
        ObjList<FuzzColumn> out = new ObjList<>();
        String tsName = table.getTsColumnName();
        for (int i = 0, n = table.getColumnCount(); i < n; i++) {
            FuzzColumn c = table.getColumn(i);
            // Skip the designated timestamp: ASOF/LT/SPLICE already match on it
            // implicitly, so an explicit ts equality key is near-always empty.
            if (tsName != null && tsName.equals(c.getName())) {
                continue;
            }
            if (c.getType().getKind().isJoinKey()) {
                out.add(c);
            }
        }
        return out;
    }

    /**
     * Returns true iff a {@code a.X = b.Y} equality over the two columns
     * compiles as a temporal join key. Mirrors QuestDB's rule in
     * {@code SqlCodeGenerator}: identical concrete type, or both columns in
     * the SYMBOL/STRING/VARCHAR family. DECIMAL and ARRAY never reach here
     * because {@link ColumnKind#isJoinKey()} excludes them.
     */
    private static boolean keysCompatible(FuzzColumn a, FuzzColumn b) {
        ColumnKind ka = a.getType().getKind();
        ColumnKind kb = b.getType().getKind();
        if (ka == ColumnKind.STRING_LIKE && kb == ColumnKind.STRING_LIKE) {
            return true;
        }
        // getDdl() uniquely identifies the concrete type for every remaining
        // join-eligible kind, so DDL equality is concrete-type equality here.
        return a.getType().getDdl().equals(b.getType().getDdl());
    }

    /**
     * Column names present on both sides under the same name with
     * join-compatible types -- the only names the shorthand {@code ON (name)}
     * form can reference. Always contains {@code sym}.
     */
    private static ObjList<String> sharedCompatibleNames(ObjList<FuzzColumn> leftKeys, ObjList<FuzzColumn> rightKeys) {
        ObjList<String> out = new ObjList<>();
        for (int i = 0, m = leftKeys.size(); i < m; i++) {
            FuzzColumn lc = leftKeys.getQuick(i);
            for (int j = 0, n = rightKeys.size(); j < n; j++) {
                FuzzColumn rc = rightKeys.getQuick(j);
                if (lc.getName().equals(rc.getName()) && keysCompatible(lc, rc)) {
                    out.add(lc.getName());
                    break;
                }
            }
        }
        return out;
    }
}
