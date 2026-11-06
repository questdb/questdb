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

import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.griffin.fuzz.FuzzColumn;
import io.questdb.test.griffin.fuzz.FuzzSource;
import io.questdb.test.griffin.fuzz.FuzzTable;
import io.questdb.test.griffin.fuzz.GeneratedQuery;
import io.questdb.test.griffin.fuzz.PredicateGenerator;
import io.questdb.test.griffin.fuzz.expr.BindContext;
import io.questdb.test.griffin.fuzz.types.ColumnKind;

/**
 * {@code SELECT k0 AS e0 [, k1 AS e1 ...] [, grouping_id(...) AS g0], agg AS a0 FROM t [WHERE p]
 * GROUP BY [k0,] ROLLUP(...) | CUBE(...) | GROUPING SETS (...)}, or the same grouping after
 * {@code SAMPLE BY <stride>} on a source with a designated timestamp.
 * <p>
 * Keys are plain column references: QuestDB rolls up columns only, so an expression key is
 * rejected and the query would only count as a skip. Every query carries a reference query, the
 * UNION ALL of one plain GROUP BY per grouping set, with a typed NULL in place of each rolled-up
 * key and the GROUPING_ID value as a constant. QueryRunner compares the two, so grouping sets are
 * checked against an independent rewrite on top of the usual differential axes. The reference
 * computes the empty set over a non-keyed aggregate, which returns one row even when no row
 * passes the filter, as the empty grouping set does.
 * <p>
 * The generator draws every random value before it builds the reference, and the reference draws
 * none, so the literal and bind forms of a query consume the same rnd sequence.
 */
public final class GroupingSetsClause {
    private static final int FORM_CUBE = 1;
    private static final int FORM_GROUPING_SETS = 2;
    private static final int FORM_ROLLUP = 0;
    private static final int MAX_EXPLICIT_SETS = 4;
    private static final int MAX_KEYS = 3;
    private static final String[] SAMPLE_BY_STRIDES = {"30m", "1h", "2h", "1d"};

    private GroupingSetsClause() {
    }

    public static GeneratedQuery generate(Rnd rnd, FuzzSource source, BindContext ctx, boolean injectFaultFn) {
        final FuzzTable table = source.getTable();
        final ObjList<FuzzColumn> columns = table.getColumns();
        final String tsColumn = table.getTsColumnName();
        boolean isSampleBy = tsColumn != null && rnd.nextInt(3) == 0;
        final String alias = rnd.nextBoolean() ? "t0" : null;

        ObjList<FuzzColumn> candidates = keyCandidates(columns, isSampleBy ? tsColumn : null);
        if (candidates.size() == 0) {
            isSampleBy = false;
            candidates = keyCandidates(columns, null);
        }
        final int keyCount = 1 + rnd.nextInt(Math.min(MAX_KEYS, candidates.size()));
        final ObjList<FuzzColumn> keys = new ObjList<>();
        for (int i = 0; i < keyCount; i++) {
            final int pick = rnd.nextInt(candidates.size());
            keys.add(candidates.getQuick(pick));
            candidates.remove(pick);
        }

        final int form = rnd.nextInt(3);
        final boolean hasPlainKey = !isSampleBy && form != FORM_GROUPING_SETS && keyCount > 1 && rnd.nextBoolean();
        final IntList sets = new IntList();
        if (form == FORM_GROUPING_SETS) {
            int keysInSets = 0;
            for (int i = 0, n = 1 + rnd.nextInt(MAX_EXPLICIT_SETS); i < n; i++) {
                final int set = rnd.nextInt(1 << keyCount);
                sets.add(set);
                keysInSets |= set;
            }
            // A selected key that no set names is not a grouping key, and the query is rejected.
            final int allKeys = (1 << keyCount) - 1;
            if (keysInSets != allKeys) {
                sets.add(allKeys);
            }
        } else {
            final int first = hasPlainKey ? 1 : 0;
            final int plainMask = hasPlainKey ? 1 : 0;
            if (form == FORM_ROLLUP) {
                for (int i = keyCount; i >= first; i--) {
                    sets.add((1 << i) - 1);
                }
            } else {
                for (int m = 0, n = 1 << (keyCount - first); m < n; m++) {
                    sets.add(plainMask | (m << first));
                }
            }
        }

        final IntList groupingIdArgs = new IntList();
        if (rnd.nextBoolean()) {
            final IntList remaining = new IntList();
            for (int i = 0; i < keyCount; i++) {
                remaining.add(i);
            }
            for (int i = 0, n = 1 + rnd.nextInt(keyCount); i < n; i++) {
                final int pick = rnd.nextInt(remaining.size());
                groupingIdArgs.add(remaining.getQuick(pick));
                remaining.removeIndex(pick);
            }
        }

        final StringSink aggregate = new StringSink();
        appendAggregate(aggregate, rnd, columns, alias);
        final String stride = isSampleBy ? SAMPLE_BY_STRIDES[rnd.nextInt(SAMPLE_BY_STRIDES.length)] : null;

        final StringSink where = new StringSink();
        PredicateGenerator.appendWhere(where, rnd, columns, alias, 2, ctx, injectFaultFn);
        final StringSink from = new StringSink();
        from.put(" FROM ").put(source.getFromSqlWithGarble(rnd));
        if (alias != null) {
            from.put(' ').put(alias);
        }

        final StringSink sql = new StringSink();
        sql.put(source.getPrefixSql()).put("SELECT ");
        if (isSampleBy) {
            appendColumn(sql, alias, tsColumn);
            sql.put(", ");
        }
        for (int i = 0; i < keyCount; i++) {
            appendColumn(sql, alias, keys.getQuick(i).getName());
            sql.put(" AS e").put(i).put(", ");
        }
        if (groupingIdArgs.size() > 0) {
            sql.put("grouping_id(");
            for (int i = 0, n = groupingIdArgs.size(); i < n; i++) {
                if (i > 0) {
                    sql.put(", ");
                }
                appendColumn(sql, alias, keys.getQuick(groupingIdArgs.getQuick(i)).getName());
            }
            sql.put(") AS g0, ");
        }
        sql.put(aggregate).put(" AS a0").put(from).put(where);
        sql.put(isSampleBy ? " SAMPLE BY " + stride + " " : " GROUP BY ");
        appendGrouping(sql, form, hasPlainKey, sets, keys, alias);

        final StringSink reference = new StringSink();
        reference.put(source.getPrefixSql());
        for (int s = 0, sn = sets.size(); s < sn; s++) {
            if (s > 0) {
                reference.put(" UNION ALL ");
            }
            appendReferenceBranch(reference, sets.getQuick(s), keys, groupingIdArgs, aggregate, alias, from, where, tsColumn, stride);
        }
        return new GeneratedQuery(sql.toString(), true).withReference(reference.toString());
    }

    private static void appendAggregate(StringSink sink, Rnd rnd, ObjList<FuzzColumn> columns, String alias) {
        final FuzzColumn column = columns.getQuick(rnd.nextInt(columns.size()));
        final ColumnKind kind = column.getType().getKind();
        switch (rnd.nextInt(4)) {
            case 0 -> sink.put("count()");
            case 1 -> {
                sink.put("count(");
                appendColumn(sink, alias, column.getName());
                sink.put(')');
            }
            case 2 -> {
                if (kind == ColumnKind.NUMERIC) {
                    sink.put(rnd.nextBoolean() ? "sum(" : "avg(");
                    appendColumn(sink, alias, column.getName());
                    sink.put(')');
                } else {
                    sink.put("count()");
                }
            }
            default -> {
                if (kind.isOrderable()) {
                    sink.put(rnd.nextBoolean() ? "min(" : "max(");
                    appendColumn(sink, alias, column.getName());
                    sink.put(')');
                } else {
                    sink.put("count()");
                }
            }
        }
    }

    private static void appendColumn(StringSink sink, String alias, String column) {
        if (alias != null) {
            sink.put(alias).put('.');
        }
        sink.put(column);
    }

    private static void appendGrouping(StringSink sink, int form, boolean hasPlainKey, IntList sets, ObjList<FuzzColumn> keys, String alias) {
        if (form == FORM_GROUPING_SETS) {
            sink.put("GROUPING SETS (");
            for (int s = 0, sn = sets.size(); s < sn; s++) {
                if (s > 0) {
                    sink.put(", ");
                }
                sink.put('(');
                boolean isFirst = true;
                for (int k = 0, kn = keys.size(); k < kn; k++) {
                    if ((sets.getQuick(s) & (1 << k)) != 0) {
                        if (!isFirst) {
                            sink.put(", ");
                        }
                        isFirst = false;
                        appendColumn(sink, alias, keys.getQuick(k).getName());
                    }
                }
                sink.put(')');
            }
            sink.put(')');
            return;
        }
        int first = 0;
        if (hasPlainKey) {
            appendColumn(sink, alias, keys.getQuick(0).getName());
            sink.put(", ");
            first = 1;
        }
        sink.put(form == FORM_ROLLUP ? "ROLLUP(" : "CUBE(");
        for (int k = first, kn = keys.size(); k < kn; k++) {
            if (k > first) {
                sink.put(", ");
            }
            appendColumn(sink, alias, keys.getQuick(k).getName());
        }
        sink.put(')');
    }

    private static void appendReferenceBranch(
            StringSink sink,
            int set,
            ObjList<FuzzColumn> keys,
            IntList groupingIdArgs,
            CharSequence aggregate,
            String alias,
            CharSequence from,
            CharSequence where,
            String tsColumn,
            String stride
    ) {
        // The empty set without SAMPLE BY is one row even over no input, which a keyed GROUP BY
        // does not give: aggregate in a non-keyed subquery and add the NULL keys around it. The
        // subquery also counts rows, because min() or max() of the designated timestamp alone
        // returns no row at all over no input.
        final boolean isTotal = set == 0 && stride == null;
        sink.put("SELECT ");
        if (stride != null) {
            sink.put("timestamp_floor('").put(stride).put("', ");
            appendColumn(sink, alias, tsColumn);
            sink.put(") AS s0, ");
        }
        for (int k = 0, kn = keys.size(); k < kn; k++) {
            if ((set & (1 << k)) != 0) {
                appendColumn(sink, alias, keys.getQuick(k).getName());
            } else {
                sink.put("NULL::").put(keys.getQuick(k).getType().getDdl());
            }
            sink.put(" AS e").put(k).put(", ");
        }
        final int argCount = groupingIdArgs.size();
        if (argCount > 0) {
            int groupingId = 0;
            for (int a = 0; a < argCount; a++) {
                if ((set & (1 << groupingIdArgs.getQuick(a))) == 0) {
                    groupingId |= 1 << (argCount - 1 - a);
                }
            }
            sink.put(groupingId).put(" AS g0, ");
        }
        if (isTotal) {
            sink.put("a0 FROM (SELECT ").put(aggregate).put(" AS a0, count() AS n0").put(from).put(where).put(')');
        } else {
            sink.put(aggregate).put(" AS a0").put(from).put(where);
        }
    }

    private static ObjList<FuzzColumn> keyCandidates(ObjList<FuzzColumn> columns, String excluded) {
        final ObjList<FuzzColumn> candidates = new ObjList<>();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final FuzzColumn column = columns.getQuick(i);
            if (column.getType().getKind().isGroupable() && !column.getName().equals(excluded)) {
                candidates.add(column);
            }
        }
        return candidates;
    }
}
