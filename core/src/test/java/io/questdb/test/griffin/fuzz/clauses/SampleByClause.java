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

import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.griffin.fuzz.FuzzNames;
import io.questdb.test.griffin.fuzz.FuzzSource;
import io.questdb.test.griffin.fuzz.FuzzTable;
import io.questdb.test.griffin.fuzz.GeneratedQuery;
import io.questdb.test.griffin.fuzz.PredicateGenerator;
import io.questdb.test.griffin.fuzz.expr.ExpressionGenerator;
import io.questdb.test.griffin.fuzz.expr.FuzzExpr;
import io.questdb.test.griffin.fuzz.types.ColumnKind;

/**
 * SELECT [keyExpr,] aggExpr, ts FROM t [WHERE p]
 *   SAMPLE BY N[s|m|h|d] [FILL(mode)] [ALIGN TO CALENDAR | FIRST OBSERVATION]
 * <p>
 * Key slot and aggregate argument are {@link FuzzExpr}s. Table alias is
 * skipped here because SAMPLE BY interacts oddly with a qualified
 * reference to the designated timestamp in some QuestDB paths; column
 * aliases are still randomized.
 */
public final class SampleByClause {

    // 1-second buckets combined with FILL over a multi-day table explode
    // into ~260k rows that overflow the ORDER BY sort buffer; 30s is the
    // smallest interval that stays comfortably inside the cap.
    // The fuzzer tables span 30..75 hours of data (FuzzConfig stepMicros * rowsPerTable).
    // SAMPLE BY with FILL(PREV/NULL/LINEAR) emits one row per (key, bucket) -- multiplying
    // a tight bucket interval, a wide span, and a high-cardinality key produces an output
    // row count that overruns the 128-page sort budget the test config uses (Overrides
    // sets cairo.sql.sort.key.max.pages=128). That manifests as LimitOverflowException
    // on otherwise legal SAMPLE BY queries. The smallest practical interval that fits
    // 75h of data plus typical fuzzer key cardinality inside 128 * 128KB of sort memory
    // is 5m -- below that, the LimitOverflow rate becomes noise dominating real bugs.
    private static final String[] INTERVALS = {"5m", "15m", "1h", "1d"};

    private SampleByClause() {
    }

    public static GeneratedQuery generate(Rnd rnd, FuzzSource source) {
        FuzzTable table = source.getTable();
        boolean useColAliases = rnd.nextBoolean();
        ExpressionGenerator exprGen = new ExpressionGenerator(rnd, table.getColumns(), null, 2);
        // Alias slot names in SELECT order. When aliases are off the
        // slots are unnamed, but their 1-based position is still a valid
        // ORDER BY target, so we always keep track of count.
        String[] aliasNames = new String[3];
        int slots = 0;

        StringSink sql = new StringSink();
        sql.put(source.getPrefixSql());
        sql.put("SELECT ");
        if (rnd.nextBoolean()) {
            FuzzExpr key = exprGen.generateOfKind(pickGroupableKind(rnd));
            key.appendSql(sql);
            if (useColAliases) {
                sql.put(" AS k_a");
                aliasNames[slots] = "k_a";
            }
            slots++;
            sql.put(", ");
        }
        if (rnd.nextBoolean()) {
            sql.put(rnd.nextBoolean() ? "avg(" : "sum(");
            exprGen.generateOfKind(ColumnKind.NUMERIC).appendSql(sql);
            sql.put(')');
        } else {
            sql.put("count()");
        }
        if (useColAliases) {
            sql.put(" AS agg_a");
            aliasNames[slots] = "agg_a";
        }
        slots++;
        sql.put(", ").put(FuzzNames.column(rnd, table.getTsColumnName()));
        if (useColAliases) {
            sql.put(" AS ts_a");
            aliasNames[slots] = "ts_a";
        }
        slots++;

        sql.put(" FROM ").put(source.getFromSqlWithGarble(rnd));

        if (rnd.nextBoolean()) {
            String pred = new PredicateGenerator(rnd, 1).generate(table.getColumns(), null);
            sql.put(" WHERE ").put(pred);
        }

        sql.put(" SAMPLE BY ").put(INTERVALS[rnd.nextInt(INTERVALS.length)]);

        // FILL only applies to a few modes and breaks FROM-TO. Keep simple.
        int fillMode = rnd.nextInt(5);
        switch (fillMode) {
            case 0:
                sql.put(" FILL(NULL)");
                break;
            case 1:
                sql.put(" FILL(PREV)");
                break;
            case 2:
                sql.put(" FILL(LINEAR)");
                break;
            case 3:
                sql.put(" FILL(0)");
                break;
            default:
                // no FILL
        }

        if (rnd.nextBoolean()) {
            sql.put(" ALIGN TO CALENDAR");
        } else if (rnd.nextInt(4) == 0) {
            sql.put(" ALIGN TO FIRST OBSERVATION");
        }

        if (rnd.nextBoolean()) {
            int picks = 1 + rnd.nextInt(Math.min(2, slots));
            sql.put(" ORDER BY ");
            for (int i = 0; i < picks; i++) {
                if (i > 0) {
                    sql.put(", ");
                }
                int idx = rnd.nextInt(slots);
                // Prefer alias names when they exist, otherwise fall back
                // to 1-based positional reference; both are valid in SQL.
                if (aliasNames[idx] != null && rnd.nextBoolean()) {
                    sql.put(aliasNames[idx]);
                } else {
                    sql.put(idx + 1);
                }
                if (rnd.nextBoolean()) {
                    sql.put(rnd.nextBoolean() ? " ASC" : " DESC");
                }
            }
        }

        // SampleByClause never emits LIMIT, so the result multiset is stable
        // across runs.
        return new GeneratedQuery(sql.toString(), true);
    }

    private static ColumnKind pickGroupableKind(Rnd rnd) {
        ColumnKind[] options = {
                ColumnKind.STRING_LIKE, ColumnKind.NUMERIC, ColumnKind.BOOLEAN,
                ColumnKind.CHAR, ColumnKind.IDENTIFIER, ColumnKind.DECIMAL
        };
        return options[rnd.nextInt(options.length)];
    }
}
