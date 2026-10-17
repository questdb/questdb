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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Fuzzes the filters after a RIGHT or FULL join in a correlated LATERAL body, see
 * {@link LateralJoinNullRejectionTest}, on random data: NULL and duplicate outer keys, random
 * typed values, random combinations of filters on trades and on refunds, random bind variable
 * values, and scalar sub-queries with a WHERE of their own. Each query must return the rows that
 * the body returns per outer row, or fail with the LateralJoinRewriter error that rejects the
 * filter. A body that fails on its own must fail because of its filter, so the filter must fail on
 * the table alone, and the query must fail too.
 * <p>
 * The rewriter may reject a query only when it cannot prove that the filter drops every
 * NULL-extended row. A query without OR whose filter holds a comparison, IN, BETWEEN or IS [NOT]
 * NULL that returns no row from null_trades, which holds the NULL-extended row, must therefore
 * return rows: with constant operands, and with a bind variable or a scalar sub-query under the
 * rules in {@link LateralJoinNullRejectionTest#testBindVariableValuesShareThePlan()}. The plan checks
 * the values of all such runtime operands of a filter, which drops the NULL-extended rows while any of
 * them is not NULL, see {@link LateralJoinNullRejectionTest#testRuntimeNullCheckOfAnyConjunct()}, so
 * the query must return rows when any of them drops the NULL-extended row.
 * <p>
 * Some shapes put a correlated sub-query on the slave side of the RIGHT join, which the rewriter
 * repairs, with no later join or with a later INNER, LEFT, CROSS, RIGHT or FULL join.
 */
public class LateralJoinNullRejectionFuzzTest extends AbstractCairoTest {
    private static final String ERROR_SUFFIX = "is not supported in a correlated lateral sub-query";
    // name, type, values of the column and constants of the filters, bind variable type and values
    private static final FuzzColumn[] COLUMNS = {
            new FuzzColumn("bo", "BOOLEAN", new String[]{"true", "false"}, new String[]{"true", "false", "'false'", "NULL"}, null, null),
            new FuzzColumn("bt", "BYTE", new String[]{"0", "1", "10", "-1"}, new String[]{"0", "1", "10", "-1", "5", "NULL", "5 + 5"}, null, null),
            new FuzzColumn("sh", "SHORT", new String[]{"0", "1", "10", "-1"}, new String[]{"0", "1", "10", "-1", "-5", "NULL", "10::short"}, "short", new Object[]{(short) 0, (short) 1, (short) 10}),
            new FuzzColumn("ch", "CHAR", new String[]{"'a'", "'b'", "'z'"}, new String[]{"'a'", "'b'", "'z'", "''", "NULL"}, null, null),
            new FuzzColumn("i", "INT", new String[]{"0", "1", "10", "-5"}, new String[]{"0", "1", "10", "-5", "NULL", "NULL::int", "-2_147_483_648", "5 + 5"}, "int", new Object[]{null, 0, 1, 10}),
            new FuzzColumn("l", "LONG", new String[]{"0", "1", "10", "-5"}, new String[]{"0", "1", "10", "-5", "NULL", "NULL::long", "CAST(NULL AS LONG)"}, "long", new Object[]{null, 0L, 10L}),
            new FuzzColumn("fl", "FLOAT", new String[]{"0.0", "1.5", "10.0", "'NaN'", "'Infinity'"}, new String[]{"0.0", "1.5", "10.0", "NaN", "'NaN'", "'Infinity'", "'-Infinity'", "NULL", "NULL::float"}, null, null),
            new FuzzColumn("d", "DOUBLE", new String[]{"0.0", "1.5", "10.0", "NaN", "'Infinity'", "'-Infinity'"}, new String[]{"0.0", "1.5", "10.0", "NaN", "'NaN'", "'Infinity'", "1e400", "NULL", "NULL::double", "-1.5"}, "double", new Object[]{null, 0.0, 10.0, Double.POSITIVE_INFINITY}),
            new FuzzColumn("dc", "DECIMAL(18,2)", new String[]{"0::DECIMAL(18,2)", "10::DECIMAL(18,2)", "-1.5::DECIMAL(18,2)"}, new String[]{"0", "10", "-1", "NULL", "10::DECIMAL(18,2)"}, null, null),
            new FuzzColumn("dt", "DATE", new String[]{"'2024-01-01'", "'1970-01-01'"}, new String[]{"'2024-01-01'", "'1970-01-01'", "''", "NULL", "NULL::date"}, null, null),
            new FuzzColumn("t2", "TIMESTAMP", new String[]{"'2024-01-01T00:00:00.000000Z'", "0::timestamp"}, new String[]{"'2024-01-01T00:00:00.000000Z'", "0::timestamp", "''", "NULL", "NULL::timestamp"}, "timestamp", new Object[]{null, 0L, 1_704_067_200_000_000L}),
            new FuzzColumn("st", "STRING", new String[]{"''", "'a'", "'abc'"}, new String[]{"''", "'a'", "'abc'", "NULL", "NULL::string"}, "str", new Object[]{null, "", "abc"}),
            new FuzzColumn("vc", "VARCHAR", new String[]{"''", "'a'", "'abc'"}, new String[]{"''", "'a'", "'abc'", "NULL"}, "str", new Object[]{null, "a"}),
            new FuzzColumn("sy", "SYMBOL", new String[]{"''", "'a'", "'abc'"}, new String[]{"''", "'a'", "'abc'", "NULL"}, "str", new Object[]{null, "abc"}),
            new FuzzColumn("ip", "IPv4", new String[]{"'0.0.0.1'", "'1.1.1.1'", "'2.2.2.2'"}, new String[]{"'0.0.0.0'", "'1.1.1.1'", "'2.2.2.2'", "NULL"}, null, null),
            new FuzzColumn("u", "UUID", new String[]{"'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"}, new String[]{"'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "'00000000-0000-0000-0000-000000000000'", "NULL"}, null, null),
            new FuzzColumn("h", "LONG256", new String[]{"0x01", "0x05"}, new String[]{"0x01", "0x00", "0x05", "NULL"}, null, null),
            new FuzzColumn("g1", "GEOHASH(1c)", new String[]{"#u", "#s"}, new String[]{"#u", "'u'", "NULL"}, null, null),
            new FuzzColumn("g2", "GEOHASH(2c)", new String[]{"#u3", "#s3"}, new String[]{"#u3", "'u3'", "NULL"}, null, null),
            new FuzzColumn("g4", "GEOHASH(4c)", new String[]{"#u33d", "#s33d"}, new String[]{"#u33d", "'u33d'", "NULL"}, null, null),
            new FuzzColumn("g8", "GEOHASH(8c)", new String[]{"#u33dc0cp", "#s33dc0cp"}, new String[]{"#u33dc0cp", "'u33dc0cp'", "NULL"}, null, null),
            new FuzzColumn("tn", "TIMESTAMP_NS", new String[]{"'2024-01-01T00:00:00.000000000Z'", "0::timestamp_ns"}, new String[]{"'2024-01-01'", "'2023-01-01'", "''", "NULL", "NULL::timestamp_ns"}, "timestamp_ns", new Object[]{null, 0L, 1_704_067_200_000_000_000L}),
            new FuzzColumn("d8", "DECIMAL(2,0)", new String[]{"0::DECIMAL(2,0)", "10::DECIMAL(2,0)", "-1::DECIMAL(2,0)"}, new String[]{"0", "10", "-1", "NULL"}, null, null),
            new FuzzColumn("d16", "DECIMAL(4,1)", new String[]{"0::DECIMAL(4,1)", "10::DECIMAL(4,1)", "-1.5::DECIMAL(4,1)"}, new String[]{"0", "10", "-1", "NULL"}, null, null),
            new FuzzColumn("d32", "DECIMAL(9,2)", new String[]{"0::DECIMAL(9,2)", "10::DECIMAL(9,2)", "-1.5::DECIMAL(9,2)"}, new String[]{"0", "10", "-1", "NULL"}, null, null),
            new FuzzColumn("d128", "DECIMAL(38,4)", new String[]{"0::DECIMAL(38,4)", "10::DECIMAL(38,4)", "-1.5::DECIMAL(38,4)"}, new String[]{"0", "10", "-1", "NULL"}, null, null),
            new FuzzColumn("d256", "DECIMAL(60,4)", new String[]{"0::DECIMAL(60,4)", "10::DECIMAL(60,4)", "-1.5::DECIMAL(60,4)"}, new String[]{"0", "10", "-1", "NULL"}, null, null),
            new FuzzColumn("arr", "DOUBLE[]", new String[]{"ARRAY[1.0, 2.0]", "ARRAY[3.0]"}, new String[]{"ARRAY[1.0, 2.0]", "NULL"}, null, null),
    };
    private static final String[] COMPARISONS = {"=", "!=", "<", "<=", ">", ">="};
    // correlated conjuncts: terminateHere() moves some into the outer-ref join's ON clause, and
    // t.x = o.k lets the rewriter remove the outer-ref join
    private static final String[] CORRELATED = {"t.id > {K}", "t.id >= {K}", "t.x = {K}", "t.id != {K}"};
    private static final String[] LATERAL_JOINS = {"JOIN LATERAL", "CROSS JOIN LATERAL", "LEFT JOIN LATERAL"};
    // the start of the CASE of nestedScalarSubQuery()
    private static final String NESTED_SCALAR_SUB_QUERY = "CASE WHEN (SELECT count() FROM";
    private static final int QUERIES_PER_ROUND = 40;
    // bind variable values of a filter on refunds, whose ids start at 100
    private static final Object[] REFUND_BIND_VALUES = {null, 0, 101};
    private static final String[] REFUND_CONSTANTS = {"0", "1", "2", "101", "NULL"};
    // a filter on trades that drops the row of null_trades drops the rows that either join
    // NULL-extends, as both hold NULL trades; a filter on refunds counts for the join with xs only,
    // and the oracle does not count it
    private static final Shape TWO_RIGHT_JOINS = new Shape("{T} t RIGHT JOIN refunds r ON r.k = {K} RIGHT JOIN xs x ON x.k = r.k WHERE {P}", false, false);
    // The ON clause of xs reads the outer key, and WHERE keys it on refunds, which the RIGHT join
    // joins after xs, so no row before the RIGHT join holds the key
    private static final Shape SLAVE_KEY_READ_EARLIER = new Shape("{T} t JOIN xs x ON x.k = {K} RIGHT JOIN refunds r ON t.x = r.k WHERE r.k = {K} AND ({P})", true, false);
    // A FULL join whose ON clause reads the outer key on refunds also NULL-extends refunds, and no
    // filter on trades drops those rows, so the rewriter rejects every such query.
    private static final Shape[] SHAPES = {
            new Shape("{T} t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K} WHERE {P}", false, false),
            new Shape("{T} t RIGHT JOIN refunds r ON r.k = {K} AND t.x = r.k WHERE {P}", false, false),
            new Shape("{T} t JOIN xs x ON x.k = {K} RIGHT JOIN refunds r ON t.x = r.k WHERE {P}", false, false),
            new Shape("{T} t JOIN xs x ON x.k = t.x AND x.k = {K} RIGHT JOIN refunds r ON t.x = r.k WHERE {P}", false, false),
            new Shape("{T} t JOIN xs x ON x.k = {K} FULL JOIN refunds r ON t.x = r.k WHERE {P}", false, false),
            new Shape("{T} t RIGHT JOIN refunds r ON t.x = r.k WHERE t.x = {K} AND {P}", true, false),
            new Shape("{T} t FULL JOIN refunds r ON t.x = r.k AND r.k = {K} WHERE {P}", false, true),
            // only the order as written keeps trades ahead of a join whose ON clause reads no trades column
            new Shape("{T} t RIGHT JOIN refunds r ON r.k = {K} WHERE {P}", false, false),
            new Shape("{T} t FULL JOIN refunds r ON r.k = {K} WHERE {P}", false, true),
            TWO_RIGHT_JOINS,
            SLAVE_KEY_READ_EARLIER,
            // A correlated sub-query as the slave of a RIGHT join, which the rewriter repairs when no
            // RIGHT, FULL or SPLICE join follows and the filter does not drop the rows that the join
            // NULL-extends, see LateralJoinRewriter.checkOuterJoinsAfterOuterRef(). A later RIGHT or
            // FULL join may make it reject the query. ({P}) keeps an OR of the filter from splitting
            // off the WHERE key.
            correlatedSlaveShape("", false),
            correlatedSlaveShape(" JOIN xs x ON x.k = r.k", true),
            correlatedSlaveShape(" LEFT JOIN xs x ON x.k = r.k", true),
            correlatedSlaveShape(" CROSS JOIN xs x", true),
            correlatedSlaveShape(" RIGHT JOIN xs x ON x.k = r.k", false),
            correlatedSlaveShape(" FULL JOIN xs x ON x.k = r.k", false)
    };
    // queries whose LATERAL rows were compared with the reference rows
    private int comparedCount;
    // queries of a shape whose repaired RIGHT join a later INNER, LEFT or CROSS join follows, whose
    // filter may keep the rows that the RIGHT join NULL-extends, and whose LATERAL rows were compared
    // with the reference rows
    private int laterJoinRepairComparedCount;
    // queries that must return rows only because a runtime-checked predicate after the first one of
    // the filter drops every NULL-extended row
    private int laterRuntimeVerdictCount;
    // queries over a mixed projection, see mixedProjection(), that returned rows and must, whose
    // filter has a runtime-checked predicate on a column of known type and whose verdict needs a
    // predicate on a computed column
    private int mixedTypeVerdictCount;
    // queries with a CASE over a scalar sub-query, see nestedScalarSubQuery(), whose LATERAL rows were
    // compared with the reference rows
    private int nestedSubQueryComparedCount;
    // queries that must return rows, as their filter provably drops every NULL-extended row
    private int nullRejectingCount;
    // queries with a valid filter that the rewriter rejected
    private int rejectedCount;
    // queries of the SLAVE_KEY_READ_EARLIER shape whose LATERAL rows were compared with the reference rows
    private int slaveKeyReadEarlierComparedCount;
    // queries of the TWO_RIGHT_JOINS shape whose LATERAL rows were compared with the reference rows
    private int twoRightJoinsComparedCount;
    // queries whose filter is valid, so the reference ran and the query was checked against it
    private int validCount;
    // queries of a shape with WHERE t.x = o.k and another condition on the outer key, whose LATERAL
    // rows were compared with the reference rows
    private int whereKeyCorrelatedComparedCount;

    @Test
    public void testFuzzDenseNulls() throws Exception {
        fuzz(12, 0.5, 6, 8);
    }

    @Test
    public void testFuzzFewRows() throws Exception {
        fuzz(18, 0.3, 2, 3);
    }

    @Test
    public void testFuzzSparseNulls() throws Exception {
        fuzz(18, 0.1, 6, 10);
    }

    // = against $1, which may be NULL, and then <= or >= against a scalar sub-query, mostly the
    // maximum of a column of a numeric or time type, which is NULL only when every trade holds NULL,
    // or against a CASE over a scalar sub-query, see nestedScalarSubQuery(). A NULL $1 makes = true
    // on the NULL-extended row, and the query must then return rows when the sub-query drops that row.
    private static void addRuntimeCheckedPair(Rnd rnd, Bind bind, List<Predicate> predicates) {
        FuzzColumn column;
        do {
            column = COLUMNS[rnd.nextInt(COLUMNS.length)];
        } while (column.bindType == null || isTypeWithoutNull(column));
        bind.type = column.bindType;
        bind.value = rnd.nextBoolean() ? null : column.bindValues[rnd.nextInt(column.bindValues.length)];
        predicates.add(new Predicate("t." + column.name + " = $1", PredicateKind.RUNTIME_CHECKED_EQUALITY, column.name));
        do {
            column = COLUMNS[rnd.nextInt(COLUMNS.length)];
        } while (column.bindType == null || column.bindType.equals("str") || isTypeWithoutNull(column));
        final String ref = "t." + column.name;
        final String op = rnd.nextBoolean() ? "<=" : ">=";
        if (rnd.nextInt(4) == 0) {
            predicates.add(new Predicate(ref + ' ' + op + ' ' + nestedScalarSubQuery(rnd, column), PredicateKind.RUNTIME_CHECKED, column.name));
            return;
        }
        final String subQuery = rnd.nextInt(4) > 0 ? "(SELECT max(" + column.name + ") FROM trades)" : randomScalarSubQuery(rnd, column);
        predicates.add(new Predicate(rnd.nextBoolean() ? ref + ' ' + op + ' ' + subQuery : subQuery + ' ' + op + ' ' + ref, PredicateKind.RUNTIME_CHECKED, column.name));
    }

    // The body may fail only because of its filter, e.g. a filter that does not apply to the column
    // type. Then the filter fails on the table alone as well, and the query, which runs the same
    // filter, fails too. A filter that fails only while it runs, e.g. on a scalar sub-query that
    // returns more than one row, fails only in a plan that runs it.
    private static void assertFilterFails(
            String filter,
            String reference,
            SqlException referenceError,
            String sql,
            String bindSuffix,
            List<String> failures
    ) {
        final FilterOutcome outcome = runOnTableAlone(filter, bindSuffix);
        if (outcome == FilterOutcome.RUNS) {
            failures.add("FAILED although its filter runs on the table alone, [" + referenceError.getPosition() + "] "
                    + referenceError.getFlyweightMessage() + bindSuffix + "\n    " + reference);
            return;
        }
        try {
            sink.clear();
            printSql(sql);
            if (outcome == FilterOutcome.COMPILE_ERROR) {
                failures.add("RETURNED ROWS although its filter does not compile, [" + referenceError.getPosition() + "] "
                        + referenceError.getFlyweightMessage() + bindSuffix + "\n    " + sql);
            }
        } catch (SqlException ignore) {
            // fails as well
        } catch (Throwable e) {
            throw new AssertionError("failed with " + e + bindSuffix + "\n    " + sql, e);
        }
    }

    // trades RIGHT JOIN a correlated sub-query of refunds, then laterJoin, with the outer key in WHERE;
    // hasLaterJoinAfterRepair tells that laterJoin is an INNER, LEFT or CROSS join
    private static Shape correlatedSlaveShape(String laterJoin, boolean hasLaterJoinAfterRepair) {
        return new Shape(
                "{T} t RIGHT JOIN (SELECT * FROM refunds WHERE k = {K}) r ON t.x = r.k" + laterJoin + " WHERE t.x = {K} AND ({P})",
                true,
                false,
                true,
                hasLaterJoinAfterRepair
        );
    }

    // True when the filter holds a predicate against a value known only when the query runs, which
    // the rewriter counts, on a column whose type it knows, see LateralJoinRewriter.runtimeNullCheckOperand()
    private static boolean hasCountedRuntimePredicateOnKnownType(
            List<Predicate> predicates,
            Set<String> computedColumns,
            boolean isOuterRefJoinKept
    ) {
        for (int i = 0, n = predicates.size(); i < n; i++) {
            final Predicate predicate = predicates.get(i);
            if (predicate.column != null
                    && !computedColumns.contains(predicate.column)
                    && (predicate.kind == PredicateKind.RUNTIME_CHECKED
                    || (predicate.kind == PredicateKind.RUNTIME_CHECKED_EQUALITY && isOuterRefJoinKept))) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasNestedScalarSubQuery(CharSequence filter) {
        return Chars.contains(filter, NESTED_SCALAR_SUB_QUERY);
    }

    private static boolean isEqualityOnlyType(FuzzColumn column) {
        return column.type.startsWith("GEOHASH") || column.type.endsWith("[]");
    }

    // True when no row from null_trades passes the predicate, so it drops every NULL-extended row
    private static boolean isFalseOnNullExtendedRow(Predicate predicate) {
        try {
            sink.clear();
            printSql("SELECT count() FROM null_trades t WHERE " + predicate.sql);
            return rowsOf(sink).getFirst().equals("0");
        } catch (SqlException e) {
            // e.g. a scalar sub-query that returns more than one row
            return false;
        }
    }

    private static boolean isTypeWithoutNull(FuzzColumn column) {
        return switch (column.type) {
            case "BOOLEAN", "BYTE", "SHORT" -> true;
            default -> false;
        };
    }

    // passes each column through or computes it with a cast to its own type, and adds the names of
    // the computed columns, whose types the rewriter does not know, to computedColumns. It passes
    // knownColumn through and computes computedColumn, either of which may be null, and decides on
    // the other columns at random.
    private static String mixedProjection(Rnd rnd, Set<String> computedColumns, String knownColumn, String computedColumn) {
        final StringBuilder sb = new StringBuilder("(SELECT id, x");
        for (FuzzColumn column : COLUMNS) {
            sb.append(", ");
            if (!column.name.equals(knownColumn) && (column.name.equals(computedColumn) || rnd.nextBoolean())) {
                sb.append("CAST(").append(column.name).append(" AS ").append(column.type).append(") ");
                computedColumns.add(column.name);
            }
            sb.append(column.name);
        }
        return sb.append(" FROM trades)").toString();
    }

    // A CASE that reads a scalar sub-query below its top and takes one value per execution: a value
    // of the column while some trade has the key, and NULL otherwise. The check of the CASE runs the
    // sub-query, and the filter reads its value.
    private static String nestedScalarSubQuery(Rnd rnd, FuzzColumn column) {
        return NESTED_SCALAR_SUB_QUERY + " trades WHERE x = " + rnd.nextInt(4) + ") > 0 THEN CAST("
                + randomValue(rnd, column, 0) + " AS " + column.type + ") END";
    }

    // Decides whether the filter provably drops every row that the join NULL-extends on trades, so
    // the rewriter must accept the query: the filter has no OR and a predicate in a form that the
    // rewriter proves is false on the NULL-extended row. The plan checks the values of <= and >=,
    // and of = while the outer-ref join stays, against a bind variable or a scalar sub-query once
    // per execution. The predicates are ANDed, so any one of them whose value makes it false on the
    // NULL-extended row drops that row, and the query fails only when none does.
    private static Verdict nullRejectionVerdict(Shape shape, boolean hasOr, boolean isOuterRefJoinKept, List<Predicate> predicates) {
        if (hasOr || shape.hasKeyOnFullJoinedTable) {
            return Verdict.UNPROVEN;
        }
        boolean isProvable = false;
        boolean isFirstRuntimeRejecting = false;
        boolean isLaterRuntimeRejecting = false;
        int runtimeCount = 0;
        for (int i = 0, n = predicates.size(); i < n; i++) {
            final Predicate predicate = predicates.get(i);
            switch (predicate.kind) {
                case PROVABLE -> isProvable |= isFalseOnNullExtendedRow(predicate);
                case RUNTIME_CHECKED, RUNTIME_CHECKED_EQUALITY -> {
                    if (predicate.kind == PredicateKind.RUNTIME_CHECKED || isOuterRefJoinKept) {
                        final boolean isRejecting = isFalseOnNullExtendedRow(predicate);
                        if (runtimeCount++ == 0) {
                            isFirstRuntimeRejecting = isRejecting;
                        } else {
                            isLaterRuntimeRejecting |= isRejecting;
                        }
                    }
                }
                default -> {
                }
            }
        }
        if (isProvable || isFirstRuntimeRejecting) {
            return Verdict.PROVEN;
        }
        return isLaterRuntimeRejecting ? Verdict.PROVEN_BY_LATER_RUNTIME_PREDICATE : Verdict.UNPROVEN;
    }

    // the predicates on refunds and on the columns that the table passes through
    private static List<Predicate> onKnownTypes(List<Predicate> predicates, Set<String> computedColumns) {
        final List<Predicate> result = new ArrayList<>();
        for (int i = 0, n = predicates.size(); i < n; i++) {
            final Predicate predicate = predicates.get(i);
            if (predicate.column == null || !computedColumns.contains(predicate.column)) {
                result.add(predicate);
            }
        }
        return result;
    }

    // passes every column through, or computes it with a cast to its own type, whose type the
    // rewriter does not know
    private static String projection(boolean isCast) {
        final StringBuilder sb = new StringBuilder("(SELECT id, x");
        for (FuzzColumn column : COLUMNS) {
            sb.append(", ");
            if (isCast) {
                sb.append("CAST(").append(column.name).append(" AS ").append(column.type).append(") ");
            }
            sb.append(column.name);
        }
        return sb.append(" FROM trades)").toString();
    }

    private static String randomValue(Rnd rnd, FuzzColumn column, double nullRate) {
        return rnd.nextDouble() < nullRate ? "NULL" : column.values[rnd.nextInt(column.values.length)];
    }

    private static String randomKey(Rnd rnd, double nullRate) {
        return rnd.nextDouble() < nullRate ? "NULL" : Integer.toString(rnd.nextInt(4));
    }

    // A filter on refunds, which a RIGHT join keeps whole, drops no row that the join NULL-extends on
    // trades, so the rewriter must not count it, and the rows-or-error check decides the query.
    private static Predicate randomRefundsPredicate(Rnd rnd, Bind bind) {
        final String ref = rnd.nextBoolean() ? "r.id" : "r.k";
        if (rnd.nextInt(3) == 0) {
            final StringBuilder sb = new StringBuilder(ref).append(" IN (");
            for (int i = 0, n = 1 + rnd.nextInt(3); i < n; i++) {
                sb.append(i > 0 ? ", " : "").append(REFUND_CONSTANTS[rnd.nextInt(REFUND_CONSTANTS.length)]);
            }
            return new Predicate(sb.append(')').toString(), PredicateKind.UNPROVEN, null);
        }
        String operand = REFUND_CONSTANTS[rnd.nextInt(REFUND_CONSTANTS.length)];
        if (bind.type == null && rnd.nextBoolean()) {
            bind.type = "int";
            bind.value = REFUND_BIND_VALUES[rnd.nextInt(REFUND_BIND_VALUES.length)];
            operand = "$1";
        }
        return new Predicate(ref + ' ' + COMPARISONS[rnd.nextInt(COMPARISONS.length)] + ' ' + operand, PredicateKind.UNPROVEN, null);
    }

    // A scalar sub-query that reads the column, which the plan runs once per execution. The code
    // generator generates it for the filter and again for the NULL check of the filter, and both
    // must read the value that the sub-query returns on its own: with its WHERE, its LIMIT, its
    // LATEST ON and the WHERE of a nested sub-query.
    private static String randomScalarSubQuery(Rnd rnd, FuzzColumn column) {
        final String key = Integer.toString(rnd.nextInt(4));
        return switch (rnd.nextInt(5)) {
            case 0 -> "(SELECT " + column.name + " FROM trades WHERE id = " + (10 + rnd.nextInt(10)) + " LIMIT 1)";
            case 1 -> "(SELECT " + column.name + " FROM trades WHERE x = " + key + " ORDER BY ts DESC LIMIT 1)";
            case 2 -> "(SELECT max(" + column.name + ") FROM trades WHERE x = " + key + ")";
            case 3 -> "(SELECT " + column.name + " FROM trades WHERE x = " + key + " LATEST ON ts PARTITION BY x)";
            default ->
                    "(SELECT " + column.name + " FROM trades WHERE id = (SELECT max(id) FROM trades WHERE x = " + key + "))";
        };
    }

    // Half of the queries read a mixed projection, see mixedProjection(), which passes knownColumn
    // through and computes computedColumn, and whose computed columns computedColumns receives. Each
    // run then has queries whose verdict needs a computed column next to a column of known type.
    // Without isUnionAllAllowed, the table is no UNION ALL.
    private static String randomTable(Rnd rnd, Set<String> computedColumns, String knownColumn, String computedColumn, boolean isUnionAllAllowed) {
        if (rnd.nextBoolean()) {
            return mixedProjection(rnd, computedColumns, knownColumn, computedColumn);
        }
        int source = rnd.nextInt(8);
        while (source == 5 && !isUnionAllAllowed) {
            source = rnd.nextInt(8);
        }
        return switch (source) {
            case 0 -> "(SELECT * FROM trades)";
            case 1 -> "(SELECT * FROM (SELECT * FROM trades))";
            case 2 -> "(SELECT DISTINCT * FROM trades)";
            case 3 -> projection(false);
            case 4 -> projection(true);
            case 5 -> "(SELECT * FROM trades UNION ALL SELECT * FROM trades WHERE id < 0)";
            case 6 -> "(SELECT * FROM trades WHERE id IS NOT NULL OR id IS NULL)";
            default -> "trades";
        };
    }

    private static List<String> rowsOf(CharSequence printed) {
        final List<String> rows = new ArrayList<>(Arrays.asList(printed.toString().split("\n")));
        // the header
        rows.removeFirst();
        rows.removeIf(String::isEmpty);
        return rows;
    }

    // A filter on refunds reads r.id or r.k, which the table alone reads as t.id and t.x, the INT
    // columns of trades, so the filter compiles and runs there as it does in the join.
    private static FilterOutcome runOnTableAlone(String filter, String bindSuffix) {
        final String sql = "SELECT id FROM trades t WHERE " + filter.replace("r.id", "t.id").replace("r.k", "t.x");
        try {
            final RecordCursorFactory factory;
            try {
                factory = select(sql);
            } catch (SqlException ignore) {
                return FilterOutcome.COMPILE_ERROR;
            }
            try (factory; RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                while (cursor.hasNext()) {
                    // the filter may fail while it runs
                }
            } catch (SqlException ignore) {
                return FilterOutcome.RUN_ERROR;
            }
            return FilterOutcome.RUNS;
        } catch (Throwable e) {
            throw new AssertionError("failed with " + e + bindSuffix + "\n    " + sql, e);
        }
    }

    private static void setBindVariable(String type, Object value) throws SqlException {
        switch (type) {
            case "short" -> bindVariableService.setShort(0, (Short) value);
            case "int" -> bindVariableService.setInt(0, value == null ? Numbers.INT_NULL : (Integer) value);
            case "long" -> bindVariableService.setLong(0, value == null ? Numbers.LONG_NULL : (Long) value);
            case "double" -> bindVariableService.setDouble(0, value == null ? Double.NaN : (Double) value);
            case "timestamp" -> bindVariableService.setTimestamp(0, value == null ? Numbers.LONG_NULL : (Long) value);
            case "timestamp_ns" ->
                    bindVariableService.setTimestampNano(0, value == null ? Numbers.LONG_NULL : (Long) value);
            default -> bindVariableService.setStr(0, (String) value);
        }
    }

    private void assertRandomQuery(Rnd rnd, List<String> outerRows, List<String> failures) throws SqlException {
        bindVariableService.clear();
        final Bind bind = new Bind();
        final List<Predicate> predicates = new ArrayList<>();
        final boolean isRuntimeCheckedPair = rnd.nextBoolean();
        if (isRuntimeCheckedPair) {
            addRuntimeCheckedPair(rnd, bind, predicates);
        } else {
            predicates.add(randomPredicate(rnd, bind));
        }
        final StringBuilder filter = new StringBuilder(predicates.getFirst().sql);
        for (int i = 1, n = predicates.size(); i < n; i++) {
            filter.append(" AND ").append(predicates.get(i).sql);
        }
        boolean hasOr = false;
        for (int i = 0, n = rnd.nextInt(3); i < n; i++) {
            final boolean isOr = rnd.nextInt(4) == 0;
            hasOr |= isOr;
            final Predicate predicate = randomPredicate(rnd, bind);
            predicates.add(predicate);
            filter.append(isOr ? " OR " : " AND ").append(predicate.sql);
        }
        final Shape shape = SHAPES[rnd.nextInt(SHAPES.length)];
        // t.x = o.k, in the shape or as the correlated conjunct, lets the rewriter remove the
        // outer-ref join, and = against a runtime value then does not count.
        boolean isOuterRefJoinKept = !shape.hasWhereKey;
        boolean hasWhereKeyCorrelated = false;
        if (rnd.nextInt(3) == 0) {
            final String correlated = CORRELATED[rnd.nextInt(CORRELATED.length)];
            isOuterRefJoinKept = !shape.hasWhereKey && !correlated.equals("t.x = {K}");
            hasWhereKeyCorrelated = shape.hasWhereKey && !correlated.equals("t.x = {K}");
            filter.insert(0, correlated + " AND ");
        }
        if (bind.type != null) {
            setBindVariable(bind.type, bind.value);
        }
        final Set<String> computedColumns = new HashSet<>();
        // The rewriter returns wrong rows when the master of the RIGHT join with a correlated
        // sub-query is a UNION ALL, which is a separate bug, so those shapes read no UNION ALL.
        final boolean isUnionAllAllowed = !shape.hasCorrelatedSlave;
        // a mixed projection passes the column of = $1 of the pair through and computes the column
        // of the sub-query comparison, so that the plan needs the latter while $1 is NULL
        final String table = isRuntimeCheckedPair
                ? randomTable(rnd, computedColumns, predicates.get(0).column, predicates.get(1).column, isUnionAllAllowed)
                : randomTable(rnd, computedColumns, null, null, isUnionAllAllowed);
        final String lateralJoin = LATERAL_JOINS[rnd.nextInt(LATERAL_JOINS.length)];
        final String body = shape.body.replace("{T}", table).replace("{P}", filter);
        final String sql = "SELECT o.id, l.tid, l.rid FROM orders o " + lateralJoin
                + " (SELECT t.id tid, r.id rid FROM " + body.replace("{K}", "o.k") + ") l";
        final String bindSuffix = bind.type != null ? " $1=" + bind.value : "";

        // per outer row: the body with the outer key as a literal, run once per key
        final List<String> expected = new ArrayList<>();
        final Map<String, List<String>> bodyRowsByKey = new HashMap<>();
        for (int i = 0, n = outerRows.size(); i < n; i++) {
            final String[] outer = outerRows.get(i).split("\t");
            final String key = outer[1].equals("null") ? "NULL" : outer[1];
            List<String> bodyRows = bodyRowsByKey.get(key);
            if (bodyRows == null) {
                final String reference = "SELECT t.id tid, r.id rid FROM " + body.replace("{K}", key);
                try {
                    sink.clear();
                    printSql(reference);
                } catch (SqlException e) {
                    assertFilterFails(filter.toString().replace("{K}", key), reference, e, sql, bindSuffix, failures);
                    return;
                } catch (Throwable e) {
                    throw new AssertionError("failed with " + e + bindSuffix + "\n    " + reference, e);
                }
                bodyRows = rowsOf(sink);
                bodyRowsByKey.put(key, bodyRows);
            }
            for (int j = 0, m = bodyRows.size(); j < m; j++) {
                expected.add(outer[0] + '\t' + bodyRows.get(j));
            }
            if (bodyRows.isEmpty() && lateralJoin.startsWith("LEFT")) {
                expected.add(outer[0] + "\tnull\tnull");
            }
        }
        Collections.sort(expected);
        validCount++;
        final Verdict verdict = nullRejectionVerdict(shape, hasOr, isOuterRefJoinKept, predicates);
        final boolean isNullRejecting = verdict != Verdict.UNPROVEN;
        if (isNullRejecting) {
            nullRejectingCount++;
        }
        if (verdict == Verdict.PROVEN_BY_LATER_RUNTIME_PREDICATE) {
            laterRuntimeVerdictCount++;
        }
        // the rewriter counts a runtime-checked predicate on a column of known type, and only a
        // predicate on a computed column, whose type only the code generator knows, proves the verdict
        final boolean isMixedTypeVerdict = isNullRejecting
                && !computedColumns.isEmpty()
                && hasCountedRuntimePredicateOnKnownType(predicates, computedColumns, isOuterRefJoinKept)
                && nullRejectionVerdict(shape, hasOr, isOuterRefJoinKept, onKnownTypes(predicates, computedColumns)) == Verdict.UNPROVEN;

        try {
            sink.clear();
            printSql(sql);
            comparedCount++;
            if (shape == TWO_RIGHT_JOINS) {
                twoRightJoinsComparedCount++;
            }
            if (shape == SLAVE_KEY_READ_EARLIER) {
                slaveKeyReadEarlierComparedCount++;
            }
            // the rewriter repairs the RIGHT join only when it cannot prove that the filter drops the
            // rows that the join NULL-extends, see LateralJoinRewriter.checkOuterJoinsAfterOuterRef()
            if (shape.hasLaterJoinAfterRepair && verdict == Verdict.UNPROVEN) {
                laterJoinRepairComparedCount++;
            }
            if (isMixedTypeVerdict) {
                mixedTypeVerdictCount++;
            }
            if (hasNestedScalarSubQuery(filter)) {
                nestedSubQueryComparedCount++;
            }
            if (hasWhereKeyCorrelated) {
                whereKeyCorrelatedComparedCount++;
            }
            final List<String> actual = rowsOf(sink);
            Collections.sort(actual);
            if (!actual.equals(expected)) {
                failures.add("WRONG ROWS " + actual + ", expected " + expected + bindSuffix + "\n    " + sql);
            }
        } catch (SqlException e) {
            if (!Chars.contains(e.getFlyweightMessage(), ERROR_SUFFIX)) {
                failures.add("failed with [" + e.getPosition() + "] " + e.getFlyweightMessage() + bindSuffix + "\n    " + sql);
            } else {
                rejectedCount++;
                if (isNullRejecting) {
                    failures.add("REJECTED although its filter drops every NULL-extended row, [" + e.getPosition() + "] "
                            + e.getFlyweightMessage() + bindSuffix + "\n    " + sql);
                }
            }
        } catch (Throwable e) {
            throw new AssertionError("failed with " + e + bindSuffix + "\n    " + sql, e);
        }
    }

    private void createTables(Rnd rnd, double nullRate, int maxOuterRows, int maxRows) throws SqlException {
        final int outerRowCount = 1 + rnd.nextInt(maxOuterRows);
        final StringBuilder sb = new StringBuilder("INSERT INTO orders VALUES ");
        for (int i = 0; i < outerRowCount; i++) {
            sb.append(i > 0 ? ", (" : "(").append(i + 1).append(", ").append(randomKey(rnd, nullRate)).append(", ").append(i).append("::timestamp)");
        }
        execute("CREATE TABLE orders (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
        execute(sb.toString());

        execute("CREATE TABLE refunds (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
        final int refundCount = rnd.nextInt(maxRows);
        if (refundCount > 0) {
            sb.setLength(0);
            sb.append("INSERT INTO refunds VALUES ");
            for (int i = 0; i < refundCount; i++) {
                sb.append(i > 0 ? ", (" : "(").append(100 + i).append(", ").append(randomKey(rnd, nullRate)).append(", ").append(i).append("::timestamp)");
            }
            execute(sb.toString());
        }

        execute("CREATE TABLE xs (k INT, v INT)");
        final int xsCount = rnd.nextInt(5);
        if (xsCount > 0) {
            sb.setLength(0);
            sb.append("INSERT INTO xs VALUES ");
            for (int i = 0; i < xsCount; i++) {
                sb.append(i > 0 ? ", (" : "(").append(randomKey(rnd, nullRate)).append(", ").append(i).append(')');
            }
            execute(sb.toString());
        }

        sb.setLength(0);
        sb.append("CREATE TABLE trades (id INT, x INT");
        for (FuzzColumn column : COLUMNS) {
            sb.append(", ").append(column.name).append(' ').append(column.type);
        }
        execute(sb.append(", ts TIMESTAMP) TIMESTAMP(ts)").toString());
        final int tradeCount = rnd.nextInt(maxRows);
        if (tradeCount > 0) {
            sb.setLength(0);
            sb.append("INSERT INTO trades VALUES ");
            for (int i = 0; i < tradeCount; i++) {
                sb.append(i > 0 ? ", (" : "(").append(rnd.nextDouble() < nullRate / 4 ? "NULL" : Integer.toString(10 + i))
                        .append(", ").append(randomKey(rnd, nullRate));
                for (FuzzColumn column : COLUMNS) {
                    sb.append(", ").append(randomValue(rnd, column, nullRate));
                }
                sb.append(", ").append(i).append("::timestamp)");
            }
            execute(sb.toString());
        }

        // the row that a RIGHT or FULL join puts in place of a trade: NULL, or 0 or false for BYTE,
        // SHORT and BOOLEAN, which have no NULL
        sb.setLength(0);
        sb.append("CREATE TABLE null_trades (id INT, x INT");
        for (FuzzColumn column : COLUMNS) {
            sb.append(", ").append(column.name).append(' ').append(column.type);
        }
        execute(sb.append(", ts TIMESTAMP) TIMESTAMP(ts)").toString());
        execute("INSERT INTO null_trades (ts) VALUES (0::timestamp)");
    }

    private void fuzz(int rounds, double nullRate, int maxOuterRows, int maxRows) throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final List<String> failures = new ArrayList<>();
            int queryCount = 0;
            validCount = 0;
            comparedCount = 0;
            laterJoinRepairComparedCount = 0;
            rejectedCount = 0;
            slaveKeyReadEarlierComparedCount = 0;
            nullRejectingCount = 0;
            laterRuntimeVerdictCount = 0;
            mixedTypeVerdictCount = 0;
            nestedSubQueryComparedCount = 0;
            twoRightJoinsComparedCount = 0;
            whereKeyCorrelatedComparedCount = 0;
            for (int round = 0; round < rounds; round++) {
                createTables(rnd, nullRate, maxOuterRows, maxRows);
                sink.clear();
                printSql("SELECT count() FROM orders");
                final int outerRowCount = Integer.parseInt(rowsOf(sink).getFirst());
                sink.clear();
                printSql("SELECT id, k FROM orders");
                final List<String> outerRows = rowsOf(sink);
                Assert.assertEquals(outerRowCount, outerRows.size());
                for (int q = 0; q < QUERIES_PER_ROUND; q++) {
                    assertRandomQuery(rnd, outerRows, failures);
                    queryCount++;
                }
                execute("DROP TABLE orders");
                execute("DROP TABLE refunds");
                execute("DROP TABLE xs");
                execute("DROP TABLE trades");
                execute("DROP TABLE null_trades");
            }
            bindVariableService.clear();
            final String counts = queryCount + " queries, " + validCount + " with a valid filter, " + comparedCount
                    + " compared, " + rejectedCount + " rejected, " + nullRejectingCount + " provably NULL-rejecting, "
                    + laterRuntimeVerdictCount + " by a later runtime-checked predicate, " + twoRightJoinsComparedCount
                    + " compared with two RIGHT joins, " + laterJoinRepairComparedCount
                    + " compared with a join after a repaired RIGHT join, " + mixedTypeVerdictCount + " proven by a computed column next to a runtime check, "
                    + nestedSubQueryComparedCount + " compared with a nested sub-query, " + whereKeyCorrelatedComparedCount
                    + " compared with a WHERE key next to a correlated condition, " + slaveKeyReadEarlierComparedCount
                    + " compared with the key on a table joined after an ON clause that reads the outer key";
            LOG.info().$("lateral null rejection fuzz [queries=").$(queryCount).$(", valid=").$(validCount)
                    .$(", compared=").$(comparedCount).$(", rejected=").$(rejectedCount)
                    .$(", nullRejecting=").$(nullRejectingCount).$(", laterRuntimeVerdict=").$(laterRuntimeVerdictCount)
                    .$(", twoRightJoinsCompared=").$(twoRightJoinsComparedCount)
                    .$(", laterJoinRepairCompared=").$(laterJoinRepairComparedCount)
                    .$(", mixedTypeVerdict=").$(mixedTypeVerdictCount)
                    .$(", nestedSubQueryCompared=").$(nestedSubQueryComparedCount)
                    .$(", whereKeyCorrelatedCompared=").$(whereKeyCorrelatedComparedCount)
                    .$(", slaveKeyReadEarlierCompared=").$(slaveKeyReadEarlierComparedCount).I$();
            if (!failures.isEmpty()) {
                Assert.fail(failures.size() + " of " + queryCount + " queries failed (" + counts + "):\n"
                        + String.join("\n", failures.subList(0, Math.min(20, failures.size()))));
            }
            // About a third of the queries return rows that the test compares. A generator that made
            // only invalid filters, or a rewriter that rejected every query beyond the reach of the
            // NULL-rejection check, would check no rows and pass.
            Assert.assertTrue("too few queries compared: " + counts, comparedCount * 4 >= queryCount);
            // About a fifth of the queries must return rows. A generator that made no provably
            // NULL-rejecting filter would leave the rewriter free to reject every query.
            Assert.assertTrue("too few queries provably NULL-rejecting: " + counts, nullRejectingCount * 8 >= queryCount);
            // A generator that put a runtime-checked predicate only first in the filter would not
            // show that the plan checks the later ones too.
            Assert.assertTrue("no query provably NULL-rejecting by a later runtime-checked predicate: " + counts, laterRuntimeVerdictCount > 0);
            Assert.assertTrue("no query with two RIGHT joins compared: " + counts, twoRightJoinsComparedCount > 0);
            // A generator that never followed the repaired RIGHT join with another join would not show
            // that the repair holds when one does.
            Assert.assertTrue("no query with a join after a repaired RIGHT join compared: " + counts, laterJoinRepairComparedCount > 0);
            // A generator that never put a computed column next to a column of known type would not
            // show that the code generator decides on both together.
            Assert.assertTrue("no query proven by a computed column next to a runtime check: " + counts, mixedTypeVerdictCount > 0);
            // A generator that never nested a sub-query in the operand of a runtime check would not
            // show that the check shares a sub-query below the top of its operand with the filter.
            Assert.assertTrue("no query with a nested sub-query compared: " + counts, nestedSubQueryComparedCount > 0);
            // A generator that never put a condition on the outer key next to WHERE t.x = o.k would
            // not show that the rewriter keeps it when it removes the outer-ref join.
            Assert.assertTrue("no query with a WHERE key next to a correlated condition compared: " + counts, whereKeyCorrelatedComparedCount > 0);
            // A generator that never keyed the outer row on a table joined after an ON clause that
            // reads it would not show that such a query returns the rows of its body.
            Assert.assertTrue("no query with the key on a table joined after an ON clause that reads the outer key compared: " + counts, slaveKeyReadEarlierComparedCount > 0);
        });
    }

    private Predicate randomPredicate(Rnd rnd, Bind bind) {
        if (rnd.nextInt(8) == 0) {
            return randomRefundsPredicate(rnd, bind);
        }
        final FuzzColumn column = COLUMNS[rnd.nextInt(COLUMNS.length)];
        final String ref = "t." + column.name;
        final String[] constants = column.constants;
        // GEOHASH and arrays have no order, IN or BETWEEN, so they take only = and !=
        final boolean isEqualityOnly = isEqualityOnlyType(column);
        switch (isEqualityOnly ? 3 : rnd.nextInt(10)) {
            case 0:
                return new Predicate(ref + (rnd.nextBoolean() ? " IS NULL" : " IS NOT NULL"), PredicateKind.PROVABLE, column.name);
            case 1: {
                final boolean isNotIn = rnd.nextInt(4) == 0;
                final StringBuilder sb = new StringBuilder(ref).append(isNotIn ? " NOT IN (" : " IN (");
                for (int i = 0, n = 1 + rnd.nextInt(3); i < n; i++) {
                    sb.append(i > 0 ? ", " : "").append(constants[rnd.nextInt(constants.length)]);
                }
                return new Predicate(sb.append(')').toString(), isNotIn ? PredicateKind.UNPROVEN : PredicateKind.PROVABLE, column.name);
            }
            case 2: {
                final boolean isNotBetween = rnd.nextInt(4) == 0;
                return new Predicate(
                        ref + (isNotBetween ? " NOT BETWEEN " : " BETWEEN ")
                                + constants[rnd.nextInt(constants.length)] + " AND " + constants[rnd.nextInt(constants.length)],
                        isNotBetween ? PredicateKind.UNPROVEN : PredicateKind.PROVABLE,
                        column.name
                );
            }
            default: {
                String operand = constants[rnd.nextInt(constants.length)];
                boolean isRuntimeOperand = true;
                if (column.bindType != null && bind.type == null && rnd.nextInt(4) == 0) {
                    bind.type = column.bindType;
                    bind.value = column.bindValues[rnd.nextInt(column.bindValues.length)];
                    operand = "$1";
                } else if (rnd.nextInt(4) == 0) {
                    operand = randomScalarSubQuery(rnd, column);
                } else {
                    isRuntimeOperand = false;
                }
                final String op = isEqualityOnly ? (rnd.nextBoolean() ? "=" : "!=") : COMPARISONS[rnd.nextInt(COMPARISONS.length)];
                final String sql = rnd.nextBoolean() ? ref + ' ' + op + ' ' + operand : operand + ' ' + op + ' ' + ref;
                if (!isRuntimeOperand) {
                    return new Predicate(sql, PredicateKind.PROVABLE, column.name);
                }
                // against a value known only when the query runs, < and > drop the NULL of a type
                // with NULL whatever the value, <= >= and = only while the value is not NULL, and
                // != not provably
                if (isTypeWithoutNull(column)) {
                    return new Predicate(sql, PredicateKind.UNPROVEN, column.name);
                }
                return switch (op) {
                    case "<", ">" -> new Predicate(sql, PredicateKind.PROVABLE, column.name);
                    case "<=", ">=" -> new Predicate(sql, PredicateKind.RUNTIME_CHECKED, column.name);
                    case "=" -> new Predicate(sql, PredicateKind.RUNTIME_CHECKED_EQUALITY, column.name);
                    default -> new Predicate(sql, PredicateKind.UNPROVEN, column.name);
                };
            }
        }
    }

    private enum FilterOutcome {
        COMPILE_ERROR, RUN_ERROR, RUNS
    }

    private enum PredicateKind {
        // drops every NULL-extended row when it is false on null_trades
        PROVABLE,
        // <= or >= against a bind variable or a scalar sub-query, whose value the plan checks for
        // NULL once per execution, on a type with NULL
        RUNTIME_CHECKED,
        // = against a bind variable or a scalar sub-query, on a type with NULL, which counts as
        // RUNTIME_CHECKED only while the outer-ref join stays
        RUNTIME_CHECKED_EQUALITY,
        // the rewriter does not claim to prove it, such as NOT IN or != against a bind variable
        UNPROVEN
    }

    private enum Verdict {
        // the filter may keep a NULL-extended row
        UNPROVEN,
        // a predicate that the rewriter proves, or the first runtime-checked one, drops every
        // NULL-extended row
        PROVEN,
        // only a runtime-checked predicate after the first one drops every NULL-extended row
        PROVEN_BY_LATER_RUNTIME_PREDICATE
    }

    private static class Bind {
        String type;
        Object value;
    }

    private record FuzzColumn(String name, String type, String[] values, String[] constants, String bindType,
                              Object[] bindValues) {
    }

    // column: the name of the trades column that the predicate reads, or null for a predicate on refunds
    private record Predicate(String sql, PredicateKind kind, String column) {
    }

    // hasCorrelatedSlave: the slave of the RIGHT join is a correlated sub-query, see correlatedSlaveShape();
    // hasLaterJoinAfterRepair: an INNER, LEFT or CROSS join follows that RIGHT join
    private record Shape(
            String body,
            boolean hasWhereKey,
            boolean hasKeyOnFullJoinedTable,
            boolean hasCorrelatedSlave,
            boolean hasLaterJoinAfterRepair
    ) {
        Shape(String body, boolean hasWhereKey, boolean hasKeyOnFullJoinedTable) {
            this(body, hasWhereKey, hasKeyOnFullJoinedTable, false, false);
        }
    }
}
