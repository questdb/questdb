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

package io.questdb.griffin.bind;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MetadataCache;
import io.questdb.cairo.RowExpiryUtil;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.ExpiryPolicyVersionChangedException;
import io.questdb.griffin.ExpiryReadPolicy;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ExpressionVisitor;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.Mutable;

/**
 * Binds the read of a materialized view under an EXPIRE ROWS policy, so that expired rows are invisible to every read
 * of the view. A policy on a value binds as a filter over the scan, and KEEP LATEST as a LATEST ON over it. A window
 * policy references a window function, which only a projection computes, so it binds as a query that computes a boolean
 * column of the whole view, and a filter over that column.
 * <p>
 * The policy and the metadata version of the table come from the one binding, so a filter chosen from an old policy is
 * never paired with a reader on the new one: a version that moved on while a policy change is in flight rejects the
 * compile, see {@link #predicateAt}.
 */
final class ExpiryReadBinder implements Mutable {
    private static final ExpressionVisitor POLICY_CALLS = expression -> {
        if (expression instanceof FunctionExpression call) {
            call.markPolicyPredicate();
        }
        return TreeWalk.CONTINUE;
    };
    private final SqlBinder binder;
    private final SqlCompilerImpl compiler;
    private final BindContext ctx;
    // The tables whose policy is being bound: what the policy itself reads, its keep query or a sub-query of its
    // predicate, reads the table without the policy rather than applying the policy a second time. Lookups ignore
    // case, as the table registry does, since a policy sub-query can name the table in a different case from the read.
    private final LowerCaseCharSequenceHashSet tablesBound = new LowerCaseCharSequenceHashSet();
    private String boundPredicate;
    private int boundPosition;

    ExpiryReadBinder(BindContext ctx, SqlBinder binder, SqlCompilerImpl compiler) {
        this.ctx = ctx;
        this.binder = binder;
        this.compiler = compiler;
    }

    /**
     * Returns the keep-filter text for an EXPIRE ROWS predicate, the rows that have NOT expired. A row expires only when
     * the predicate is TRUE, so the keep-filter keeps every row whose predicate result is anything else, which
     * {@code NOT (predicate)} spells for every predicate shape. Over a comparison of the designated timestamp,
     * {@link io.questdb.griffin.model.IntervalExtractor} reads an interval out of that {@code NOT} and prunes partitions,
     * see its negated range comparison.
     * <p>
     * What happens to a row whose predicate operand is NULL follows QuestDB's two-valued comparison semantics, which the
     * keep-filter reports rather than overrides: a comparison against NULL is FALSE, so {@code v < 2.0} keeps a row whose
     * {@code v} is NULL, while a predicate that is TRUE on a NULL operand expires it. {@code NOT (v >= 2.0)} is such a
     * predicate ({@code NULL >= 2.0} is FALSE, so the {@code NOT} is TRUE), and so are {@code v != 2.0} and
     * {@code v IS NULL}. {@code v < 2.0} and {@code NOT (v >= 2.0)} therefore express DIFFERENT policies for NULL rows
     * although they read as the same rule, and the expired NULL rows are physically deleted by the cleanup sweep, which
     * keeps the same rows through a {@code CASE} wrap of its own ({@link RowExpiryUtil#buildRowExpiryKeepFilter} explains
     * why the sweep spells it differently). {@code MatViewExpireRowsTest.testExpireScalarNullRowsFollowPredicateTruthValue}
     * pins this across the spellings.
     */
    private static String keepFilterText(String predicate) {
        return "NOT (" + predicate.trim() + ")";
    }

    private LogicalPlan bindKeepFilter(ScanPlan scan, TableToken token, String predicate, SqlExecutionContext executionContext) throws SqlException {
        final ExpressionNode keepFilter = SqlUtil.optimiseBooleanNot(
                compiler.parseExpiryExpression(keepFilterText(predicate), binder.getParserCallback()), ctx.bindingExpressions);
        final BoundExpression bound = binder.bindPredicate(keepFilter, scan, token.getTableName(), executionContext);
        bound.walk(POLICY_CALLS);
        return ctx.planNodes.filters.next().of(scan, bound, scan.getPosition());
    }

    private void beginBinding(TableToken token, String predicate, int position) {
        tablesBound.add(token.getTableName());
        boundPredicate = predicate;
        boundPosition = position;
    }

    private void endBinding(TableToken token) {
        tablesBound.remove(token.getTableName());
        boundPredicate = null;
        boundPosition = 0;
    }

    /**
     * True when the policy binds a query of its own, see {@link #bindQueryRead}, rather than over the table's scan.
     */
    static boolean isQueryPolicy(String predicate) {
        return RowExpiryUtil.isKeepBy(predicate) || RowExpiryUtil.isWindow(predicate);
    }

    /**
     * Binds the read of the table under a window policy: the keep-filter references a window function, illegal in a
     * plain WHERE, so a query over the WHOLE view computes it as a boolean column, which a filter reads and a projection
     * then drops, so the keep column never leaks through the caller's SELECT *. The query drops the designated
     * timestamp, the extra column making it a general projection, so the read declares it again for what requires the
     * timestamp over the read. The query's selections read a column only as what reads the view asks for it, or as the
     * policy reads it: they are implied, as a {@code SELECT *} a query leaves out, so a read asks for no SELECT on the
     * rest of the view.
     */
    LogicalPlan bindQueryRead(TableToken token, MetadataCache.ExpiryPolicyInfo policy, ExpressionNode viewName, int position, SqlExecutionContext executionContext) throws SqlException {
        final String sql = "SELECT *, " + RowExpiryUtil.buildKeepCase(RowExpiryUtil.windowPredicate(policy.getPredicate(), policy.getTimestampName()))
                + " " + RowExpiryUtil.KEEP_COLUMN + " FROM " + RowExpiryUtil.quoteIdentifier(token.getTableName());
        final LogicalPlan keepQuery;
        beginBinding(token, policy.getPredicate(), position);
        try {
            final QueryModel query = compiler.parseExpiryQuery(sql, binder.getParserCallback());
            query.setArtificialStar(true);
            query.setViewNameExpr(viewName);
            keepQuery = binder.bindQuery(query, executionContext);
        } finally {
            endBinding(token);
        }
        final OutputSchema output = keepQuery.getOutput();
        final int keepIndex = output.getColumnCount() - 1;
        final FilterPlan filter = ctx.planNodes.filters.next().of(
                binder.declareTimestamp(keepQuery, SqlBinder.timestampIndex(output, policy.getTimestampName(), position), position),
                ctx.planNodes.columns.next().of(output.getColumnId(keepIndex), ColumnType.BOOLEAN, position),
                position
        );
        final ProjectPlan read = ctx.identityProjection(filter, position);
        read.getExpressions().remove(keepIndex);
        read.getOutput().remove(keepIndex);
        return read;
    }

    /**
     * Binds the read of the table over its scan under a policy on a value, as a filter, or KEEP LATEST, as a LATEST ON.
     */
    LogicalPlan bindScanRead(ScanPlan scan, MetadataCache.ExpiryPolicyInfo policy, int position, SqlExecutionContext executionContext) throws SqlException {
        final TableToken token = scan.getTableToken();
        final String predicate = policy.getPredicate();
        beginBinding(token, predicate, position);
        try {
            markBoundRead(scan);
            if (RowExpiryUtil.isKeepLatest(predicate)) {
                return binder.bindLatestBy(scan, compiler.parseExpiryLatestBy(
                        RowExpiryUtil.quoteIdentifier(policy.getTimestampName()) + " PARTITION BY " + RowExpiryUtil.keepLatestKeys(predicate), position));
            }
            return bindKeepFilter(scan, token, predicate, executionContext);
        } finally {
            endBinding(token);
        }
    }

    /**
     * What the policy looked up by {@link #policyOf} keeps of a read at the metadata version of the table: its
     * predicate, null when the table has none. A lookup that a change in flight read straight from the table metadata
     * has to come from the version of the table binding reads, whether it found a policy or none: the policy epoch can
     * read the same value before and after the change, while the metadata version cannot, and a table that moved past
     * the lookup has a stale keep-filter, so the statement compiles again.
     */
    String predicateAt(MetadataCache.ExpiryPolicyInfo policy, long metadataVersion) {
        if (policy.isPending() && policy.getMetadataVersion() != metadataVersion) {
            throw ExpiryPolicyVersionChangedException.INSTANCE;
        }
        return policy.getPredicate();
    }

    @Override
    public void clear() {
        tablesBound.clear();
        boundPredicate = null;
        boundPosition = 0;
    }

    /**
     * Records on a scan of a table whose policy is being bound the policy its read applies, at the position of the read
     * rather than in the text of the policy.
     */
    void markBoundRead(ScanPlan scan) {
        scan.setExpiryPolicy(boundPredicate);
        scan.setPosition(boundPosition);
    }

    /**
     * True when the table is read for the policy being bound on it, which reads it without the policy.
     */
    boolean isPolicyBinding(TableToken token) {
        return tablesBound.contains(token.getTableName());
    }

    /**
     * The EXPIRE ROWS policy the read of the table applies, null when it reads every row: the table carries no policy,
     * the context reads it raw, or the read is what a policy of the table reads. A lookup while a policy change is in
     * flight returns even with no policy, see {@link #predicateAt}. A read a context rejects fails here.
     */
    MetadataCache.ExpiryPolicyInfo policyOf(TableToken token, int position, SqlExecutionContext executionContext) throws SqlException {
        final MetadataCache metadataCache = executionContext.getCairoEngine().getMetadataCache();
        if (!metadataCache.mayHaveExpiryPolicy() || isPolicyBinding(token)) {
            return null;
        }
        final ExpiryReadPolicy readPolicy = executionContext.getExpiryReadPolicy(token);
        if (readPolicy == ExpiryReadPolicy.RAW || !metadataCache.mayTableHaveExpiryPolicy(token)) {
            return null;
        }
        final boolean isRejected = readPolicy == ExpiryReadPolicy.REJECT;
        if (isRejected && metadataCache.isExpiryPolicyUpdatePending(token)) {
            throw ExpiryPolicyVersionChangedException.INSTANCE;
        }
        final MetadataCache.ExpiryPolicyInfo policy;
        try {
            policy = metadataCache.lookupExpiryPolicy(token);
        } catch (CairoException e) {
            if (isRejected && metadataCache.isExpiryPolicyUpdatePending(token)) {
                throw ExpiryPolicyVersionChangedException.INSTANCE;
            }
            throw e;
        }
        if (isRejected && policy.isPending()) {
            throw ExpiryPolicyVersionChangedException.INSTANCE;
        }
        if (policy.getPredicate() == null) {
            return policy.isPending() ? policy : null;
        }
        if (isRejected) {
            final CharSequence dependentName = executionContext.getExpiryMaterializingViewName();
            assert dependentName != null;
            throw SqlException.materializationExpiryConflict(position, dependentName, token.getTableName());
        }
        return policy;
    }
}
