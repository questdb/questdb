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
import io.questdb.cairo.MetadataCache;
import io.questdb.cairo.RowExpiryUtil;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.ExpiryPolicyParser;
import io.questdb.griffin.ExpiryPolicyVersionChangedException;
import io.questdb.griffin.ExpiryReadPolicy;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ExpressionVisitor;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.LogicalPlans;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.Mutable;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

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
    private final BindContext ctx;
    private final StringSink failure = new StringSink();
    private final ExpiryPolicyParser policyParser;
    // The tables whose policy is being bound: what the policy itself reads, its keep query or a sub-query of its
    // predicate, reads the table without the policy rather than applying the policy a second time. Lookups ignore
    // case, as the table registry does, since a policy sub-query can name the table in a different case from the read.
    private final LowerCaseCharSequenceHashSet tablesBound = new LowerCaseCharSequenceHashSet();

    ExpiryReadBinder(BindContext ctx, SqlBinder binder, ExpiryPolicyParser policyParser) {
        this.ctx = ctx;
        this.binder = binder;
        this.policyParser = policyParser;
    }

    /**
     * The error of a read whose policy fails to bind, which DDL refuses but WAL apply replays as an older version
     * stored it: it names the policy of the table, at the read's position, since the cause sits in the policy text.
     */
    private SqlException policyError(SqlException e, TableToken token, int position) {
        failure.clear();
        failure.put(e.getFlyweightMessage());
        return SqlException.$(position, "invalid EXPIRE ROWS policy of '").put(token.getTableName()).put("': ").put(failure);
    }

    /**
     * True when the policy binds a query of its own, see {@link #bindQueryRead}, rather than over the table's scan.
     */
    static boolean isQueryPolicy(String predicate) {
        return RowExpiryUtil.isKeepBy(predicate) || RowExpiryUtil.isWindow(predicate);
    }

    /**
     * Binds the keep-filter of a policy on a value over the scan of its table: the predicate every read of the table
     * filters the scan with, the rows the policy has not expired, see {@link RowExpiryUtil#appendKeepFilter}. A read
     * parses the stored policy into the keep-filter, and DDL negates the policy it parsed, see
     * {@link #bindKeepFilter(ExpressionNode, OutputSchema, CharSequence, SqlExecutionContext)}; both bind it through
     * {@link #bindParsedKeepFilter}, so a policy DDL accepts binds in every read. What the policy reads binds without
     * the policy of the table.
     */
    BoundExpression bindKeepFilter(ScanPlan scan, CharSequence predicate, SqlExecutionContext executionContext) throws SqlException {
        final String tableName = scan.getTableToken().getTableName();
        tablesBound.add(tableName);
        try {
            final ExpressionNode keepFilter = policyParser.parseKeepFilter(predicate, binder.getParserCallback());
            return bindParsedKeepFilter(keepFilter, scan.getOutput(), scan, tableName, executionContext);
        } finally {
            tablesBound.remove(tableName);
        }
    }

    /**
     * Binds the keep-filter of the parsed scalar policy {@code predicate} over {@code input}, the columns of the table
     * the policy belongs to, as every read of the table binds it, see {@link #bindKeepFilter(ScanPlan, CharSequence,
     * SqlExecutionContext)}, with qualified names resolved against {@code alias}, the name of the table. DDL binds a
     * policy with it before any read does, at CREATE before the table exists, so no scan reads the table: every column
     * of {@code input} comes from the one table. The keep-filter is the negation of {@code predicate}, as the parse of
     * the stored text a read binds spells it, see {@link RowExpiryUtil#appendKeepFilter}; pushing the negation down
     * rewrites {@code predicate} in place.
     */
    BoundExpression bindKeepFilter(ExpressionNode predicate, OutputSchema input, CharSequence alias, SqlExecutionContext executionContext)
            throws SqlException {
        tablesBound.add(alias);
        try {
            return bindParsedKeepFilter(SqlUtil.negate(predicate, ctx.bindingExpressions), input, null, alias, executionContext);
        } finally {
            tablesBound.remove(alias);
        }
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
        final LogicalPlan keepQuery;
        tablesBound.add(token.getTableName());
        try {
            final QueryModel query = policyParser.parseKeepQuery(token, policy, binder.getParserCallback());
            query.setViewNameExpr(viewName);
            keepQuery = binder.bindQuery(query, executionContext);
        } catch (SqlException e) {
            throw policyError(e, token, position);
        } finally {
            tablesBound.remove(token.getTableName());
        }
        final ScanPlan scan = LogicalPlans.scanUnderChain(keepQuery);
        scan.markExpiringRead();
        scan.setPosition(position);
        // the query selects the columns of the scan first, in the order of the scan
        return ctx.keepRows(ctx.declareTimestamp(keepQuery, scan.getOutput().getTimestampIndex(), position), position);
    }

    /**
     * Binds the read of the table over its scan under a policy on a value, as a filter, or KEEP LATEST, as a LATEST ON.
     */
    LogicalPlan bindScanRead(ScanPlan scan, MetadataCache.ExpiryPolicyInfo policy, int position, SqlExecutionContext executionContext) throws SqlException {
        final TableToken token = scan.getTableToken();
        final String predicate = policy.getPredicate();
        scan.markExpiringRead();
        try {
            if (RowExpiryUtil.isKeepLatest(predicate)) {
                tablesBound.add(token.getTableName());
                try {
                    return binder.bindLatestBy(scan, policyParser.parseKeepLatest(policy, position));
                } finally {
                    tablesBound.remove(token.getTableName());
                }
            }
            return ctx.planNodes.filters.next().of(scan, bindKeepFilter(scan, predicate, executionContext), scan.getPosition());
        } catch (SqlException e) {
            throw policyError(e, token, position);
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
        policyParser.clear();
        tablesBound.clear();
    }

    /**
     * Pushes the negation of a parsed keep-filter down and binds the result as a filter over {@code input}, the output
     * of {@code source}, or of a table no plan reads when {@code source} is null.
     */
    private BoundExpression bindParsedKeepFilter(
            ExpressionNode keepFilter,
            OutputSchema input,
            @Nullable LogicalPlan source,
            CharSequence alias,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final BoundExpression bound = binder.bindPredicate(
                SqlUtil.optimiseBooleanNot(keepFilter, ctx.bindingExpressions), input, source, alias, executionContext);
        bound.walk(POLICY_CALLS);
        return bound;
    }

    /**
     * The EXPIRE ROWS policy the read of the table applies, null when it reads every row: the table carries no policy,
     * the context reads it raw, or the read is what a policy of the table reads. A lookup while a policy change is in
     * flight returns even with no policy, see {@link #predicateAt}. A read a context rejects fails here.
     */
    MetadataCache.ExpiryPolicyInfo policyOf(TableToken token, int position, SqlExecutionContext executionContext) throws SqlException {
        final MetadataCache metadataCache = executionContext.getCairoEngine().getMetadataCache();
        if (!metadataCache.mayHaveExpiryPolicy() || tablesBound.contains(token.getTableName())) {
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
