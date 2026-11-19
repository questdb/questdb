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

package io.questdb.cairo.mv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.seq.TableMetadataChange;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

/**
 * The bare-name guard. A materialized view's refresh reads a bare select-list name that matches no column as a call
 * to the zero-argument function of that name, such as {@code version} or {@code count}. A base column added, dropped
 * or renamed under such a name therefore switches the name between a column and a call, and the view's SQL still
 * compiles, so the refresh's compile cannot tell that the view's rows would change meaning. The WAL apply job calls
 * {@link #beforeColumnChange} before it applies a base column change, and the guard invalidates each dependent view
 * whose own SQL text uses a changed name bare.
 * <p>
 * The guard publishes the invalidation as {@code MatViewRefreshJob.invalidateView()} does: it sets the view's
 * pending-invalidation marker, which incremental, period and range refreshes check once they have fixed their base
 * reader and before each compile, and enqueues the view's own INVALIDATE task. It must not ride on the apply batch's
 * notification, whose INVALIDATE would replace the incremental notification that the other dependent views still
 * need, and its INVALIDATE task names the view, so the refresh job does not stop the base's re-notification for
 * them. A view that is already invalid, and not being rebuilt by a pending FULL refresh, is left with its reason,
 * as for any other invalidation.
 * <p>
 * Before it publishes, the guard records each switched name on the view's state, see
 * {@link MatViewState#recordBareNameSwitch}, invalid view or not, so that REFRESH ... RESUME refuses a view whose SQL
 * still uses the name bare, see {@link MatViewResume}. Only a FULL refresh that rebuilds the view from a base
 * snapshot that includes the change covers the switch. The guard records before it reads the view's resuming flag,
 * and RESUME sets the flag before it reads the switches, so either RESUME sees the switch or the guard sees RESUME and
 * keeps its invalidation for the release of RESUME's latch.
 * <p>
 * The guard publishes before the change applies, so its invalidation can be settled while the change is not visible
 * yet: the INVALIDATE handler declines a view that never completed a refresh, or marks the view invalid, and a FULL
 * refresh or a first build can then rebuild the view from a base snapshot without the change. The recorded switch
 * outlives the invalidation, so a refresh that commits a refresh position below it publishes the guard's invalidation
 * again, whatever settled the first one, see {@code MatViewRefreshJob.publishBareNameSwitchAboveRefreshPosition()}.
 * <p>
 * A view's SQL can also reach the base's dependent views while the change applies and is not visible yet: CREATE
 * MATERIALIZED VIEW registers the view, the WAL apply of ALTER MATERIALIZED VIEW ... AS or ... WITH BASE installs a new
 * SQL or moves the view from another base table, or the view's state appears after the guard skipped the view for
 * having none. Such a view can refresh from the base before and after the change, so the WAL apply job calls
 * {@link #afterColumnChange} once the change applied, and the guard checks each view whose table id and SQL text
 * {@link #beforeColumnChange} did not check. It records the switch at the change's txn, as before the change, and
 * publishes the invalidation only to a view with a refresh position, whose rows may come from a refresh that read the
 * base before the change. A view without one keeps its queued first build, whose commit publishes the invalidation when
 * the build read the base before the change. A view or SQL that reaches the dependent views after that check refreshes
 * only from base snapshots that include the change. The check cannot tell a view that holds rows from before the
 * change from one whose refresh position reached the change between the apply and the check, so the latter goes
 * invalid too, and a FULL refresh recovers it.
 * <p>
 * The guard runs only on the node that refreshes views; a replica's state store holds no view states. Its
 * notification and the switches it records live in memory, so a crash before the refresh job handles the
 * notification loses it, and the next refresh then reads the switched name, as the refresh did before the guard.
 */
public class MatViewBareNameGuard implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(MatViewBareNameGuard.class);
    // The SQL text of each view that beforeColumnChange() checked, by the view's table id, for afterColumnChange() of
    // the same change. A view whose check failed is not in it.
    private final IntObjHashMap<String> checkedViewSqls = new IntObjHashMap<>();
    private final ObjList<TableToken> dependentViews = new ObjList<>();
    private final CairoEngine engine;
    // The names of the change's columns that are names of zero-argument functions, as the change writes them.
    private final ObjList<String> matchedNames = new ObjList<>();
    // Parses the views' SQL, allocated at the first column change under the name of a zero-argument function.
    private SqlExecutionContextImpl parseContext;

    public MatViewBareNameGuard(CairoEngine engine) {
        this.engine = engine;
    }

    public static String invalidationReason(CharSequence bareName) {
        return "base column change would switch bare name '" + bareName
                + "' between a column and a function call; write " + bareName
                + "() to call the function or qualify the column";
    }

    /**
     * Checks the dependent materialized views of the base table that {@link #beforeColumnChange} did not check, after
     * the change applied: a view registered while the change applied, or one whose SQL changed meanwhile. It records
     * the switches of such a view, as {@link #beforeColumnChange} does, and invalidates the view only when it has a
     * refresh position, see the class comment. The WAL apply job calls it right after it applied the change that it
     * passed to {@link #beforeColumnChange}. Errors are logged and swallowed: the change has applied.
     *
     * @param baseTableToken the base table the change applied to
     * @param seqTxn         the base table's sequencer txn of the change
     */
    public void afterColumnChange(TableToken baseTableToken, long seqTxn) {
        try {
            if (matchedNames.size() == 0) {
                // the change has no column under the name of a zero-argument function
                return;
            }
            dependentViews.clear();
            engine.getDependentViewGraph().getDependentViews(baseTableToken, dependentViews);
            final MatViewStateStore stateStore = engine.getMatViewStateStore();
            for (int v = 0, n = dependentViews.size(); v < n; v++) {
                final TableToken viewToken = dependentViews.getQuick(v);
                if (!viewToken.isMatView()) {
                    // a live view, which the guard does not cover
                    continue;
                }
                final MatViewState viewState = stateStore.getViewState(viewToken);
                if (viewState == null) {
                    // the view is being dropped, or the node refreshes no views
                    continue;
                }
                final MatViewDefinition viewDefinition = viewState.getViewDefinition();
                final String checkedViewSql = checkedViewSqls.get(viewToken.getTableId());
                if (checkedViewSql != null && checkedViewSql.equals(viewDefinition.getMatViewSql())) {
                    // beforeColumnChange() checked this SQL of the view
                    continue;
                }
                final CharSequence firstBareName = recordBareNameSwitches(viewState, viewDefinition, baseTableToken, seqTxn);
                // The refresh position is read after the switches are recorded, and a refresh commits its position
                // before it reads the switches, so a first build that commits meanwhile publishes the invalidation
                // itself when its base reader predates the change.
                if (firstBareName != null && viewState.getLastRefreshBaseTxn() != -1) {
                    publishInvalidation(stateStore, viewToken, viewState, invalidationReason(firstBareName), baseTableToken, seqTxn);
                }
            }
        } catch (Throwable th) {
            // A failed enqueue has already recorded its retry, and the view's marker is set, so the refresh job
            // still delivers the invalidation.
            LOG.error().$("could not check materialized views that reached the base table while a column change applied [table=")
                    .$(baseTableToken)
                    .$(", ex=").$(th)
                    .I$();
        } finally {
            matchedNames.clear();
            checkedViewSqls.clear();
        }
    }

    /**
     * Invalidates each dependent materialized view of the base table whose own SQL text uses a column name that the
     * change adds, removes or renames as a bare select-list name, when that name is also the name of a
     * zero-argument function. The WAL apply job calls it before it applies the change, so that no refresh whose
     * base reader sees the change compiles the view's SQL before the view's marker is set. Comparisons ignore case.
     * Errors are logged and swallowed: a failed guard must not fail the base table's WAL apply. A failed parse of
     * a view's SQL leaves the view to its next refresh, whose compile fails on the same error. A view in the middle
     * of DROP MATERIALIZED VIEW may still be published to; the state store drops the request. The guard remembers
     * the SQL text of each view it checked for {@link #afterColumnChange}.
     *
     * @param baseTableToken the base table the change applies to
     * @param change         the column change
     * @param seqTxn         the base table's sequencer txn of the change
     */
    public void beforeColumnChange(TableToken baseTableToken, TableMetadataChange change, long seqTxn) {
        // the state of the previous change, whose apply may have failed before afterColumnChange()
        matchedNames.clear();
        checkedViewSqls.clear();
        final int nameCount = change.getChangedColumnNameCount();
        if (nameCount == 0) {
            return;
        }
        try {
            final FunctionFactoryCache functionFactoryCache = engine.getFunctionFactoryCache();
            for (int i = 0; i < nameCount; i++) {
                final CharSequence name = change.getChangedColumnName(i);
                if (functionFactoryCache.isValidNoArgFunction(name)) {
                    matchedNames.add(Chars.toString(name));
                }
            }
            if (matchedNames.size() == 0) {
                return;
            }
            dependentViews.clear();
            engine.getDependentViewGraph().getDependentViews(baseTableToken, dependentViews);
            final MatViewStateStore stateStore = engine.getMatViewStateStore();
            for (int v = 0, n = dependentViews.size(); v < n; v++) {
                final TableToken viewToken = dependentViews.getQuick(v);
                if (!viewToken.isMatView()) {
                    // a live view, which the guard does not cover
                    continue;
                }
                final MatViewState viewState = stateStore.getViewState(viewToken);
                if (viewState == null) {
                    // the view is being dropped, or the node refreshes no views
                    continue;
                }
                final MatViewDefinition viewDefinition = viewState.getViewDefinition();
                final String viewSql = viewDefinition.getMatViewSql();
                final CharSequence firstBareName = recordBareNameSwitches(viewState, viewDefinition, baseTableToken, seqTxn);
                if (firstBareName != null) {
                    publishInvalidation(stateStore, viewToken, viewState, invalidationReason(firstBareName), baseTableToken, seqTxn);
                }
                checkedViewSqls.put(viewToken.getTableId(), viewSql);
            }
        } catch (Throwable th) {
            // A failed enqueue has already recorded its retry, and the view's marker is set, so the refresh job
            // still delivers the invalidation.
            LOG.error().$("could not check dependent materialized views for bare names switched by a column change [table=")
                    .$(baseTableToken)
                    .$(", ex=").$(th)
                    .I$();
        }
    }

    @Override
    public void close() {
        parseContext = Misc.free(parseContext);
    }

    private static void publishInvalidation(
            MatViewStateStore stateStore,
            TableToken viewToken,
            MatViewState viewState,
            String invalidationReason,
            TableToken baseTableToken,
            long seqTxn
    ) {
        // The same gate as MatViewRefreshJob.invalidateView(): an invalid view keeps its reason, unless a pending
        // FULL refresh is rebuilding it, which a change that its fixed snapshot does not cover must invalidate again,
        // or RESUME is recovering it, which must not leave the view valid on a SQL whose bare name the change
        // switches. The resuming flag is read before the invalid flag: RESUME makes the view valid before it clears it.
        final boolean isResuming = viewState.isResuming();
        if (viewState.isInvalid() && !viewState.isPendingFullRefresh() && !isResuming) {
            LOG.info().$("materialized view is already invalid, keeping its reason [view=").$(viewToken)
                    .$(", guardReason=").$safe(invalidationReason)
                    .I$();
            return;
        }
        LOG.info().$("base column change switches a bare name of materialized view, invalidating it [view=").$(viewToken)
                .$(", baseTable=").$(baseTableToken)
                .$(", seqTxn=").$(seqTxn)
                .$(", reason=").$safe(invalidationReason)
                .I$();
        // The marker goes first: a refresh that opens its base reader after the change applies sees it. The base
        // txn lets a FULL refresh whose snapshot includes the change consume the invalidation, as for any base
        // table invalidation, and the view's INVALIDATE handler declines a view that never completed a refresh.
        viewState.markAsPendingInvalidationAndGetMarker(invalidationReason, baseTableToken, seqTxn, false);
        stateStore.enqueueInvalidate(viewToken, invalidationReason, baseTableToken, seqTxn, false);
    }

    private SqlExecutionContextImpl getParseContext() {
        if (parseContext == null) {
            parseContext = new SqlExecutionContextImpl(engine, 1).with(
                    engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext()
            );
        }
        return parseContext;
    }

    // Records the switch of each matched name that the view's SQL uses bare, and returns the first such name as the SQL
    // writes it, or null when the SQL uses none of them or does not parse. Runs the test seam after it recorded one.
    private CharSequence recordBareNameSwitches(
            MatViewState viewState,
            MatViewDefinition viewDefinition,
            TableToken baseTableToken,
            long seqTxn
    ) {
        final LowerCaseCharSequenceHashSet bareNames = viewDefinition.getBareSelectNames(engine, getParseContext());
        if (bareNames == null) {
            return null;
        }
        CharSequence firstBareName = null;
        for (int i = 0, m = matchedNames.size(); i < m; i++) {
            final int keyIndex = bareNames.keyIndex(matchedNames.getQuick(i));
            if (keyIndex < 0) {
                // the name as the view's SQL writes it
                final CharSequence bareName = bareNames.keyAt(keyIndex);
                viewState.recordBareNameSwitch(bareName, baseTableToken.getTableId(), seqTxn);
                if (firstBareName == null) {
                    firstBareName = bareName;
                }
            }
        }
        if (firstBareName != null) {
            viewState.runBareNameSwitchRecordedSeamForTesting();
        }
        return firstBareName;
    }
}
