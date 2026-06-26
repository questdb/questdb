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

package io.questdb.cairo.security;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.engine.functions.catalogue.Constants;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class AbstractReadOnlySecurityContext implements SecurityContext {
    protected final boolean settingsReadOnly;

    // the reported principal; the singletons seed it with Constants.USER_NAME ("admin"), which is also
    // the value forPrincipal treats as the default/anonymous case (it returns the shared singleton)
    private final CharSequence principal;
    private volatile SecurityContext principalContextCache;

    protected AbstractReadOnlySecurityContext() {
        this(false, Constants.USER_NAME);
    }

    protected AbstractReadOnlySecurityContext(boolean settingsReadOnly, CharSequence principal) {
        this.settingsReadOnly = settingsReadOnly;
        this.principal = principal;
    }

    @Override
    public void authorizeAlterMatViewSetRefreshLimit(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeAlterMatViewSetRefreshType(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeAlterTableAddColumn(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeAlterTableAddIndex(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeAlterTableAlterColumnCache(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeAlterTableAlterColumnType(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeAlterTableAlterSymbolCapacity(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeAlterTableAttachPartition(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeAlterTableConvertPartitionToNative(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeAlterTableConvertPartitionToParquet(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeAlterTableDedupDisable(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeAlterTableDedupEnable(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeAlterTableDetachPartition(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeAlterTableDropColumn(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeAlterTableDropIndex(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeAlterTableDropPartition(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeAlterTableRenameColumn(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeAlterTableSetFormat(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeAlterTableSetParam(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeAlterTableSetParquetSettings(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeAlterTableSetType(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeAlterView(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeCopyCancel(SecurityContext cancellingSecurityContext) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeDatabaseBackup() {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeDatabaseSnapshot() {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeHttp() {
    }

    @Override
    public void authorizeInsert(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeLineTcp() {
    }

    @Override
    public void authorizeMatViewCreate() {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeMatViewDrop(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeMatViewRefresh(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizePGWire() {
    }

    @Override
    public void authorizeRebaseWal(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeResumeWal(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeSelect(ViewDefinition viewDefinition) {
    }

    @Override
    public void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
    }

    @Override
    public void authorizeSelectOnAnyColumn(TableToken tableToken) {
    }

    @Override
    public void authorizeSettings() {
        if (settingsReadOnly) {
            throw CairoException.authorization().put("The /settings endpoint is read-only").setCacheable(true);
        }
    }

    @Override
    public void authorizeSqlEngineAdmin() {
    }

    @Override
    public void authorizeSystemAdmin() {
    }

    @Override
    public void authorizeTableCreate() {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeTableDrop(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeTableReindex(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeTableRename(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeTableTruncate(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeTableUpdate(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeTableVacuum(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeViewCompile(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeViewCreate() {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeViewDrop(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void checkEntityEnabled() {
    }

    /**
     * Returns a read-only context that reports the given principal, so that
     * {@code current_user()} and session handling reflect the authenticated user rather
     * than the hardcoded default. Returns {@code this} when the principal is null (anonymous)
     * or already matches, to keep the singleton path allocation-free.
     * <p>
     * The HTTP authentication path re-derives the security context on every request (see
     * {@code HttpConnectionContext.configureSecurityContext}), so the last derived context is
     * cached to avoid allocating a context and copying the principal on every request when the
     * principal does not change. The cache holds only the most recently derived context, which fits
     * the common case of a single configured user; a varying principal degrades to allocate-per-call.
     * <p>
     * The method is {@code final} and routes instance creation through
     * {@link #newPrincipalContext(CharSequence)} so subclasses preserve their runtime type
     * instead of being silently downgraded to a plain {@code ReadOnlySecurityContext}.
     */
    public final SecurityContext forPrincipal(@Transient @Nullable CharSequence principal) {
        // compare against getPrincipal(), not the raw field, so a subclass that overrides getPrincipal()
        // is matched consistently here and in the cache check below; equalsNc tolerates a null
        // getPrincipal() (e.g. a validation context delegating to a null-principal delegate)
        if (principal == null || principal.isEmpty() || Chars.equalsNc(principal, getPrincipal())) {
            return this;
        }
        final SecurityContext cached = principalContextCache;
        if (cached != null && Chars.equalsNc(principal, cached.getPrincipal())) {
            return cached;
        }
        final SecurityContext context = newPrincipalContext(Chars.toString(principal));
        principalContextCache = context;
        return context;
    }

    @Override
    public CharSequence getPrincipal() {
        return principal;
    }

    @Override
    public boolean isQueryCancellationAllowed() {
        return false;
    }

    @Override
    public boolean isSystemAdmin() {
        return true;
    }

    /**
     * Creates the concrete context returned by {@link #forPrincipal(CharSequence)} for a new
     * principal. The {@code principal} is already a stable copy. Subclasses must override this
     * to return their own type so {@code forPrincipal} does not downgrade them.
     * <p>
     * The derived context overrides only the reported principal; {@code getAuthType()} and
     * {@code isExternal()} keep their read-only defaults ({@code AUTH_TYPE_NONE} / not external).
     * These contexts model identity only and are used when ACL is not enforced; the full
     * authentication metadata is modelled by the ACL-enforcing security contexts.
     */
    protected abstract SecurityContext newPrincipalContext(CharSequence principal);
}
