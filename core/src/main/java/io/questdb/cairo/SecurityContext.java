/*******************************************************************************
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

package io.questdb.cairo;

import io.questdb.cairo.view.ViewDefinition;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("unused")
public interface SecurityContext extends Mutable {
    // Implementations are free to define unique authentication types.
    // The user authenticated with credentials.
    byte AUTH_TYPE_CREDENTIALS = 1;
    // The user authenticated with a JWK token.
    byte AUTH_TYPE_JWK_TOKEN = 2;
    // The user is not authenticated.
    // Either tried to authenticate and failed, or did not try to authenticate at all.
    byte AUTH_TYPE_NONE = 0;

    void authorizeAlterTableAddColumn(TableToken tableToken);

    void authorizeAlterTableAddIndex(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames);

    void authorizeAlterTableAlterColumnCache(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames);

    void authorizeAlterTableAlterColumnType(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames);

    void authorizeAlterTableAttachPartition(TableToken tableToken);

    void authorizeAlterTableDedupDisable(TableToken tableToken);

    void authorizeAlterTableDedupEnable(TableToken tableToken);

    void authorizeAlterTableDetachPartition(TableToken tableToken);

    void authorizeAlterTableDropColumn(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames);

    void authorizeAlterTableDropIndex(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames);

    void authorizeAlterTableDropPartition(TableToken tableToken);

    // the names are pairs from-to
    void authorizeAlterTableRenameColumn(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames);

    void authorizeAlterTableSetType(TableToken tableToken);

    void authorizeAlterView(TableToken tableToken);

    void authorizeCopyCancel(SecurityContext cancellingSecurityContext);

    void authorizeDatabaseBackup();

    void authorizeDatabaseSnapshot();

    void authorizeHttp();

    void authorizeInsert(TableToken tableToken);

    void authorizeLineTcp();

    void authorizeMatViewCreate();

    void authorizeMatViewDrop(TableToken tableToken);

    void authorizeMatViewRefresh(TableToken tableToken);

    void authorizePGWire();

    void authorizeResumeWal(TableToken tableToken);

    void authorizeSelect(ViewDefinition viewDefinition);

    void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames);

    void authorizeSelectOnAnyColumn(TableToken tableToken);

    void authorizeSettings();

    void authorizeSqlEngineAdmin();

    void authorizeSystemAdmin();

    void authorizeTableCreate();

    default void authorizeTableCreate(int tableKind) {
        switch (tableKind) {
            case TableUtils.TABLE_KIND_REGULAR_TABLE:
                authorizeTableCreate();
                break;
            case TableUtils.TABLE_KIND_TEMP_PARQUET_EXPORT:
                // Allowed even in read-only mode
                return;
            default:
                throw new UnsupportedOperationException("Unsupported table kind: " + tableKind);
        }
    }


    void authorizeTableDrop(TableToken tableToken);

    // columnNames - empty means all indexed columns
    void authorizeTableReindex(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames);

    void authorizeTableRename(TableToken tableToken);

    void authorizeTableTruncate(TableToken tableToken);

    void authorizeTableUpdate(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames);

    void authorizeTableVacuum(TableToken tableToken);

    void authorizeViewCompile(TableToken tableToken);

    void authorizeViewCreate();

    void authorizeViewDrop(TableToken tableToken);

    /**
     * Should throw an exception if:
     * - logged in as a user and the user has been disabled, or it has no permissions to connect via the endpoint used,
     * - logged in as a service account and the service account has been disabled, or it has no permissions to connect via the endpoint used,
     * - logged in as a user, then assumed a service account and either the user or the service account has been disabled,
     * or either the user or the service account has no permissions to connect via the endpoint used,
     * or access to the service account has been revoked from the user
     */
    void checkEntityEnabled();

    /**
     * Clears per-statement state stored in the security context, e.g. entity defined by OWNED BY clause.
     */
    @Override
    default void clear() {
        // no-op
    }

    default CharSequence getAssumedServiceAccount() {
        final CharSequence principal = getPrincipal();
        final CharSequence sessionPrincipal = getSessionPrincipal();
        return sessionPrincipal == null || sessionPrincipal.equals(principal) ? null : principal;
    }

    /**
     * User account used for permission checks, i.e. the session user account
     * or the service account defined by an executed ASSUME statement.
     */
    CharSequence getPrincipal();

    /**
     * User account used in initial authentication, i.e. to start the session.
     */
    default CharSequence getSessionPrincipal() {
        return getPrincipal();
    }

    /**
     * User account is stored in an external system, e.g. OpenID Connect provider.
     */
    default boolean isExternal() {
        return false;
    }

    default boolean isQueryCancellationAllowed() {
        return true;
    }

    boolean isSystemAdmin();
}
