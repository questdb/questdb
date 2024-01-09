/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

public interface SecurityContext {
    // Implementations are free to define unique authentication types.
    // The user authenticated with credentials.
    byte AUTH_TYPE_CREDENTIALS = 1;
    // The user authenticated with a JWK token.
    byte AUTH_TYPE_JWK_TOKEN = 2;
    // The context is not aware of authentication types.
    byte AUTH_TYPE_NONE = 0;

    void authorizeAdminAction();

    void authorizeAlterTableAddColumn(TableToken tableToken);

    void authorizeAlterTableAddIndex(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames);

    void authorizeAlterTableAlterColumnCache(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames);

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

    default void authorizeCancelQuery() {
    }

    void authorizeCopyCancel(SecurityContext cancellingSecurityContext);

    void authorizeDatabaseSnapshot();

    void authorizeHttp();

    void authorizeInsert(TableToken tableToken);

    void authorizeLineTcp();

    void authorizePGWire();

    void authorizeResumeWal(TableToken tableToken);

    void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames);

    void authorizeSelectOnAnyColumn(TableToken tableToken);

    void authorizeTableBackup(ObjHashSet<TableToken> tableTokens);

    void authorizeTableCreate();

    void authorizeTableDrop(TableToken tableToken);

    // columnNames - empty means all indexed columns
    void authorizeTableReindex(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames);

    void authorizeTableRename(TableToken tableToken);

    void authorizeTableTruncate(TableToken tableToken);

    void authorizeTableUpdate(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames);

    void authorizeTableVacuum(TableToken tableToken);

    /**
     * Should throw an exception if:
     * - logged in as a user and the user has been disabled, or it has no permissions to connect via the endpoint used,
     * - logged in as a service account and the service account has been disabled, or it has no permissions to connect via the endpoint used,
     * - logged in as a user, then assumed a service account and either the user or the service account has been disabled,
     * or either the user or the service account has no permissions to connect via the endpoint used,
     * or access to the service account has been revoked from the user
     */
    void checkEntityEnabled();

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
}
