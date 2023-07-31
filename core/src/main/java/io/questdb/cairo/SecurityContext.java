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

import io.questdb.std.LongList;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("unused")
public interface SecurityContext {

    void assumeServiceAccount(CharSequence serviceAccountName);

    void authorizeAddPassword();

    void authorizeAddUser();

    void authorizeAlterTableAddColumn(TableToken tableToken);

    void authorizeAlterTableAddIndex(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames);

    void authorizeAlterTableAlterColumnCache(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames);

    void authorizeAlterTableAttachPartition(TableToken tableToken);

    void authorizeAlterTableDetachPartition(TableToken tableToken);

    void authorizeAlterTableDropColumn(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames);

    void authorizeAlterTableDropIndex(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames);

    void authorizeAlterTableDropPartition(TableToken tableToken);

    // the names are pairs from-to
    void authorizeAlterTableRenameColumn(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames);

    void authorizeAlterTableSetDedup(TableToken tableToken);

    void authorizeAlterTableSetType(TableToken tableToken);

    void authorizeAssignServiceAccount();

    void authorizeCopy();

    void authorizeCopyCancel(SecurityContext cancellingSecurityContext);

    void authorizeCreateGroup();

    void authorizeCreateJwk();

    void authorizeCreateServiceAccount();

    void authorizeCreateUser();

    void authorizeDatabaseSnapshot();

    void authorizeDisableUser();

    void authorizeDropGroup();

    void authorizeDropJwk();

    void authorizeDropServiceAccount();

    void authorizeDropUser();

    void authorizeEnableUser();

    void authorizeGrant(LongList permissions, CharSequence tableName, @NotNull ObjList<CharSequence> columns);

    // columnNames.size() = 0 means all columns
    void authorizeInsert(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames);

    // Add column over ILP/TCP.
    void authorizeLineAlterTableAddColumn(TableToken tableToken);

    // Insert over ILP/TCP.
    void authorizeLineInsert(TableToken tableToken);

    // Create table over ILP/TCP.
    void authorizeLineTableCreate();

    void authorizeRemovePassword();

    void authorizeRemoveUser();

    void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames);

    void authorizeTableBackup(ObjHashSet<TableToken> tableTokens);

    void authorizeTableCreate();

    void authorizeTableDrop(TableToken tableToken);

    // columnName = null means all columns
    void authorizeTableReindex(TableToken tableToken, @Nullable CharSequence columnName);

    void authorizeTableRename(TableToken tableToken);

    void authorizeTableTruncate(TableToken tableToken);

    void authorizeTableUpdate(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames);

    void authorizeTableVacuum(TableToken tableToken);

    void authorizeUnassignServiceAccount();

    void exitServiceAccount(CharSequence serviceAccountName);

    String getPrincipal();
}
