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
import org.jetbrains.annotations.Nullable;

public interface SecurityContext {

    default void authorizeAlterTableAddColumn(TableToken tableToken) {
    }

    // the names are pairs from-to
    default void authorizeAlterTableRenameColumns(TableToken tableToken, ObjList<CharSequence> columnNames) {
    }

    default void authorizeAlterTableSetType(TableToken tableToken) {
    }

    default void authorizeAlterTableAlterColumn(TableToken tableToken, ObjList<CharSequence> columnNames) {
    }

    default void authorizeAlterTableDropColumn(TableToken tableToken, ObjList<CharSequence> columnNames) {
    }

    // when insert SQL doesn't specify any columns (this means all columns) the 'columnName' list
    // will be empty
    default void authorizeInsert(TableToken tableToken, ObjList<CharSequence> columnNames) {
    }

    default void authorizeSelect(TableToken tableToken, ObjList<CharSequence> columnNames) {
    }

    default void authorizeTableBackup(ObjHashSet<TableToken> tableTokens) {
    }

    default void authorizeTableCreate(CharSequence tableName) {
    }

    default void authorizeCopyCancel(SecurityContext cancellingSecurityContext) {
    }

    default void authorizeCopyExecute() {
    }

    default void authorizeDatabaseSnapshot() {
    }

    default void authorizeTableDrop(TableToken tableToken) {
    }

    default void authorizeGrant(TableToken tableToken) {
    }

    default void authorizeTableReindex(TableToken tableToken, @Nullable CharSequence columnName) {
    }

    default void authorizeTableRename(TableToken tableToken) {
    }

    default void authorizeTableTruncate(TableToken tableToken) {
    }

    default void authorizeTableVacuum(TableToken tableToken) {
    }

    default void authorizeTableUpdate(TableToken tableToken, ObjList<CharSequence> columnNames) {
    }
}
