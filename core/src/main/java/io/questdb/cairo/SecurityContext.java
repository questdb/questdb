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

    default void assumeRole(CharSequence roleName) {
    }

    default void authorizeAddPassword() {
    }

    default void authorizeAddUser() {
    }

    default void authorizeAlterTableAddColumn(TableToken tableToken) {
    }

    default void authorizeAlterTableAddIndex(TableToken tableToken, ObjList<CharSequence> columnNames) {
    }

    default void authorizeAlterTableAlterColumnCache(TableToken tableToken, ObjList<CharSequence> columnNames) {
    }

    default void authorizeAlterTableAttachPartition(TableToken tableToken) {
    }

    default void authorizeAlterTableDetachPartition(TableToken tableToken) {
    }

    default void authorizeAlterTableDropColumn(TableToken tableToken, ObjList<CharSequence> columnNames) {
    }

    default void authorizeAlterTableDropIndex(TableToken tableToken, ObjList<CharSequence> columnNames) {
    }

    default void authorizeAlterTableDropPartition(TableToken tableToken) {
    }

    // the names are pairs from-to
    default void authorizeAlterTableRenameColumn(TableToken tableToken, ObjList<CharSequence> columnNames) {
    }

    default void authorizeAlterTableSetType(TableToken tableToken) {
    }

    default void authorizeAssignRole() {
    }

    default void authorizeCopy() {
    }

    default void authorizeCopyCancel(SecurityContext cancellingSecurityContext) {
    }

    default void authorizeCreateGroup() {
    }

    default void authorizeCreateJwk() {
    }

    default void authorizeCreateRole() {
    }

    default void authorizeCreateUser() {
    }

    default void authorizeDatabaseSnapshot() {
    }

    default void authorizeDisableUser() {
    }

    default void authorizeDropGroup() {
    }

    default void authorizeDropJwk() {
    }

    default void authorizeDropRole() {
    }

    default void authorizeDropUser() {
    }

    default void authorizeEnableUser() {
    }

    @SuppressWarnings("unused")
    default void authorizeGrant(ObjHashSet<TableToken> tableTokens) {
    }

    // columnNames.size() = 0 means all columns
    default void authorizeInsert(TableToken tableToken, ObjList<CharSequence> columnNames) {
    }

    default void authorizeRemovePassword() {
    }

    default void authorizeRemoveUser() {
    }

    default void authorizeSelect(TableToken tableToken, ObjList<CharSequence> columnNames) {
    }

    default void authorizeTableBackup(ObjHashSet<TableToken> tableTokens) {
    }

    default void authorizeTableCreate() {
    }

    default void authorizeTableDrop(TableToken tableToken) {
    }

    // columnName = null means all columns
    default void authorizeTableReindex(TableToken tableToken, @Nullable CharSequence columnName) {
    }

    default void authorizeTableRename(TableToken tableToken) {
    }

    default void authorizeTableTruncate(TableToken tableToken) {
    }

    default void authorizeTableUpdate(TableToken tableToken, ObjList<CharSequence> columnNames) {
    }

    default void authorizeTableVacuum(TableToken tableToken) {
    }

    default void authorizeUnassignRole() {
    }

    default void exitRole(CharSequence roleName) {
    }
}
