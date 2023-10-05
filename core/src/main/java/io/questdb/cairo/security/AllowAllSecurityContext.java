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

package io.questdb.cairo.security;

import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.engine.functions.catalogue.Constants;
import io.questdb.std.LongList;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

public class AllowAllSecurityContext implements SecurityContext {
    public static final AllowAllSecurityContext INSTANCE = new AllowAllSecurityContext();

    private AllowAllSecurityContext() {
    }

    @Override
    public void assumeServiceAccount(CharSequence serviceAccountName) {
    }

    @Override
    public void authorizeAddPassword(CharSequence userOrServiceAccountName) {
    }

    @Override
    public void authorizeAddUser() {
    }

    @Override
    public void authorizeAdminAction() {
    }

    @Override
    public void authorizeAlterTableAddColumn(TableToken tableToken) {
    }

    @Override
    public void authorizeAlterTableAddIndex(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
    }

    @Override
    public void authorizeAlterTableAlterColumnCache(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
    }

    @Override
    public void authorizeAlterTableAttachPartition(TableToken tableToken) {
    }

    @Override
    public void authorizeAlterTableDedupDisable(TableToken tableToken) {
    }

    @Override
    public void authorizeAlterTableDedupEnable(TableToken tableToken) {
    }

    @Override
    public void authorizeAlterTableDetachPartition(TableToken tableToken) {
    }

    @Override
    public void authorizeAlterTableDropColumn(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
    }

    @Override
    public void authorizeAlterTableDropIndex(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
    }

    @Override
    public void authorizeAlterTableDropPartition(TableToken tableToken) {
    }

    @Override
    public void authorizeAlterTableRenameColumn(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
    }

    @Override
    public void authorizeAlterTableSetType(TableToken tableToken) {
    }

    @Override
    public void authorizeAssignServiceAccount(CharSequence serviceAccountName) {
    }

    @Override
    public void authorizeCopyCancel(SecurityContext cancellingSecurityContext) {
    }

    @Override
    public void authorizeCreateGroup() {
    }

    @Override
    public void authorizeCreateJwk(CharSequence userOrServiceAccountName) {
    }

    @Override
    public void authorizeCreateServiceAccount() {
    }

    @Override
    public void authorizeCreateUser() {
    }

    @Override
    public void authorizeDatabaseSnapshot() {
    }

    @Override
    public void authorizeDisableUser() {
    }

    @Override
    public void authorizeDropGroup() {
    }

    @Override
    public void authorizeDropJwk(CharSequence userOrServiceAccountName) {
    }

    @Override
    public void authorizeDropServiceAccount() {
    }

    @Override
    public void authorizeDropUser() {
    }

    @Override
    public void authorizeEnableUser() {
    }

    @Override
    public void authorizeGrant(LongList permissions, CharSequence tableName, @NotNull ObjList<CharSequence> columns) {
    }

    @Override
    public void authorizeInsert(TableToken tableToken) {
    }

    @Override
    public void authorizeRemovePassword(CharSequence userOrServiceAccountName) {
    }

    @Override
    public void authorizeRemoveUser() {
    }

    @Override
    public void authorizeResumeWal(TableToken tableToken) {
    }

    @Override
    public void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
    }

    @Override
    public void authorizeSelectOnAnyColumn(TableToken tableToken) {
    }

    @Override
    public void authorizeShowGroups() {
    }

    @Override
    public void authorizeShowGroups(CharSequence userName) {
    }

    @Override
    public void authorizeShowPermissions(CharSequence entityName) {
    }

    @Override
    public void authorizeShowServiceAccount(CharSequence serviceAccountName) {
    }

    @Override
    public void authorizeShowServiceAccounts() {
    }

    @Override
    public void authorizeShowServiceAccounts(CharSequence userOrGroupName) {
    }

    @Override
    public void authorizeShowUser(CharSequence userName) {
    }

    @Override
    public void authorizeShowUsers() {
    }

    @Override
    public void authorizeTableBackup(ObjHashSet<TableToken> tableTokens) {
    }

    @Override
    public void authorizeTableCreate() {
    }

    @Override
    public void authorizeTableDrop(TableToken tableToken) {
    }

    @Override
    public void authorizeTableReindex(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
    }

    @Override
    public void authorizeTableRename(TableToken tableToken) {
    }

    @Override
    public void authorizeTableTruncate(TableToken tableToken) {
    }

    @Override
    public void authorizeTableUpdate(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
    }

    @Override
    public void authorizeTableVacuum(TableToken tableToken) {
    }

    @Override
    public void authorizeUnassignServiceAccount(CharSequence serviceAccountName) {
    }

    @Override
    public void exitServiceAccount(CharSequence serviceAccountName) {
    }

    @Override
    public String getPrincipal() {
        return Constants.USER_NAME;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }
}
