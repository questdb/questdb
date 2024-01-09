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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.engine.functions.catalogue.Constants;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

public class ReadOnlySecurityContext implements SecurityContext {
    public static final ReadOnlySecurityContext INSTANCE = new ReadOnlySecurityContext();

    protected ReadOnlySecurityContext() {
    }

    @Override
    public void authorizeAdminAction() {
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
    public void authorizeAlterTableAttachPartition(TableToken tableToken) {
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
    public void authorizeAlterTableSetType(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeCancelQuery() {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeCopyCancel(SecurityContext cancellingSecurityContext) {
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
    public void authorizePGWire() {
    }

    @Override
    public void authorizeResumeWal(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
    }

    @Override
    public void authorizeSelectOnAnyColumn(TableToken tableToken) {
    }

    @Override
    public void authorizeTableBackup(ObjHashSet<TableToken> tableTokens) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
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
    public void checkEntityEnabled() {
    }

    @Override
    public CharSequence getPrincipal() {
        return Constants.USER_NAME;
    }
}
