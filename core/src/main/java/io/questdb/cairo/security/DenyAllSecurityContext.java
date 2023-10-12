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
import io.questdb.cairo.TableToken;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

public class DenyAllSecurityContext extends ReadOnlySecurityContext {
    public static final DenyAllSecurityContext INSTANCE = new DenyAllSecurityContext();

    @Override
    public void authorizeAdminAction() {
        throw CairoException.nonCritical().put("permission denied");
    }

    @Override
    public void authorizeHTTP() {
        throw CairoException.nonCritical().put("permission denied");
    }

    @Override
    public void authorizeILP() {
        throw CairoException.nonCritical().put("permission denied");
    }

    @Override
    public void authorizePGWIRE() {
        throw CairoException.nonCritical().put("permission denied");
    }

    @Override
    public void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
        throw CairoException.nonCritical().put("permission denied");
    }

    @Override
    public void authorizeSelectOnAnyColumn(TableToken tableToken) {
        throw CairoException.nonCritical().put("permission denied");
    }

    @Override
    public void authorizeShowGroups() {
        throw CairoException.nonCritical().put("permission denied");
    }

    @Override
    public void authorizeShowGroups(CharSequence userName) {
        throw CairoException.nonCritical().put("permission denied");
    }

    @Override
    public void authorizeShowPermissions(CharSequence entityName) {
        throw CairoException.nonCritical().put("permission denied");
    }

    @Override
    public void authorizeShowServiceAccount(CharSequence serviceAccountName) {
        throw CairoException.nonCritical().put("permission denied");
    }

    @Override
    public void authorizeShowServiceAccounts() {
        throw CairoException.nonCritical().put("permission denied");
    }

    @Override
    public void authorizeShowServiceAccounts(CharSequence userOrGroupName) {
        throw CairoException.nonCritical().put("permission denied");
    }

    @Override
    public void authorizeShowUser(CharSequence userName) {
        throw CairoException.nonCritical().put("permission denied");
    }

    @Override
    public void authorizeShowUsers() {
        throw CairoException.nonCritical().put("permission denied");
    }
}
