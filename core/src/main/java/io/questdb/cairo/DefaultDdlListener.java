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

public class DefaultDdlListener implements DdlListener {
    public static final DdlListener INSTANCE = new DefaultDdlListener();

    @Override
    public void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName) {
    }

    @Override
    public void onColumnDropped(TableToken tableToken, CharSequence columnName, boolean cascadePermissions) {
    }

    @Override
    public void onColumnRenamed(TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName) {
    }

    @Override
    public void onTableOrViewOrMatViewCreated(SecurityContext securityContext, TableToken tableToken, int tableKind) {
    }

    @Override
    public void onTableDropped(String tableName, boolean cascadePermissions) {
    }

    @Override
    public void onTableRenamed(TableToken oldTableToken, TableToken newTableToken) {
    }
}
