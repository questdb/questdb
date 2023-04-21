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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.str.Path;

class TableRenameSupportExecutionContext extends SqlExecutionContextImpl {
    private TableToken tableToken;

    TableRenameSupportExecutionContext(CairoEngine cairoEngine, int workerCount, int sharedWorkerCount) {
        super(cairoEngine, workerCount, sharedWorkerCount);
    }

    @Override
    public TableRecordMetadata getMetadata(TableToken tableToken) {
        final CairoEngine engine = getCairoEngine();
        return engine.getMetadata(this.tableToken);
    }

    @Override
    public TableRecordMetadata getMetadata(TableToken tableToken, long structureVersion) {
        final CairoEngine engine = getCairoEngine();
        return engine.getMetadata(this.tableToken, structureVersion);
    }

    @Override
    public TableReader getReader(TableToken tableToken, long version) {
        return getCairoEngine().getReader(this.tableToken, version);
    }

    @Override
    public TableReader getReader(TableToken tableName) {
        return getCairoEngine().getReader(this.tableToken);
    }

    @Override
    public int getTableStatus(Path path, TableToken tableName) {
        return getCairoEngine().getTableStatus(path, this.tableToken);
    }

    @Override
    public TableToken getTableToken(CharSequence tableName, int lo, int hi) {
        return this.tableToken;
    }

    @Override
    public TableToken getTableToken(CharSequence tableName) {
        return tableToken;
    }

    @Override
    public TableToken getTableTokenIfExists(CharSequence tableName) {
        return this.tableToken;
    }

    @Override
    public TableToken getTableTokenIfExists(CharSequence tableName, int lo, int hi) {
        return this.tableToken;
    }

    @Override
    public boolean isWalApplication() {
        return true;
    }

    public void remapTableNameResolutionTo(TableToken tableToken) {
        this.tableToken = tableToken;
    }
}
