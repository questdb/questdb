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
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

class WalApplySqlExecutionContext extends SqlExecutionContextImpl {
    private TableToken tableToken;

    WalApplySqlExecutionContext(CairoEngine cairoEngine, int workerCount, int sharedWorkerCount) {
        super(cairoEngine, workerCount, sharedWorkerCount);
    }

    @Override
    public @NotNull SqlExecutionCircuitBreaker getCircuitBreaker() {
        return getSimpleCircuitBreaker();//wal operations should use cancellable circuit breaker instead of noop
    }

    public TableMetadata getMetadataForWrite(TableToken tableToken, long desiredVersion) {
        // When WAL is applied and SQL is re-compiled
        // the correct metadata for writing is reader metadata,
        // because the sequencer metadata looks at the future.
        return getCairoEngine().getTableMetadata(this.tableToken, desiredVersion);
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
    public int getTableStatus(Path path, CharSequence tableName) {
        return getCairoEngine().getTableStatus(path, this.tableToken.getTableName());
    }

    @Override
    public int getTableStatus(Path path, TableToken tableToken) {
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
    public boolean isUninterruptible() {
        return true;
    }

    @Override
    public boolean isWalApplication() {
        return true;
    }

    public void remapTableNameResolutionTo(TableToken tableToken) {
        this.tableToken = tableToken;
    }
}
