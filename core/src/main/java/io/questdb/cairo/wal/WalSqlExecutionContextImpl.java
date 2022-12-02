/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import static io.questdb.cairo.TableUtils.TABLE_EXISTS;

public class WalSqlExecutionContextImpl extends SqlExecutionContextImpl {
    private TableToken tableToken;

    public WalSqlExecutionContextImpl(CairoEngine cairoEngine, int workerCount, int sharedWorkerCount) {
        super(cairoEngine, workerCount, sharedWorkerCount);
    }

    @Override
    public TableRecordMetadata getMetadata(CharSequence tableName) {
        return getCairoEngine().getMetadata(
                getCairoSecurityContext(),
                tableToken
        );
    }

    @Override
    public TableRecordMetadata getMetadata(CharSequence tableName, long structureVersion) {
        final CairoEngine engine = getCairoEngine();
        return engine.getMetadata(
                getCairoSecurityContext(),
                tableToken,
                structureVersion
        );
    }

    @Override
    public TableReader getReader(CharSequence tableName, int tableId, long version) {
        return getCairoEngine().getReaderByTableToken(
                getCairoSecurityContext(),
                tableToken
        );
    }

    @Override
    public TableReader getReader(CharSequence tableName) {
        return getCairoEngine().getReaderByTableToken(
                getCairoSecurityContext(),
                tableToken
        );
    }

    @Override
    public int getStatus(Path path, CharSequence tableName, int lo, int hi) {
        return TABLE_EXISTS;
    }

    @Override
    public int getStatus(Path path, CharSequence tableName) {
        return TABLE_EXISTS;
    }

    @Override
    public String getTableNameAsString(CharSequence tableName) {
        return tableToken.getTableName();
    }

    @Override
    public boolean isWalApplication() {
        return true;
    }

    public void remapTableNameResolutionTo(TableToken tableToken) {
        this.tableToken = tableToken;
    }
}
