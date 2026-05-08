/*+*****************************************************************************
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

package io.questdb.cairo.lv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;

/**
 * Execution context used by {@link LiveViewRefreshJob} when compiling and running
 * the view's base SELECT during refresh. Pins the base table {@link TableReader}
 * for the duration of compile and cursor execution so SQL machinery's
 * {@code getReader} calls return a snapshot at a consistent transaction.
 */
public class LiveViewRefreshSqlExecutionContext extends SqlExecutionContextImpl {

    private TableReader baseTableReader;

    public LiveViewRefreshSqlExecutionContext(CairoEngine engine, int sharedQueryWorkerCount) {
        super(engine, sharedQueryWorkerCount);
        this.securityContext = AllowAllSecurityContext.INSTANCE;
        this.bindVariableService = new BindVariableServiceImpl(engine.getConfiguration());
    }

    public void clearReader() {
        this.baseTableReader = null;
    }

    @Override
    public TableReader getReader(TableToken tableToken, long version) {
        if (baseTableReader != null && tableToken.equals(baseTableReader.getTableToken())) {
            return getCairoEngine().getReaderAtTxn(baseTableReader);
        }
        return getCairoEngine().getReader(tableToken, version);
    }

    @Override
    public TableReader getReader(TableToken tableToken) {
        if (baseTableReader != null && tableToken.equals(baseTableReader.getTableToken())) {
            return getCairoEngine().getReaderAtTxn(baseTableReader);
        }
        return getCairoEngine().getReader(tableToken);
    }

    public boolean hasReader() {
        return baseTableReader != null;
    }

    public void of(TableReader baseTableReader) {
        this.baseTableReader = baseTableReader;
    }
}
