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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.functions.bind.IndexedParameterLinkFunction;
import io.questdb.griffin.model.IntrinsicModel;

/**
 * Execution context used by {@link LiveViewRefreshJob} when compiling and running
 * the view's base SELECT during bootstrap.
 * <p>
 * Two responsibilities:
 * <ul>
 *     <li>Inject a runtime {@code BETWEEN :1 AND :2} timestamp intrinsic on the base
 *     table so the partition-frame cursor prunes partitions outside the backfill
 *     window. The lower bound is updated per refresh via {@link #setRange}; the
 *     upper bound is fixed to {@link Long#MAX_VALUE} and stored as
 *     {@code Long.MAX_VALUE - 1} per {@code BETWEEN}'s inclusive-hi semantics.</li>
 *     <li>Pin the base table {@link TableReader} for the duration of compile and
 *     cursor execution so SQL machinery's {@code getReader} calls return a
 *     snapshot at a consistent transaction.</li>
 * </ul>
 * Mirrors {@code MatViewRefreshSqlExecutionContext}.
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

    @Override
    public boolean isOverriddenIntrinsics(TableToken tableToken) {
        return baseTableReader != null && tableToken.equals(baseTableReader.getTableToken());
    }

    public void of(TableReader baseTableReader) {
        this.baseTableReader = baseTableReader;
    }

    @Override
    public void overrideWhereIntrinsics(TableToken tableToken, IntrinsicModel intrinsicModel, int timestampType) {
        if (baseTableReader == null || !tableToken.equals(baseTableReader.getTableToken())) {
            return;
        }
        // Cannot reuse function instances; the planner caches them in the compiled
        // factory and would resurface them in unrelated execution contexts.
        intrinsicModel.setBetweenBoundary(new IndexedParameterLinkFunction(1, timestampType, 0));
        intrinsicModel.setBetweenBoundary(new IndexedParameterLinkFunction(2, timestampType, 0));
    }

    /**
     * Updates the BETWEEN intrinsic's lower and upper boundaries. {@code tsLo} is
     * inclusive; {@code tsHi} is exclusive at the API level and is stored as
     * {@code tsHi - 1} to match {@code BETWEEN}'s inclusive-hi semantics. Callers
     * that want strict-greater-than semantics on the lower end should pass
     * {@code lowerBound + 1}.
     */
    public void setRange(long tsLo, long tsHi, int timestampType) throws SqlException {
        getBindVariableService().setTimestampWithType(1, timestampType, tsLo);
        getBindVariableService().setTimestampWithType(2, timestampType, tsHi - 1);
    }
}
