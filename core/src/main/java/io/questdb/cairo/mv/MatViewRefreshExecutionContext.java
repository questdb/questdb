/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo.mv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.functions.bind.IndexedParameterLinkFunction;
import io.questdb.griffin.model.IntrinsicModel;
import org.jetbrains.annotations.NotNull;

public class MatViewRefreshExecutionContext extends SqlExecutionContextImpl {
    private TableReader baseTableReader;
    private TableToken viewTableToken;

    public MatViewRefreshExecutionContext(CairoEngine engine, int workerCount, int sharedWorkerCount) {
        super(engine, workerCount, sharedWorkerCount);
        if (!engine.getConfiguration().isMatViewParallelSqlEnabled()) {
            setParallelFilterEnabled(false);
            setParallelGroupByEnabled(false);
            setParallelReadParquetEnabled(false);
        }
        with(
                new ReadOnlySecurityContext() {
                    @Override
                    public void authorizeInsert(TableToken tableToken) {
                        if (!tableToken.equals(viewTableToken)) {
                            throw CairoException.authorization().put("Write permission denied").setCacheable(true);
                        }
                    }
                },
                new BindVariableServiceImpl(engine.getConfiguration())
        );
    }

    @Override
    public @NotNull SqlExecutionCircuitBreaker getCircuitBreaker() {
        return getSimpleCircuitBreaker(); // mat view refresh should use cancellable circuit breaker instead of no-op
    }

    @Override
    public TableReader getReader(TableToken tableToken, long version) {
        if (tableToken.equals(baseTableReader.getTableToken())) {
            // The base table reader is fixed throughout the mat view refresh.
            if (version > -1 && baseTableReader.getMetadataVersion() != version) {
                final int tableId = tableToken.getTableId();
                throw TableReferenceOutOfDateException.of(
                        tableToken,
                        tableId,
                        tableId,
                        version,
                        baseTableReader.getMetadataVersion()
                );
            }
            return baseTableReader;
        }
        return getCairoEngine().getReader(tableToken, version);
    }

    @Override
    public TableReader getReader(TableToken tableToken) {
        if (tableToken.equals(baseTableReader.getTableToken())) {
            // The base table reader is fixed throughout the mat view refresh.
            return baseTableReader;
        }
        return getCairoEngine().getReader(tableToken);
    }

    @Override
    public boolean isOverriddenIntrinsics(TableToken tableToken) {
        return tableToken == baseTableReader.getTableToken();
    }

    public void of(TableReader baseTableReader) {
        this.viewTableToken = baseTableReader.getTableToken();
        this.baseTableReader = baseTableReader;
    }

    @Override
    public void overrideWhereIntrinsics(TableToken tableToken, IntrinsicModel intrinsicModel) {
        if (tableToken != baseTableReader.getTableToken()) {
            return;
        }
        // Cannot re-use function instances, they will be cached in the query plan
        // and then can be re-used in another execution context.
        intrinsicModel.setBetweenBoundary(new IndexedParameterLinkFunction(1, ColumnType.TIMESTAMP, 0));
        intrinsicModel.setBetweenBoundary(new IndexedParameterLinkFunction(2, ColumnType.TIMESTAMP, 0));
    }

    public void setRange(long minTs, long maxTs) throws SqlException {
        getBindVariableService().setTimestamp(1, minTs);
        getBindVariableService().setTimestamp(2, maxTs - 1);
    }
}
