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
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.functions.bind.IndexedParameterLinkFunction;
import io.questdb.griffin.model.IntrinsicModel;
import io.questdb.std.Numbers;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

public class MatViewRefreshSqlExecutionContext extends SqlExecutionContextImpl {
    private TableReader baseTableReader;
    private TableToken viewTableToken;

    public MatViewRefreshSqlExecutionContext(CairoEngine engine, int sharedQueryWorkerCount) {
        super(engine, sharedQueryWorkerCount);
        if (!engine.getConfiguration().isMatViewParallelSqlEnabled()) {
            setParallelFilterEnabled(false);
            setParallelGroupByEnabled(false);
            setParallelTopKEnabled(false);
            setParallelReadParquetEnabled(false);
        }
        this.securityContext = new ReadOnlySecurityContext() {
            @Override
            public void authorizeInsert(TableToken tableToken) {
                if (!tableToken.equals(viewTableToken)) {
                    throw CairoException.authorization().put("Write permission denied").setCacheable(true);
                }
            }
        };
        this.bindVariableService = new BindVariableServiceImpl(engine.getConfiguration());
    }

    @Override
    public boolean allowNonDeterministicFunctions() {
        return false;
    }

    public void clearReader() {
        this.viewTableToken = null;
        this.baseTableReader = null;
    }

    @Override
    public @NotNull SqlExecutionCircuitBreaker getCircuitBreaker() {
        return getSimpleCircuitBreaker(); // mat view refresh should use cancellable circuit breaker instead of no-op
    }

    @Override
    public TableReader getReader(TableToken tableToken, long version) {
        if (tableToken.equals(baseTableReader.getTableToken())) {
            // Base table reader txn is fixed throughout the mat view refresh.
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
            return getCairoEngine().getReaderAtTxn(baseTableReader);
        }
        return getCairoEngine().getReader(tableToken, version);
    }

    @Override
    public TableReader getReader(TableToken tableToken) {
        if (tableToken.equals(baseTableReader.getTableToken())) {
            // Base table reader txn is fixed throughout the mat view refresh.
            return getCairoEngine().getReaderAtTxn(baseTableReader);
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
    public void overrideWhereIntrinsics(TableToken tableToken, IntrinsicModel intrinsicModel, int timestampType) {
        if (tableToken != baseTableReader.getTableToken()) {
            return;
        }
        // Cannot re-use function instances, they will be cached in the query plan
        // and then can be re-used in another execution context.
        intrinsicModel.setBetweenBoundary(new IndexedParameterLinkFunction(1, timestampType, 0));
        intrinsicModel.setBetweenBoundary(new IndexedParameterLinkFunction(2, timestampType, 0));
    }

    @Override
    public void setAllowNonDeterministicFunction(boolean value) {
        // no-op
    }

    // tsLo is inclusive, tsHi is exclusive
    public void setRange(long tsLo, long tsHi, int timestampType) throws SqlException {
        getBindVariableService().setTimestampWithType(1, timestampType, tsLo);
        getBindVariableService().setTimestampWithType(2, timestampType, tsHi - 1);
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        super.toSink(sink);
        if (baseTableReader != null) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(baseTableReader.getMetadata().getTimestampType());
            sink.putAscii(", refreshMinTs=").putISODate(driver, getTimestamp(1));
            sink.putAscii(", refreshMaxTs=").putISODate(driver, getTimestamp(2));
        }
    }

    private long getTimestamp(int index) {
        final Function func = getBindVariableService().getFunction(index);
        if (func == null || !ColumnType.isTimestamp(func.getType())) {
            return Numbers.LONG_NULL;
        }
        return func.getTimestamp(null);
    }
}
