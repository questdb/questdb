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

package io.questdb.cairo;

import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.QueryFutureUpdateListener;
import io.questdb.griffin.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.analytic.AnalyticContext;
import io.questdb.std.Rnd;
import org.jetbrains.annotations.Nullable;

public final class AllowAllSqlSecurityContext {
    public static final SqlExecutionContext INSTANCE = new SqlExecutionContext() {
        @Override
        public QueryFutureUpdateListener getQueryFutureUpdateListener() {
            return QueryFutureUpdateListener.EMPTY;
        }

        @Override
        public BindVariableService getBindVariableService() {
            return null;
        }

        @Override
        public CairoSecurityContext getCairoSecurityContext() {
            return AllowAllCairoSecurityContext.INSTANCE;
        }

        @Override
        public boolean isTimestampRequired() {
            return false;
        }

        @Override
        public void popTimestampRequiredFlag() {
        }

        @Override
        public void pushTimestampRequiredFlag(boolean flag) {
        }

        @Override
        public int getWorkerCount() {
            return 0;
        }

        @Override
        public Rnd getRandom() {
            return null;
        }

        @Override
        public void setRandom(Rnd rnd) {

        }

        @Override
        public CairoEngine getCairoEngine() {
            return null;
        }

        @Override
        public long getRequestFd() {
            return 0;
        }

        @Override
        public SqlExecutionCircuitBreaker getCircuitBreaker() {
            return null;
        }

        @Override
        public void storeTelemetry(short event, short origin) {
        }

        @Override
        public AnalyticContext getAnalyticContext() {
            return null;
        }

        @Override
        public void configureAnalyticContext(
                @Nullable VirtualRecord partitionByRecord,
                @Nullable RecordSink partitionBySink,
                @Nullable ColumnTypes keyTypes,
                boolean isOrdered,
                boolean baseSupportsRandomAccess
        ) {
        }

        @Override
        public void initNow() {
        }

        @Override
        public long getNow() {
            return 0;
        }

        @Override
        public int getJitMode() {
            return SqlJitMode.JIT_MODE_ENABLED;
        }

        @Override
        public void setJitMode(int jitMode) {
        }
    };
}
