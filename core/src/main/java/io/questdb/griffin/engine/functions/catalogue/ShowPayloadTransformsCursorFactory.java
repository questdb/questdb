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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.pt.PayloadTransformStore;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

/**
 * Cursor factory for {@code SHOW PAYLOAD TRANSFORMS}.
 * <p>
 * Access control: payload transform metadata (name, target table, SELECT body, DLQ
 * configuration) is intentionally readable by any authenticated user, mirroring the
 * convention used by all other {@code SHOW *} catalogue factories in this package
 * (e.g. {@code SHOW TABLES}, {@code SHOW DATE STYLE}). The {@code SecurityContext}
 * permission boundary is enforced at create/drop time via
 * {@link io.questdb.cairo.SecurityContext#authorizePayloadTransformCreate()} and
 * {@link io.questdb.cairo.SecurityContext#authorizePayloadTransformDrop()}; the
 * SHOW path does not introduce a new permission.
 */
public class ShowPayloadTransformsCursorFactory extends AbstractRecordCursorFactory {
    private static final GenericRecordMetadata METADATA = new GenericRecordMetadata();

    static {
        METADATA.add(new TableColumnMetadata("name", ColumnType.STRING));
        METADATA.add(new TableColumnMetadata("target_table", ColumnType.STRING));
        METADATA.add(new TableColumnMetadata("select_sql", ColumnType.STRING));
        METADATA.add(new TableColumnMetadata("dlq_table", ColumnType.STRING));
    }

    private RecordCursorFactory innerFactory;

    public ShowPayloadTransformsCursorFactory() {
        super(METADATA);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final CairoEngine engine = executionContext.getCairoEngine();
        final PayloadTransformStore store = engine.getPayloadTransformStore();

        // If the system table doesn't exist yet, return empty results
        if (engine.getTableTokenIfExists(store.getTableName()) == null) {
            return EmptyCursor.INSTANCE;
        }

        closeInnerFactory();
        final String sql = "SELECT name::STRING name, target_table, select_sql, dlq_table FROM ("
                + "SELECT name, target_table, select_sql, dlq_table, status FROM \""
                + store.getTableName()
                + "\" LATEST ON ts PARTITION BY name"
                + ") WHERE status = 'A'";
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            innerFactory = compiler.query().$(sql).compile(executionContext).getRecordCursorFactory();
        }
        return innerFactory.getCursor(executionContext);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("show payload transforms");
    }

    @Override
    protected void _close() {
        closeInnerFactory();
    }

    private void closeInnerFactory() {
        innerFactory = Misc.free(innerFactory);
    }

    private enum EmptyCursor implements NoRandomAccessRecordCursor {
        INSTANCE;

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return null;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public void toTop() {
        }
    }
}
