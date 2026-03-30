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
package io.questdb.griffin.engine.table;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.ev.ExpiringViewDefinition;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.NotNull;

public class ShowCreateExpiringViewRecordCursorFactory extends AbstractRecordCursorFactory {
    public static final int N_DDL_COL = 0;
    private static final long MICROS_PER_DAY = 86_400_000_000L;
    private static final long MICROS_PER_HOUR = 3_600_000_000L;
    private static final long MICROS_PER_MINUTE = 60_000_000L;
    private static final long MICROS_PER_SECOND = 1_000_000L;
    private static final long MICROS_PER_WEEK = 604_800_000_000L;
    private static final RecordMetadata METADATA;
    private final ShowCreateExpiringViewCursor cursor;

    public ShowCreateExpiringViewRecordCursorFactory(ExpiringViewDefinition def, int tokenPosition) {
        super(METADATA);
        this.cursor = new ShowCreateExpiringViewCursor(def);
    }

    static void appendHumanReadableDuration(Utf8StringSink sink, long micros) {
        if (micros > 0 && micros % MICROS_PER_WEEK == 0) {
            sink.put(micros / MICROS_PER_WEEK);
            sink.putAscii('w');
        } else if (micros % MICROS_PER_DAY == 0) {
            sink.put(micros / MICROS_PER_DAY);
            sink.putAscii('d');
        } else if (micros % MICROS_PER_HOUR == 0) {
            sink.put(micros / MICROS_PER_HOUR);
            sink.putAscii('h');
        } else if (micros % MICROS_PER_MINUTE == 0) {
            sink.put(micros / MICROS_PER_MINUTE);
            sink.putAscii('m');
        } else {
            sink.put(micros / MICROS_PER_SECOND);
            sink.putAscii('s');
        }
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.toTop();
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("show_create_expiring_view");
    }

    private static class ShowCreateExpiringViewCursor implements NoRandomAccessRecordCursor {
        private final ExpiringViewDefinition def;
        private final ShowCreateExpiringViewRecord record = new ShowCreateExpiringViewRecord();
        private final Utf8StringSink sink = new Utf8StringSink();
        private boolean hasRun;

        ShowCreateExpiringViewCursor(ExpiringViewDefinition def) {
            this.def = def;
        }

        @Override
        public void close() {
            sink.clear();
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (!hasRun) {
                sink.clear();
                buildDdl();
                hasRun = true;
                return true;
            }
            return false;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return 1;
        }

        @Override
        public void toTop() {
            sink.clear();
            hasRun = false;
        }

        private void buildDdl() {
            sink.putAscii("CREATE EXPIRING VIEW '");
            sink.put(def.getViewToken().getTableName());
            sink.putAscii("' ON ");
            sink.put(def.getBaseTableName());
            sink.putAscii(" EXPIRE WHEN ");
            sink.put(def.getExpiryPredicateSql());
            sink.putAscii(" CLEANUP INTERVAL ");
            appendHumanReadableDuration(sink, def.getCleanupIntervalMicros());
            sink.putAscii(';');
        }

        private class ShowCreateExpiringViewRecord implements Record {

            @Override
            @NotNull
            public Utf8Sequence getVarcharA(int col) {
                if (col == N_DDL_COL) {
                    return sink;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public Utf8Sequence getVarcharB(int col) {
                return getVarcharA(col);
            }

            @Override
            public int getVarcharSize(int col) {
                return getVarcharA(col).size();
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("ddl", ColumnType.VARCHAR));
        METADATA = metadata;
    }
}
