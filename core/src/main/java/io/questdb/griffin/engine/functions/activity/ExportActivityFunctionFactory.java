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

package io.questdb.griffin.engine.functions.activity;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

import java.util.Objects;

@SuppressWarnings("unused")
public class ExportActivityFunctionFactory implements FunctionFactory {
    private static final RecordMetadata METADATA;
    private static final String SIGNATURE = "export_activity()";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new CursorFunction(new ExportActivityCursorFactory(METADATA, sqlExecutionContext));
    }

    private static class ExportActivityCursor implements NoRandomAccessRecordCursor {
        private final CopyExportContext copyExportContext;
        private final CopyExportContext.ExportTaskData entry;
        private final LongList entryIds = new LongList();
        private final ExportActivityRecord record = new ExportActivityRecord();
        private int entryIndex;
        private boolean isAdmin;
        private CharSequence principal;
        private int size;

        private ExportActivityCursor(SqlExecutionContext executionContext) {
            copyExportContext = executionContext.getCairoEngine().getCopyExportContext();
            entry = new CopyExportContext.ExportTaskData();
        }

        @Override
        public void close() {
            entryIds.clear();
            isAdmin = false;
            principal = null;
            size = 0;
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            while (++entryIndex < size) {
                if (copyExportContext.getAndCopyEntry(entryIds.get(entryIndex), entry)) {
                    if (isAdmin || Objects.equals(entry.getPrincipal(), principal)) {
                        return true;
                    }
                }
            }
            return false;
        }

        public void of(SqlExecutionContext executionContext) {
            try {
                executionContext.getSecurityContext().authorizeSqlEngineAdmin();
                isAdmin = true;
            } catch (CairoException e) {
                isAdmin = false;
                principal = executionContext.getSecurityContext().getPrincipal();
            }
            entryIds.clear();
            copyExportContext.getActiveExportIds(entryIds);
            size = entryIds.size();
            toTop();
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            entryIndex = -1;
        }

        private class ExportActivityRecord implements Record {
            private final StringSink idSink = new StringSink();
            private final StringSink msgSink = new StringSink();

            @Override
            public boolean getBool(int col) {
                return Record.super.getBool(col);
            }

            @Override
            public long getLong(int col) {
                if (col == 1) {
                    return entry.getWorkerId();
                }

                return Record.super.getLong(col);
            }

            @Override
            public CharSequence getStrA(int col) {
                switch (col) {
                    case 0: // export id
                        idSink.clear();
                        Numbers.appendHex(idSink, entry.getId(), true);
                        return idSink;
                    case 2: // username
                        return entry.getPrincipal();
                    case 4: // phase
                        return entry.getPhase() != null ? entry.getPhase().getName() : null;
                    case 5: // request source
                        return entry.getTrigger();
                    case 6: // export path
                        return entry.getFileName();
                    case 7: // export sql
                        return entry.getSqlText();
                    case 8: // message
                        if (entry.getPhase() == null) {
                            return null;
                        }
                        switch (entry.getPhase()) {
                            case POPULATING_TEMP_TABLE:
                                msgSink.clear();
                                msgSink.putAscii("rows: ").put(entry.getPopulatedRowCount());
                                if (entry.getTotalRowCount() > 0) {
                                    msgSink.putAscii(" / ").put(entry.getTotalRowCount());
                                }
                                return msgSink;
                            case CONVERTING_PARTITIONS:
                                msgSink.clear();
                                msgSink.putAscii("finish partition count: ").put(entry.getFinishedPartitionCount()).putAscii(" / ").put(entry.getTotalPartitionCount());
                                return msgSink;
                            case STREAM_SENDING_DATA:
                                msgSink.clear();
                                msgSink.putAscii("exported rows: ").put(entry.getStreamingSendRowCount());
                                return msgSink;
                            default:
                                return null;
                        }
                    default:
                        return Record.super.getStrA(col);
                }
            }

            @Override
            public CharSequence getStrB(int col) {
                return getStrA(col);
            }

            @Override
            public int getStrLen(int col) {
                return TableUtils.lengthOf(getStrA(col));
            }

            @Override
            public long getTimestamp(int col) {
                if (col == 3) {
                    return entry.getStartTime();
                }

                return Record.super.getTimestamp(col);
            }
        }
    }

    private static class ExportActivityCursorFactory extends AbstractRecordCursorFactory {

        private final ExportActivityCursor cursor;

        public ExportActivityCursorFactory(RecordMetadata metadata, SqlExecutionContext sqlExecutionContext) {
            super(metadata);
            cursor = new ExportActivityCursor(sqlExecutionContext);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            cursor.of(executionContext);
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SIGNATURE);
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("export_id", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("worker_id", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("username", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("start_time", ColumnType.TIMESTAMP_MICRO));
        metadata.add(new TableColumnMetadata("phase", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("request_source", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("export_path", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("export_sql", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("message", ColumnType.STRING));
        METADATA = metadata;
    }
}