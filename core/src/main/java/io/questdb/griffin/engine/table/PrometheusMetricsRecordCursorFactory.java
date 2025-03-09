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

package io.questdb.griffin.engine.table;

import io.questdb.Metrics;
import io.questdb.ServerMain;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.metrics.MetricsRegistry;
import io.questdb.metrics.Target;
import org.jetbrains.annotations.Nullable;

// todo(nwoolmer): figure out how to handle labeled case
// maybe requires some name mangling
public final class PrometheusMetricsRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final RecordMetadata METADATA;
    private final PrometheusMetricsCursor prometheusMetricsCursor = new PrometheusMetricsCursor();

    public PrometheusMetricsRecordCursorFactory() {
        super(METADATA);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext sqlExecutionContext) {
        prometheusMetricsCursor.of(sqlExecutionContext.getCairoEngine().getMetrics());
        return prometheusMetricsCursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("prometheus_metrics");
    }

    public class PrometheusMetricsCursor implements RecordCursor {
        private static final int NAME = 0;
        private static final int TYPE = NAME + 1;
        private static final int VALUE = TYPE + 1;
        private static final int KIND = VALUE + 1;
        PrometheusMetricsRecord record = new PrometheusMetricsRecord();
        private int pos;
        private MetricsRegistry registry;
        private int size;

        public PrometheusMetricsCursor() {
        }

        @Override
        public void close() {

        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public Record getRecordB() {
            return null;
        }

        @Override
        public boolean hasNext() throws DataUnavailableException {
            pos++;
            if (pos < size) {
                record.of(registry.getTarget(pos));
                if (record.target instanceof ServerMain) {
                    return hasNext();
                }
                return true;
            }
            return false;
        }

        public void of(Metrics metrics) {
            this.registry = metrics.getRegistry();
            this.size = registry.getSize();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() throws DataUnavailableException {
            return size;
        }

        @Override
        public void toTop() {
            pos = -1;
        }

        public class PrometheusMetricsRecord implements Record {
            Target target;

            @Override
            public long getRowId() {
                return Record.super.getRowId();
            }

            @Override
            public @Nullable CharSequence getStrA(int col) {
                if (col == VALUE) {
                    return target.getValueAsString();
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public int getStrLen(int col) {
                if (col == VALUE) {
                    return target.getValueAsString().length();
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public CharSequence getSymA(int col) {
                switch (col) {
                    case NAME:
                        return target.getName();
                    case TYPE:
                        return target.getType();
                    case KIND:
                        return target.getValueType();
                }
                throw new UnsupportedOperationException();
            }

            public void of(Target target) {
                this.target = target;
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("name", ColumnType.SYMBOL));
        metadata.add(new TableColumnMetadata("type", ColumnType.SYMBOL));
        metadata.add(new TableColumnMetadata("value", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("kind", ColumnType.SYMBOL));
        METADATA = metadata;
    }

}

