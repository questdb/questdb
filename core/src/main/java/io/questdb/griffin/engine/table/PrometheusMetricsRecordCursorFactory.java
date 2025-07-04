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
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.metrics.MetricsRegistry;
import io.questdb.metrics.PrometheusFormatUtils;
import io.questdb.metrics.Target;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;

import static io.questdb.metrics.MemoryTagLongGauge.MEMORY_TAG_PREFIX;

public final class PrometheusMetricsRecordCursorFactory extends AbstractRecordCursorFactory {
    public static final RecordMetadata METADATA;
    private final PrometheusMetricsCursor cursor;
    private final PrometheusMetricsRecord record;

    public PrometheusMetricsRecordCursorFactory(CairoConfiguration configuration) throws SqlException {
        super(METADATA);
        if (!configuration.getMetrics().isEnabled()) {
            throw SqlException.$(0, "metrics are disabled! try setting `metrics.enabled=true` in `server.conf`");
        }

        DirectUtf8Sink sink = new DirectUtf8Sink(255);
        LongList values = new LongList(10);
        DirectUtf8String value = new DirectUtf8String();
        record = new PrometheusMetricsRecord(sink, values, value);
        cursor = new PrometheusMetricsCursor(record);
    }

    @Override
    public void _close() {
        record.close();
        cursor.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext sqlExecutionContext) {
        cursor.of(sqlExecutionContext.getCairoEngine().getMetrics());
        cursor.toTop();
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("prometheus_metrics");
    }

    public static class PrometheusMetricsCursor implements RecordCursor {
        PrometheusMetricsRecord record;
        private int pos;
        private MetricsRegistry registry;
        private int size;
        private int subLimit;
        private int subPos;

        public PrometheusMetricsCursor(PrometheusMetricsRecord record) {
            this.record = record;
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
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() throws DataUnavailableException {
            if (subPos > -1 && subPos < subLimit - 1) {
                subPos++;
                record.of(registry.getTarget(pos), subPos);
                return true;
            }

            subPos = -1;
            subLimit = -1;

            pos++;
            if (pos < size) {
                try {
                    int amount = record.of(registry.getTarget(pos));
                    if (amount > 1) {
                        subPos = 0;
                        subLimit = amount;
                    }
                    return true;
                } catch (UnsupportedOperationException e) {
                    return hasNext();
                }
            }
            return false;
        }

        public void of(Metrics metrics) {
            of(metrics.getRegistry());
        }

        public void of(MetricsRegistry registry) {
            this.registry = registry;
            this.size = registry.getSize();
            this.toTop();
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() throws DataUnavailableException {
            return -1;
        }

        @Override
        public void toTop() {
            pos = -1;
            subPos = -1;
            subLimit = -1;
            if (record.isClosed) {
                Misc.free(record.sink);
                record.sink = new DirectUtf8Sink(255);
                record.isClosed = false;
            }
        }
    }

    public class PrometheusMetricsRecord implements Record {
        public static final int NAME = 0;
        public static final int TYPE = NAME + 1;
        public static final int VALUE = TYPE + 1;
        public static final int KIND = VALUE + 1;
        public static final int LABELS = KIND + 1;
        public DirectUtf8Sink sink;
        boolean isClosed;
        Target target;
        DirectUtf8String value;
        LongList values;

        public PrometheusMetricsRecord(DirectUtf8Sink sink, LongList values, DirectUtf8String value) {
            this.sink = sink;
            this.values = values;
            this.value = value;
            isClosed = false;
        }

        public void close() {
            this.values.clear();
            this.value.clear();
            this.sink.close();
            isClosed = true;
        }

        public Utf8Sequence getProp(int slot) {
            final long lo = getLo(slot), hi = getHi(slot);
            if (lo == -1 || hi == -1 || lo == hi) {
                return Utf8String.EMPTY;
            } else {
                return value.of(getLo(slot), getHi(slot), sink.isAscii());
            }
        }

        @Override
        public long getRowId() {
            return Record.super.getRowId();
        }

        @Override
        public @Nullable Utf8Sequence getVarcharA(int col) {
            switch (col) {
                case VALUE:
                case NAME:
                case TYPE:
                case KIND:
                case LABELS:
                    return getProp(col);
            }
            throw new UnsupportedOperationException();
        }

        @Override
        public int getVarcharSize(int col) {
            return Objects.requireNonNull(getVarcharA(col)).size();
        }

        public int of(Target target) {
            this.target = target;
            sink.clear();
            this.values.setAll(10, -1);
            return this.target.scrapeIntoRecord(this);
        }

        public void of(Target target, int index) {
            this.target = target;
            sink.clear();
            this.values.setAll(10, -1);
            this.target.scrapeIntoRecord(this, index);
        }

        public PrometheusMetricsRecord setCounterName(CharSequence name) {
            setLo(NAME, sink.hi());
            PrometheusFormatUtils.appendCounterNamePrefix(name, sink);
            setHi(NAME, sink.hi());
            return this;
        }

        public PrometheusMetricsRecord setGaugeName(CharSequence name) {
            setLo(NAME, sink.hi());
            sink.putAscii(PrometheusFormatUtils.METRIC_NAME_PREFIX);
            sink.putAscii(name);
            setHi(NAME, sink.hi());
            return this;
        }

        public PrometheusMetricsRecord setKind(CharSequence type) {
            return setProp(KIND, type);
        }

        // replace with array of strings later
        public PrometheusMetricsRecord setLabels(CharSequence... labels) {
            setLo(LABELS, sink.hi());
            sink.putAscii("{ ");
            for (int i = 0, n = labels.length; i < n; i += 2) {
                sink.putAscii('"');
                sink.putAscii(labels[i]);
                sink.putAscii("\" : \"");
                sink.putAscii(labels[i + 1]);
                sink.putAscii('"');
                if (i + 2 < n) {
                    sink.putAscii(", ");
                }
            }
            sink.putAscii(" }");
            setHi(LABELS, sink.hi());
            return this;
        }

        public PrometheusMetricsRecord setMemoryTagName(CharSequence name) {
            setLo(NAME, sink.hi());
            sink.putAscii(PrometheusFormatUtils.METRIC_NAME_PREFIX);
            sink.putAscii(MEMORY_TAG_PREFIX);
            sink.putAscii(name);
            setHi(NAME, sink.hi());
            return this;
        }

        public PrometheusMetricsRecord setName(CharSequence name) {
            return setProp(NAME, name);
        }

        // assume ascii by convention
        public PrometheusMetricsRecord setProp(int slot, CharSequence value) {
            setLo(slot, sink.hi());
            sink.putAscii(value);
            setHi(slot, sink.hi());
            return this;
        }

        public PrometheusMetricsRecord setType(CharSequence type) {
            return setProp(TYPE, type);
        }

        public PrometheusMetricsRecord setValue(long l) {
            setLo(VALUE, sink.hi());
            sink.put(l);
            setHi(VALUE, sink.hi());
            return this;
        }

        public PrometheusMetricsRecord setValue(double d) {
            setLo(VALUE, sink.hi());
            sink.put(d);
            setHi(VALUE, sink.hi());
            return this;
        }

        long getHi(int slot) {
            return values.get(slot * 2 + 1);
        }

        long getLo(int slot) {
            return values.get(slot * 2);
        }

        void setHi(int slot, long hi) {
            values.set(slot * 2 + 1, hi);
        }

        void setLo(int slot, long lo) {
            values.set(slot * 2, lo);
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("name", ColumnType.VARCHAR));
        metadata.add(new TableColumnMetadata("type", ColumnType.VARCHAR));
        metadata.add(new TableColumnMetadata("value", ColumnType.VARCHAR));
        metadata.add(new TableColumnMetadata("kind", ColumnType.VARCHAR));
        metadata.add(new TableColumnMetadata("labels", ColumnType.VARCHAR));
        METADATA = metadata;
    }

}

