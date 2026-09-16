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

package io.questdb.metrics;

import io.questdb.Metrics;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.QueryBuilder;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.StringSink;

import java.io.Closeable;
import java.util.Arrays;
import java.util.regex.Pattern;

public class MetricsPersistenceJob extends SynchronizedJob implements Closeable {
    public static final String TABLE_NAME = "sys.metrics";
    public static final String WRITER_LOCK_REASON = "metrics persistence";
    private static final Log LOG = LogFactory.getLog(MetricsPersistenceJob.class);
    private final MicrosecondClock clock;
    private final ObjList<MetricColumn> columns = new ObjList<>();
    private final MetricsConfiguration configuration;
    private final CairoEngine engine;
    private final Pattern excludePattern;
    private final Metrics metrics;
    private final StringSink nameSink = new StringSink();
    private final CharSequenceIntHashMap nameToIndex = new CharSequenceIntHashMap();
    private final SCSequence operationSequence = new SCSequence();
    private final double parquetBloomFilterFpp;
    private final MetricSnapshotVisitor sampleVisitor = new MetricSnapshotVisitor() {
        @Override
        public boolean isVirtualMetricsEnabled() {
            return isVirtualMetricsEnabled;
        }

        @Override
        public void visitDouble(CharSequence name, double value) {
            int index = findColumn(name, null, null);
            if (index < 0) {
                index = addColumn(name, MetricType.DOUBLE_GAUGE);
            }
            setDouble(index, value);
        }

        @Override
        public void visitLong(CharSequence name, MetricType type, long value) {
            int index = findColumn(name, null, null);
            if (index < 0) {
                index = addColumn(name, type);
            }
            setLong(index, type, value);
        }

        @Override
        public void visitLong(CharSequence name, MetricType type, CharSequence labelValue0, long value) {
            int index = findColumn(name, labelValue0, null);
            if (index < 0) {
                index = addColumn(name, type, labelValue0);
            }
            setLong(index, type, value);
        }

        @Override
        public void visitLong(
                CharSequence name,
                MetricType type,
                CharSequence labelValue0,
                CharSequence labelValue1,
                long value
        ) {
            int index = findColumn(name, labelValue0, labelValue1);
            if (index < 0) {
                index = addColumn(name, type, labelValue0, labelValue1);
            }
            setLong(index, type, value);
        }
    };
    private double[] doubleValues;
    private boolean isEnabled;
    private boolean[] isSeen;
    private boolean isVirtualMetricsEnabled;
    private long lastDay = Long.MIN_VALUE;
    private long lastTimestamp = Long.MIN_VALUE;
    private long[] longValues;
    private long nextSampleMicros = Long.MIN_VALUE;
    private long nextVirtualSampleMicros = Long.MIN_VALUE;
    private TableWriter writer;

    public MetricsPersistenceJob(CairoEngine engine, MetricsConfiguration configuration) {
        this.configuration = configuration;
        this.clock = engine.getConfiguration().getMicrosecondClock();
        this.engine = engine;
        this.metrics = engine.getMetrics();
        this.parquetBloomFilterFpp = engine.getConfiguration().getPartitionEncoderParquetBloomFilterFpp();
        this.isEnabled = configuration.isPersistEnabled();
        Pattern pattern = null;
        if (isEnabled) {
            try {
                pattern = compileExcludePattern(configuration.getPersistExclude());
            } catch (Throwable th) {
                disable(th);
            }
        }
        this.excludePattern = pattern;
        if (isEnabled) {
            try {
                initialize(engine);
            } catch (Throwable th) {
                disable(th);
            }
        }
    }

    @Override
    public void close() {
        isEnabled = false;
        writer = Misc.free(writer);
    }

    public boolean isEnabled() {
        return isEnabled;
    }

    @Override
    public boolean runSerially() {
        if (!isEnabled) {
            return false;
        }

        final long now = clock.getTicks();
        if (now < nextSampleMicros) {
            return false;
        }
        if (now <= lastTimestamp) {
            nextSampleMicros = lastTimestamp + configuration.getPersistIntervalMicros();
            return false;
        }

        try {
            sample(now);
            nextSampleMicros = now + configuration.getPersistIntervalMicros();
        } catch (Throwable th) {
            disable(th);
        }
        return false;
    }

    private static Pattern compileExcludePattern(CharSequence expression) {
        if (Chars.empty(expression)) {
            return null;
        }
        return Pattern.compile(Chars.toString(expression));
    }

    private int addColumn(CharSequence name, MetricType type) {
        buildColumnName(name, null, null);
        if (nameSink.length() == 0) {
            throw new IllegalArgumentException("metric name is empty");
        }
        return addBuiltColumn(type);
    }

    private int addColumn(CharSequence name, MetricType type, CharSequence labelValue0) {
        buildColumnName(name, labelValue0, null);
        return addBuiltColumn(type);
    }

    private int addColumn(
            CharSequence name,
            MetricType type,
            CharSequence labelValue0,
            CharSequence labelValue1
    ) {
        buildColumnName(name, labelValue0, labelValue1);
        return addBuiltColumn(type);
    }

    private int addBuiltColumn(MetricType type) {
        if (excludePattern != null && excludePattern.matcher(nameSink).matches()) {
            return -1;
        }
        final int existingIndex = nameToIndex.get(nameSink);
        if (existingIndex > -1) {
            throw new IllegalArgumentException("duplicate flattened metric name: " + nameSink);
        }
        final String columnName = nameSink.toString();
        final int columnIndex = columns.size();
        nameToIndex.put(columnName, columnIndex);
        columns.add(new MetricColumn(columnName, type));
        return columnIndex;
    }

    private void alterMissingColumns(CairoEngine engine, SqlCompiler compiler, SqlExecutionContextImpl context) throws Exception {
        final TableToken tableToken = engine.verifyTableName(TABLE_NAME);
        try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
            if (tableToken.isWal() || metadata.getPartitionBy() != PartitionBy.DAY) {
                throw new IllegalStateException("sys.metrics must be a non-WAL table partitioned by day");
            }
            final int timestampIndex = metadata.getTimestampIndex();
            if (timestampIndex < 0
                    || ColumnType.tagOf(metadata.getColumnType(timestampIndex)) != ColumnType.TIMESTAMP
                    || !Chars.equals("ts", metadata.getColumnName(timestampIndex))) {
                throw new IllegalStateException("sys.metrics must have a designated ts timestamp column");
            }
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (i != timestampIndex) {
                    final int type = ColumnType.tagOf(metadata.getColumnType(i));
                    if (type != ColumnType.LONG && type != ColumnType.DOUBLE) {
                        throw new IllegalStateException("sys.metrics columns must be LONG or DOUBLE");
                    }
                }
            }
            for (int i = 0, n = columns.size(); i < n; i++) {
                final MetricColumn column = columns.getQuick(i);
                final int columnIndex = metadata.getColumnIndexQuiet(column.name);
                if (columnIndex > -1) {
                    validateColumnType(metadata, column, columnIndex);
                }
            }
        }

        for (int i = 0, n = columns.size(); i < n; i++) {
            final MetricColumn column = columns.getQuick(i);
            try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
                if (metadata.getColumnIndexQuiet(column.name) > -1) {
                    continue;
                }
            }
            final CompiledQuery query = compiler.query()
                    .$("ALTER TABLE \"").$(TABLE_NAME).$("\" ADD COLUMN IF NOT EXISTS \"")
                    .$(column.name).$("\" ").$(column.isDouble ? "DOUBLE" : "LONG")
                    .compile(context);
            try (OperationFuture future = query.execute(operationSequence)) {
                future.await();
            }
        }

        final CompiledQuery ttlQuery = compiler.query()
                .$("ALTER TABLE \"").$(TABLE_NAME).$("\" SET TTL ")
                .$(configuration.getPersistTtl())
                .compile(context);
        try (OperationFuture future = ttlQuery.execute(operationSequence)) {
            future.await();
        }
    }

    private void appendSanitized(CharSequence value) {
        for (int i = 0, n = value.length(); i < n; i++) {
            final char c = value.charAt(i);
            if ((c >= 'a' && c <= 'z')
                    || (c >= 'A' && c <= 'Z')
                    || (c >= '0' && c <= '9' && nameSink.length() > 0)
                    || c == '_') {
                nameSink.put(c);
            } else {
                nameSink.put('_');
            }
        }
    }

    private void buildColumnName(CharSequence name, CharSequence labelValue0, CharSequence labelValue1) {
        nameSink.clear();
        appendSanitized(name);
        if (labelValue0 != null) {
            nameSink.put("__");
            appendSanitized(labelValue0);
        }
        if (labelValue1 != null) {
            nameSink.put("__");
            appendSanitized(labelValue1);
        }
    }

    private void convertPreviousPartitions(long activeDay) {
        for (int i = 0, n = writer.getPartitionCount(); i < n; i++) {
            final long partitionTimestamp = writer.getPartitionTimestamp(i);
            if (partitionTimestamp >= activeDay) {
                break;
            }
            if (!writer.isPartitionParquet(i)) {
                writer.convertPartitionNativeToParquet(
                        partitionTimestamp,
                        null,
                        parquetBloomFilterFpp
                );
            }
        }
    }

    private void createTable(SqlCompiler compiler, SqlExecutionContextImpl context) throws Exception {
        final QueryBuilder builder = compiler.query()
                .$("CREATE TABLE IF NOT EXISTS \"").$(TABLE_NAME).$("\" (ts TIMESTAMP");
        for (int i = 0, n = columns.size(); i < n; i++) {
            final MetricColumn column = columns.getQuick(i);
            builder.$(", \"").$(column.name).$("\" ").$(column.isDouble ? "DOUBLE" : "LONG");
        }
        builder.$(") TIMESTAMP(ts) PARTITION BY DAY TTL ")
                .$(configuration.getPersistTtl())
                .$(" BYPASS WAL")
                .createTable(context);
    }

    private void disable(Throwable th) {
        writer = Misc.free(writer);
        isEnabled = false;
        LOG.error().$("metrics persistence disabled [error=").$(th).$(']').$();
    }

    private int findColumn(CharSequence name, CharSequence labelValue0, CharSequence labelValue1) {
        buildColumnName(name, labelValue0, labelValue1);
        return nameToIndex.get(nameSink);
    }

    private void initialize(CairoEngine engine) throws Exception {
        metrics.snapshot(new MetricSnapshotVisitor() {
            @Override
            public void visitDouble(CharSequence name, double value) {
                addColumn(name, MetricType.DOUBLE_GAUGE);
            }

            @Override
            public void visitLong(CharSequence name, MetricType type, long value) {
                addColumn(name, type);
            }

            @Override
            public void visitLong(CharSequence name, MetricType type, CharSequence labelValue0, long value) {
                addColumn(name, type, labelValue0);
            }

            @Override
            public void visitLong(
                    CharSequence name,
                    MetricType type,
                    CharSequence labelValue0,
                    CharSequence labelValue1,
                    long value
            ) {
                addColumn(name, type, labelValue0, labelValue1);
            }
        });

        doubleValues = new double[columns.size()];
        isSeen = new boolean[columns.size()];
        longValues = new long[columns.size()];

        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1) {
                @Override
                public boolean shouldLogSql() {
                    return false;
                }
            };
            context.with(
                    engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(),
                    null,
                    null
            );
            createTable(compiler, context);
            alterMissingColumns(engine, compiler, context);
        }

        final TableToken tableToken = engine.verifyTableName(TABLE_NAME);
        try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
            for (int i = 0, n = columns.size(); i < n; i++) {
                final MetricColumn column = columns.getQuick(i);
                column.columnIndex = metadata.getColumnIndexQuiet(column.name);
                if (column.columnIndex < 0) {
                    throw new IllegalStateException("missing sys.metrics column: " + column.name);
                }
                validateColumnType(metadata, column, column.columnIndex);
            }
        }
        writer = engine.getWriter(tableToken, WRITER_LOCK_REASON);
        lastTimestamp = writer.getMaxTimestamp();
        if (lastTimestamp != Long.MIN_VALUE) {
            lastDay = Micros.floorDD(lastTimestamp);
            if (configuration.isPersistParquetEnabled()) {
                convertPreviousPartitions(lastDay);
            }
        }
    }

    private void reconcileSchema() throws Exception {
        final int oldColumnCount = longValues.length;
        final int newColumnCount = columns.size();
        if (newColumnCount == oldColumnCount) {
            return;
        }

        writer = Misc.free(writer);
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1) {
                @Override
                public boolean shouldLogSql() {
                    return false;
                }
            };
            context.with(
                    engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(),
                    null,
                    null
            );
            alterMissingColumns(engine, compiler, context);
        }

        doubleValues = Arrays.copyOf(doubleValues, newColumnCount);
        isSeen = Arrays.copyOf(isSeen, newColumnCount);
        longValues = Arrays.copyOf(longValues, newColumnCount);
        final TableToken tableToken = engine.verifyTableName(TABLE_NAME);
        try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
            for (int i = oldColumnCount; i < newColumnCount; i++) {
                final MetricColumn column = columns.getQuick(i);
                column.columnIndex = metadata.getColumnIndexQuiet(column.name);
                if (column.columnIndex < 0) {
                    throw new IllegalStateException("missing sys.metrics column: " + column.name);
                }
                validateColumnType(metadata, column, column.columnIndex);
                if (column.pendingValue) {
                    if (column.isDouble) {
                        doubleValues[i] = column.pendingDoubleValue;
                    } else {
                        longValues[i] = column.pendingLongValue;
                    }
                    isSeen[i] = true;
                    column.hasValue = true;
                    column.pendingValue = false;
                }
            }
        }
        writer = engine.getWriter(tableToken, WRITER_LOCK_REASON);
    }

    private void sample(long timestamp) throws Exception {
        Arrays.fill(isSeen, false);
        isVirtualMetricsEnabled = timestamp >= nextVirtualSampleMicros;
        metrics.snapshot(sampleVisitor);
        reconcileSchema();
        if (isVirtualMetricsEnabled) {
            nextVirtualSampleMicros = timestamp + configuration.getPersistVirtualIntervalMicros();
        }

        final TableWriter.Row row = writer.newRow(timestamp);
        try {
            for (int i = 0, n = columns.size(); i < n; i++) {
                final MetricColumn column = columns.getQuick(i);
                if (isSeen[i] || column.isVirtual && column.hasValue) {
                    if (column.isDouble) {
                        row.putDouble(column.columnIndex, doubleValues[i]);
                    } else {
                        row.putLong(column.columnIndex, longValues[i]);
                    }
                }
            }
            row.append();
        } catch (Throwable th) {
            row.cancel();
            throw th;
        }
        writer.commit();
        lastTimestamp = timestamp;

        final long day = Micros.floorDD(timestamp);
        if (configuration.isPersistParquetEnabled() && day >= lastDay && day != lastDay) {
            convertPreviousPartitions(day);
        }
        if (day > lastDay) {
            lastDay = day;
        }
    }

    private void setDouble(int index, double value) {
        if (index > -1) {
            final MetricColumn column = columns.getQuick(index);
            if (!column.isDouble) {
                throw new IllegalStateException("metric type changed: " + column.name);
            }
            if (index < doubleValues.length) {
                if (isSeen[index]) {
                    throw new IllegalStateException("duplicate metric name: " + column.name);
                }
                doubleValues[index] = value;
                isSeen[index] = true;
                column.hasValue = true;
            } else {
                if (column.pendingValue) {
                    throw new IllegalStateException("duplicate metric name: " + column.name);
                }
                column.pendingDoubleValue = value;
                column.pendingValue = true;
            }
        }
    }

    private void setLong(int index, MetricType type, long value) {
        if (index > -1) {
            final MetricColumn column = columns.getQuick(index);
            if (column.isDouble || column.type != type) {
                throw new IllegalStateException("metric type changed: " + column.name);
            }
            if (index < longValues.length) {
                if (isSeen[index]) {
                    throw new IllegalStateException("duplicate metric name: " + column.name);
                }
                longValues[index] = value;
                isSeen[index] = true;
                column.hasValue = true;
            } else {
                if (column.pendingValue) {
                    throw new IllegalStateException("duplicate metric name: " + column.name);
                }
                column.pendingLongValue = value;
                column.pendingValue = true;
            }
        }
    }

    private static void validateColumnType(TableMetadata metadata, MetricColumn column, int columnIndex) {
        final int expectedType = column.isDouble ? ColumnType.DOUBLE : ColumnType.LONG;
        if (ColumnType.tagOf(metadata.getColumnType(columnIndex)) != expectedType) {
            throw new IllegalStateException("unexpected sys.metrics column type: " + column.name);
        }
    }

    private static final class MetricColumn {
        private int columnIndex;
        private boolean hasValue;
        private final boolean isDouble;
        private final boolean isVirtual;
        private final String name;
        private double pendingDoubleValue;
        private long pendingLongValue;
        private boolean pendingValue;
        private final MetricType type;

        private MetricColumn(String name, MetricType type) {
            this.isDouble = type == MetricType.DOUBLE_GAUGE;
            this.isVirtual = type == MetricType.VIRTUAL_LONG_GAUGE;
            this.name = name;
            this.type = type;
        }
    }
}
