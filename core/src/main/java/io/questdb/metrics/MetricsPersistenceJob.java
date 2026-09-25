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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.QueryBuilder;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.Operation;
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
    public static final String TABLE_NAME = "metrics";
    public static final String WRITER_LOCK_REASON = "metrics persistence";
    private static final Log LOG = LogFactory.getLog(MetricsPersistenceJob.class);
    private static final long RETRY_BACKOFF_MAX_MICROS = Micros.MINUTE_MICROS;
    private static final long RETRY_BACKOFF_START_MICROS = Micros.SECOND_MICROS;
    private final MicrosecondClock clock;
    private final ObjList<MetricColumn> columns = new ObjList<>();
    private final MetricsConfiguration configuration;
    private final MetricSnapshotVisitor definitionVisitor = new MetricSnapshotVisitor() {
        @Override
        public void visitDouble(CharSequence name, double value) {
            addPrecreatedColumn(name, MetricType.DOUBLE_GAUGE, null, null);
        }

        @Override
        public void visitLong(CharSequence name, MetricType type, long value) {
            addPrecreatedColumn(name, type, null, null);
        }

        @Override
        public void visitLong(CharSequence name, MetricType type, CharSequence labelValue0, long value) {
            addPrecreatedColumn(name, type, labelValue0, null);
        }

        @Override
        public void visitLong(
                CharSequence name,
                MetricType type,
                CharSequence labelValue0,
                CharSequence labelValue1,
                long value
        ) {
            addPrecreatedColumn(name, type, labelValue0, labelValue1);
        }
    };
    private final CairoEngine engine;
    private final Pattern excludePattern;
    private final Metrics metrics;
    private final StringSink nameSink = new StringSink();
    private final CharSequenceIntHashMap nameToIndex = new CharSequenceIntHashMap();
    private final SCSequence operationSequence = new SCSequence();
    private final double parquetBloomFilterFpp;
    private final MetricSnapshotVisitor sampleVisitor = new MetricSnapshotVisitor() {
        @Override
        public boolean isReapDroppedTableMetricsEnabled() {
            return !metrics.isScrapeEnabled();
        }

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
    private final String tableName;
    private double[] doubleValues;
    private boolean isEnabled;
    private boolean isInitialized;
    private boolean[] isSeen;
    private boolean isVirtualMetricsEnabled;
    private long lastDay = Long.MIN_VALUE;
    private long lastTimestamp = Long.MIN_VALUE;
    private long[] longValues;
    private long nextSampleMicros = Long.MIN_VALUE;
    private long nextVirtualSampleMicros = Long.MIN_VALUE;
    private long retryBackoffMicros;
    private TableWriter writer;

    public MetricsPersistenceJob(CairoEngine engine, MetricsConfiguration configuration) {
        this.configuration = configuration;
        this.clock = engine.getConfiguration().getMicrosecondClock();
        this.engine = engine;
        this.metrics = engine.getMetrics();
        this.parquetBloomFilterFpp = engine.getConfiguration().getPartitionEncoderParquetBloomFilterFpp();
        this.isEnabled = configuration.isPersistEnabled();
        this.tableName = getTableName(engine.getConfiguration());
        Pattern pattern = null;
        if (isEnabled) {
            try {
                pattern = compileExcludePattern(configuration.getPersistExclude());
            } catch (Throwable th) {
                disable(th);
            }
        }
        this.excludePattern = pattern;
    }

    /**
     * Name of the metrics table under the given configuration's system table prefix. Enterprise
     * backup restore uses it to recognize this node-local table, so derive the name here only.
     */
    public static String getTableName(CairoConfiguration configuration) {
        return configuration.getSystemTableNamePrefix() + TABLE_NAME;
    }

    @Override
    public void close() {
        isEnabled = false;
        writer = Misc.free(writer);
    }

    public String getTableName() {
        return tableName;
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
        try {
            if (!isInitialized) {
                initialize(engine);
                isInitialized = true;
            }
            if (now <= lastTimestamp) {
                nextSampleMicros = lastTimestamp + configuration.getPersistIntervalMicros();
                retryBackoffMicros = 0;
                return false;
            }
            sample(now);
            nextSampleMicros = now + configuration.getPersistIntervalMicros();
            retryBackoffMicros = 0;
        } catch (CairoException th) {
            retry(th, now);
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

    private int addBuiltColumn(MetricType type, boolean discovered) {
        if (excludePattern != null && excludePattern.matcher(nameSink).matches()) {
            return -1;
        }
        if (nameSink.length() == 0) {
            throw new IllegalArgumentException("metric name is empty");
        }
        final int existingIndex = nameToIndex.get(nameSink);
        if (existingIndex > -1) {
            final MetricColumn column = columns.getQuick(existingIndex);
            if (column.type != type) {
                throw new IllegalArgumentException("metric type changed: " + nameSink);
            }
            if (discovered && column.discovered) {
                throw new IllegalArgumentException("duplicate flattened metric name: " + nameSink);
            }
            column.discovered |= discovered;
            return existingIndex;
        }
        final String columnName = nameSink.toString();
        final int columnIndex = columns.size();
        nameToIndex.put(columnName, columnIndex);
        columns.add(new MetricColumn(columnName, type, discovered));
        return columnIndex;
    }

    private int addColumn(CharSequence name, MetricType type) {
        return addColumn(name, type, null, null);
    }

    private int addColumn(CharSequence name, MetricType type, CharSequence labelValue0) {
        return addColumn(name, type, labelValue0, null);
    }

    private int addColumn(
            CharSequence name,
            MetricType type,
            CharSequence labelValue0,
            CharSequence labelValue1
    ) {
        buildColumnName(name, labelValue0, labelValue1);
        return addBuiltColumn(type, true);
    }

    private void addMissingColumns(TableWriter tableWriter, SecurityContext securityContext) {
        final TableMetadata metadata = tableWriter.getMetadata();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final MetricColumn column = columns.getQuick(i);
            if (metadata.getColumnIndexQuiet(column.name) < 0) {
                tableWriter.addColumn(
                        column.name,
                        column.isDouble ? ColumnType.DOUBLE : ColumnType.LONG,
                        securityContext
                );
            }
        }
    }

    private int addPrecreatedColumn(
            CharSequence name,
            MetricType type,
            CharSequence labelValue0,
            CharSequence labelValue1
    ) {
        buildColumnName(name, labelValue0, labelValue1);
        return addBuiltColumn(type, false);
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
                .$("CREATE TABLE IF NOT EXISTS \"").$(tableName).$("\" (ts TIMESTAMP");
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

    private void dropTable(SqlCompiler compiler, SqlExecutionContextImpl context) throws Exception {
        try (
                Operation operation = compiler.query()
                        .$("DROP TABLE IF EXISTS \"").$(tableName).$('\"')
                        .compile(context)
                        .getOperation();
                OperationFuture future = operation.execute(context, null)
        ) {
            future.await();
        }
    }

    private int findColumn(CharSequence name, CharSequence labelValue0, CharSequence labelValue1) {
        buildColumnName(name, labelValue0, labelValue1);
        return nameToIndex.get(nameSink);
    }

    private void initialize(CairoEngine engine) throws Exception {
        configuration.appendPersistedMetricDefinitions(definitionVisitor);
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

        final SecurityContext securityContext = engine.getConfiguration()
                .getFactoryProvider()
                .getSecurityContextFactory()
                .getRootContext();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1) {
                @Override
                public boolean shouldLogSql() {
                    return false;
                }
            };
            context.with(securityContext, null, null);
            prepareTable(engine, compiler, context);
        }

        final TableToken tableToken = engine.verifyTableName(tableName);
        writer = engine.getWriter(tableToken, WRITER_LOCK_REASON);
        addMissingColumns(writer, securityContext);
        final TableMetadata metadata = writer.getMetadata();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final MetricColumn column = columns.getQuick(i);
            column.columnIndex = metadata.getColumnIndexQuiet(column.name);
            if (column.columnIndex < 0) {
                throw new IllegalStateException("missing metrics table column: " + column.name);
            }
            validateColumnType(metadata, column, column.columnIndex);
        }
        lastTimestamp = writer.getMaxTimestamp();
        if (lastTimestamp != Long.MIN_VALUE) {
            lastDay = Micros.floorDD(lastTimestamp);
            if (configuration.isPersistParquetEnabled()) {
                convertPreviousPartitions(lastDay);
            }
        }
    }

    private boolean isTableSchemaCompatible(CairoEngine engine) {
        final TableToken tableToken = engine.verifyTableName(tableName);
        if (tableToken.isWal()) {
            return false;
        }
        try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
            if (metadata.getPartitionBy() != PartitionBy.DAY) {
                return false;
            }
            final int timestampIndex = metadata.getTimestampIndex();
            if (timestampIndex < 0
                    || ColumnType.tagOf(metadata.getColumnType(timestampIndex)) != ColumnType.TIMESTAMP
                    || !Chars.equals("ts", metadata.getColumnName(timestampIndex))) {
                return false;
            }
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (i != timestampIndex) {
                    final int type = ColumnType.tagOf(metadata.getColumnType(i));
                    if (type != ColumnType.LONG && type != ColumnType.DOUBLE) {
                        return false;
                    }
                }
            }
            for (int i = 0, n = columns.size(); i < n; i++) {
                final MetricColumn column = columns.getQuick(i);
                final int columnIndex = metadata.getColumnIndexQuiet(column.name);
                if (columnIndex > -1) {
                    final int expectedType = column.isDouble ? ColumnType.DOUBLE : ColumnType.LONG;
                    if (ColumnType.tagOf(metadata.getColumnType(columnIndex)) != expectedType) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    private void prepareTable(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContextImpl context
    ) throws Exception {
        createTable(compiler, context);
        if (!isTableSchemaCompatible(engine)) {
            LOG.info().$("recreating incompatible metrics table [table=").$(tableName).I$();
            dropTable(compiler, context);
            createTable(compiler, context);
        } else if (!engine.isReadOnlyMode()) {
            // CREATE already applies the configured TTL. On an Enterprise replica, ALTER would use
            // the client-facing TableWriterAPI path and be rejected before the internal metrics writer opens.
            setTtl(compiler, context);
        }
    }

    private void reconcileSchema() throws Exception {
        final int oldColumnCount = longValues.length;
        final int newColumnCount = columns.size();
        if (newColumnCount == oldColumnCount) {
            return;
        }

        final SecurityContext securityContext = engine.getConfiguration()
                .getFactoryProvider()
                .getSecurityContextFactory()
                .getRootContext();
        addMissingColumns(writer, securityContext);

        doubleValues = Arrays.copyOf(doubleValues, newColumnCount);
        isSeen = Arrays.copyOf(isSeen, newColumnCount);
        longValues = Arrays.copyOf(longValues, newColumnCount);
        final TableMetadata metadata = writer.getMetadata();
        for (int i = 0; i < newColumnCount; i++) {
            final MetricColumn column = columns.getQuick(i);
            column.columnIndex = metadata.getColumnIndexQuiet(column.name);
            if (column.columnIndex < 0) {
                throw new IllegalStateException("missing metrics table column: " + column.name);
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

    private void retry(CairoException th, long now) {
        final TableWriter writerToClose = writer;
        writer = null;
        Misc.free(writerToClose, th);
        columns.clear();
        nameToIndex.clear();
        doubleValues = null;
        isInitialized = false;
        isSeen = null;
        isVirtualMetricsEnabled = false;
        lastDay = Long.MIN_VALUE;
        lastTimestamp = Long.MIN_VALUE;
        longValues = null;
        nextSampleMicros = now + retryBackoffMicros;
        nextVirtualSampleMicros = Long.MIN_VALUE;
        retryBackoffMicros = retryBackoffMicros == 0
                ? RETRY_BACKOFF_START_MICROS
                : Math.min(RETRY_BACKOFF_MAX_MICROS, 2 * retryBackoffMicros);
        LOG.error().$("metrics persistence failed, will retry [error=").$((Throwable) th).$(']').$();
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
                if (isVirtualMetricsEnabled && column.isVirtual && !isSeen[i]) {
                    column.hasValue = false;
                }
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
        if (configuration.isPersistParquetEnabled() && day > lastDay) {
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

    private void setTtl(SqlCompiler compiler, SqlExecutionContextImpl context) throws Exception {
        final CompiledQuery ttlQuery = compiler.query()
                .$("ALTER TABLE \"").$(tableName).$("\" SET TTL ")
                .$(configuration.getPersistTtl())
                .compile(context);
        try (OperationFuture future = ttlQuery.execute(operationSequence)) {
            future.await();
        }
    }

    private static void validateColumnType(TableMetadata metadata, MetricColumn column, int columnIndex) {
        final int expectedType = column.isDouble ? ColumnType.DOUBLE : ColumnType.LONG;
        if (ColumnType.tagOf(metadata.getColumnType(columnIndex)) != expectedType) {
            throw new IllegalStateException("unexpected metrics table column type: " + column.name);
        }
    }

    private static final class MetricColumn {
        private int columnIndex;
        private boolean discovered;
        private boolean hasValue;
        private final boolean isDouble;
        private final boolean isVirtual;
        private final String name;
        private double pendingDoubleValue;
        private long pendingLongValue;
        private boolean pendingValue;
        private final MetricType type;

        private MetricColumn(String name, MetricType type, boolean discovered) {
            this.discovered = discovered;
            this.isDouble = type == MetricType.DOUBLE_GAUGE;
            this.isVirtual = type == MetricType.VIRTUAL_LONG_GAUGE;
            this.name = name;
            this.type = type;
        }
    }
}
