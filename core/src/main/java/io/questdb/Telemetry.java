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

package io.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.QueryBuilder;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MPSequence;
import io.questdb.mp.QueueConsumer;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjectFactory;
import io.questdb.std.Os;
import io.questdb.std.datetime.Clock;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.AbstractTelemetryTask;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.SYSTEM_TABLE_NAME_SUFFIX;
import static io.questdb.std.Files.DT_FILE;
import static io.questdb.std.Files.notDots;

public class Telemetry<T extends AbstractTelemetryTask> implements Closeable {
    private static final Log LOG = LogFactory.getLog(Telemetry.class);

    private final boolean enabled;
    private final int maxFileNameLen;
    private Clock clock;
    private long dbSizeEstimateStartTimestamp;
    private long dbSizeEstimateTimeout; //micros
    private MPSequence telemetryPubSeq;
    private RingQueue<T> telemetryQueue;
    private SCSequence telemetrySubSeq;
    private TelemetryType<T> telemetryType;
    private TableWriter writer;
    private final QueueConsumer<T> taskConsumer = this::consume;

    public Telemetry(TelemetryTypeBuilder<T> builder, CairoConfiguration configuration) {
        TelemetryType<T> type = builder.build(configuration);
        final TelemetryConfiguration telemetryConfiguration = type.getTelemetryConfiguration(configuration);
        enabled = telemetryConfiguration.getEnabled();
        maxFileNameLen = configuration.getMaxFileNameLength();
        if (enabled) {
            telemetryType = type;
            clock = configuration.getMicrosecondClock();
            dbSizeEstimateTimeout = telemetryConfiguration.getDbSizeEstimateTimeout() * 1000;
            telemetryQueue = new RingQueue<>(type.getTaskFactory(), telemetryConfiguration.getQueueCapacity());
            telemetryPubSeq = new MPSequence(telemetryQueue.getCycle());
            telemetrySubSeq = new SCSequence();
            telemetryPubSeq.then(telemetrySubSeq).then(telemetryPubSeq);
        }
    }

    public void clear() {
        if (writer != null) {
            consumeAll();
            telemetryType.logStatus(writer, TelemetrySystemEvent.SYSTEM_DOWN, clock.getTicks());
            writer = Misc.free(writer);
        }
    }

    @Override
    public void close() {
        try {
            clear();
        } finally {
            telemetryQueue = Misc.free(telemetryQueue);
        }
    }

    public void consume(T task) {
        task.writeTo(writer, clock.getTicks());
    }

    public void consumeAll() {
        if (enabled && telemetrySubSeq.consumeAll(telemetryQueue, taskConsumer)) {
            writer.commit();
        }
    }

    public String getName() {
        return telemetryType.getName();
    }

    public void init(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        if (!enabled) {
            return;
        }
        String tableName = telemetryType.getTableName();
        boolean shouldDropTable = false;
        try {
            TableToken tableToken = engine.verifyTableName(tableName);
            try (TableMetadata meta = engine.getTableMetadata(tableToken)) {
                shouldDropTable = (meta.getTtlHoursOrMonths() == 0);
            }
        } catch (CairoException e) {
            if (!Chars.contains(e.getFlyweightMessage(), "table does not exist")) {
                throw e;
            }
        }
        if (shouldDropTable) {
            compiler.query().$("DROP TABLE '").$(tableName).$("'")
                    .compile(sqlExecutionContext)
                    .getOperation()
                    .execute(sqlExecutionContext, null)
                    .await();
        }
        telemetryType.getCreateSql(compiler.query()).createTable(sqlExecutionContext);
        TableToken tableToken = engine.verifyTableName(tableName);
        try {
            writer = engine.getWriter(tableToken, "telemetry");
        } catch (CairoException ex) {
            LOG.error()
                    .$("could not open [table=").$(tableToken)
                    .$(", msg=").$safe(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno())
                    .$(']').$();
        }

        telemetryType.logStatus(writer, TelemetrySystemEvent.SYSTEM_UP, clock.getTicks());

        if (telemetryType.shouldLogClasses()) {
            telemetryType.logStatus(writer, getOSClass(), clock.getTicks());
            telemetryType.logStatus(writer, getEnvTypeClass(), clock.getTicks());
            telemetryType.logStatus(writer, getCpuClass(), clock.getTicks());
            telemetryType.logStatus(writer, getDBSizeClass(engine.getConfiguration()), clock.getTicks());
            telemetryType.logStatus(writer, getTableCountClass(engine), clock.getTicks());
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public T nextTask() {
        if (!enabled) {
            return null;
        }

        long cursor = telemetryPubSeq.next();
        if (cursor < 0) {
            return null;
        }

        var task = telemetryQueue.get(cursor);
        task.setQueueCursor(cursor);
        return task;
    }

    public void store(T task) {
        telemetryPubSeq.done(task.getQueueCursor());
    }

    private static short getCpuClass() {
        final int cpus = Runtime.getRuntime().availableProcessors();
        if (cpus <= 4) {         // 0 - 1-4 cores
            return TelemetrySystemEvent.SYSTEM_CPU_CLASS_BASE;
        } else if (cpus <= 8) {  // 1 - 5-8 cores
            return TelemetrySystemEvent.SYSTEM_CPU_CLASS_BASE - 1;
        } else if (cpus <= 16) { // 2 - 9-16 cores
            return TelemetrySystemEvent.SYSTEM_CPU_CLASS_BASE - 2;
        } else if (cpus <= 32) { // 3 - 17-32 cores
            return TelemetrySystemEvent.SYSTEM_CPU_CLASS_BASE - 3;
        } else if (cpus <= 64) { // 4 - 33-64 cores
            return TelemetrySystemEvent.SYSTEM_CPU_CLASS_BASE - 4;
        }
        // 5 - 65+ cores
        return TelemetrySystemEvent.SYSTEM_CPU_CLASS_BASE - 5;
    }

    private static short getEnvTypeClass() {
        final int type = Os.getEnvironmentType();
        return (short) (TelemetrySystemEvent.SYSTEM_ENV_TYPE_BASE - type);
    }

    private static short getOSClass() {
        if (Os.isLinux()) {          // 0 - Linux
            return TelemetrySystemEvent.SYSTEM_OS_CLASS_BASE;
        } else if (Os.isOSX()) {     // 1 - OS X
            return TelemetrySystemEvent.SYSTEM_OS_CLASS_BASE - 1;
        } else if (Os.isWindows()) { // 2 - Windows
            return TelemetrySystemEvent.SYSTEM_OS_CLASS_BASE - 2;
        }
        // 3 - BSD
        return TelemetrySystemEvent.SYSTEM_OS_CLASS_BASE - 3;
    }

    private static short getTableCountClass(CairoEngine engine) {
        final long tableCount = engine.getTableTokenCount(false);
        if (tableCount <= 10) {          // 0 - 0-10 tables
            return TelemetrySystemEvent.SYSTEM_TABLE_COUNT_CLASS_BASE;
        } else if (tableCount <= 25) {   // 1 - 11-25 tables
            return TelemetrySystemEvent.SYSTEM_TABLE_COUNT_CLASS_BASE - 1;
        } else if (tableCount <= 50) {   // 2 - 26-50 tables
            return TelemetrySystemEvent.SYSTEM_TABLE_COUNT_CLASS_BASE - 2;
        } else if (tableCount <= 100) {  // 3 - 51-100 tables
            return TelemetrySystemEvent.SYSTEM_TABLE_COUNT_CLASS_BASE - 3;
        } else if (tableCount <= 250) {  // 4 - 101-250 tables
            return TelemetrySystemEvent.SYSTEM_TABLE_COUNT_CLASS_BASE - 4;
        } else if (tableCount <= 1000) { // 5 - 251-1000 tables
            return TelemetrySystemEvent.SYSTEM_TABLE_COUNT_CLASS_BASE - 5;
        }
        // 6 - 1001+ tables
        return TelemetrySystemEvent.SYSTEM_TABLE_COUNT_CLASS_BASE - 6;
    }

    private short getDBSizeClass(CairoConfiguration configuration) {
        dbSizeEstimateStartTimestamp = clock.getTicks();

        final long dbSize;
        try {
            final Path path = Path.PATH.get().of(configuration.getDbRoot());
            dbSize = getDirSize(configuration.getFilesFacade(), path, true, Misc.getThreadLocalSink());
            if (hasTimedOut()) {
                LOG.info().$("Unable to estimate DB size, disk scanning timed out").$();
                return TelemetrySystemEvent.SYSTEM_DB_SIZE_CLASS_UNKNOWN;
            }
        } catch (Throwable e) {
            LOG.info().$("Unable to estimate DB size, error=").$(e).$();
            return TelemetrySystemEvent.SYSTEM_DB_SIZE_CLASS_UNKNOWN;
        }

        if (dbSize <= 10 * Numbers.SIZE_1GB) {          // 0 - <10GB
            return TelemetrySystemEvent.SYSTEM_DB_SIZE_CLASS_BASE;
        } else if (dbSize <= 50 * Numbers.SIZE_1GB) {   // 1 - (10GB,50GB]
            return TelemetrySystemEvent.SYSTEM_DB_SIZE_CLASS_BASE - 1;
        } else if (dbSize <= 100 * Numbers.SIZE_1GB) {  // 2 - (50GB,100GB]
            return TelemetrySystemEvent.SYSTEM_DB_SIZE_CLASS_BASE - 2;
        } else if (dbSize <= 500 * Numbers.SIZE_1GB) {  // 3 - (100GB,500GB]
            return TelemetrySystemEvent.SYSTEM_DB_SIZE_CLASS_BASE - 3;
        } else if (dbSize <= Numbers.SIZE_1TB) {        // 4 - (500GB,1TB]
            return TelemetrySystemEvent.SYSTEM_DB_SIZE_CLASS_BASE - 4;
        } else if (dbSize <= 5 * Numbers.SIZE_1TB) {    // 5 - (1TB,5TB]
            return TelemetrySystemEvent.SYSTEM_DB_SIZE_CLASS_BASE - 5;
        } else if (dbSize <= 10 * Numbers.SIZE_1TB) {   // 6 - (5TB,10TB]
            return TelemetrySystemEvent.SYSTEM_DB_SIZE_CLASS_BASE - 6;
        }
        // 7 - >10TB
        return TelemetrySystemEvent.SYSTEM_DB_SIZE_CLASS_BASE - 7;
    }

    private long getDirSize(FilesFacade ff, Path path, boolean topLevel, StringSink sink) {
        long totalSize = 0L;

        final long pFind = ff.findFirst(path.$());
        if (pFind > 0L) {
            final int len = path.size();
            try {
                do {
                    if (hasTimedOut()) {
                        return 0L;
                    }

                    final long nameUtf8Ptr = ff.findName(pFind);
                    path.trimTo(len).concat(nameUtf8Ptr).$();
                    if (ff.findType(pFind) == DT_FILE) {
                        totalSize += ff.length(path.$());
                    } else if (notDots(nameUtf8Ptr)) {
                        if (topLevel) {
                            sink.clear();
                            Utf8s.utf8ToUtf16(path, len + 1, path.size(), sink);
                            final int tableNameLen = Chars.indexOf(sink, SYSTEM_TABLE_NAME_SUFFIX);
                            if (tableNameLen > -1) {
                                sink.trimTo(tableNameLen);
                            }
                            if (!TableUtils.isValidTableName(sink, maxFileNameLen)) {
                                // This is not a table, skip
                                continue;
                            }
                        }
                        totalSize += getDirSize(ff, path, false, sink);
                    }
                }
                while (ff.findNext(pFind) > 0);
            } finally {
                ff.findClose(pFind);
                path.trimTo(len);
            }
        }
        return totalSize;
    }

    protected boolean hasTimedOut() {
        return clock.getTicks() - dbSizeEstimateStartTimestamp > dbSizeEstimateTimeout;
    }

    public interface TelemetryType<T extends AbstractTelemetryTask> {
        QueryBuilder getCreateSql(QueryBuilder builder);

        String getName();

        String getTableName();

        ObjectFactory<T> getTaskFactory();

        // TODO(glasstiger): we could tailor the config for each telemetry type
        //                   we could set different queue sizes or disable telemetry per type, for example
        default TelemetryConfiguration getTelemetryConfiguration(@NotNull CairoConfiguration configuration) {
            return configuration.getTelemetryConfiguration();
        }

        default void logStatus(TableWriter writer, short systemStatus, long micros) {
        }

        default boolean shouldLogClasses() {
            return false;
        }
    }

    public interface TelemetryTypeBuilder<T extends AbstractTelemetryTask> {
        TelemetryType<T> build(CairoConfiguration configuration);
    }
}
