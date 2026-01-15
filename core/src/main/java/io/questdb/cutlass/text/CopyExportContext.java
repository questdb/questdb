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

package io.questdb.cutlass.text;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cutlass.parquet.CopyExportRequestTask;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.CreateTableOperation;
import io.questdb.griffin.engine.ops.CreateTableOperationImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjectPool;
import io.questdb.std.SimpleReadWriteLock;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.LongSupplier;

public class CopyExportContext {
    public static final long INACTIVE_COPY_ID = -1;
    private static final Log LOG = LogFactory.getLog(CopyExportContext.class);
    private final LongObjHashMap<ExportTaskEntry> activeExports = new LongObjHashMap<>();
    private final LongSupplier copyIDSupplier;
    private final CairoEngine engine;
    private final CharSequenceObjHashMap<ExportTaskEntry> exportBySqlText = new CharSequenceObjHashMap<>();
    private final CharSequenceObjHashMap<ExportTaskEntry> exportEntriesByFileName = new CharSequenceObjHashMap<>();
    private final ObjectPool<ExportTaskEntry> exportTaskEntryPools = new ObjectPool<>(ExportTaskEntry::new, 6);
    private final ReadWriteLock lock = new SimpleReadWriteLock();
    private boolean initialized = false;
    private TableToken statusTableToken;

    public CopyExportContext(CairoEngine engine) {
        this.engine = engine;
        this.copyIDSupplier = engine.getConfiguration().getCopyIDSupplier();
    }

    public static boolean canStreamExportParquet(RecordCursorFactory factory) throws SqlException {
        return factory.supportsPageFrameCursor();
    }

    public ExportTaskEntry assignExportEntry(
            SecurityContext securityContext,
            @NotNull CharSequence sqlText,
            @NotNull CharSequence fileName,
            SqlExecutionCircuitBreaker sqlExecutionCircuitBreaker,
            CopyTrigger trigger) throws SqlException {
        assert trigger != CopyTrigger.NONE;
        // This context acts as the registry for exports in-flight. It will contain all requests
        // that were submitted via SQL execution in order to prevent duplicate exports. Ideally this is
        // to disallow repeated exports by the same user, but in OSS we are not able to tell users apart, so
        // the key is SQL, and it would be global across the server for now.

        // The requests triggered by REST API are not verified on duplicates, so it is less of a friction.
        lock.writeLock().lock();
        try {
            ExportTaskEntry entry;
            if (trigger == CopyTrigger.SQL) {
                entry = exportBySqlText.get(sqlText);
                if (entry != null) {
                    StringSink sink = Misc.getThreadLocalSink();
                    Numbers.appendHex(sink, entry.id, true);
                    throw SqlException.$(0, "duplicate sql statement: ").put(sqlText).put(" [id=").put(sink).put(']');
                }
                if (!fileName.isEmpty()) {
                    entry = exportEntriesByFileName.get(fileName);
                    if (entry != null) {
                        StringSink sink = Misc.getThreadLocalSink();
                        Numbers.appendHex(sink, entry.id, true);
                        throw SqlException.$(0, "duplicate export path: ").put(fileName).put(" [id=").put(sink).put(']');
                    }
                }
            }

            long id;
            int index;
            do {
                id = copyIDSupplier.getAsLong();
            } while ((index = activeExports.keyIndex(id)) < 0);

            entry = exportTaskEntryPools.next().of(
                    engine,
                    id,
                    securityContext,
                    sqlText,
                    fileName,
                    sqlExecutionCircuitBreaker,
                    trigger
            );

            activeExports.putAt(index, id, entry);

            if (trigger == CopyTrigger.SQL) {
                exportBySqlText.put(sqlText, entry);
                if (!fileName.isEmpty()) {
                    exportEntriesByFileName.put(fileName, entry);
                }
            }
            return entry;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean cancel(long id, SecurityContext securityContext) {
        lock.readLock().lock();
        try {
            ExportTaskEntry e = activeExports.get(id);
            if (e != null) {
                var cb = e.getCircuitBreaker();
                if (cb != null) {
                    if (securityContext != null) {
                        securityContext.authorizeCopyCancel(securityContext);
                    }
                    cb.cancel();
                }
                return true;
            }
            return false;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void clear() {
        this.initialized = false;
    }

    @TestOnly
    public long getActiveExportId() {
        lock.readLock().lock();
        try {
            long[] keys = activeExports.keys();
            for (int i = 0; i < keys.length; i++) {
                if (keys[i] >= 0) {
                    return keys[i];
                }
            }
            return INACTIVE_COPY_ID;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void getActiveExportIds(LongList entryIds) {
        lock.readLock().lock();
        try {
            entryIds.clear();
            long[] keys = activeExports.keys();
            for (int i = 0; i < keys.length; i++) {
                if (keys[i] >= 0) {
                    entryIds.add(keys[i]);
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean getAndCopyEntry(long id, ExportTaskData entry) {
        lock.readLock().lock();
        try {
            ExportTaskEntry e = activeExports.get(id);
            if (e != null) {
                entry.id = e.id;
                entry.fileName.clear();
                entry.fileName.put(e.fileName);
                entry.phase = e.phase;
                entry.populatedRowCount = e.populatedRowCount;
                entry.streamingSendRowCount = e.streamingSendRowCount;
                entry.startTime = e.startTime;
                entry.workerId = e.workerId;
                entry.finishedPartitionCount = e.finishedPartitionCount;
                entry.totalPartitionCount = e.totalPartitionCount;
                entry.totalRowCount = e.totalRowCount;
                entry.principal = e.securityContext != null ? e.securityContext.getPrincipal() : null;
                entry.sqlText.clear();
                entry.sqlText.put(e.sqlText);
                entry.trigger = e.trigger.name;
                return true;
            }
            return false;
        } finally {
            lock.readLock().unlock();
        }
    }

    public ExportTaskEntry getEntry(long id) {
        lock.readLock().lock();
        try {
            return activeExports.get(id);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void init() {
        CairoConfiguration configuration = engine.getConfiguration();
        int logRetentionDays = configuration.getSqlCopyLogRetentionDays();
        final String statusTableName = configuration.getSystemTableNamePrefix() + "copy_export_log";
        try (SqlCompiler compiler = engine.getSqlCompiler(); var sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)) {
            sqlExecutionContext.with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null);
            statusTableToken = compiler.query()
                    .$("CREATE TABLE IF NOT EXISTS \"")
                    .$(statusTableName)
                    .$("\" (" +
                            "ts TIMESTAMP, " + // 0
                            "id VARCHAR, " + // 1
                            "table_name SYMBOL, " + // 2
                            "export_path SYMBOL, " + // 3
                            "num_exported_files INT, " + // 4
                            "phase SYMBOL, " + // 5
                            "status SYMBOL, " + // 6
                            "message VARCHAR, " + // 7
                            "errors LONG" + // 8
                            ") timestamp(ts) PARTITION BY DAY\n" +
                            "TTL " + logRetentionDays + " DAYS Bypass WAL;"
                    )
                    .createTable(sqlExecutionContext);
        } catch (SqlException e) {
            LOG.critical().$("cannot create system table [table=").$(statusTableName).$(", error=").$((Throwable) e).I$();
            throw CairoException.critical(e.getErrorCode()).put("cannot create system table ").put(statusTableName).put(": ").put(e.getMessage());
        }
    }

    public void releaseEntry(ExportTaskEntry entry) {
        lock.writeLock().lock();
        try {
            activeExports.remove(entry.id);
            exportBySqlText.remove(entry.sqlText);
            if (!entry.fileName.isEmpty()) {
                exportEntriesByFileName.remove(entry.fileName);
            }
            exportTaskEntryPools.release(entry);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void updateStatus(
            CopyExportRequestTask.Phase phase,
            CopyExportRequestTask.Status status,
            CharSequence exportDir,
            int numOfFiles,
            @Nullable final CharSequence msg,
            long errors,
            String tableName,
            long copyID
    ) {
        Throwable error = null;
        synchronized (this) {
            if (!initialized) {
                init();
                initialized = true;
            }

            if (statusTableToken == null) {
                return;
            }

            // serial insert to copy_export_log table to avoid table busy
            try (TableWriter statusTableWriter = engine.getWriter(statusTableToken, "QuestDB system")) {
                try {
                    MicrosecondClock microsecondClock = engine.getConfiguration().getMicrosecondClock();
                    TableWriter.Row row = statusTableWriter.newRow(microsecondClock.getTicks());
                    var utf8StringSink = Misc.getThreadLocalUtf8Sink();
                    Numbers.appendHex(utf8StringSink, copyID, true);
                    row.putVarchar(1, utf8StringSink);
                    row.putSym(2, tableName);
                    row.putSym(3, exportDir);
                    row.putInt(4, numOfFiles);
                    row.putSym(5, phase.getName());
                    row.putSym(6, status.getName());
                    if (msg != null) {
                        utf8StringSink.clear();
                        utf8StringSink.put(msg);
                        row.putVarchar(7, utf8StringSink);
                    }
                    row.putLong(8, errors);
                    row.append();
                    statusTableWriter.commit();
                } catch (Throwable th) {
                    error = th;
                    LOG.error().$("update status failed [table=").$(statusTableToken).$(", error=").$(th).I$();
                }
            } catch (Throwable e) {
                error = e;
                LOG.error().$("update status failed [table=").$(statusTableToken).$(", error=").$(e).I$();
            }
        }

        if (error != null) {
            LOG.error()
                    .$("could not update status table [exportId=").$hexPadded(copyID)
                    .$(", statusTableName=").$(statusTableToken)
                    .$(", tableName=").$safe(tableName)
                    .$(", exportDir=").$safe(exportDir)
                    .$(", numOfFiles=").$(numOfFiles)
                    .$(", phase=").$(phase.getName())
                    .$(", status=").$(status.getName())
                    .$(", msg=").$safe(msg)
                    .$(", errors=").$(errors)
                    .I$();
        }
    }

    public CreateTableOperation validateAndCreateParquetExportTableOp(
            SqlExecutionContext executionContext,
            CharSequence selectText,
            int partitionBy,
            String tableName,
            String sqlText,
            int tableOrSelectTextPos
    ) throws SqlException {
        CreateTableOperationImpl createOp = null;
        final CairoEngine engine = executionContext.getCairoEngine();
        CompiledQuery selectQuery;
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            selectQuery = compiler.compile(selectText, executionContext);
            if (selectQuery.getType() != CompiledQuery.SELECT) {
                selectQuery.closeAllButSelect();
                throw SqlException.$(0, "Copy command only accepts SELECT queries");
            }
            try (RecordCursorFactory rcf = selectQuery.getRecordCursorFactory()) {
                if (partitionBy == -1) {
                    partitionBy = PartitionBy.NONE;
                }
                createOp = new CreateTableOperationImpl(
                        Chars.toString(selectText),
                        tableName,
                        partitionBy,
                        false,
                        executionContext.getCairoEngine().getConfiguration().getDefaultSymbolCapacity(),
                        sqlText,
                        false
                );
                createOp.setTableKind(TableUtils.TABLE_KIND_TEMP_PARQUET_EXPORT);
                createOp.validateAndUpdateMetadataFromSelect(rcf.getMetadata(), rcf.getScanDirection());
            }
        } catch (SqlException ex) {
            ex.setPosition(ex.getPosition() + tableOrSelectTextPos);
            Misc.free(createOp);
            throw ex;
        } catch (CairoException ex) {
            ex.position(tableOrSelectTextPos + ex.getPosition());
            Misc.free(createOp);
            throw ex;
        } catch (Throwable ex) {
            Misc.free(createOp);
            throw ex;
        }

        return createOp;
    }

    public enum CopyTrigger {
        NONE(null),
        SQL("copy sql"),
        HTTP("http export");
        private final String name;

        CopyTrigger(String name) {
            this.name = name;
        }
    }

    public static class ExportTaskData {
        private final StringSink fileName = new StringSink();
        private final StringSink sqlText = new StringSink();
        public CharSequence principal;
        private int finishedPartitionCount = 0;
        private long id;
        private CopyExportRequestTask.Phase phase;
        private long populatedRowCount = 0;
        private long startTime;
        private long streamingSendRowCount = 0;
        private int totalPartitionCount = 0;
        private long totalRowCount = 0;
        private CharSequence trigger;
        private int workerId = -1;

        public CharSequence getFileName() {
            return fileName;
        }

        public int getFinishedPartitionCount() {
            return finishedPartitionCount;
        }

        public long getId() {
            return id;
        }

        public CopyExportRequestTask.Phase getPhase() {
            return phase;
        }

        public long getPopulatedRowCount() {
            return populatedRowCount;
        }

        public CharSequence getPrincipal() {
            return principal;
        }

        public CharSequence getSqlText() {
            return sqlText;
        }

        public long getStartTime() {
            return startTime;
        }

        public long getStreamingSendRowCount() {
            return streamingSendRowCount;
        }

        public int getTotalPartitionCount() {
            return totalPartitionCount;
        }

        public long getTotalRowCount() {
            return totalRowCount;
        }

        public CharSequence getTrigger() {
            return trigger;
        }

        public int getWorkerId() {
            return workerId;
        }
    }

    public static class ExportTaskEntry implements Mutable {
        final StringSink fileName = new StringSink();
        final StringSink sqlText = new StringSink();
        AtomicBooleanCircuitBreaker atomicBooleanCircuitBreaker;
        int finishedPartitionCount = 0;
        long id = INACTIVE_COPY_ID;
        CopyExportRequestTask.Phase phase;
        long populatedRowCount = 0;
        SqlExecutionCircuitBreaker realCircuitBreaker;
        SecurityContext securityContext;
        long startTime = Numbers.LONG_NULL;
        long streamingSendRowCount = 0;
        int totalPartitionCount = 0;
        long totalRowCount = 0;
        CopyTrigger trigger = CopyTrigger.NONE;
        int workerId = -1;

        @Override
        public void clear() {
            this.securityContext = null;
            if (atomicBooleanCircuitBreaker != null) {
                atomicBooleanCircuitBreaker.clear();
            }
            this.id = INACTIVE_COPY_ID;
            this.sqlText.clear();
            this.fileName.clear();
            this.phase = null;
            this.startTime = Numbers.LONG_NULL;
            this.workerId = -1;
            this.populatedRowCount = 0;
            this.finishedPartitionCount = 0;
            realCircuitBreaker = null;
            this.totalPartitionCount = 0;
            this.totalRowCount = 0;
            this.trigger = CopyTrigger.NONE;
            this.streamingSendRowCount = 0;
        }

        public SqlExecutionCircuitBreaker getCircuitBreaker() {
            return realCircuitBreaker;
        }

        public long getId() {
            return id;
        }

        public SecurityContext getSecurityContext() {
            return securityContext;
        }

        public ExportTaskEntry of(
                CairoEngine engine,
                long id,
                SecurityContext context,
                CharSequence sqlText,
                CharSequence fileName,
                SqlExecutionCircuitBreaker circuitBreaker,
                CopyTrigger trigger) {
            if (atomicBooleanCircuitBreaker == null) {
                atomicBooleanCircuitBreaker = new AtomicBooleanCircuitBreaker(engine);
            }
            this.id = id;
            this.securityContext = context;
            this.sqlText.clear();
            this.sqlText.put(sqlText);
            this.fileName.clear();
            this.fileName.put(fileName);
            if (circuitBreaker == null) {
                atomicBooleanCircuitBreaker.reset();
                this.realCircuitBreaker = atomicBooleanCircuitBreaker;
            } else {
                this.realCircuitBreaker = circuitBreaker;
            }
            this.phase = CopyExportRequestTask.Phase.WAITING;
            this.trigger = trigger;
            return this;
        }

        public void setFinishedPartitionCount(int finishedPartitionCount) {
            this.finishedPartitionCount = finishedPartitionCount;
        }

        public void setPhase(CopyExportRequestTask.Phase phase) {
            this.phase = phase;
        }

        public void setPopulatedRowCount(long populatedRowCount) {
            this.populatedRowCount = populatedRowCount;
        }

        public void setStartTime(long startTime, int workerId) {
            this.startTime = startTime;
            this.workerId = workerId;
        }

        public void setStreamingSendRowCount(long streamingSendRowCount) {
            this.streamingSendRowCount = streamingSendRowCount;
        }

        public void setTotalPartitionCount(int totalPartitionCount) {
            this.totalPartitionCount = totalPartitionCount;
        }

        public void setTotalRowCount(long totalRowCount) {
            this.totalRowCount = totalRowCount;
        }
    }
}


