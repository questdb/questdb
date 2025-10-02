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

package io.questdb.cutlass.text;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cutlass.parquet.CopyExportRequestTask;
import io.questdb.cutlass.parquet.SerialParquetExporter;
import io.questdb.griffin.SqlException;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.LongList;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjectPool;
import io.questdb.std.SimpleReadWriteLock;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.LongSupplier;

public class CopyExportContext {
    public static final long INACTIVE_COPY_ID = -1;
    private final LongObjHashMap<ExportTaskEntry> activeExports = new LongObjHashMap<>();
    private final LongSupplier copyIDSupplier;
    private final CharSequenceObjHashMap<ExportTaskEntry> exportPath = new CharSequenceObjHashMap<>();
    private final CharSequenceObjHashMap<ExportTaskEntry> exportSql = new CharSequenceObjHashMap<>();
    private final ObjectPool<ExportTaskEntry> exportTaskEntryPools = new ObjectPool<>(ExportTaskEntry::new, 6);
    private final ReadWriteLock lock = new SimpleReadWriteLock();
    private SerialParquetExporter.PhaseStatusReporter reporter;

    public CopyExportContext(CairoConfiguration configuration) {
        this.copyIDSupplier = configuration.getCopyIDSupplier();
    }

    public ExportTaskEntry assignExportEntry(SecurityContext securityContext, CharSequence sql, CharSequence path, SqlExecutionCircuitBreaker sqlExecutionCircuitBreaker) throws SqlException {
        lock.writeLock().lock();
        try {
            ExportTaskEntry entry = exportSql.get(sql);
            if (entry != null) {
                StringSink sink = Misc.getThreadLocalSink();
                Numbers.appendHex(sink, entry.id, true);
                throw SqlException.$(0, "duplicate sql statement: ").put(sql).put(" [id=").put(sink).put(']');
            }
            if (path != null) {
                entry = exportPath.get(path);
                if (entry != null) {
                    StringSink sink = Misc.getThreadLocalSink();
                    Numbers.appendHex(sink, entry.id, true);
                    throw SqlException.$(0, "duplicate export path: ").put(path).put(" [id=").put(sink).put(']');
                }
            }
            long id;
            int index;
            do {
                id = copyIDSupplier.getAsLong();
            } while ((index = activeExports.keyIndex(id)) < 0);
            entry = exportTaskEntryPools.next().of(id, securityContext, sql, path, sqlExecutionCircuitBreaker);
            activeExports.putAt(index, id, entry);
            exportSql.put(sql, entry);
            if (path != null) {
                exportPath.put(path, entry);
            }
            return entry;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean cancel(long id, SecurityContext securityContext) {
        SecurityContext owner = null;
        SqlExecutionCircuitBreaker cb = null;
        lock.readLock().lock();
        try {
            ExportTaskEntry e = activeExports.get(id);
            if (e == null) {
                return false;
            }
            owner = e.getSecurityContext();
            cb = e.getCircuitBreaker();
        } finally {
            lock.readLock().unlock();
        }
        if (securityContext != null && owner != null) {
            owner.authorizeCopyCancel(securityContext);
        }
        if (cb != null) {
            cb.cancel();
        }
        return true;
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

    public boolean getAndCopyEntry(long id, ExportTaskEntry entry) {
        lock.readLock().lock();
        try {
            ExportTaskEntry e = activeExports.get(id);
            if (e != null) {
                entry.id = e.id;
                entry.path = e.path;
                entry.phase = e.phase;
                entry.populatedRowCount = e.populatedRowCount;
                entry.startTime = e.startTime;
                entry.workerId = e.workerId;
                entry.finishedPartitionCount = e.finishedPartitionCount;
                entry.totalPartitionCount = e.totalPartitionCount;
                entry.totalRowCount = e.totalRowCount;
                entry.context = e.context;
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

    public SerialParquetExporter.PhaseStatusReporter getReporter() {
        return reporter;
    }

    public void setReporter(SerialParquetExporter.PhaseStatusReporter reporter) {
        this.reporter = reporter;
    }

    public class ExportTaskEntry implements Mutable {
        AtomicBooleanCircuitBreaker atomicBooleanCircuitBreaker = new AtomicBooleanCircuitBreaker();
        SecurityContext context;
        int finishedPartitionCount = 0;
        long id = INACTIVE_COPY_ID;
        CharSequence path;
        CopyExportRequestTask.Phase phase;
        long populatedRowCount = 0;
        SqlExecutionCircuitBreaker realCircuitBreaker;
        CharSequence sql;
        long startTime = Numbers.LONG_NULL;
        int totalPartitionCount = 0;
        long totalRowCount = 0;
        int workerId = -1;

        @Override
        public void clear() {
            if (id == INACTIVE_COPY_ID) {
                return;
            }
            lock.writeLock().lock();
            try {
                activeExports.remove(id);
                exportSql.remove(sql);
                if (path != null) {
                    exportPath.remove(path);
                }
                atomicBooleanCircuitBreaker.clear();
                context = null;
                realCircuitBreaker = null;
                sql = null;
                path = null;
                phase = null;
                startTime = Numbers.LONG_NULL;
                workerId = -1;
                populatedRowCount = 0;
                finishedPartitionCount = 0;
                totalPartitionCount = 0;
                totalRowCount = 0;
                id = INACTIVE_COPY_ID;
                exportTaskEntryPools.release(this);
            } finally {
                lock.writeLock().unlock();
            }
        }

        public SqlExecutionCircuitBreaker getCircuitBreaker() {
            return realCircuitBreaker;
        }

        public int getFinishedPartitionCount() {
            return finishedPartitionCount;
        }

        public long getId() {
            return id;
        }

        public CharSequence getPath() {
            return path;
        }

        public CopyExportRequestTask.Phase getPhase() {
            return phase;
        }

        public long getPopulatedRowCount() {
            return populatedRowCount;
        }

        public SecurityContext getSecurityContext() {
            return context;
        }

        public CharSequence getSql() {
            return sql;
        }

        public long getStartTime() {
            return startTime;
        }

        public int getTotalPartitionCount() {
            return totalPartitionCount;
        }

        public long getTotalRowCount() {
            return totalRowCount;
        }

        public int getWorkerId() {
            return workerId;
        }

        public ExportTaskEntry of(long id,
                                  SecurityContext context,
                                  CharSequence sql,
                                  CharSequence path,
                                  SqlExecutionCircuitBreaker circuitBreaker) {
            this.id = id;
            this.context = context;
            this.sql = sql;
            this.path = path;
            if (circuitBreaker == null) {
                atomicBooleanCircuitBreaker.reset();
                this.realCircuitBreaker = atomicBooleanCircuitBreaker;
            } else {
                this.realCircuitBreaker = circuitBreaker;
            }
            this.phase = CopyExportRequestTask.Phase.WAITING;
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

        public void setTotalPartitionCount(int totalPartitionCount) {
            this.totalPartitionCount = totalPartitionCount;
        }

        public void setTotalRowCount(long totalRowCount) {
            this.totalRowCount = totalRowCount;
        }
    }
}


