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
import io.questdb.cutlass.parquet.SerialParquetExporter;
import io.questdb.griffin.SqlException;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.ObjectPool;
import io.questdb.std.SimpleReadWriteLock;
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

    public ExportTaskEntry assignExportEntry(SecurityContext securityContext, CharSequence sql, CharSequence path) throws SqlException {
        lock.writeLock().lock();
        try {
            ExportTaskEntry entry = exportSql.get(sql);
            if (entry != null) {
                throw SqlException.$(0, "duplicate sql statement: ").put(sql).put(" [id=").put(entry.id).put(']');
            }
            if (path != null) {
                entry = exportPath.get(path);
                if (entry != null) {
                    throw SqlException.$(0, "duplicate export path: ").put(path).put(" [id=").put(entry.id).put(']');
                }
            }
            long id;
            int index;
            do {
                id = copyIDSupplier.getAsLong();
            } while ((index = activeExports.keyIndex(id)) < 0);
            entry = exportTaskEntryPools.next().of(id, securityContext, sql, path);
            activeExports.putAt(index, id, entry);
            exportSql.put(sql, entry);
            exportPath.put(path, entry);
            return entry;
        } finally {
            lock.writeLock().unlock();
        }
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

    private void releaseEntry(ExportTaskEntry entry) {
        lock.writeLock().lock();
        try {
            activeExports.remove(entry.id);
            exportSql.remove(entry.sql);
            if (entry.path != null) {
                exportPath.remove(entry.path);
            }
            exportTaskEntryPools.release(entry);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public class ExportTaskEntry implements Mutable {
        AtomicBooleanCircuitBreaker circuitBreaker = new AtomicBooleanCircuitBreaker();
        SecurityContext context;
        long id = INACTIVE_COPY_ID;
        CharSequence path;
        CharSequence sql;

        @Override
        public void clear() {
            if (id != INACTIVE_COPY_ID) {
                this.context = null;
                circuitBreaker.clear();
                releaseEntry(this);
                this.id = INACTIVE_COPY_ID;
            }
        }

        public AtomicBooleanCircuitBreaker getCircuitBreaker() {
            return circuitBreaker;
        }

        public long getId() {
            return id;
        }

        public SecurityContext getSecurityContext() {
            return context;
        }

        public ExportTaskEntry of(long id, SecurityContext context, CharSequence sql, CharSequence path) {
            this.id = id;
            this.context = context;
            this.sql = sql;
            this.path = path;
            circuitBreaker.reset();
            return this;
        }
    }
}


