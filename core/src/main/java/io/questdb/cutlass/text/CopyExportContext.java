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
import io.questdb.cairo.security.DenyAllSecurityContext;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cutlass.parquet.CopyExportRequestTask;
import io.questdb.cutlass.parquet.SerialParquetExporter;
import io.questdb.std.BoolList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

public class CopyExportContext implements Mutable {
    public static final long INACTIVE_COPY_ID = -1;
    private final AtomicLong activeExportID = new AtomicLong(INACTIVE_COPY_ID);
    private final AtomicBooleanCircuitBreaker circuitBreaker = new AtomicBooleanCircuitBreaker();
    private final LongSupplier copyIDSupplier;
    private CopyExportResult copyExportResult = new CopyExportResult();
    private SecurityContext exportOriginatorSecurityContext = DenyAllSecurityContext.INSTANCE;
    private SerialParquetExporter.PhaseStatusReporter reporter;

    public CopyExportContext(CairoConfiguration configuration) {
        this.copyIDSupplier = configuration.getCopyIDSupplier();
    }

    public long assignActiveExportId(SecurityContext securityContext) {
        final long id = copyIDSupplier.getAsLong();
        if (activeExportID.compareAndSet(INACTIVE_COPY_ID, id)) {
            this.exportOriginatorSecurityContext = securityContext;
            copyExportResult.copyID = id;
            return id;
        }
        return INACTIVE_COPY_ID;
    }

    @Override
    public void clear() {
        activeExportID.set(INACTIVE_COPY_ID);
        exportOriginatorSecurityContext = DenyAllSecurityContext.INSTANCE;
        copyExportResult.clear();
    }

    public long getActiveExportID() {
        return activeExportID.get();
    }

    public AtomicBooleanCircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    public SecurityContext getExportOriginatorSecurityContext() {
        return exportOriginatorSecurityContext;
    }

    public SerialParquetExporter.PhaseStatusReporter getReporter() {
        return reporter;
    }

    public void setReporter(SerialParquetExporter.PhaseStatusReporter reporter) {
        this.reporter = reporter;
    }

    static class CopyExportResult {
        long copyID = INACTIVE_COPY_ID;
        BoolList needCleanUps = new BoolList();
        ObjList<String> paths = new ObjList<>();
        CopyExportRequestTask.Phase phase = CopyExportRequestTask.Phase.NONE;
        CopyExportRequestTask.Status status = CopyExportRequestTask.Status.NONE;

        public void addFilePath(Path path, boolean needCleanUp) {
            paths.add(path.toString());
            needCleanUps.add(needCleanUp);
        }

        public void clear() {
            copyID = INACTIVE_COPY_ID;
            needCleanUps.clear();
            paths.clear();
            phase = CopyExportRequestTask.Phase.NONE;
            status = CopyExportRequestTask.Status.NONE;
        }
    }
}


