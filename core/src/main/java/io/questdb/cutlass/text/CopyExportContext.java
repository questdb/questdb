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
import io.questdb.std.Mutable;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

public class CopyExportContext implements Mutable {
    public static final long INACTIVE_COPY_ID = -1;
    private final AtomicLong activeExportID = new AtomicLong(INACTIVE_COPY_ID);
    private final AtomicBooleanCircuitBreaker circuitBreaker = new AtomicBooleanCircuitBreaker();
    private final LongSupplier copyIDSupplier;
    private SecurityContext exportOriginatorSecurityContext = DenyAllSecurityContext.INSTANCE;

    public CopyExportContext(CairoConfiguration configuration) {
        this.copyIDSupplier = configuration.getCopyIDSupplier();
    }

    public long assignActiveExportId(SecurityContext securityContext) {
        final long id = copyIDSupplier.getAsLong();
        if (activeExportID.compareAndSet(INACTIVE_COPY_ID, id)) {
            this.exportOriginatorSecurityContext = securityContext;
            return id;
        }
        return INACTIVE_COPY_ID;
    }

    @Override
    public void clear() {
        activeExportID.set(INACTIVE_COPY_ID);
        exportOriginatorSecurityContext = DenyAllSecurityContext.INSTANCE;
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
}


