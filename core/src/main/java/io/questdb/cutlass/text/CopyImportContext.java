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
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.security.DenyAllSecurityContext;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.std.Mutable;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

public class CopyImportContext implements Mutable {
    public static final long INACTIVE_COPY_ID = -1;
    private final AtomicLong activeImportID = new AtomicLong(INACTIVE_COPY_ID);
    private final AtomicBooleanCircuitBreaker circuitBreaker;
    // Important assumption: We never access the rnd concurrently, so no need for additional synchronization.
    private final LongSupplier copyIDSupplier;
    private SecurityContext importOriginatorSecurityContext = DenyAllSecurityContext.INSTANCE;

    public CopyImportContext(CairoEngine engine, CairoConfiguration configuration) {
        this.circuitBreaker = new AtomicBooleanCircuitBreaker(engine);
        this.copyIDSupplier = configuration.getCopyIDSupplier();
    }

    public long assignActiveImportId(SecurityContext securityContext) {
        final long id = copyIDSupplier.getAsLong();
        if (activeImportID.compareAndSet(INACTIVE_COPY_ID, id)) {
            this.importOriginatorSecurityContext = securityContext;
            return id;
        }
        return INACTIVE_COPY_ID;
    }

    @Override
    public void clear() {
        activeImportID.set(INACTIVE_COPY_ID);
        importOriginatorSecurityContext = DenyAllSecurityContext.INSTANCE;
    }

    public long getActiveImportID() {
        return activeImportID.get();
    }

    public AtomicBooleanCircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    public SecurityContext getImportOriginatorSecurityContext() {
        return importOriginatorSecurityContext;
    }
}
