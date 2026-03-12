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

package io.questdb.cairo;

import io.questdb.std.FilesFacade;
import io.questdb.std.MemFdFilesFacade;
import io.questdb.std.RoutingFilesFacade;
import org.jetbrains.annotations.NotNull;

/**
 * CairoConfiguration that supports mixed-mode operation: some tables use
 * anonymous memory (memfd_create) while others use the real filesystem.
 * <p>
 * Uses {@link RoutingFilesFacade} to direct per-table I/O to either
 * {@link MemFdFilesFacade} or the default disk facade.
 * <p>
 * Tables created with {@code CREATE MEMORY TABLE} or
 * {@code CREATE TABLE ... IN VOLUME ':memory:'} are memory-backed.
 * All other tables use the normal disk path.
 */
public class HybridCairoConfiguration extends DefaultCairoConfiguration {

    private final RoutingFilesFacade routingFf;

    public HybridCairoConfiguration(CharSequence dbRoot) {
        this(dbRoot, new RoutingFilesFacade(
                io.questdb.std.FilesFacadeImpl.INSTANCE,
                MemFdFilesFacade.INSTANCE
        ));
    }

    public HybridCairoConfiguration(CharSequence dbRoot, RoutingFilesFacade routingFf) {
        super(dbRoot);
        this.routingFf = routingFf;
        routingFf.setDbRoot(dbRoot);
    }

    @Override
    public @NotNull FilesFacade getFilesFacade() {
        return routingFf;
    }

    /**
     * Returns the RoutingFilesFacade for direct memory-table registration.
     */
    public RoutingFilesFacade getRoutingFilesFacade() {
        return routingFf;
    }
}
