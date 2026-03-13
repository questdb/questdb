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
import io.questdb.std.FilesFacadeImpl;
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

    // Initialized via static holder so it is available during super() constructor
    // call, which invokes getFilesFacade() through virtual dispatch before the
    // subclass field assignment executes.
    private static final ThreadLocal<RoutingFilesFacade> INIT_FF = new ThreadLocal<>();

    private final RoutingFilesFacade routingFf;

    public HybridCairoConfiguration(CharSequence dbRoot) {
        this(dbRoot, new RoutingFilesFacade(
                FilesFacadeImpl.INSTANCE,
                MemFdFilesFacade.INSTANCE
        ));
    }

    public HybridCairoConfiguration(CharSequence dbRoot, RoutingFilesFacade routingFf) {
        // Store in thread-local before super() so getFilesFacade() can find it.
        super(initAndReturn(dbRoot, routingFf));
        this.routingFf = INIT_FF.get();
        INIT_FF.remove();
    }

    private static CharSequence initAndReturn(CharSequence dbRoot, RoutingFilesFacade routingFf) {
        routingFf.setDbRoot(dbRoot);
        INIT_FF.set(routingFf);
        return dbRoot;
    }

    @Override
    public @NotNull FilesFacade getFilesFacade() {
        // During super() construction, routingFf is still null — use thread-local.
        RoutingFilesFacade ff = routingFf;
        if (ff == null) {
            ff = INIT_FF.get();
        }
        return ff;
    }

    /**
     * Returns the RoutingFilesFacade for direct memory-table registration.
     */
    public RoutingFilesFacade getRoutingFilesFacade() {
        return routingFf;
    }
}
