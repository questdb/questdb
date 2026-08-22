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

package io.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TickCalendarServiceFactory;
import io.questdb.cairo.WalJobFactory;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.async.WorkStealingStrategy;
import io.questdb.cairo.sql.async.WorkStealingStrategyFactory;
import io.questdb.cutlass.auth.LineAuthenticatorFactory;
import io.questdb.cutlass.http.DefaultRejectProcessorFactory;
import io.questdb.cutlass.http.HttpAuthenticatorFactory;
import io.questdb.cutlass.http.HttpCookieHandler;
import io.questdb.cutlass.http.HttpHeaderParserFactory;
import io.questdb.cutlass.http.HttpSessionStore;
import io.questdb.cutlass.http.RejectProcessorFactory;
import io.questdb.cutlass.http.processors.TextImportRequestHeaderProcessor;
import io.questdb.cutlass.pgwire.PGAuthenticatorFactory;
import io.questdb.network.SocketFactory;
import io.questdb.std.MemoryTrackerProvider;
import io.questdb.std.PerQueryMemoryTrackerProvider;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;

public interface FactoryProvider extends QuietCloseable {

    @Override
    default void close() {
    }

    @NotNull
    HttpAuthenticatorFactory getHttpAuthenticatorFactory();

    @NotNull
    HttpCookieHandler getHttpCookieHandler();

    @NotNull
    HttpHeaderParserFactory getHttpHeaderParserFactory();

    @NotNull
    SocketFactory getHttpMinSocketFactory();

    @NotNull
    HttpSessionStore getHttpSessionStore();

    @NotNull
    SocketFactory getHttpSocketFactory();

    @NotNull
    LineAuthenticatorFactory getLineAuthenticatorFactory();

    @NotNull
    SocketFactory getLineSocketFactory();

    /**
     * Per-engine source of per-workload {@link io.questdb.std.MemoryTracker}
     * instances. Called once at engine construction; the returned provider is
     * owned by the engine and closed from {@code CairoEngine.close()}.
     * <p>
     * The OSS default returns a {@link PerQueryMemoryTrackerProvider} backed by
     * the supplied {@link CairoConfiguration}; the provider reads each workload's
     * limit from that configuration on every acquisition, so a dynamic reload of
     * the limit takes effect without rebuilding the provider. An enterprise build
     * overrides this to return its per-principal implementation.
     */
    @NotNull
    default MemoryTrackerProvider getMemoryTrackerProvider(@NotNull CairoConfiguration cairoConfiguration) {
        return new PerQueryMemoryTrackerProvider(cairoConfiguration);
    }

    @NotNull
    SocketFactory getPGWireSocketFactory();

    @NotNull
    PGAuthenticatorFactory getPgWireAuthenticatorFactory();

    @NotNull
    default RejectProcessorFactory getRejectProcessorFactory() {
        return DefaultRejectProcessorFactory.INSTANCE;
    }

    @NotNull
    SecurityContextFactory getSecurityContextFactory();

    @NotNull
    default TextImportRequestHeaderProcessor getTextImportRequestHeaderProcessor() {
        return TextImportRequestHeaderProcessor.DEFAULT;
    }

    @NotNull
    TickCalendarServiceFactory getTickCalendarServiceFactory();

    @NotNull
    WalJobFactory getWalJobFactory();

    /**
     * Supplies the work stealing strategy a page frame sequence binds for the given atom. The
     * default is the one {@link WorkStealingStrategyFactory} picks from the configured threshold;
     * overriding it lets a test wrap the owner's stealing decision, which is otherwise unreachable
     * because the sequence builds its own strategy in its constructor.
     */
    @NotNull
    default WorkStealingStrategy getWorkStealingStrategy(
            @NotNull CairoConfiguration configuration,
            int workerCount,
            @NotNull StatefulAtom atom
    ) {
        return WorkStealingStrategyFactory.getInstance(configuration, workerCount);
    }
}
