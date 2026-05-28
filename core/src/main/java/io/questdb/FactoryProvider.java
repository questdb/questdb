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
    TickCalendarServiceFactory getTickCalendarServiceFactory();

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
     * The OSS default returns a {@link PerQueryMemoryTrackerProvider}
     * configured from the supplied {@link CairoConfiguration}. An enterprise
     * build overrides this to return its per-principal implementation.
     */
    @NotNull
    default MemoryTrackerProvider getMemoryTrackerProvider(@NotNull CairoConfiguration cairoConfiguration) {
        return new PerQueryMemoryTrackerProvider(
                cairoConfiguration.getQueryMemoryLimitBytes(),
                cairoConfiguration.getMatViewRefreshMemoryLimitBytes(),
                cairoConfiguration.getWalApplyMemoryLimitBytes(),
                cairoConfiguration.getMemoryTrackerPoolCapacity()
        );
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
    WalJobFactory getWalJobFactory();
}
