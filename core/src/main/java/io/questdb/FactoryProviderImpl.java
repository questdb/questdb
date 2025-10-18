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

package io.questdb;

import io.questdb.cairo.DefaultWalJobFactory;
import io.questdb.cairo.WalJobFactory;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cutlass.auth.LineAuthenticatorFactory;
import io.questdb.cutlass.http.DefaultHttpCookieHandler;
import io.questdb.cutlass.http.DefaultHttpHeaderParserFactory;
import io.questdb.cutlass.http.DefaultHttpSessionStore;
import io.questdb.cutlass.http.HttpAuthenticatorFactory;
import io.questdb.cutlass.http.HttpCookieHandler;
import io.questdb.cutlass.http.HttpSessionStore;
import io.questdb.cutlass.pgwire.DefaultPGAuthenticatorFactory;
import io.questdb.cutlass.pgwire.PGAuthenticatorFactory;
import io.questdb.network.PlainSocketFactory;
import io.questdb.network.SocketFactory;
import org.jetbrains.annotations.NotNull;

public class FactoryProviderImpl implements FactoryProvider {
    private final DefaultWalJobFactory defaultWalJobFactory = new DefaultWalJobFactory();
    private final HttpAuthenticatorFactory httpAuthenticatorFactory;
    private final LineAuthenticatorFactory lineAuthenticatorFactory;
    private final PGAuthenticatorFactory pgAuthenticatorFactory;
    private final SecurityContextFactory securityContextFactory;

    public FactoryProviderImpl(ServerConfiguration configuration) {
        this.lineAuthenticatorFactory = ServerMain.getLineAuthenticatorFactory(configuration);
        this.securityContextFactory = ServerMain.getSecurityContextFactory(configuration);
        this.pgAuthenticatorFactory = new DefaultPGAuthenticatorFactory(configuration);
        this.httpAuthenticatorFactory = ServerMain.getHttpAuthenticatorFactory(configuration);
    }

    @Override
    public @NotNull HttpAuthenticatorFactory getHttpAuthenticatorFactory() {
        return httpAuthenticatorFactory;
    }

    @Override
    public @NotNull HttpCookieHandler getHttpCookieHandler() {
        return DefaultHttpCookieHandler.INSTANCE;
    }

    @Override
    public @NotNull DefaultHttpHeaderParserFactory getHttpHeaderParserFactory() {
        return DefaultHttpHeaderParserFactory.INSTANCE;
    }

    @Override
    public @NotNull SocketFactory getHttpMinSocketFactory() {
        return PlainSocketFactory.INSTANCE;
    }

    @Override
    public @NotNull HttpSessionStore getHttpSessionStore() {
        return DefaultHttpSessionStore.INSTANCE;
    }

    @Override
    public @NotNull SocketFactory getHttpSocketFactory() {
        return PlainSocketFactory.INSTANCE;
    }

    @Override
    public @NotNull LineAuthenticatorFactory getLineAuthenticatorFactory() {
        return lineAuthenticatorFactory;
    }

    @Override
    public @NotNull SocketFactory getLineSocketFactory() {
        return PlainSocketFactory.INSTANCE;
    }

    @Override
    public @NotNull SocketFactory getPGWireSocketFactory() {
        return PlainSocketFactory.INSTANCE;
    }

    @Override
    public @NotNull PGAuthenticatorFactory getPgWireAuthenticatorFactory() {
        return pgAuthenticatorFactory;
    }

    @Override
    public @NotNull SecurityContextFactory getSecurityContextFactory() {
        return securityContextFactory;
    }

    @Override
    public @NotNull WalJobFactory getWalJobFactory() {
        return defaultWalJobFactory;
    }
}
