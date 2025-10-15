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
import io.questdb.cairo.security.AllowAllSecurityContextFactory;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cutlass.auth.DefaultLineAuthenticatorFactory;
import io.questdb.cutlass.auth.LineAuthenticatorFactory;
import io.questdb.cutlass.http.DefaultHttpAuthenticatorFactory;
import io.questdb.cutlass.http.DefaultHttpCookieHandler;
import io.questdb.cutlass.http.DefaultHttpHeaderParserFactory;
import io.questdb.cutlass.http.DefaultHttpSessionStore;
import io.questdb.cutlass.http.HttpAuthenticatorFactory;
import io.questdb.cutlass.http.HttpCookieHandler;
import io.questdb.cutlass.http.HttpHeaderParserFactory;
import io.questdb.cutlass.http.HttpSessionStore;
import io.questdb.cutlass.pgwire.DefaultPGAuthenticatorFactory;
import io.questdb.cutlass.pgwire.PGAuthenticatorFactory;
import io.questdb.network.PlainSocketFactory;
import io.questdb.network.SocketFactory;
import org.jetbrains.annotations.NotNull;

public class DefaultFactoryProvider implements FactoryProvider {
    public static final DefaultFactoryProvider INSTANCE = new DefaultFactoryProvider();

    @Override
    public @NotNull HttpAuthenticatorFactory getHttpAuthenticatorFactory() {
        return DefaultHttpAuthenticatorFactory.INSTANCE;
    }

    @Override
    public @NotNull HttpCookieHandler getHttpCookieHandler() {
        return DefaultHttpCookieHandler.INSTANCE;
    }

    @Override
    public @NotNull HttpHeaderParserFactory getHttpHeaderParserFactory() {
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
        return DefaultLineAuthenticatorFactory.INSTANCE;
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
        return DefaultPGAuthenticatorFactory.INSTANCE;
    }

    @Override
    public @NotNull SecurityContextFactory getSecurityContextFactory() {
        return AllowAllSecurityContextFactory.INSTANCE;
    }

    @Override
    public @NotNull WalJobFactory getWalJobFactory() {
        return DefaultWalJobFactory.INSTANCE;
    }
}
