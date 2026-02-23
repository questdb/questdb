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

package io.questdb;

import io.questdb.cairo.DefaultTickCalendarServiceFactory;
import io.questdb.cairo.DefaultWalJobFactory;
import io.questdb.cairo.TickCalendarServiceFactory;
import io.questdb.cairo.WalJobFactory;
import io.questdb.cairo.security.ReadOnlySecurityContextFactory;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cutlass.auth.AuthUtils;
import io.questdb.cutlass.auth.DefaultLineAuthenticatorFactory;
import io.questdb.cutlass.auth.EllipticCurveAuthenticatorFactory;
import io.questdb.cutlass.auth.LineAuthenticatorFactory;
import io.questdb.cutlass.http.DefaultHttpAuthenticatorFactory;
import io.questdb.cutlass.http.DefaultHttpCookieHandler;
import io.questdb.cutlass.http.DefaultHttpHeaderParserFactory;
import io.questdb.cutlass.http.EmptyHttpSessionStore;
import io.questdb.cutlass.http.HttpAuthenticatorFactory;
import io.questdb.cutlass.http.HttpContextConfiguration;
import io.questdb.cutlass.http.HttpCookieHandler;
import io.questdb.cutlass.http.HttpCookieHandlerImpl;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpSessionStore;
import io.questdb.cutlass.http.HttpSessionStoreImpl;
import io.questdb.cutlass.http.StaticHttpAuthenticatorFactory;
import io.questdb.cutlass.line.tcp.StaticChallengeResponseMatcher;
import io.questdb.cutlass.pgwire.DefaultPGAuthenticatorFactory;
import io.questdb.cutlass.pgwire.PGAuthenticatorFactory;
import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.cutlass.pgwire.ReadOnlyUsersAwareSecurityContextFactory;
import io.questdb.network.PlainSocketFactory;
import io.questdb.network.SocketFactory;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.security.PublicKey;

public class FactoryProviderImpl implements FactoryProvider {
    private final DefaultWalJobFactory defaultWalJobFactory = new DefaultWalJobFactory();
    private final HttpAuthenticatorFactory httpAuthenticatorFactory;
    private final HttpCookieHandler httpCookieHandler;
    private final HttpSessionStore httpSessionStore;
    private final LineAuthenticatorFactory lineAuthenticatorFactory;
    private final PGAuthenticatorFactory pgAuthenticatorFactory;
    private final SecurityContextFactory securityContextFactory;

    public FactoryProviderImpl(ServerConfiguration configuration) {
        httpCookieHandler = getHttpCookieHandler(configuration);
        httpSessionStore = getHttpSessionStore(configuration);
        lineAuthenticatorFactory = getLineAuthenticatorFactory(configuration);
        securityContextFactory = getSecurityContextFactory(configuration);
        pgAuthenticatorFactory = new DefaultPGAuthenticatorFactory(configuration);
        httpAuthenticatorFactory = getHttpAuthenticatorFactory(configuration);
    }

    public static LineAuthenticatorFactory getLineAuthenticatorFactory(ServerConfiguration configuration) {
        LineAuthenticatorFactory authenticatorFactory;
        // create default authenticator for Line TCP protocol
        if (configuration.getLineTcpReceiverConfiguration().isEnabled() && configuration.getLineTcpReceiverConfiguration().getAuthDB() != null) {
            // we need "root/" here, not "root/db/"
            final String rootDir = new File(configuration.getCairoConfiguration().getDbRoot()).getParent();
            final String absPath = new File(rootDir, configuration.getLineTcpReceiverConfiguration().getAuthDB()).getAbsolutePath();
            CharSequenceObjHashMap<PublicKey> authDb = AuthUtils.loadAuthDb(absPath);
            authenticatorFactory = new EllipticCurveAuthenticatorFactory(() -> new StaticChallengeResponseMatcher(authDb));
        } else {
            authenticatorFactory = DefaultLineAuthenticatorFactory.INSTANCE;
        }
        return authenticatorFactory;
    }

    @Override
    public @NotNull HttpAuthenticatorFactory getHttpAuthenticatorFactory() {
        return httpAuthenticatorFactory;
    }

    @Override
    public @NotNull HttpCookieHandler getHttpCookieHandler() {
        return httpCookieHandler;
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
        return httpSessionStore;
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

    @Override
    public @NotNull TickCalendarServiceFactory getTickCalendarServiceFactory() {
        return DefaultTickCalendarServiceFactory.INSTANCE;
    }

    private static HttpAuthenticatorFactory getHttpAuthenticatorFactory(ServerConfiguration configuration) {
        HttpFullFatServerConfiguration httpConfig = configuration.getHttpServerConfiguration();
        String username = httpConfig.getUsername();
        if (Chars.empty(username)) {
            return DefaultHttpAuthenticatorFactory.INSTANCE;
        }
        return new StaticHttpAuthenticatorFactory(username, httpConfig.getPassword());
    }

    private static HttpCookieHandler getHttpCookieHandler(ServerConfiguration configuration) {
        if (Chars.empty(configuration.getHttpServerConfiguration().getUsername())) {
            return DefaultHttpCookieHandler.INSTANCE;
        }
        return new HttpCookieHandlerImpl();
    }

    private static HttpSessionStore getHttpSessionStore(ServerConfiguration configuration) {
        if (Chars.empty(configuration.getHttpServerConfiguration().getUsername())) {
            return EmptyHttpSessionStore.INSTANCE;
        }
        return new HttpSessionStoreImpl(configuration);
    }

    private static SecurityContextFactory getSecurityContextFactory(ServerConfiguration configuration) {
        boolean readOnlyInstance = configuration.getCairoConfiguration().isReadOnlyInstance();
        if (readOnlyInstance) {
            return ReadOnlySecurityContextFactory.INSTANCE;
        } else {
            PGConfiguration pgConfiguration = configuration.getPGWireConfiguration();
            HttpContextConfiguration httpContextConfiguration = configuration.getHttpServerConfiguration().getHttpContextConfiguration();
            boolean settingsReadOnly = configuration.getHttpServerConfiguration().isSettingsReadOnly();
            boolean pgWireReadOnlyContext = pgConfiguration.readOnlySecurityContext();
            boolean pgWireReadOnlyUserEnabled = pgConfiguration.isReadOnlyUserEnabled();
            String pgWireReadOnlyUsername = pgWireReadOnlyUserEnabled ? pgConfiguration.getReadOnlyUsername() : null;
            boolean httpReadOnly = httpContextConfiguration.readOnlySecurityContext();
            return new ReadOnlyUsersAwareSecurityContextFactory(pgWireReadOnlyContext, pgWireReadOnlyUsername, httpReadOnly, settingsReadOnly);
        }
    }
}
