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

package io.questdb.cutlass.pgwire;

import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cutlass.auth.Authenticator;
import io.questdb.std.Chars;

public final class DefaultPgWireAuthenticatorFactory implements PgWireAuthenticatorFactory {
    public static final PgWireAuthenticatorFactory INSTANCE = new DefaultPgWireAuthenticatorFactory();

    public static UsernamePasswordMatcher newPgWireUsernamePasswordMatcher(PGWireConfiguration configuration) {
        String defaultUsername = configuration.getDefaultUsername();
        String defaultPassword = configuration.getDefaultPassword();
        boolean defaultUserEnabled = !Chars.empty(defaultUsername) && !Chars.empty(defaultPassword);

        String readOnlyUsername = configuration.getReadOnlyUsername();
        String readOnlyPassword = configuration.getReadOnlyPassword();
        boolean readOnlyUserValid = !Chars.empty(readOnlyUsername) && !Chars.empty(readOnlyPassword);
        boolean readOnlyUserEnabled = configuration.isReadOnlyUserEnabled() && readOnlyUserValid;

        if (defaultUserEnabled && readOnlyUserEnabled) {
            return new CombiningUsernamePasswordMatcher(
                    new StaticUsernamePasswordMatcher(defaultUsername, defaultPassword),
                    new StaticUsernamePasswordMatcher(readOnlyUsername, readOnlyPassword)
            );
        } else if (defaultUserEnabled) {
            return new StaticUsernamePasswordMatcher(defaultUsername, defaultPassword);
        } else if (readOnlyUserEnabled) {
            return new StaticUsernamePasswordMatcher(readOnlyUsername, readOnlyPassword);
        } else {
            return NeverMatchUsernamePasswordMatcher.INSTANCE;
        }
    }

    @Override
    public Authenticator getPgWireAuthenticator(
            PGWireConfiguration configuration,
            NetworkSqlExecutionCircuitBreaker circuitBreaker,
            CircuitBreakerRegistry registry,
            OptionsListener optionsListener
    ) {
        // Normally, all authenticators instances share native buffers for static passwords as the buffers are allocated
        // and owned by FactoryProviders.
        // But the Default implementation does not use FactoryProviders at all. There is a single static field INSTANCE, see above.
        // Thus, there is nothing what could own and close the buffers. So we allocate buffers for each authenticator
        // and the authenticator will be responsible for closing them.
        final UsernamePasswordMatcher matcher = newPgWireUsernamePasswordMatcher(configuration);

        return new CleartextPasswordPgWireAuthenticator(
                configuration,
                circuitBreaker,
                registry,
                optionsListener,
                matcher,
                true
        );
    }
}
