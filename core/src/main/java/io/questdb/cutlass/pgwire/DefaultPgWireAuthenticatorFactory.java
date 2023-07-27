/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.ServerMain;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cutlass.auth.Authenticator;
import io.questdb.network.NetworkFacade;
import io.questdb.std.str.DirectByteCharSink;

public final class DefaultPgWireAuthenticatorFactory implements PgWireAuthenticatorFactory {
    public static final PgWireAuthenticatorFactory INSTANCE = new DefaultPgWireAuthenticatorFactory();

    @Override
    public Authenticator getPgWireAuthenticator(
            NetworkFacade nf,
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
        DirectByteCharSink defaultUserPasswordSink = new DirectByteCharSink(4);
        DirectByteCharSink readOnlyUserPasswordSink = new DirectByteCharSink(4);
        UsernamePasswordMatcher matcher = new CustomCloseActionPasswordMatcherDelegate(
                ServerMain.newPgWireUsernamePasswordMatcher(configuration, defaultUserPasswordSink, readOnlyUserPasswordSink),
                () -> {
                    defaultUserPasswordSink.close();
                    readOnlyUserPasswordSink.close();
                }
        );

        return new CleartextPasswordPgWireAuthenticator(
                nf,
                configuration,
                circuitBreaker,
                registry,
                optionsListener,
                matcher,
                true
        );
    }
}
