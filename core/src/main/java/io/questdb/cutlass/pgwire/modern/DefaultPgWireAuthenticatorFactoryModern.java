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

package io.questdb.cutlass.pgwire.modern;

import io.questdb.BuildInformation;
import io.questdb.BuildInformationHolder;
import io.questdb.DynamicUsernamePasswordMatcher;
import io.questdb.ServerConfiguration;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cutlass.auth.SocketAuthenticator;
import io.questdb.cutlass.auth.UsernamePasswordMatcher;
import io.questdb.cutlass.pgwire.CircuitBreakerRegistry;
import io.questdb.cutlass.pgwire.HexTestsCircuitBreakRegistry;
import io.questdb.cutlass.pgwire.OptionsListener;
import io.questdb.cutlass.pgwire.PGWireConfiguration;
import io.questdb.cutlass.pgwire.PgWireAuthenticatorFactory;
import org.jetbrains.annotations.Nullable;

public final class DefaultPgWireAuthenticatorFactoryModern implements PgWireAuthenticatorFactory {
    public static final PgWireAuthenticatorFactory INSTANCE = new DefaultPgWireAuthenticatorFactoryModern(null);
    private final BuildInformation buildInformation;
    private final ServerConfiguration serverConfiguration;

    public DefaultPgWireAuthenticatorFactoryModern(@Nullable ServerConfiguration serverConfiguration) {
        this.serverConfiguration = serverConfiguration;
        if (serverConfiguration == null) {
            buildInformation = new BuildInformationHolder();
        } else {
            buildInformation = serverConfiguration.getCairoConfiguration().getBuildInformation();
        }
    }

    @Override
    public SocketAuthenticator getPgWireAuthenticator(
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
        final UsernamePasswordMatcher matcher = new DynamicUsernamePasswordMatcher(serverConfiguration, configuration);

        // HexTestsCircuitBreakRegistry implies we are either recording or replaying a hex test.
        // In this case, we don't sends build information to the client. Build informations are volatile by nature, we
        // only record what does not change over time.
        BuildInformation buildInformationToUse = (registry == HexTestsCircuitBreakRegistry.INSTANCE ? null : buildInformation);

        return new CleartextPasswordPgWireAuthenticatorModern(
                configuration,
                buildInformationToUse,
                circuitBreaker,
                registry,
                optionsListener,
                matcher,
                true
        );
    }
}
