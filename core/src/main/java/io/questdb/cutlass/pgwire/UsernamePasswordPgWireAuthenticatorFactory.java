package io.questdb.cutlass.pgwire;

import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cutlass.auth.Authenticator;
import io.questdb.network.NetworkFacade;
import io.questdb.std.ObjectFactory;

public class UsernamePasswordPgWireAuthenticatorFactory implements PgWireAuthenticatorFactory {

    private final ObjectFactory<UsernamePasswordMatcher> matcherFactory;

    public UsernamePasswordPgWireAuthenticatorFactory(ObjectFactory<UsernamePasswordMatcher> matcherFactory) {
        this.matcherFactory = matcherFactory;
    }

    @Override
    public Authenticator getPgWireAuthenticator(
            NetworkFacade nf,
            PGWireConfiguration configuration,
            NetworkSqlExecutionCircuitBreaker circuitBreaker,
            CircuitBreakerRegistry registry,
            OptionsListener optionsListener
    ) {
        return new CleartextPasswordPgWireAuthenticator(nf, configuration, circuitBreaker, registry, optionsListener, matcherFactory.newInstance());
    }
}
