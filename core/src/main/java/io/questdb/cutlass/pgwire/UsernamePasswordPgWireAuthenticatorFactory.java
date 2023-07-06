package io.questdb.cutlass.pgwire;

import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cutlass.auth.Authenticator;
import io.questdb.network.NetworkFacade;

public class UsernamePasswordPgWireAuthenticatorFactory implements PgWireAuthenticatorFactory {

    private final UsernamePasswordMatcher matcher;

    public UsernamePasswordPgWireAuthenticatorFactory(UsernamePasswordMatcher matcher) {
        this.matcher = matcher;
    }

    @Override
    public Authenticator getPgWireAuthenticator(
            NetworkFacade nf,
            PGWireConfiguration configuration,
            NetworkSqlExecutionCircuitBreaker circuitBreaker,
            CircuitBreakerRegistry registry,
            OptionsListener optionsListener
    ) {
        return new CleartextPasswordPgWireAuthenticator(nf, configuration, circuitBreaker, registry, optionsListener, matcher);
    }
}
