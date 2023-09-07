package io.questdb.cutlass.pgwire;

import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cutlass.auth.Authenticator;

public class UsernamePasswordPgWireAuthenticatorFactory implements PgWireAuthenticatorFactory {

    private final UsernamePasswordMatcher matcher;

    public UsernamePasswordPgWireAuthenticatorFactory(UsernamePasswordMatcher matcher) {
        this.matcher = matcher;
    }

    @Override
    public Authenticator getPgWireAuthenticator(
            PGWireConfiguration configuration,
            NetworkSqlExecutionCircuitBreaker circuitBreaker,
            CircuitBreakerRegistry registry,
            OptionsListener optionsListener
    ) {
        return new CleartextPasswordPgWireAuthenticator(configuration, circuitBreaker, registry, optionsListener, matcher, false);
    }
}
