package io.questdb.cutlass.pgwire;

import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cutlass.auth.Authenticator;
import io.questdb.network.NetworkFacade;

public class UserDatabasePgWireAuthenticatorFactory implements PgWireAuthenticatorFactory {

    private final PgWireUserDatabase userDatabase;

    public UserDatabasePgWireAuthenticatorFactory(PgWireUserDatabase userDatabase) {
        this.userDatabase = userDatabase;
    }

    @Override
    public Authenticator getPgWireAuthenticator(
            NetworkFacade nf,
            PGWireConfiguration configuration,
            NetworkSqlExecutionCircuitBreaker circuitBreaker,
            CircuitBreakerRegistry registry,
            OptionsListener optionsListener
    ) {
        return new CleartextPasswordPgWireAuthenticator(nf, configuration, circuitBreaker, registry, optionsListener, userDatabase);
    }
}
