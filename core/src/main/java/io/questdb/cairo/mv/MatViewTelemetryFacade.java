package io.questdb.cairo.mv;

import io.questdb.cairo.TableToken;

@FunctionalInterface
public interface MatViewTelemetryFacade {
    void store(short event, TableToken tableToken, long baseTableTxn, CharSequence errorMessage, long latencyUs);
}
