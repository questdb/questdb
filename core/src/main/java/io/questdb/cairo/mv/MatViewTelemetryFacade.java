package io.questdb.cairo.mv;

import io.questdb.cairo.TableToken;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@FunctionalInterface
public interface MatViewTelemetryFacade {
    void store(
            short event,
            @NotNull TableToken tableToken,
            long baseTableTxn,
            @Nullable CharSequence errorMessage,
            long latencyUs
    );
}
