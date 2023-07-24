package io.questdb.cairo;

import io.questdb.Metrics;
import io.questdb.cairo.wal.WalTxnYieldEvents;

@FunctionalInterface
public interface CairoEngineFactory {
    CairoEngine createInstance(CairoConfiguration configuration, WalTxnYieldEvents walTxnYieldEvents, Metrics metrics);
}
