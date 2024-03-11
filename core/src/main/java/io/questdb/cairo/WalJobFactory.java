package io.questdb.cairo;

import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.mp.SynchronizedJob;

public interface WalJobFactory {
    SynchronizedJob createCheckWalTransactionsJob(CairoEngine engine);

    WalPurgeJob createWalPurgeJob(CairoEngine engine);
}
