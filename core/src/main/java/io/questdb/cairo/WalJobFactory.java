package io.questdb.cairo;

import io.questdb.mp.SynchronizedJob;

public interface WalJobFactory {
    SynchronizedJob createCheckWalTransactionsJob(CairoEngine engine);
}
