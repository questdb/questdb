package io.questdb.cairo;

import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.mp.SynchronizedJob;

public class DefaultWalJobFactory implements WalJobFactory {
    public static final WalJobFactory INSTANCE = new DefaultWalJobFactory();

    @Override
    public SynchronizedJob createCheckWalTransactionsJob(CairoEngine engine) {
        return new CheckWalTransactionsJob(engine);
    }

    @Override
    public WalPurgeJob createWalPurgeJob(CairoEngine engine) {
        return new WalPurgeJob(engine);
    }
}
