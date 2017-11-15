package com.questdb.cutlass.receiver;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.pool.WriterPool;
import com.questdb.mp.Job;

@FunctionalInterface
public interface ReceiverFactory {
    Job createReceiver(ReceiverConfiguration receiverCfg, CairoConfiguration cairoCfg, WriterPool pool);
}
