package com.questdb.net.udp.receiver;

import com.questdb.cairo.*;
import com.questdb.cairo.pool.WriterPool;
import com.questdb.misc.Os;
import com.questdb.mp.Job;
import com.questdb.mp.Worker;
import com.questdb.net.udp.client.LineProtoSender;
import com.questdb.std.ObjHashSet;
import com.questdb.std.str.CompositePath;
import com.questdb.store.factory.configuration.JournalStructure;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

public class LineProtoReceiverTest extends AbstractCairoTest {

    @Test
    @Ignore
    public void testSimpleReceive() throws Exception {
        CairoConfiguration cairoCfg = new DefaultCairoConfiguration(root);
        ReceiverConfiguration receiverCfg = new TestReceiverConfiguration();
        WriterPool pool = new WriterPool(cairoCfg);
        LineProtoReceiver receiver = new LineProtoReceiver(receiverCfg, cairoCfg, pool);
        CountDownLatch workerHaltLatch = new CountDownLatch(1);
        JournalStructure struct = new JournalStructure("tab").$sym("colour").$sym("shape").$double("size").$ts();
        try (CompositePath path = new CompositePath();
             AppendMemory mem = new AppendMemory()) {
            TableUtils.create(cairoCfg.getFilesFacade(), path, mem, root, struct.build(), cairoCfg.getMkDirMode());
        }

        Os.schedSetAffinity(Os.getPid(), 0);
        try (TableWriter w = pool.get("tab")) {
            TableWriter.Row r = w.newRow(0);
            r.putStr(0, null);
            r.putStr(1, null);
            r.putDouble(2, Double.NaN);
            r.cancel();
            w.rollback();
        }

        ObjHashSet<Job> jobs = new ObjHashSet<>();
        jobs.add(receiver);

        Worker worker = new Worker(jobs, workerHaltLatch);
        worker.start();
//
        LineProtoSender sender = new LineProtoSender("127.0.0.1", receiverCfg.getPort(), 1200);

        for (int i = 0; i < 10000000; i++) {
            sender.metric("tab").tag("colour", "blue").tag("shape", "square").field("size", 3.4, 4).$();
        }
        sender.flush();

        System.out.println("SENDER DONE");

        Thread.sleep(2000);

        worker.halt();
        receiver.close();

        System.out.println("WORKER HALTED");
        workerHaltLatch.await();

        try (TableReader reader = new TableReader(cairoCfg.getFilesFacade(), root, "tab")) {
            System.out.println(reader.size());
        }

//        System.out.println("WAITING");
//        Thread.sleep(1000000000);
    }

    private static class TestReceiverConfiguration implements ReceiverConfiguration {
        @Override
        public CharSequence getBindIPv4Address() {
            return "127.0.0.1";
        }

        @Override
        public CharSequence getGroupIPv4Address() {
            return "234.5.6.7";
        }

        @Override
        public int getMsgBufferSize() {
            return 2048;
        }

        @Override
        public int getMsgCount() {
            return 10000;
        }

        @Override
        public int getPort() {
            return 4567;
        }

        @Override
        public int getReceiveBufferSize() {
            return 8 * 1024 * 1024;
        }
    }

    static {
        Os.init();
    }
}