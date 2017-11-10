package com.questdb.net.udp.receiver;

import com.questdb.cairo.*;
import com.questdb.cairo.pool.WriterPool;
import com.questdb.misc.NetFacade;
import com.questdb.misc.NetFacadeImpl;
import com.questdb.misc.Os;
import com.questdb.mp.Job;
import com.questdb.mp.Worker;
import com.questdb.net.udp.client.LineProtoSender;
import com.questdb.ql.RecordSourcePrinter;
import com.questdb.std.ObjHashSet;
import com.questdb.std.str.CompositePath;
import com.questdb.std.str.StringSink;
import com.questdb.store.factory.configuration.JournalStructure;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class LinuxLineProtoReceiverTest extends AbstractCairoTest {

    @Test
    public void testSimpleReceive() throws Exception {

        TestUtils.assertMemoryLeak(() -> {
            final String expected = "colour\tshape\tsize\ttimestamp\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000Z\n";

            CairoConfiguration cairoCfg = new DefaultCairoConfiguration(root);
            ReceiverConfiguration receiverCfg = new TestReceiverConfiguration();
            try (WriterPool pool = new WriterPool(cairoCfg);
                 LinuxLineProtoReceiver receiver = new LinuxLineProtoReceiver(receiverCfg, cairoCfg, pool)) {

                CountDownLatch workerHaltLatch = new CountDownLatch(1);

                // create table

                JournalStructure struct = new JournalStructure("tab").$sym("colour").$sym("shape").$double("size").$ts();
                try (CompositePath path = new CompositePath();
                     AppendMemory mem = new AppendMemory()) {
                    TableUtils.create(cairoCfg.getFilesFacade(), path, mem, root, struct.build(), cairoCfg.getMkDirMode());
                }

                // warm writer up
                try (TableWriter w = pool.get("tab")) {
                    w.warmUp();
                }

                ObjHashSet<Job> jobs = new ObjHashSet<>();
                jobs.add(receiver);
                Worker worker = new Worker(jobs, workerHaltLatch);
                worker.start();

                try (LineProtoSender sender = new LineProtoSender("127.0.0.1", receiverCfg.getPort(), 1400)) {
                    for (int i = 0; i < 10; i++) {
                        sender.metric("tab").tag("colour", "blue").tag("shape", "square").field("size", 3.4, 4).$(100000);
                    }
                    sender.flush();
                }

                try (TableReader reader = new TableReader(cairoCfg.getFilesFacade(), root, "tab")) {
                    int count = 1000000;
                    while (true) {
                        if (count-- > 0 && reader.size() < 10) {
                            reader.reload();
                            LockSupport.parkNanos(1);
                        } else {
                            break;
                        }
                    }
                    Assert.assertTrue(count > 0);
                    worker.halt();
                    Assert.assertTrue(workerHaltLatch.await(3, TimeUnit.SECONDS));

                    StringSink sink = new StringSink();
                    RecordSourcePrinter printer = new RecordSourcePrinter(sink);
                    printer.print(reader, true, reader.getMetadata());
                    TestUtils.assertEquals(expected, sink);
                }
            }
        });
    }

    private static class TestReceiverConfiguration implements ReceiverConfiguration {

        @Override
        public NetFacade getNetFacade() {
            return NetFacadeImpl.INSTANCE;
        }

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
            return -1;
        }

        @Override
        public int getCommitRate() {
            return 1024 * 1024;
        }
    }

    static {
        Os.init();
    }
}