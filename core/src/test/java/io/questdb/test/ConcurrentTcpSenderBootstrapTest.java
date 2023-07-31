package io.questdb.test;

import io.questdb.ServerMain;
import io.questdb.client.Sender;
import io.questdb.std.Files;
import io.questdb.test.cutlass.line.tcp.AbstractLineTcpReceiverTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

public class ConcurrentTcpSenderBootstrapTest extends AbstractBootstrapTest {

    private static final int CONCURRENCY_LEVEL = 64;

    @Before
    public void setUp() {
        super.setUp();
        final String confPath = root + Files.SEPARATOR + "conf";
        String file = confPath + Files.SEPARATOR + "auth.txt";
        TestUtils.unchecked(() -> {
            ILP_WORKER_COUNT = CONCURRENCY_LEVEL;
            createDummyConfiguration("line.tcp.auth.db.path=conf/auth.txt");
            try (PrintWriter writer = new PrintWriter(file, CHARSET)) {
                writer.println("testUser1	ec-p-256-sha256	AKfkxOBlqBN8uDfTxu2Oo6iNsOPBnXkEH4gt44tBJKCY	AL7WVjoH-IfeX_CXo5G1xXKp_PqHUrdo3xeRyDuWNbBX");
            } catch (FileNotFoundException | UnsupportedEncodingException e) {
                throw new AssertionError("Failed to create auth.txt", e);
            }
        });
        dbPath.parent().$();
    }


    @Test
    public void testConcurrentAuth() throws Exception {
        int nThreads = CONCURRENCY_LEVEL;
        int iterationCount = 10;
        // this test uses more complex bootstrap configuration than most of the other TCP Sender/Receiver tests
        // the goal is to have it as close to the real production QuestDB server as possible.
        // including using the same authentication factories and configuration
        TestUtils.assertMemoryLeak(() -> {
            // run multiple iteration to increase chances of hitting a race condition
            for (int i = 0; i < iterationCount; i++) {
                testConcurrentSenders(nThreads, i + nThreads);
            }
        });
    }

    private static void testConcurrentSenders(int nThreads, int startingOffset) {
        try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
            serverMain.start();
            for (int i = startingOffset; i < nThreads + startingOffset; i++) {
                int threadNo = i;
                new Thread(() -> {
                    try (Sender sender = Sender.builder()
                            .address("localhost")
                            .port(serverMain.getConfiguration().getLineTcpReceiverConfiguration().getDispatcherConfiguration().getBindPort())
                            .enableAuth(AbstractLineTcpReceiverTest.AUTH_KEY_ID1)
                            .authToken(AbstractLineTcpReceiverTest.AUTH_TOKEN_KEY1)
                            .build()) {
                        sender.table("test" + threadNo).stringColumn("value", "test").atNow();
                        sender.flush();
                    }
                }).start();
            }
            for (int i = startingOffset; i < nThreads + startingOffset; i++) {
                while (serverMain.getEngine().getTableTokenIfExists("test" + i) == null) {
                    // intentionally empty
                }
            }
        }
    }
}
