package io.questdb.test.cutlass.http;

import io.questdb.Bootstrap;
import io.questdb.DefaultBootstrapConfiguration;
import io.questdb.ServerMain;
import io.questdb.test.BootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class HttpMinBootstrapTest extends BootstrapTest {

    private static final String HEALTHY_RESPONSE = "HTTP/1.1 200 OK\r\n" +
            "Server: questDB/1.0\r\n" +
            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
            "Transfer-Encoding: chunked\r\n" +
            "Content-Type: text/plain\r\n" +
            "\r\n" +
            "0f\r\n" +
            "Status: Healthy\r\n" +
            "00\r\n" +
            "\r\n";

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testHttpServerEnabledMinStillWorking() throws Exception {
        Map<String, String> env = new HashMap<>(System.getenv());
        env.put("QDB_HTTP_ENABLED", "false");
        Bootstrap bootstrap = new Bootstrap(
                new DefaultBootstrapConfiguration() {
                    @Override
                    public Map<String, String> getEnv() {
                        return env;
                    }
                },
                getServerMainArgs()
        );
        TestUtils.assertMemoryLeak(() -> {
            try (ServerMain serverMain = new ServerMain(bootstrap)) {
                serverMain.start();

                new SendAndReceiveRequestBuilder().withPort(HTTP_MIN_PORT).execute("GET / HTTP/1.1\r\n\r\n", HEALTHY_RESPONSE);
            }
        });
    }

    @Test
    public void testMinHttpEnabledByDefault() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();

                new SendAndReceiveRequestBuilder().withPort(HTTP_MIN_PORT).execute("GET / HTTP/1.1\r\n\r\n", HEALTHY_RESPONSE);
            }
        });
    }
}
