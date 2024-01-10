package io.questdb.test.cutlass.http;

import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.ServerMain;
import io.questdb.cutlass.http.client.Chunk;
import io.questdb.cutlass.http.client.ChunkedResponse;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class SettingsEndpointTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testConfiguration() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newInstance(new DefaultHttpClientConfiguration())) {
                    assertSettingsRequest(httpClient, "{}");
                }
            }
        });
    }

    private void assertSettingsRequest(
            HttpClient httpClient,
            String expectedHttpResponse
    ) {
        final HttpClient.Request request = httpClient.newRequest();
        request.GET().url("/settings");
        HttpClient.ResponseHeaders response = request.send("localhost", HTTP_PORT);
        response.await();

        TestUtils.assertEquals(String.valueOf(200), response.getStatusCode());

        final StringSink sink = new StringSink();

        Chunk chunk;
        final ChunkedResponse chunkedResponse = response.getChunkedResponse();
        while ((chunk = chunkedResponse.recv()) != null) {
            Utf8s.utf8ToUtf16(chunk.lo(), chunk.hi(), sink);
        }

        TestUtils.assertEquals(expectedHttpResponse, sink);
        sink.clear();
    }
}
