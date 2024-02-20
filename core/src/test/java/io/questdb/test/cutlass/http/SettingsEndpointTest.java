package io.questdb.test.cutlass.http;

import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.ServerMain;
import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.http.client.Response;
import io.questdb.std.str.Utf8StringSink;
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

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertSettingsRequest(httpClient, "{}");
                }
            }
        });
    }

    private void assertSettingsRequest(
            HttpClient httpClient,
            String expectedHttpResponse
    ) {
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
        request.GET().url("/settings");
        try (HttpClient.ResponseHeaders responseHeaders = request.send()) {
            responseHeaders.await();

            TestUtils.assertEquals(String.valueOf(200), responseHeaders.getStatusCode());

            final Utf8StringSink sink = new Utf8StringSink();

            Fragment fragment;
            final Response response = responseHeaders.getResponse();
            while ((fragment = response.recv()) != null) {
                Utf8s.strCpy(fragment.lo(), fragment.hi(), sink);
            }

            TestUtils.assertEquals(expectedHttpResponse, sink.toString());
            sink.clear();
        }
    }
}
