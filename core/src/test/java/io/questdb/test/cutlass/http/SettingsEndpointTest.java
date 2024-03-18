package io.questdb.test.cutlass.http;

import io.questdb.*;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.http.client.Response;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.PropServerConfiguration.JsonPropertyValueFormatter.*;

public class SettingsEndpointTest extends AbstractBootstrapTest {
    private static final String SETTINGS_PAYLOAD = "{" +
            "\"cairo.snapshot.instance.id\":\"db\"," +
            "\"cairo.max.file.name.length\":127," +
            "\"cairo.wal.supported\":true" +
            "}";

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testSettingsOSS() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertSettingsRequest(httpClient, "{}");
                }
            }
        });
    }

    @Test
    public void testSettingsWithProps() throws Exception {
        final Bootstrap bootstrap = new Bootstrap(
                new PropBootstrapConfiguration() {
                    @Override
                    public ServerConfiguration getServerConfiguration(Bootstrap bootstrap) throws Exception {
                        return new PropServerConfiguration(
                                bootstrap.getRootDirectory(),
                                bootstrap.loadProperties(),
                                getEnv(),
                                bootstrap.getLog(),
                                bootstrap.getBuildInformation(),
                                new FilesFacadeImpl(),
                                bootstrap.getMicrosecondClock(),
                                (configuration, engine, freeOnExit) -> new FactoryProviderImpl(configuration)
                        ) {
                            @Override
                            public CairoConfiguration getCairoConfiguration() {
                                return new DefaultCairoConfiguration(bootstrap.getRootDirectory()) {
                                    @Override
                                    public void populateSettings(CharSequenceObjHashMap<CharSequence> settings) {
                                        final CairoConfiguration config = getCairoConfiguration();
                                        settings.put(PropertyKey.CAIRO_SNAPSHOT_INSTANCE_ID.getPropertyPath(), str(config.getDbDirectory().toString()));
                                        settings.put(PropertyKey.CAIRO_MAX_FILE_NAME_LENGTH.getPropertyPath(), integer(config.getMaxFileNameLength()));
                                        settings.put(PropertyKey.CAIRO_WAL_SUPPORTED.getPropertyPath(), bool(config.isWalSupported()));
                                    }
                                };
                            }
                        };
                    }
                },
                getServerMainArgs()
        );

        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(bootstrap)) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertSettingsRequest(httpClient, SETTINGS_PAYLOAD);
                }
            }
        });
    }

    private void assertSettingsRequest(HttpClient httpClient, String expectedHttpResponse) {
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
