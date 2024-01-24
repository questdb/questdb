package io.questdb.test;

import io.questdb.*;
import io.questdb.cutlass.json.JsonException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class ReloadingPropServerConfigurationTest {
    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    protected static final Log LOG = LogFactory.getLog(ReloadingPropServerConfigurationTest.class);
    protected static String root;

    @AfterClass
    public static void afterClass() {
        TestUtils.removeTestPath(root);
    }

    @BeforeClass
    public static void setupMimeTypes() throws IOException {
        File root = new File(temp.getRoot(), "root");
        TestUtils.copyMimeTypes(root.getAbsolutePath());
        ReloadingPropServerConfigurationTest.root = root.getAbsolutePath();
    }


    @Test
    public void testSimpleReload() throws Exception {
        Properties properties = new Properties();
        ReloadingPropServerConfiguration configuration = newReloadingPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(4, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getConnectionPoolInitialCapacity());

        properties.setProperty(String.valueOf(PropertyKey.HTTP_CONNECTION_POOL_INITIAL_CAPACITY), "99");
        Assert.assertEquals("99", properties.getProperty(String.valueOf(PropertyKey.HTTP_CONNECTION_POOL_INITIAL_CAPACITY)));
        configuration.reload(properties, null);
        Assert.assertEquals(99, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getConnectionPoolInitialCapacity());
    }

    @Test
    public void testConcurrentReload() throws Exception {
        Properties properties = new Properties();
        ReloadingPropServerConfiguration configuration = newReloadingPropServerConfiguration(root, properties, null, new BuildInformationHolder());

        int concurrencyLevel = 128;
        String newValueStr = "99";
        int newValue = Integer.parseInt(newValueStr);
        Duration timeout =  Duration.ofSeconds(5);
        LocalTime start = LocalTime.now();
        AtomicInteger threadsFinished = new AtomicInteger();

        Assert.assertNotEquals(newValue, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getConnectionPoolInitialCapacity());

        for (int i = 0; i < concurrencyLevel; i++) {

            new Thread(() -> {
                while (true) {
                    if (configuration.getHttpServerConfiguration().getHttpContextConfiguration().getConnectionPoolInitialCapacity() == newValue) {
                        threadsFinished.incrementAndGet();
                        return;
                    }
                    try {
                        Thread.sleep(ThreadLocalRandom.current().nextInt(0, 500));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();
        }

        properties.setProperty(String.valueOf(PropertyKey.HTTP_CONNECTION_POOL_INITIAL_CAPACITY), newValueStr);
        configuration.reload(properties, null);

        while (threadsFinished.get() < concurrencyLevel) {
            Thread.sleep(50);
            Assert.assertTrue(LocalTime.now().isBefore(start.plus(timeout)));
        }

    }

    @NotNull
    protected ReloadingPropServerConfiguration newReloadingPropServerConfiguration(
            String root,
            Properties properties,
            @Nullable Map<String, String> env,
            BuildInformation buildInformation
    ) throws ServerConfigurationException, JsonException {
        return new ReloadingPropServerConfiguration(root, properties, env, ReloadingPropServerConfigurationTest.LOG, buildInformation);
    }

}
