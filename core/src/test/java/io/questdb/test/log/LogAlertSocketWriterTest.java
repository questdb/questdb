/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.log;

import io.questdb.log.LogAlertSocket;
import io.questdb.log.LogAlertSocketWriter;
import io.questdb.log.LogError;
import io.questdb.log.LogFactory;
import io.questdb.log.LogLevel;
import io.questdb.log.LogRecordUtf8Sink;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.network.NetworkError;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.CyclicBarrier;
import java.util.function.Consumer;

import static io.questdb.log.LogAlertSocketWriter.ALERT_PROPS;
import static io.questdb.log.LogAlertSocketWriter.DEFAULT_ALERT_TPT_FILE;

public class LogAlertSocketWriterTest {
    private static final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    private static String root;
    private Rnd rand;
    private Utf8StringSink sink;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        root = temp.newFolder("dbRoot").getAbsolutePath();
    }

    @Before
    public void setUp() {
        rand = new Rnd();
        sink = new Utf8StringSink();
    }

    @Test
    public void testBindPropertiesAlertTargetsDefault() throws Exception {
        withLogAlertSocketWriter(writer -> {
            writer.setOutBufferSize(String.valueOf(1024));
            writer.setAlertTargets("\"\"");
            writer.bindProperties(LogFactory.getInstance());
            Assert.assertNotNull(LogAlertSocket.localHostIp);
            Assert.assertEquals("127.0.0.1:9093", writer.getAlertTargets());
        });
    }

    @Test
    public void testBindPropertiesBadTemplateFile() throws Exception {
        withLogAlertSocketWriter(writer -> {
            writer.bindProperties(LogFactory.getInstance());
            Assert.assertEquals(LogAlertSocket.OUT_BUFFER_SIZE, writer.getOutBufferSize());
            Assert.assertEquals(LogAlertSocketWriter.DEFAULT_ALERT_TPT_FILE, writer.getLocation());
            Assert.assertEquals(LogAlertSocket.DEFAULT_HOST + ":" + LogAlertSocket.DEFAULT_PORT, writer.getAlertTargets());
            writer.close();

            writer.setOutBufferSize("1978");
            writer.setLocation(Files.getResourcePath(getClass().getResource("/log-file.conf")));
            writer.setAlertTargets("127.0.0.1:8989");
            try {
                writer.bindProperties(LogFactory.getInstance());
                Assert.fail();
            } catch (LogError e) {
                TestUtils.assertContains(
                        e.getMessage(),
                        "Bad template, no ${ALERT_MESSAGE} declaration found"
                );
            }
            Assert.assertEquals(1978, writer.getOutBufferSize());
            Assert.assertTrue(writer.getLocation().endsWith("/log-file.conf"));
            Assert.assertEquals("127.0.0.1:8989", writer.getAlertTargets());
        });
    }

    @Test
    public void testBindPropertiesDefaultAlertHost() throws Exception {
        withLogAlertSocketWriter(writer -> {
            writer.setDefaultAlertHost("127.0.0.1");
            writer.setAlertTargets("\"\"");
            writer.bindProperties(LogFactory.getInstance());
            Assert.assertNotNull(LogAlertSocket.localHostIp);
            Assert.assertEquals("127.0.0.1", writer.getDefaultAlertHost());
        });
    }

    @Test
    public void testBindPropertiesDefaultAlertHostBadValue() throws Exception {
        withLogAlertSocketWriter(writer -> {
            writer.setDefaultAlertHost("pineapple");
            writer.setAlertTargets("\"\"");
            try {
                writer.bindProperties(LogFactory.getInstance());
            } catch (NetworkError e) {
                Assert.assertEquals("[0] invalid address [pineapple]", e.getMessage());
            }
        });
    }

    @Test
    public void testBindPropertiesDefaultAlertPort() throws Exception {
        withLogAlertSocketWriter(writer -> {
            writer.setDefaultAlertPort(String.valueOf(12));
            writer.setAlertTargets("\"\"");
            writer.bindProperties(LogFactory.getInstance());
            Assert.assertNotNull(LogAlertSocket.localHostIp);
            Assert.assertEquals(12, writer.getDefaultAlertPort());
        });
    }

    @Test
    public void testBindPropertiesDefaultAlertPortBadValue() throws Exception {
        withLogAlertSocketWriter(writer -> {
            writer.setDefaultAlertPort("pineapple");
            writer.setAlertTargets("\"\"");
            try {
                writer.bindProperties(LogFactory.getInstance());
            } catch (LogError e) {
                Assert.assertEquals("Invalid value for defaultAlertPort: pineapple", e.getMessage());
            }
        });
    }

    @Test
    public void testBindPropertiesInBufferSize() throws Exception {
        withLogAlertSocketWriter(writer -> {
            writer.setInBufferSize(String.valueOf(12));
            writer.setAlertTargets("\"\"");
            writer.bindProperties(LogFactory.getInstance());
            Assert.assertNotNull(LogAlertSocket.localHostIp);
            Assert.assertEquals(12, writer.getInBufferSize());
        });
    }

    @Test
    public void testBindPropertiesInBufferSizeBadValue() throws Exception {
        withLogAlertSocketWriter(writer -> {
            writer.setInBufferSize("anaconda");
            writer.setAlertTargets("\"\"");
            try {
                writer.bindProperties(LogFactory.getInstance());
            } catch (LogError e) {
                Assert.assertEquals("Invalid value for inBufferSize: anaconda", e.getMessage());
            }
        });
    }

    @Test
    public void testBindPropertiesLocationDefault() throws Exception {
        withLogAlertSocketWriter(writer -> {
            writer.setAlertTargets("");
            writer.setLocation("");
            writer.bindProperties(LogFactory.getInstance());
            Assert.assertEquals("/alert-manager-tpt.json", writer.getLocation());
        });
    }

    @Test
    public void testBindPropertiesOutBufferSize() throws Exception {
        withLogAlertSocketWriter(writer -> {
            writer.setOutBufferSize(String.valueOf(12));
            writer.setAlertTargets("\"\"");
            writer.bindProperties(LogFactory.getInstance());
            Assert.assertNotNull(LogAlertSocket.localHostIp);
            Assert.assertEquals(12, writer.getOutBufferSize());
        });
    }

    @Test
    public void testBindPropertiesOutBufferSize_BadValue() throws Exception {
        withLogAlertSocketWriter(writer -> {
            writer.setOutBufferSize("coconut");
            writer.setAlertTargets("\"\"");
            try {
                writer.bindProperties(LogFactory.getInstance());
            } catch (LogError e) {
                Assert.assertEquals("Invalid value for outBufferSize: coconut", e.getMessage());
            }
        });
    }

    @Test
    public void testBindPropertiesReconnectDelay() throws Exception {
        withLogAlertSocketWriter(writer -> {
            writer.setReconnectDelay(String.valueOf(50));
            writer.setAlertTargets("\"\"");
            writer.bindProperties(LogFactory.getInstance());
            Assert.assertNotNull(LogAlertSocket.localHostIp);
            Assert.assertEquals(50_000_000, writer.getReconnectDelay());
        });
    }

    @Test
    public void testBindPropertiesReconnectDelayBadValue() throws Exception {
        withLogAlertSocketWriter(writer -> {
            writer.setReconnectDelay("banana");
            writer.setAlertTargets("\"\"");
            try {
                writer.bindProperties(LogFactory.getInstance());
            } catch (LogError e) {
                Assert.assertEquals("Invalid value for reconnectDelay: banana", e.getMessage());
            }
        });
    }

    @Test
    public void testBindPropertiesTemplateFileDoesNotExist() throws Exception {
        withLogAlertSocketWriter(writer -> {
            writer.setLocation("some-silly-path.conf");
            try {
                writer.bindProperties(LogFactory.getInstance());
                Assert.fail();
            } catch (LogError e) {
                Assert.assertEquals("Cannot read some-silly-path.conf [errno=2]", e.getMessage());
            }
        });
    }

    @Test
    public void testBindPropertiesTemplateFileDoesNotHaveMessageKey() throws Exception {
        withLogAlertSocketWriter(writer -> {
            writer.setLocation(Files.getResourcePath(getClass().getResource("/alert-manager-tpt-test-missing-message-key.json")));
            try {
                writer.bindProperties(LogFactory.getInstance());
                Assert.fail();
            } catch (LogError e) {
                TestUtils.assertContains(
                        e.getMessage(),
                        "Bad template, no ${ALERT_MESSAGE} declaration found"
                );
            }
        });
    }

    @Test
    public void testOnLogRecord() throws Exception {
        testOnLogRecord(null);
    }

    @Test
    public void testOnLogRecordInternationalTemplate() throws Exception {
        withLogAlertSocketWriter(
                (MicrosecondClock) () -> 1637091363010000L,
                writer -> {
                    // this test does not interact with server
                    final int logRecordBuffSize = 1024; // plenty, to allow for encoding/escaping
                    final long logRecordBuffPtr = Unsafe.malloc(logRecordBuffSize, MemoryTag.NATIVE_DEFAULT);
                    try {
                        // create mock alert target server
                        final SOCountDownLatch haltLatch = new SOCountDownLatch(1);
                        final CyclicBarrier startBarrier = new CyclicBarrier(2);
                        final MockAlertTarget alertTarget = new MockAlertTarget(
                                1234,
                                haltLatch::countDown,
                                () -> TestUtils.await(startBarrier)
                        );
                        alertTarget.start();

                        TestUtils.await(startBarrier);
                        writer.setLocation(Files.getResourcePath(getClass().getResource("/alert-manager-tpt-international.json")));
                        writer.setAlertTargets("localhost:1234");
                        writer.setReconnectDelay("100");
                        writer.bindProperties(LogFactory.getInstance());

                        LogRecordUtf8Sink recordSink = new LogRecordUtf8Sink(logRecordBuffPtr, logRecordBuffSize);
                        recordSink.setLevel(LogLevel.ERROR);
                        recordSink.put("A \"simple\" $message$\n");

                        writer.onLogRecord(recordSink);
                        TestUtils.assertEquals(
                                "POST /api/v1/alerts HTTP/1.1\r\n" +
                                        "Host: " + LogAlertSocket.localHostIp + "\r\n" +
                                        "User-Agent: QuestDB/LogAlert\r\n" +
                                        "Accept: */*\r\n" +
                                        "Content-Type: application/json\r\n" +
                                        "Content-Length:      560\r\n" +
                                        "\r\n" +
                                        "[\n" +
                                        "  {\n" +
                                        "    \"Status\": \"firing\",\n" +
                                        "    \"Labels\": {\n" +
                                        "      \"alertname\": \"உலகனைத்தும்\",\n" +
                                        "      \"category\": \"воно мені не\",\n" +
                                        "      \"severity\": \"łódź jeża lub osiem\",\n" +
                                        "      \"orgid\": \"GLOBAL\",\n" +
                                        "      \"service\": \"QuestDB\",\n" +
                                        "      \"namespace\": \"GLOBAL\",\n" +
                                        "      \"cluster\": \"GLOBAL\",\n" +
                                        "      \"instance\": \"GLOBAL\",\n" +
                                        "      \"我能吞下玻璃而不傷身體\": \"ππππππππππππππππππππ 11\"\n" +
                                        "    },\n" +
                                        "    \"Annotations\": {\n" +
                                        "      \"description\": \"ERROR/GLOBAL/GLOBAL/GLOBAL/GLOBAL\",\n" +
                                        "      \"message\": \"A \\\"simple\\\" \\$message\\$\"\n" +
                                        "    }\n" +
                                        "  }\n" +
                                        "]\n" +
                                        "\n",
                                (Utf8Sequence) writer.getAlertSink()
                        );

                        Assert.assertTrue(haltLatch.await(10_000_000_000L));
                        Assert.assertFalse(alertTarget.isRunning());
                    } finally {
                        Unsafe.free(logRecordBuffPtr, logRecordBuffSize, MemoryTag.NATIVE_DEFAULT);
                    }
                }
        );
    }

    @Test
    public void testOnLogRecordWithExternalTemplate() throws Exception {
        final Path dstPath = Path.getThreadLocal(root).concat("test-alert-manager.json");
        String resourcePath = TestUtils.getResourcePath(DEFAULT_ALERT_TPT_FILE);
        Path template = Path.getThreadLocal2(resourcePath);
        int result = Files.copy(template.$(), dstPath.$());
        Assert.assertTrue("Copying " + resourcePath + " to " + dstPath + " result: " + result, result >= 0);
        String location = dstPath.toString();

        testOnLogRecord(location);
    }

    @Test
    public void testReadFile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String fileName = rand.nextString(10);
            final String fileContent = "யாமறிந்த மொழிகளிலே தமிழ்மொழி போல் இனிதாவது எங்கும் காணோம்,\n" +
                    "பாமரராய் விலங்குகளாய், உலகனைத்தும் இகழ்ச்சிசொலப் பான்மை கெட்டு,\n" +
                    "நாமமது தமிழரெனக் கொண்டு இங்கு வாழ்ந்திடுதல் நன்றோ? சொல்லீர்!\n" +
                    "தேமதுரத் தமிழோசை உலகமெலாம் பரவும்வகை செய்தல் வேண்டும்.";
            final int buffSize = fileContent.length() * 3;
            final long buffPtr = Unsafe.malloc(buffSize, MemoryTag.NATIVE_DEFAULT);
            final byte[] bytes = fileContent.getBytes(Files.UTF_8);
            long p = buffPtr;
            for (int i = 0, n = bytes.length; i < n; i++) {
                Unsafe.getUnsafe().putByte(p++, bytes[i]);
            }
            try (Path path = new Path()) {
                path.put(fileName).$();
                long fd = ff.openAppend(path.$());
                ff.truncate(fd, 0);
                ff.append(fd, buffPtr, bytes.length);
                ff.close(fd);

                // clear buffer
                p = buffPtr;
                for (int i = 0; i < bytes.length; i++) {
                    Unsafe.getUnsafe().putByte(p++, (byte) 0);
                }
                LogAlertSocketWriter.readFile(fileName, buffPtr, buffSize, ff, sink);
                TestUtils.assertEquals(fileContent, sink);
                ff.remove(path.$());
            } finally {
                Unsafe.free(buffPtr, buffSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testReadFileNotExists() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String fileName = rand.nextString(10);
            try (Path path = new Path()) {
                path.put(fileName).$();
                try {
                    LogAlertSocketWriter.readFile(fileName, 0, 0, ff, sink);
                } catch (LogError e) {
                    String message = e.getMessage();
                    Assert.assertTrue(
                            message.equals("Template file is too big") ||
                                    message.startsWith("Cannot read VTJWCPSWHY [errno=")
                    );
                }
            }
        });
    }

    @Test
    public void testReadFileTooBig() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String fileName = rand.nextString(10);
            final String fileContent = "Pchnąć w tę łódź jeża lub osiem skrzyń fig";
            final int buffSize = fileContent.length() * 4;
            final long buffPtr = Unsafe.malloc(buffSize, MemoryTag.NATIVE_DEFAULT);
            Path path = new Path();
            long fd = -1;
            try {
                final byte[] bytes = fileContent.getBytes(Files.UTF_8);
                final int len = bytes.length;
                long p = buffPtr;
                for (int i = 0; i < len; i++) {
                    Unsafe.getUnsafe().putByte(p++, bytes[i]);
                }

                path.put(fileName).$();
                fd = ff.openCleanRW(path.$(), buffSize);
                ff.append(fd, buffPtr, len);
                try {
                    LogAlertSocketWriter.readFile(fileName, buffPtr, 17, ff, sink);
                } catch (LogError e) {
                    String message = e.getMessage();
                    Assert.assertTrue(
                            message.equals("Template file is too big") ||
                                    message.startsWith("Cannot read VTJWCPSWHY [errno=")
                    );
                }
            } finally {
                ff.close(fd);
                ff.remove(path.$());
                path.close();
                Unsafe.free(buffPtr, buffSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static void withLogAlertSocketWriter(Consumer<LogAlertSocketWriter> consumer) throws Exception {
        final NetworkFacade nf = new NetworkFacadeImpl() {
            @Override
            public int connect(long fd, long pSockaddr) {
                return -1;
            }
        };
        withLogAlertSocketWriter(MicrosecondClockImpl.INSTANCE, consumer, nf);
    }

    private static void withLogAlertSocketWriter(
            Clock clock,
            Consumer<LogAlertSocketWriter> consumer
    ) throws Exception {
        withLogAlertSocketWriter(clock, consumer, NetworkFacadeImpl.INSTANCE);
    }

    private static void withLogAlertSocketWriter(
            Clock clock,
            Consumer<LogAlertSocketWriter> consumer,
            NetworkFacade nf
    ) throws Exception {
        withLogAlertSocketWriter(
                clock,
                consumer,
                nf,
                ALERT_PROPS
        );
    }

    private static void withLogAlertSocketWriter(
            Clock clock,
            Consumer<LogAlertSocketWriter> consumer,
            NetworkFacade nf,
            CharSequenceObjHashMap<CharSequence> properties
    ) throws Exception {
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, "/test-log-silent.conf");
        TestUtils.assertMemoryLeak(() -> {
            try (LogAlertSocketWriter writer = new LogAlertSocketWriter(
                    TestFilesFacadeImpl.INSTANCE,
                    nf,
                    clock,
                    null,
                    null,
                    LogLevel.ERROR,
                    properties
            )) {
                consumer.accept(writer);
            }
        });
    }

    private void testOnLogRecord(String location) throws Exception {
        CharSequenceObjHashMap<CharSequence> properties = new CharSequenceObjHashMap<>();
        properties.putAll(ALERT_PROPS);

        // replace build info

        withLogAlertSocketWriter(
                (MicrosecondClock) () -> 1637091363010000L,
                writer -> {

                    final int logRecordBuffSize = 1024; // plenty, to allow for encoding/escaping
                    final long logRecordBuffPtr = Unsafe.malloc(logRecordBuffSize, MemoryTag.NATIVE_DEFAULT);
                    try {
                        // create mock alert target servers
                        // Vlad: we are wasting time here not connecting anywhere
                        final SOCountDownLatch haltLatch = new SOCountDownLatch(2);
                        final MockAlertTarget[] alertsTarget = new MockAlertTarget[2];
                        final CyclicBarrier startBarrier = new CyclicBarrier(3);
                        alertsTarget[0] = new MockAlertTarget(
                                1234,
                                haltLatch::countDown,
                                () -> TestUtils.await(startBarrier)
                        );
                        alertsTarget[1] = new MockAlertTarget(
                                1242,
                                haltLatch::countDown,
                                () -> TestUtils.await(startBarrier)
                        );
                        alertsTarget[0].start();
                        alertsTarget[1].start();

                        TestUtils.await(startBarrier);
                        writer.setAlertTargets("localhost:1234,localhost:1242");
                        if (location != null) {
                            writer.setLocation(location);
                        }
                        writer.bindProperties(LogFactory.getInstance());

                        LogRecordUtf8Sink recordSink = new LogRecordUtf8Sink(logRecordBuffPtr, logRecordBuffSize);
                        recordSink.setLevel(LogLevel.ERROR);
                        recordSink.put("A \"simple\" $message$\n");

                        writer.onLogRecord(recordSink);
                        TestUtils.assertEquals(
                                "POST /api/v1/alerts HTTP/1.1\r\n" +
                                        "Host: " + LogAlertSocket.localHostIp + "\r\n" +
                                        "User-Agent: QuestDB/LogAlert\r\n" +
                                        "Accept: */*\r\n" +
                                        "Content-Type: application/json\r\n" +
                                        "Content-Length:      498\r\n" +
                                        "\r\n" +
                                        "[\n" +
                                        "  {\n" +
                                        "    \"Status\": \"firing\",\n" +
                                        "    \"Labels\": {\n" +
                                        "      \"alertname\": \"QuestDbInstanceLogs\",\n" +
                                        "      \"service\": \"QuestDB\",\n" +
                                        "      \"category\": \"application-logs\",\n" +
                                        "      \"severity\": \"critical\",\n" +
                                        "      \"cluster\": \"GLOBAL\",\n" +
                                        "      \"orgid\": \"GLOBAL\",\n" +
                                        "      \"namespace\": \"GLOBAL\",\n" +
                                        "      \"instance\": \"GLOBAL\",\n" +
                                        "      \"alertTimestamp\": \"2021/11/16T19:36:03.010\"\n" +
                                        "    },\n" +
                                        "    \"Annotations\": {\n" +
                                        "      \"description\": \"ERROR/cl:GLOBAL/org:GLOBAL/ns:GLOBAL/db:GLOBAL\",\n" +
                                        "      \"message\": \"A \\\"simple\\\" \\$message\\$\"" +
                                        "\n" +
                                        "    }\n" +
                                        "  }\n" +
                                        "]\n",
                                (Utf8Sequence) writer.getAlertSink()
                        );

                        recordSink.clear();
                        recordSink.put("A second log message");
                        writer.onLogRecord(recordSink);
                        TestUtils.assertEquals(
                                "POST /api/v1/alerts HTTP/1.1\r\n" +
                                        "Host: " + LogAlertSocket.localHostIp + "\r\n" +
                                        "User-Agent: QuestDB/LogAlert\r\n" +
                                        "Accept: */*\r\n" +
                                        "Content-Type: application/json\r\n" +
                                        "Content-Length:      494\r\n" +
                                        "\r\n" +
                                        "[\n" +
                                        "  {\n" +
                                        "    \"Status\": \"firing\",\n" +
                                        "    \"Labels\": {\n" +
                                        "      \"alertname\": \"QuestDbInstanceLogs\",\n" +
                                        "      \"service\": \"QuestDB\",\n" +
                                        "      \"category\": \"application-logs\",\n" +
                                        "      \"severity\": \"critical\",\n" +
                                        "      \"cluster\": \"GLOBAL\",\n" +
                                        "      \"orgid\": \"GLOBAL\",\n" +
                                        "      \"namespace\": \"GLOBAL\",\n" +
                                        "      \"instance\": \"GLOBAL\",\n" +
                                        "      \"alertTimestamp\": \"2021/11/16T19:36:03.010\"\n" +
                                        "    },\n" +
                                        "    \"Annotations\": {\n" +
                                        "      \"description\": \"ERROR/cl:GLOBAL/org:GLOBAL/ns:GLOBAL/db:GLOBAL\",\n" +
                                        "      \"message\": \"A second log message\"" +
                                        "\n" +
                                        "    }\n" +
                                        "  }\n" +
                                        "]\n",
                                (Utf8Sequence) writer.getAlertSink()
                        );

                        haltLatch.await();
                        Assert.assertFalse(alertsTarget[0].isRunning());
                        Assert.assertFalse(alertsTarget[1].isRunning());
                    } finally {
                        Unsafe.free(logRecordBuffPtr, logRecordBuffSize, MemoryTag.NATIVE_DEFAULT);
                    }
                },
                NetworkFacadeImpl.INSTANCE,
                properties
        );
    }

    static {
        DateLocaleFactory.load();
    }
}
