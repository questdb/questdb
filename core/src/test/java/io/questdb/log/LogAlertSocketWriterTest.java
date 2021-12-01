/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.log;

import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.StdoutSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Consumer;


public class LogAlertSocketWriterTest {

    private static final FilesFacade ff = FilesFacadeImpl.INSTANCE;

    private Rnd rand;

    @Before
    public void setUp() {
        rand = new Rnd();
    }

    @Test
    public void testOnLogRecord() throws Exception {
        withLogAlertSocketWriter(
                () -> 1637091363010000L,
                writer -> {

                    final int logRecordBuffSize = 1024; // plenty, to allow for encoding/escaping
                    final long logRecordBuffPtr = Unsafe.malloc(logRecordBuffSize, MemoryTag.NATIVE_DEFAULT);
                    try {
                        // create mock alert target servers
                        final SOCountDownLatch haltLatch = new SOCountDownLatch(2);
                        final MockAlertTarget[] alertsTarget = new MockAlertTarget[2];
                        alertsTarget[0] = new MockAlertTarget(1234, haltLatch::countDown);
                        alertsTarget[1] = new MockAlertTarget(1242, haltLatch::countDown);
                        alertsTarget[0].start();
                        alertsTarget[1].start();

                        writer.setAlertTargets("localhost:1234,localhost:1242");
                        writer.bindProperties();

                        LogRecordSink recordSink = new LogRecordSink(logRecordBuffPtr, logRecordBuffSize);
                        recordSink.setLevel(LogLevel.ERROR);
                        recordSink.put("A \"simple\" $message$\n");

                        writer.onLogRecord(recordSink);
                        Assert.assertEquals(
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
                                writer.getAlertSink().toString()
                        );

                        recordSink.clear();
                        recordSink.put("A second log message");
                        writer.onLogRecord(recordSink);
                        Assert.assertEquals(
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
                                writer.getAlertSink().toString()
                        );

                        Assert.assertTrue(haltLatch.await(10_000_000_000L));
                        Assert.assertFalse(alertsTarget[0].isRunning());
                        Assert.assertFalse(alertsTarget[1].isRunning());
                    } finally {
                        Unsafe.free(logRecordBuffPtr, logRecordBuffSize, MemoryTag.NATIVE_DEFAULT);
                    }
                });
    }

    @Test
    public void test_BindProperties_BadTemplateFile() throws Exception {
        withLogAlertSocketWriter(writer -> {
            writer.bindProperties();
            Assert.assertEquals(LogAlertSocket.OUT_BUFFER_SIZE, writer.getBufferSize());
            Assert.assertEquals(LogAlertSocketWriter.DEFAULT_ALERT_TPT_FILE, writer.getLocation());
            Assert.assertEquals(LogAlertSocket.DEFAULT_HOST + ":" + LogAlertSocket.DEFAULT_PORT, writer.getAlertTargets());
            writer.close();

            writer.setBufferSize("1978");
            writer.setLocation("/log-file.conf");
            writer.setAlertTargets("127.0.0.1:8989");
            try {
                writer.bindProperties();
                Assert.fail();
            } catch (LogError e) {
                Assert.assertEquals(
                        "Bad template, no ${ALERT_MESSAGE} declaration found /log-file.conf",
                        e.getMessage()
                );
            }
            Assert.assertEquals(1978, writer.getBufferSize());
            Assert.assertEquals("/log-file.conf", writer.getLocation());
            Assert.assertEquals("127.0.0.1:8989", writer.getAlertTargets());
        });
    }

    @Test
    public void test_BindProperties_TemplateFileDoesNotExist() throws Exception {
        withLogAlertSocketWriter(writer -> {
            writer.setLocation("some-silly-path.conf");
            try {
                writer.bindProperties();
                Assert.fail();
            } catch (LogError e) {
                Assert.assertEquals("Cannot read some-silly-path.conf [errno=2]", e.getMessage());
            }
        });
    }

    @Test
    public void test_BindProperties_TemplateFileDoesNotHaveMessageKey() throws Exception {
        withLogAlertSocketWriter(writer -> {
            writer.setLocation("/alert-manager-tpt-test-missing-message-key.json");
            try {
                writer.bindProperties();
                Assert.fail();
            } catch (LogError e) {
                Assert.assertEquals(
                        "Bad template, no ${ALERT_MESSAGE} declaration found /alert-manager-tpt-test-missing-message-key.json",
                        e.getMessage()
                );
            }
        });
    }

    @Test
    public void test_BindProperties_SocketAddress_Default() throws Exception {
        withLogAlertSocketWriter(writer -> {
            writer.setBufferSize(String.valueOf(1024));
            writer.setAlertTargets("\"\"");
            writer.bindProperties();
            Assert.assertNotNull(LogAlertSocket.localHostIp);
            Assert.assertEquals("127.0.0.1:9093", writer.getAlertTargets());
        });
    }

    @Test
    public void test_BindProperties_ReconnectDelay() throws Exception {
        withLogAlertSocketWriter(writer -> {
            writer.setReconnectDelay(String.valueOf(50));
            writer.setAlertTargets("\"\"");
            writer.bindProperties();
            Assert.assertNotNull(LogAlertSocket.localHostIp);
            Assert.assertEquals(50_000_000, writer.getReconnectDelay());
        });
    }

    @Test
    public void test_BindProperties_ReconnectDelay_BadValue() throws Exception {
        withLogAlertSocketWriter(writer -> {
            writer.setReconnectDelay("banana");
            writer.setAlertTargets("\"\"");
            try {
                writer.bindProperties();
            } catch (LogError e) {
                Assert.assertEquals("Invalid value for reconnectDelay: banana", e.getMessage());
            }
        });
    }

    @Test
    public void test_BindProperties_BufferSize() throws Exception {
        withLogAlertSocketWriter(writer -> {
            writer.setBufferSize(String.valueOf(12));
            writer.setAlertTargets("\"\"");
            writer.bindProperties();
            Assert.assertNotNull(LogAlertSocket.localHostIp);
            Assert.assertEquals(12, writer.getBufferSize());
        });
    }

    @Test
    public void test_BindProperties_BufferSize_BadValue() throws Exception {
        withLogAlertSocketWriter(writer -> {
            writer.setBufferSize("coconut");
            writer.setAlertTargets("\"\"");
            try {
                writer.bindProperties();
            } catch (LogError e) {
                Assert.assertEquals("Invalid value for bufferSize: coconut", e.getMessage());
            }
        });
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
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.getUnsafe().putByte(p++, bytes[i]);
            }
            try (Path path = new Path()) {
                path.put(fileName).$();
                long fd = ff.openAppend(path);
                ff.truncate(fd, 0);
                ff.append(fd, buffPtr, bytes.length);
                ff.close(fd);

                // clear buffer
                p = buffPtr;
                for (int i = 0; i < bytes.length; i++) {
                    Unsafe.getUnsafe().putByte(p++, (byte) 0);
                }

                DirectByteCharSequence file = LogAlertSocketWriter.readFile(fileName, buffPtr, buffSize, ff);
                Assert.assertEquals(bytes.length, file.length());
                Assert.assertEquals(fileContent, Chars.stringFromUtf8Bytes(buffPtr, buffPtr + file.length()));
                ff.remove(path);
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
                    LogAlertSocketWriter.readFile(fileName, 0, 0, ff);
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
                fd = ff.openCleanRW(path, buffSize);
                ff.append(fd, buffPtr, len);
                try {
                    LogAlertSocketWriter.readFile(fileName, buffPtr, 17, ff);
                } catch (LogError e) {
                    String message = e.getMessage();
                    Assert.assertTrue(
                            message.equals("Template file is too big") ||
                                    message.startsWith("Cannot read VTJWCPSWHY [errno=")
                    );
                }
            } finally {
                if (fd != -1) {
                    ff.close(fd);
                }
                ff.remove(path);
                path.close();
                Unsafe.free(buffPtr, buffSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static void withLogAlertSocketWriter(Consumer<LogAlertSocketWriter> consumer) throws Exception {
        withLogAlertSocketWriter(MicrosecondClockImpl.INSTANCE, consumer);
    }

    private static void withLogAlertSocketWriter(
            MicrosecondClock clock,
            Consumer<LogAlertSocketWriter> consumer
    ) throws Exception {
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, "/test-log-silent.conf");
        TestUtils.assertMemoryLeak(() -> {
            try (LogAlertSocketWriter writer = new LogAlertSocketWriter(
                    FilesFacadeImpl.INSTANCE,
                    clock,
                    null,
                    null,
                    LogLevel.ERROR
            )) {
                consumer.accept(writer);
            }
        });
    }
}
