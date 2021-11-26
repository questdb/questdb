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

import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LogAlertSocketWriterTest {

    private static final FilesFacade ff = FilesFacadeImpl.INSTANCE;

    private Rnd rand;

    @Before
    public void setUp() {
        rand = new Rnd();
    }

    @Test
    public void test_BindProperties_AlertTemplateAndBuilder_OnLogRecord() {
        String message = "$Sîne klâwen durh die wolken sint geslagen,\r\n" +
                "sîn vil manegiu tugent michz leisten hiez.$\r\n";
        int len = message.length();

        // to be safe, the template takes some bytes, and each char in the
        // message can take up to 3 bytes when encoded
        final int buffSize = len * 4;
        final long buffPtr = Unsafe.malloc(buffSize, MemoryTag.NATIVE_DEFAULT);
        try (LogAlertSocketWriter writer = new LogAlertSocketWriter(
                ff,
                MicrosecondClockImpl.INSTANCE,
                null,
                null,
                LogLevel.ERROR
        )) {
            writer.bindProperties();

            LogRecordSink recordSink = new LogRecordSink(buffPtr, buffSize);
            recordSink.setLevel(LogLevel.ERROR);
            recordSink.put(message);
            writer.onLogRecord(recordSink, ack -> Assert.assertEquals(LogAlertSocket.NACK, ack));
            Assert.assertEquals(
                    "POST /api/v1/alerts HTTP/1.1\r\n" +
                            "Host: " + LogAlertSocket.localHostIp + "\r\n" +
                            "User-Agent: QuestDB/LogAlert\r\n" +
                            "Accept: */*\r\n" +
                            "Content-Type: application/json\r\n" +
                            "Content-Length:    502\r\n" +
                            "\r\n" +
                            "[\n" +
                            "  {\n" +
                            "    \"Status\": \"firing\",\n" +
                            "    \"Labels\": {\n" +
                            "      \"alertname\": \"QuestDbInstanceLogs\",\n" +
                            "      \"category\": \"application-logs\",\n" +
                            "      \"severity\": \"critical\",\n" +
                            "      \"orgid\": \"GLOBAL\",\n" +
                            "      \"service\": \"QuestDB\",\n" +
                            "      \"namespace\": \"GLOBAL\",\n" +
                            "      \"cluster\": \"GLOBAL\",\n" +
                            "      \"instance\": \"GLOBAL\"\n" +
                            "    },\n" +
                            "    \"Annotations\": {\n" +
                            "      \"description\": \"ERROR/GLOBAL/GLOBAL/GLOBAL/GLOBAL\",\n" +
                            "      \"message\": \"\\\\$Sîne klâwen durh die wolken sint geslagen,sîn vil " +
                            "manegiu tugent michz leisten hiez.\\\\$\"" +
                            "\n" +
                            "    }\n" +
                            "  }\n" +
                            "]",
                    writer.getAlertBuilder().toString()
            );

            recordSink.clear();
            recordSink.put("A second log message");
            writer.onLogRecord(recordSink, ack -> Assert.assertEquals(LogAlertSocket.NACK, ack));
            Assert.assertEquals(
                    "POST /api/v1/alerts HTTP/1.1\r\n" +
                            "Host: " + LogAlertSocket.localHostIp + "\r\n" +
                            "User-Agent: QuestDB/LogAlert\r\n" +
                            "Accept: */*\r\n" +
                            "Content-Type: application/json\r\n" +
                            "Content-Length:    429\r\n" +
                            "\r\n" +
                            "[\n" +
                            "  {\n" +
                            "    \"Status\": \"firing\",\n" +
                            "    \"Labels\": {\n" +
                            "      \"alertname\": \"QuestDbInstanceLogs\",\n" +
                            "      \"category\": \"application-logs\",\n" +
                            "      \"severity\": \"critical\",\n" +
                            "      \"orgid\": \"GLOBAL\",\n" +
                            "      \"service\": \"QuestDB\",\n" +
                            "      \"namespace\": \"GLOBAL\",\n" +
                            "      \"cluster\": \"GLOBAL\",\n" +
                            "      \"instance\": \"GLOBAL\"\n" +
                            "    },\n" +
                            "    \"Annotations\": {\n" +
                            "      \"description\": \"ERROR/GLOBAL/GLOBAL/GLOBAL/GLOBAL\",\n" +
                            "      \"message\": \"A second log message\"\n" +
                            "    }\n" +
                            "  }\n" +
                            "]",
                    writer.getAlertBuilder().toString()
            );
        } finally {
            Unsafe.free(buffPtr, buffSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void test_BindProperties_BadTemplateFile() {
        try (LogAlertSocketWriter writer = new LogAlertSocketWriter(
                ff,
                MicrosecondClockImpl.INSTANCE,
                null,
                null,
                LogLevel.ERROR
        )) {
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
        }
    }

    @Test
    public void test_BindProperties_TemplateFileDoesNotExist() {
        try (LogAlertSocketWriter writer = new LogAlertSocketWriter(
                ff,
                MicrosecondClockImpl.INSTANCE,
                null,
                null,
                LogLevel.ERROR
        )) {
            writer.setLocation("some-silly-path.conf");
            try {
                writer.bindProperties();
                Assert.fail();
            } catch (LogError e) {
                Assert.assertEquals("Cannot read some-silly-path.conf [errno=2]", e.getMessage());
            }
        }
    }

    @Test
    public void test_BindProperties_TemplateFileDoesNotHaveMessageKey() {
        try (LogAlertSocketWriter writer = new LogAlertSocketWriter(
                ff,
                MicrosecondClockImpl.INSTANCE,
                null,
                null,
                LogLevel.ERROR
        )) {
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
        }
    }

    @Test
    public void test_BindProperties_SocketAddress_Default() {
        try (LogAlertSocketWriter writer = new LogAlertSocketWriter(
                null,
                null,
                LogLevel.ERROR
        )) {
            writer.setBufferSize(String.valueOf(1024));
            writer.setAlertTargets("\"\"");
            writer.bindProperties();
            Assert.assertNotNull(LogAlertSocket.localHostIp);
            Assert.assertEquals("127.0.0.1:9093", writer.getAlertTargets());
        }
    }

    @Test
    public void testReadFile() {
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
    }

    @Test
    public void testReadFileNotExists() {
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
    }

    @Test
    public void testReadFileTooBig() {
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
    }
}
