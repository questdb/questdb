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
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

public class LogAlertManagerWriterTest {

    private static final FilesFacade ff = FilesFacadeImpl.INSTANCE;


    @Test
    public void testBindProperties() {

        final String message = "$Sîne klâwen durh die wolken sint geslagen,\r\n" +
                "er stîget ûf mit grôzer kraft,\r\n" +
                "ich sih in grâwen tägelîch als er wil tagen,\r\n" +
                "den tac, der \"im\" ${KARTOFEN} geselleschaft\r\n" +
                "erwenden wil, dem werden man,\r\n" +
                "den ich mit sorgen în verliez.\r\n" +
                "ich bringe in hinnen, ob ich kan.\r\n" +
                "sîn vil manegiu tugent michz leisten hiez.$\r\n";
        final int len = message.length();
        final int buffSize = len * 3;
        final long buffPtr = Unsafe.malloc(buffSize, MemoryTag.NATIVE_DEFAULT);
        try(LogAlertManagerWriter writer = new LogAlertManagerWriter(
                ff,
                MicrosecondClockImpl.INSTANCE,
                null,
                null,
                LogLevel.ERROR
        )) {
            writer.bindProperties();
            LogRecordSink recordSink = new LogRecordSink(buffPtr, buffSize);
            recordSink.setLevel(LogLevel.ERROR);
            recordSink.put(message.toCharArray(), 0, len);
            writer.onLogRecord(recordSink);
            HttpAlertBuilder alertBuilder = writer.getAlertBuilder();
            Assert.assertNotNull(alertBuilder);
            Assert.assertEquals(
                    "POST /api/v1/alerts HTTP/1.1\r\n" +
                            "Host: 192.168.1.58\r\n" +
                            "User-Agent: QuestDB/7.71.1\r\n" +
                            "Accept: */*\r\n" +
                            "Content-Type: application/json\r\n" +
                            "Content-Length:    722\r\n" +
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
                            "      \"message\": \"\\\\$Sîne klâwen durh die wolken sint geslagen,er stîget ûf mit grôzer kraft,ich sih in grâwen tägelîch als er wil tagen,den tac, der \\\"im\\\" \\\\${KARTOFEN} geselleschafterwenden wil, dem werden man,den ich mit sorgen în verliez.ich bringe in hinnen, ob ich kan.sîn vil manegiu tugent michz leisten hiez.\\\\$\"\n" +
                            "    }\n" +
                            "  }\n" +
                            "]",
                    alertBuilder.toString()
            );
        } finally {
            Unsafe.free(buffPtr, buffSize, MemoryTag.NATIVE_DEFAULT);
        }
    }


    @Test
    public void testReadFile() {
        final Rnd rand = new Rnd();
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

            p = buffPtr;
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.getUnsafe().putByte(p++, (byte) 0);
            }

            long size = LogAlertManagerWriter.readFile(fileName, buffPtr, buffPtr + buffSize, ff);
            Assert.assertEquals(bytes.length, size);
            Assert.assertEquals(fileContent, Chars.stringFromUtf8Bytes(buffPtr, buffPtr + size));
            ff.remove(path);
        } finally {
            Unsafe.free(buffPtr, buffSize, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
