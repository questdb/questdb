/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.http;

import com.nfsdb.collections.DirectByteCharSequence;
import com.nfsdb.collections.ObjList;
import com.nfsdb.collections.ObjectPool;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class MultipartParserTest {

    private static final String file = "<project xmlns=\"http://maven.apache.org/POM/4.0.0\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
            "         xsi:schemaLocation=\"http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd\">\n" +
            "    <modelVersion>4.0.0</modelVersion>\n" +
            "    <groupId>org.sandbox</groupId>\n" +
            "    <artifactId>factor</artifactId>\n" +
            "    <packaging>jar</packaging>\n" +
            "    <version>1.0-SNAPSHOT</version>\n" +
            "    <name>factor</name>\n" +
            "    <url>http://maven.apache.org</url>\n" +
            "    <dependencies>\n" +
            "        <dependency>\n" +
            "            <groupId>junit</groupId>\n" +
            "            <artifactId>junit</artifactId>\n" +
            "            <version>4.6</version>\n" +
            "            <scope>test</scope>\n" +
            "        </dependency>\n" +
            "        <dependency>\n" +
            "            <groupId>com.lmax</groupId>\n" +
            "            <artifactId>disruptor</artifactId>\n" +
            "            <version>3.2.0</version>\n" +
            "        </dependency>\n" +
            "        <dependency>\n" +
            "            <groupId>net.openhft</groupId>\n" +
            "            <artifactId>affinity</artifactId>\n" +
            "            <version>2.0</version>\n" +
            "        </dependency>\n" +
            "    </dependencies>\n" +
            "</project>\n";

    private static final String content = "------WebKitFormBoundaryxFKYDBybTLu2rb8P\r\n" +
            "Content-Disposition: form-data; name=\"textline\"\r\n" +
            "\r\n" +
            "\r\n" +
            "------WebKitFormBoundaryxFKYDBybTLu2rb8P\n" +
            "Content-Disposition: form-data; name=\"textline2\"\n" +
            "\r\n" +
            "\r\n" +
            "------WebKitFormBoundaryxFKYDBybTLu2rb8P\n" +
            "Content-Disposition: form-data; name=\"datafile\"; filename=\"pom.xml\"\r\n" +
            "Content-Type: text/xml\r\n" +
            "\r\n" +
            file +
            "\r\n" +
            "------WebKitFormBoundaryxFKYDBybTLu2rb8P--\r\n";

    @Test
    public void testParse() throws Exception {
        LineCollectingListener lsnr = new LineCollectingListener();
        ObjectPool<DirectByteCharSequence> pool = new ObjectPool<>(DirectByteCharSequence.FACTORY, 64);
        MultipartParser parser = new MultipartParser(1024, pool);

        Request.BoundaryAugmenter augmenter = new Request.BoundaryAugmenter();

        long p = TestUtils.toMemory(content);
        try {
            for (int i = 0; i < content.length(); i++) {
                parser.of(augmenter.of("----WebKitFormBoundaryxFKYDBybTLu2rb8P"));
                parser.parse(null, p, i, lsnr);
                parser.parse(null, p + i, content.length() - i, lsnr);
                lsnr.assertLine();
                parser.clear();
                lsnr.clear();
            }
        } finally {
            Unsafe.getUnsafe().freeMemory(p);
        }

    }

    private static final class LineCollectingListener implements MultipartListener {
        private final ObjList<String> lines = new ObjList<>();

        public void assertLine() {
            Assert.assertEquals(3, lines.size());
            Assert.assertEquals("", lines.get(0));
            Assert.assertEquals("", lines.get(1));
            Assert.assertEquals(file, lines.get(2));
        }

        public void clear() {
            lines.clear();
        }

        @Override
        public void onChunk(IOContext context, RequestHeaderBuffer hb, DirectByteCharSequence data, boolean continued) {
            if (continued) {
                String s = lines.getLast();
                lines.setQuick(lines.size() - 1, s + data.toString());
            } else {
                lines.add(data.toString());
            }
        }
    }
}