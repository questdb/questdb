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

package com.nfsdb.net.http;

import com.nfsdb.net.HttpServer;
import com.nfsdb.net.http.handlers.UploadHandler;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;

public class HttpServerTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testStartStop() throws Exception {
        HttpServer server = new HttpServer(new InetSocketAddress(9090), 2, 1024);
        server.start();
        server.halt();
    }

    @Test
    public void testUpload() throws Exception {
        HttpServer server = new HttpServer(new InetSocketAddress(9090), 2, 1024);
        File dir = temp.newFolder();
        server.add("/upload", new UploadHandler(dir));
        server.start();

        File expected = new File(this.getClass().getResource("/csv/test-import.csv").getFile());
        File actual = new File(dir, "test-import.csv");
        upload(expected, "http://localhost:9090/upload");

        TestUtils.assertEquals(expected, actual);
        server.halt();
    }

    private static void upload(File file, String url) throws IOException {
        String charset = "UTF-8";
        String param = "value";
        String boundary = Long.toHexString(System.currentTimeMillis()); // Just generate some unique random value.
        String CRLF = "\r\n"; // Line separator required by multipart/form-data.

        URLConnection connection = new URL(url).openConnection();
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);

        try (
                OutputStream output = connection.getOutputStream();
                PrintWriter writer = new PrintWriter(new OutputStreamWriter(output, charset), true)
        ) {
            // Send normal param.
            writer.append("--").append(boundary).append(CRLF);
            writer.append("Content-Disposition: form-data; name=\"param\"").append(CRLF);
            writer.append("Content-Type: text/plain; charset=").append(charset).append(CRLF);
            writer.append(CRLF).append(param).append(CRLF).flush();

            // Send text file.
            writer.append("--").append(boundary).append(CRLF);
            writer.append("Content-Disposition: form-data; name=\"textFile\"; filename=\"").append(file.getName()).append("\"").append(CRLF);
            writer.append("Content-Type: text/plain; charset=").append(charset).append(CRLF); // Text file itself must be saved in this charset!
            writer.append(CRLF).flush();
            Files.copy(file.toPath(), output);
            output.flush(); // Important before continuing with writer!
            writer.append(CRLF).flush(); // CRLF is important! It indicates end of boundary.

            // End of multipart/form-data.
            writer.append("--").append(boundary).append("--").append(CRLF).flush();
        }
        // Request is lazily fired whenever you need to obtain information about response.
        System.out.println(((HttpURLConnection) connection).getResponseCode()); // Should be 200
    }
}
