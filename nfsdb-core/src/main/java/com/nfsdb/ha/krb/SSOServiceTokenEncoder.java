/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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
 */

package com.nfsdb.ha.krb;

import com.nfsdb.utils.Base64;
import com.nfsdb.utils.Files;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.*;
import java.net.URL;
import java.nio.channels.FileChannel;

public class SSOServiceTokenEncoder implements Closeable {

    private static final String NFSKRB_EXE = "/nfskrb.exe";
    private static final String OK_RESPONSE = "OK: ";
    private final File temp;
    private final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    private final boolean clean;
    private final String osName = System.getProperty("os.name");
    private final String osArch = System.getProperty("os.arch");

    public SSOServiceTokenEncoder(File dir) {
        this.temp = dir;
        clean = false;
    }

    public SSOServiceTokenEncoder() throws IOException {
        temp = Files.makeTempDir();
        clean = true;
        copy(NFSKRB_EXE, temp);
        copy("/Microsoft.IdentityModel.dll", temp);
    }

    @Override
    public void close() throws IOException {
        if (clean) {
            Files.delete(temp);
        }
    }

    @SuppressFBWarnings({"COMMAND_INJECTION"})
    public byte[] encodeServiceToken(String serviceName) throws IOException {

        if (!isAvailable()) {
            throw new IOException("ActiveDirectory SSO is only available on Windows platforms");
        }

        try (InputStream in = Runtime.getRuntime().exec(temp.getAbsolutePath() + NFSKRB_EXE + " " + serviceName).getInputStream()) {
            bos.reset();
            byte buf[] = new byte[4096];
            int len;

            while ((len = in.read(buf)) != -1) {
                bos.write(buf, 0, len);
            }

            String response = bos.toString("UTF8");
            if (!response.startsWith(OK_RESPONSE)) {
                throw new IOException(response);
            }
            return Base64._parseBase64Binary(response.substring(OK_RESPONSE.length()));
        }
    }

    public boolean isAvailable() {
        return osName != null && osName.startsWith("Windows") && "amd64".equals(osArch);
    }

    @SuppressFBWarnings({"PATH_TRAVERSAL_IN"})
    private static void copy(String resource, File dir) throws IOException {
        URL url = SSOServiceTokenEncoder.class.getResource(resource);
        if (url == null) {
            throw new IOException("Broken package? Resource not found: " + resource);
        }

        File file = new File(url.getPath());
        try (FileChannel in = new FileInputStream(file).getChannel()) {
            try (FileChannel out = new FileOutputStream(new File(dir, file.getName())).getChannel()) {
                out.transferFrom(in, 0, in.size());
            }
        }
    }
}
