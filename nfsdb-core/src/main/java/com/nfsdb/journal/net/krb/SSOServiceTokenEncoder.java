/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.journal.net.krb;

import com.nfsdb.journal.utils.Base64;
import com.nfsdb.journal.utils.Files;

import java.io.*;
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
        copy(new File(this.getClass().getResource(NFSKRB_EXE).getPath()), temp);
        copy(new File(this.getClass().getResource("/Microsoft.IdentityModel.dll").getPath()), temp);

    }

    @Override
    public void close() throws IOException {
        if (clean) {
            Files.delete(temp);
        }
    }

    public boolean isAvailable() {
        return osName != null && osName.startsWith("Windows") && "amd64".equals(osArch);
    }

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

            String response = bos.toString();
            if (!response.startsWith(OK_RESPONSE)) {
                throw new IOException(response);
            }
            return Base64._parseBase64Binary(response.substring(OK_RESPONSE.length()));
        }
    }

    private static void copy(File file, File dir) throws IOException {
        try (FileChannel in = new FileInputStream(file).getChannel()) {
            try (FileChannel out = new FileOutputStream(new File(dir, file.getName())).getChannel()) {
                out.transferFrom(in, 0, in.size());
            }
        }
    }
}
