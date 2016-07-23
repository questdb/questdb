/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.net.ha.krb;

import com.questdb.misc.Base64;
import com.questdb.misc.Files;

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

    private SSOServiceTokenEncoder(File dir) {
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

    public byte[] encodeServiceToken(String serviceName) throws IOException {

        if (!isAvailable()) {
            throw new IOException("ActiveDirectory SSO is only available on Windows platforms");
        }

        try (InputStream in = Runtime.getRuntime().exec(temp.getAbsolutePath() + NFSKRB_EXE + ' ' + serviceName).getInputStream()) {
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
            return Base64.parseBase64Binary(response.substring(OK_RESPONSE.length()));
        }
    }

    public boolean isAvailable() {
        return osName != null && osName.startsWith("Windows") && "amd64".equals(osArch);
    }

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
