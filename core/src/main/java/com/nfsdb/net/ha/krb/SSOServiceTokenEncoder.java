/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.net.ha.krb;

import com.nfsdb.misc.Base64;
import com.nfsdb.misc.Files;
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

    @SuppressFBWarnings({"COMMAND_INJECTION"})
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
