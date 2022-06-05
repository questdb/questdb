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

package io.questdb.cutlass.line.tcp;

import io.questdb.cutlass.line.LineChannel;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public final class JavaSecureSocketTlsChannel implements LineChannel {
    private static final Log LOG = LogFactory.getLog(PlanTcpLineChannel.class);

    private final byte[] arr;

    private final Socket socket;
    private final InputStream inputStream;
    private final OutputStream outputStream;


    private static final TrustManager ALLOW_ALL_TRUSTMANAGER = new X509TrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {

        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {

        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    };

    public JavaSecureSocketTlsChannel(int address, int port, int sndBufferSize) {
        try {
            SSLContext sslContext = SSLContext.getInstance("SSL");
            TrustManager[] trustManagers = new TrustManager[]{ALLOW_ALL_TRUSTMANAGER};
            sslContext.init(null, trustManagers, new SecureRandom());

            InetAddress inetAddress = addrToInetAddress(address);

            this.socket = sslContext.getSocketFactory().createSocket(inetAddress, port);
            int orgSndBufSz = socket.getSendBufferSize();
            socket.setSendBufferSize(sndBufferSize);
            int newSndBufSz = socket.getSendBufferSize();
            LOG.info().$("Send buffer size change from ").$(orgSndBufSz).$(" to ").$(newSndBufSz).$();
            this.inputStream = socket.getInputStream();
            this.outputStream = socket.getOutputStream();
            this.arr = new byte[64 * 1024];
        } catch (NoSuchAlgorithmException | IOException | KeyManagementException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }

    @Override
    public void send(long ptr, int len) {
        do {
            int i = ptrToByteArray(ptr, len, arr);
            ptr += i;
            len -= i;
            try {
                outputStream.write(arr, 0, i);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } while (len > 0);
    }

    @Override
    public int receive(long ptr, int len) {
        try {
            int i = inputStream.read(arr, 0, Math.min(len, arr.length));
            return byteArrayToPtr(arr, ptr, i);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static int byteArrayToPtr(byte[] arr, long ptr, int len) {
        int i;
        for (i = 0; i < len && i < arr.length; i++) {
            Unsafe.getUnsafe().putByte(ptr + i, arr[i]);
        }
        return i;
    }

    private static int ptrToByteArray(long ptr, int len, byte[] dst) {
        int i ;
        for (i = 0; i < len && i < dst.length; i++) {
            dst[i] = Unsafe.getUnsafe().getByte(ptr + i);
        }
        return i;
    }

    @NotNull
    private static InetAddress addrToInetAddress(int address) throws UnknownHostException {
        byte[] addr = new byte[4];
        addr[0] = (byte) ((address >>> 24) & 0xFF);
        addr[1] = (byte) ((address >>> 16) & 0xFF);
        addr[2] = (byte) ((address >>> 8) & 0xFF);
        addr[3] = (byte) (address & 0xFF);
        return InetAddress.getByAddress(addr);
    }

    @Override
    public int errno() {
        // todo?
        return 0;
    }
}
