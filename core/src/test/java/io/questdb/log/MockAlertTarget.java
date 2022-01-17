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

import org.junit.Assert;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

class MockAlertTarget extends Thread {
    static final String ACK = "Ack";
    static final String DEATH_PILL = "]"; // /alert-manager-tpt.json ends with "]\n"


    private final int portNumber;
    private final Runnable onTargetStart;
    private final Runnable onTargetEnd;
    private final AtomicBoolean isRunning;

    MockAlertTarget(int portNumber, Runnable onTargetEnd, Runnable onTargetStart) {
        this.portNumber = portNumber;
        this.onTargetStart = onTargetStart;
        this.onTargetEnd = onTargetEnd;
        this.isRunning = new AtomicBoolean();
    }


    boolean isRunning() {
        return isRunning.get();
    }

    @Override
    public void run() {
        if (isRunning.compareAndSet(false, true)) {
            ServerSocket serverSkt = null;
            Socket clientSkt = null;
            BufferedReader in = null;
            PrintWriter out = null;
            try {
                // setup server socket and accept client
                serverSkt = new ServerSocket(portNumber);
                serverSkt.setReuseAddress(true);
                serverSkt.setSoTimeout(5000);
                onTargetStart.run();
                clientSkt = serverSkt.accept();
                in = new BufferedReader(new InputStreamReader(clientSkt.getInputStream()));
                out = new PrintWriter(clientSkt.getOutputStream(), true);
                clientSkt.setSoTimeout(5000);
                clientSkt.setReuseAddress(true);
                clientSkt.setTcpNoDelay(true);
                clientSkt.setKeepAlive(false);
                clientSkt.setSoLinger(true, 0);

                // read until end or until death pill is read
                String line = in.readLine();
                while (line != null) {
                    if (line.equals(DEATH_PILL)) {
                        break;
                    }
                    line = in.readLine();
                }
                // send ACK, equivalent to status: ok in http
                out.print(ACK);
                out.flush();
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            } finally {
                safeClose(out);
                safeClose(in);
                safeClose(clientSkt);
                safeClose(serverSkt);
                isRunning.set(false);
                if (onTargetEnd != null) {
                    onTargetEnd.run();
                }
            }
        }
    }

    private static void safeClose(Closeable target) {
        if (target != null) {
            try {
                target.close();
            } catch (IOException ignored) {
                // ignore
            }
        }
    }
}
