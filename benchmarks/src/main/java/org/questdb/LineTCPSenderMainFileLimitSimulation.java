/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package org.questdb;

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cutlass.line.AbstractLineTcpSender;
import io.questdb.cutlass.line.LineTcpSenderV2;
import io.questdb.network.Net;
import io.questdb.std.Os;
import io.questdb.std.Rnd;

import java.util.concurrent.locks.LockSupport;

/*
CREATE TABLE 'request_logs' (
    rid STRING,
    auui SYMBOL capacity 256 CACHE index capacity 262144,
    puui SYMBOL capacity 256 CACHE index capacity 262144,
    buuid SYMBOL capacity 256 CACHE,
    atuuid SYMBOL capacity 256 CACHE index capacity 262144,
    creg STRING, node_uid SYMBOL capacity 256 CACHE index capacity 262144,
    nnet SYMBOL capacity 256 CACHE,
    nv STRING,
    nr SYMBOL capacity 256 CACHE,
    code1 SYMBOL capacity 256 CACHE,
    name1 STRING, 
    id5 STRING,
    il FLOAT,
    iat FLOAT,
    id3 STRING,
    rpcm SYMBOL capacity 256 CACHE index capacity 262144,
    id4 STRING,
    origin STRING,
    ts TIMESTAMP,
    lo6 LONG,
    rst6 SHORT,
    t5 SYMBOL capacity 256 CACHE,
    lo5 LONG,
    b1 STRING,
    b2 STRING,
    mob BOOLEAN
)
timestamp (ts) PARTITION BY HOUR;

alter TABLE 'request_logs' set PARAM maxUncommittedRows = 20000;
 */
public class LineTCPSenderMainFileLimitSimulation {
    private static final String[] atuuid = new String[101];
    private static final String[] auui = new String[50];
    private static final String[] buuid = new String[71];
    private static final String[] code1 = new String[70];
    private static final String[] nnet = new String[70];
    private static final String[] node_uid = new String[46];
    private static final String[] nr = new String[70];
    private static final String[] puui = new String[70];
    private static final String[] rpcm = new String[41];
    private static final String[] t5 = new String[70];

    public static void main(String[] args) {
        Rnd rnd = new Rnd();
        generateStrings(rnd, auui, 16);
        generateStrings(rnd, puui, 16);
        generateStrings(rnd, atuuid, 16);
        generateStrings(rnd, node_uid, 16);
        generateStrings(rnd, rpcm, 8);
        generateStrings(rnd, buuid, 8);
        generateStrings(rnd, nnet, 8);
        generateStrings(rnd, nr, 8);
        generateStrings(rnd, code1, 8);
        generateStrings(rnd, t5, 8);
        String hostid4v4 = "127.0.0.1";
        int port = 9009;
        int bufferCapacity = 8 * 1024;

        try (AbstractLineTcpSender sender = LineTcpSenderV2.newSender(Net.parseIPv4(hostid4v4), port, bufferCapacity)) {
//            fillDates(rnd, sender);

            long ts = Os.currentTimeNanos();
            while (true) {
                long shift = rnd.nextLong(MicrosTimestampDriver.INSTANCE.fromHours((int) (1000L / 3)));
                if (rnd.nextBoolean()) {
                    shift = 0;
                }
                sendLine(rnd, sender, ts - shift);

                LockSupport.parkNanos(10);
                ts += 1000_000L;
            }
        }
    }

    private static void generateStrings(Rnd rnd, String[] auui, int length) {
        for (int i = 0; i < auui.length; i++) {
            auui[i] = rnd.nextString(length);
        }
    }

    private static void sendLine(Rnd rnd, AbstractLineTcpSender sender, long ts) {
        sender.metric("request_logs")
                .tag("auui", auui[rnd.nextInt(auui.length)])
                .tag("puui", puui[rnd.nextInt(puui.length)])
                .tag("atuuid", atuuid[rnd.nextInt(atuuid.length)])
                .tag("node_uid", node_uid[rnd.nextInt(node_uid.length)])
                .tag("rpcm", rpcm[rnd.nextInt(rpcm.length)])
                .tag("buuid", buuid[rnd.nextInt(buuid.length)])
                .tag("nnet", nnet[rnd.nextInt(nnet.length)])
                .tag("nr", nr[rnd.nextInt(nr.length)])
                .tag("code1", code1[rnd.nextInt(code1.length)])
                .tag("t5", code1[rnd.nextInt(t5.length)])
                .field("rid", rnd.nextString(16))
                .field("nv", rnd.nextString(8))
                .field("name1", rnd.nextString(rnd.nextPositiveInt() % 15))
                .field("id5", rnd.nextString(rnd.nextPositiveInt() % 12))
                .field("il", rnd.nextFloat())
                .field("iat", rnd.nextFloat())
                .field("id3", rnd.nextString(rnd.nextPositiveInt() % 12))
                .field("id4", rnd.nextString(15))
                .field("origin", rnd.nextString(rnd.nextPositiveInt() % 10))
                .field("lo6", rnd.nextLong())
                .field("rst6", rnd.nextShort())
                .field("lo5", rnd.nextLong())
                .field("b1", rnd.nextString(rnd.nextPositiveInt() % 25))
                .field("b2", rnd.nextString(rnd.nextPositiveInt() % 15))
                .field("mob", rnd.nextBoolean())
                .$(ts);
    }
}
