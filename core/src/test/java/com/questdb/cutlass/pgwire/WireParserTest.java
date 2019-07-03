/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.cutlass.pgwire;

import com.questdb.griffin.AbstractGriffinTest;
import com.questdb.griffin.engine.functions.rnd.SharedRandom;
import com.questdb.network.*;
import com.questdb.std.*;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.StringSink;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static com.questdb.std.Numbers.hexDigits;

public class WireParserTest extends AbstractGriffinTest {

    private static void toSink(InputStream is, CharSink sink) throws IOException {
        // limit what we print
        byte[] bb = new byte[1];
        int i = 0;
        while (is.read(bb) > 0) {
            byte b = bb[0];
            if (i > 0) {
                if ((i % 16) == 0) {
                    sink.put('\n');
                    Numbers.appendHexPadded(sink, i);
                }
            } else {
                Numbers.appendHexPadded(sink, i);
            }
            sink.put(' ');

            final int v;
            if (b < 0) {
                v = 256 + b;
            } else {
                v = b;
            }

            if (v < 0x10) {
                sink.put('0');
                sink.put(hexDigits[b]);
            } else {
                sink.put(hexDigits[v / 0x10]);
                sink.put(hexDigits[v % 0x10]);
            }

            i++;
        }
    }

    @Test
    public void testSimple() throws SQLException, IOException {
        // start simple server

        long fd = Net.socketTcp(true);
        try {

            WireParser wireParser = new WireParser(new WireParserConfiguration() {
                @Override
                public NetworkFacade getNetworkFacade() {
                    return NetworkFacadeImpl.INSTANCE;
                }

                @Override
                public int getRecvBufferSize() {
                    return 1024 * 1024;
                }

                @Override
                public int getSendBufferSize() {
                    return 1024 * 1024;
                }

                @Override
                public int getIdleSendCountBeforeGivingUp() {
                    return 10_000;
                }
            }, engine);

            Net.setReusePort(fd);

            if (Net.bindTcp(fd, 0, 9120)) {
                Net.listen(fd, 128);

                new Thread(() -> {
                    SharedRandom.RANDOM.set(new Rnd());
                    final long clientFd = Net.accept(fd);
                    while (true) {
                        try {
                            wireParser.recv(clientFd);
                        } catch (PeerDisconnectedException e) {
                            break;
                        } catch (PeerIsSlowToReadException ignored) {
                        }
                    }
                }).start();

                Properties properties = new Properties();
                properties.setProperty("user", "xyz");
                properties.setProperty("password", "oh");
                properties.setProperty("sslmode", "disable");

                final Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:9120/nabu_app", properties);
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(
                        "select " +
                                "rnd_str(4,4,4) s, " +
                                "rnd_int(0, 256, 4) i, " +
                                "rnd_double(4) d, " +
                                "timestamp_sequence(to_timestamp(0),10000) t, " +
                                "rnd_float(4) f, " +
                                "rnd_short() _short, " +
                                "rnd_long(0, 10000000, 5) l, " +
                                "rnd_timestamp(to_timestamp('2015','yyyy'),to_timestamp('2016','yyyy'),2) ts2, " +
                                "rnd_byte(0,127) bb, " +
                                "rnd_boolean() b, " +
                                "rnd_symbol(4,4,4,2), " +
                                "rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2)," +
                                "rnd_bin(10,20,2) " +
                                "from long_sequence(50)");

                final String expected = "s[VARCHAR],i[INTEGER],d[DOUBLE],t[TIMESTAMP],f[REAL],_short[SMALLINT],l[BIGINT],ts2[TIMESTAMP],bb[SMALLINT],b[BIT],rnd_symbol[VARCHAR],rnd_date[DATE],rnd_bin[BINARY]\n" +
                        "null,57,0.62500000,1970-01-01 00:00:00.0,0.4620,-1593,3425232,null,121,false,PEHN,2015-03-17,00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\n" +
                        "XYSB,142,0.57900000,1970-01-01 00:00:00.01,0.9690,20088,1517490,2015-01-17 20:41:19.480685,100,true,PEHN,2015-06-20,00000000 79 5f 8b 81 2b 93 4d 1a 8e 78 b5 b9 11 53 d0 fb\n" +
                        "00000010 64\n" +
                        "OZZV,219,0.16400000,1970-01-01 00:00:00.02,0.6590,-12303,9489508,2015-08-13 17:10:19.752521,6,false,null,2015-05-20,00000000 2b 4d 5f f6 46 90 c3 b3 59 8e e5 61 2f 64 0e\n" +
                        "OLYX,30,0.71300000,1970-01-01 00:00:00.03,0.6550,6610,6504428,2015-08-08 00:42:24.545639,123,false,null,2015-01-03,null\n" +
                        "TIQB,42,0.68100000,1970-01-01 00:00:00.04,0.6260,-1605,8814086,2015-07-28 15:08:53.462495,28,true,CPSW,null,00000000 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                        "LTOV,137,0.76300000,1970-01-01 00:00:00.05,0.8820,9054,null,2015-04-20 05:09:03.580574,106,false,PEHN,2015-01-09,null\n" +
                        "ZIMN,125,null,1970-01-01 00:00:00.06,null,11524,8335261,2015-10-26 02:10:50.688394,111,true,PEHN,2015-08-21,null\n" +
                        "OPJO,168,0.10500000,1970-01-01 00:00:00.07,0.5350,-5920,7080704,2015-07-11 09:15:38.342717,103,false,VTJW,null,null\n" +
                        "GLUO,145,0.53900000,1970-01-01 00:00:00.08,0.7670,14242,2499922,2015-11-02 09:01:31.312804,84,false,PEHN,2015-11-14,null\n" +
                        "ZVQE,103,0.67300000,1970-01-01 00:00:00.09,null,13727,7875846,2015-12-12 13:16:26.134562,22,true,PEHN,2015-01-20,00000000 14 33 80 c9 eb a3 67 7a 1a 79 e4 35 e4 3a dc 5c\n" +
                        "00000010 65 ff\n" +
                        "LIGY,199,0.28400000,1970-01-01 00:00:00.1,null,30426,3215562,2015-08-21 14:55:07.055722,11,false,VTJW,null,00000000 ff 70 3a c7 8a b3 14 cd 47 0b 0c 39 12\n" +
                        "MQNT,43,0.58600000,1970-01-01 00:00:00.11,0.3350,27019,null,null,27,true,PEHN,2015-07-12,00000000 26 fb 2e 42 fa f5 6e 8f 80 e3 54 b8 07 b1 32 57\n" +
                        "00000010 ff 9a ef\n" +
                        "WWCC,213,0.76700000,1970-01-01 00:00:00.12,0.5800,13640,4121923,2015-08-06 02:27:30.469762,73,false,PEHN,2015-04-30,00000000 71 a7 d5 af 11 96 37 08 dd 98 ef 54 88 2a a2 ad\n" +
                        "00000010 e7 d4\n" +
                        "VFGP,120,0.84000000,1970-01-01 00:00:00.13,0.7730,7223,7241423,2015-12-18 07:32:18.456025,43,false,VTJW,null,00000000 24 4e 44 a8 0d fe 27 ec 53 13 5d b2 15 e7 b8 35\n" +
                        "00000010 67\n" +
                        "RMDG,134,0.11000000,1970-01-01 00:00:00.14,0.0430,21227,7155708,2015-07-03 04:12:45.774281,42,true,CPSW,2015-02-24,null\n" +
                        "WFOQ,255,null,1970-01-01 00:00:00.15,0.1160,31569,6688277,2015-05-19 03:30:45.779999,126,true,PEHN,2015-12-09,null\n" +
                        "MXDK,56,1.00000000,1970-01-01 00:00:00.16,0.5230,-32372,6884132,null,58,false,null,2015-01-20,null\n" +
                        "XMKJ,139,0.84100000,1970-01-01 00:00:00.17,0.3060,25856,null,2015-05-18 03:50:22.731437,2,true,VTJW,2015-06-25,00000000 00 7c fb 01 19 ca f2 bf 84 5a 6f 38 35\n" +
                        "VIHD,null,null,1970-01-01 00:00:00.18,0.5500,22280,9109842,2015-01-25 13:51:38.270583,94,false,CPSW,2015-10-27,00000000 2d 16 f3 89 a3 83 64 de d6 fd c4 5b c4 e9\n" +
                        "WPNX,null,0.94700000,1970-01-01 00:00:00.19,0.4150,-17933,674261,2015-03-04 15:43:15.213686,43,true,HYRX,2015-12-18,00000000 b3 4c 0e 8f f1 0c c5 60 b7 d1\n" +
                        "YPOV,36,0.67400000,1970-01-01 00:00:00.2,0.0310,-5888,1375423,2015-12-10 20:50:35.866614,3,true,null,2015-07-23,00000000 d4 ab be 30 fa 8d ac 3d 98 a0 ad 9a 5d\n" +
                        "NUHN,null,0.69400000,1970-01-01 00:00:00.21,0.3390,-25226,3524748,2015-05-07 04:07:18.152968,39,true,VTJW,2015-04-04,00000000 b8 be f8 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0\n" +
                        "00000010 35 d8\n" +
                        "BOSE,240,0.06000000,1970-01-01 00:00:00.22,0.3790,23904,9069339,2015-03-21 03:42:42.643186,84,true,null,null,null\n" +
                        "INKG,124,0.86200000,1970-01-01 00:00:00.23,0.4040,-30383,7233542,2015-07-21 16:42:47.012148,99,false,null,2015-08-27,00000000 87 fc 92 83 fc 88 f3 32 27 70 c8 01 b0 dc c9 3a\n" +
                        "00000010 5b 7e\n" +
                        "FUXC,52,0.74300000,1970-01-01 00:00:00.24,null,-14729,1042064,2015-08-21 02:10:58.949674,28,true,CPSW,2015-08-29,null\n" +
                        "UNYQ,71,0.44200000,1970-01-01 00:00:00.25,0.5390,-22611,null,2015-12-23 18:41:42.319859,98,true,PEHN,2015-01-26,00000000 28 ed 97 99 d8 77 33 3f b2 67 da 98 47 47 bf\n" +
                        "KBMQ,null,0.28000000,1970-01-01 00:00:00.26,null,12240,null,2015-08-16 01:02:55.766622,21,false,null,2015-05-19,00000000 6a de 46 04 d3 81 e7 a2 16 22 35 3b 1c\n" +
                        "JSOL,243,null,1970-01-01 00:00:00.27,0.0680,-17468,null,null,20,true,null,2015-06-19,00000000 3d e0 2d 04 86 e7 ca 29 98 07 69 ca 5b d6 cf 09\n" +
                        "00000010 69\n" +
                        "HNSS,150,null,1970-01-01 00:00:00.28,0.1480,14841,5992443,null,25,false,PEHN,null,00000000 14 d6 fc ee 03 22 81 b8 06 c4 06 af\n" +
                        "PZPB,101,0.06200000,1970-01-01 00:00:00.29,null,12237,9878179,2015-09-03 22:13:18.852465,79,false,VTJW,2015-12-17,00000000 12 61 3a 9a ad 98 2e 75 52 ad 62 87 88 45 b9 9d\n" +
                        "OYNN,25,0.33900000,1970-01-01 00:00:00.3,0.6280,22412,4736378,2015-10-10 12:19:42.528224,106,true,CPSW,2015-07-01,00000000 54 13 3f ff b6 7e cd 04 27 66 94 89 db\n" +
                        "null,117,0.56400000,1970-01-01 00:00:00.31,null,-5604,6353018,null,84,false,null,null,00000000 2b ad 25 07 db 62 44 33 6e 00 8e\n" +
                        "HVRI,233,0.22400000,1970-01-01 00:00:00.32,0.4250,10469,1715213,null,86,false,null,2015-02-02,null\n" +
                        "OYTO,96,0.74100000,1970-01-01 00:00:00.33,0.5280,-12239,3499620,2015-02-07 22:35:03.212268,17,false,PEHN,2015-03-29,null\n" +
                        "LFCY,63,0.72200000,1970-01-01 00:00:00.34,null,23344,9523982,null,123,false,CPSW,2015-05-18,00000000 05 e5 c0 4e cc d6 e3 7b 34 cd 15 35 bb a4\n" +
                        "GHLX,148,0.30600000,1970-01-01 00:00:00.35,0.6360,-31457,2322337,2015-10-22 12:06:05.544701,91,true,HYRX,2015-05-21,00000000 57 1d 91 72 30 04 b7 02 cb 03\n" +
                        "YTSZ,123,null,1970-01-01 00:00:00.36,0.5190,22534,4446236,2015-07-27 07:23:37.233711,53,false,CPSW,2015-01-13,null\n" +
                        "SWLU,251,null,1970-01-01 00:00:00.37,0.1790,7734,4082475,2015-10-21 18:24:34.400345,69,false,PEHN,2015-04-01,null\n" +
                        "TQJL,245,null,1970-01-01 00:00:00.38,0.8650,9516,929340,2015-05-28 04:18:18.640567,69,false,VTJW,2015-06-12,00000000 6c 3e 51 d7 eb b1 07 71 32 1f af 40 4e 8c 47\n" +
                        "REIJ,94,null,1970-01-01 00:00:00.39,0.1300,-29924,null,2015-03-20 22:14:46.204718,113,true,HYRX,2015-12-19,null\n" +
                        "HDHQ,94,0.72300000,1970-01-01 00:00:00.4,0.7300,19970,654131,2015-01-10 22:56:08.48045,84,true,null,2015-03-05,00000000 4f 56 6b 65 a4 53 38 e9 cd c1 a7 ee 86 75 ad a5\n" +
                        "00000010 2d 49\n" +
                        "UMEU,40,0.00800000,1970-01-01 00:00:00.41,0.8050,-11623,4599862,2015-11-20 04:02:44.335947,76,false,PEHN,2015-05-17,null\n" +
                        "YJIH,184,null,1970-01-01 00:00:00.42,0.3830,17614,3101671,2015-01-28 12:05:46.683001,105,true,null,2015-12-07,00000000 ec 69 cd 73 bb 9b c5 95 db 61 91 ce\n" +
                        "CYXG,27,0.29200000,1970-01-01 00:00:00.43,0.9530,3944,249165,null,67,true,null,2015-03-02,00000000 01 48 15 3e 0c 7f 3f 8f e4 b5 ab 34 21 29\n" +
                        "MRTG,143,0.02600000,1970-01-01 00:00:00.44,0.9430,-27320,1667842,2015-01-24 19:56:15.973109,11,false,null,2015-01-24,null\n" +
                        "DONP,246,0.65400000,1970-01-01 00:00:00.45,0.5560,27477,4160018,2015-12-14 03:40:05.911839,20,true,PEHN,2015-10-29,00000000 07 92 01 f5 6a a1 31 cd cb c2 a2 b4 8e 99\n" +
                        "IQXS,232,0.23100000,1970-01-01 00:00:00.46,0.0490,-18113,4005228,2015-06-11 13:00:07.248188,8,true,CPSW,2015-08-16,00000000 fa 1f 92 24 b1 b8 67 65 08 b7 f8 41 00\n" +
                        "null,178,null,1970-01-01 00:00:00.47,0.9030,-14626,2934570,2015-04-04 08:51:54.068154,88,true,null,2015-07-01,00000000 84 36 25 63 2b 63 61 43 1c 47 7d b6 46 ba bb 98\n" +
                        "00000010 ca 08 be a4\n" +
                        "HUWZ,94,0.11000000,1970-01-01 00:00:00.48,0.4200,-3736,5687514,2015-01-02 17:18:05.627633,74,false,null,2015-03-29,null\n" +
                        "SRED,66,0.11300000,1970-01-01 00:00:00.49,0.0600,-10543,3669377,2015-10-22 02:53:02.381351,77,true,PEHN,null,00000000 7c 3f d6 88 3a 93 ef 24 a5 e2 bc\n";

                StringSink sink = new StringSink();

                // dump metadata
                assertResultSet(expected, sink, rs);
                connection.close();
            } else {
                throw NetworkError.instance(Os.errno()).couldNotBindSocket();
            }
        } finally {
            Net.close(fd);
        }
    }

    @Test
    @Ignore
    public void testSimpleHex() throws SQLException, IOException, BrokenBarrierException, InterruptedException, NumericException {
        // start simple server

        long fd = Net.socketTcp(true);
        try {

            WireParser wireParser = new WireParser(new WireParserConfiguration() {
                @Override
                public NetworkFacade getNetworkFacade() {
                    return NetworkFacadeImpl.INSTANCE;
                }

                @Override
                public int getRecvBufferSize() {
                    return 1024 * 1024;
                }

                @Override
                public int getSendBufferSize() {
                    return 1024 * 1024;
                }

                @Override
                public int getIdleSendCountBeforeGivingUp() {
                    return 10_000;
                }
            }, engine);

            Net.setReusePort(fd);

            CyclicBarrier barrier = new CyclicBarrier(2);

            if (Net.bindTcp(fd, 0, 9120)) {
                Net.listen(fd, 128);

                new Thread(() -> {
                    SharedRandom.RANDOM.set(new Rnd());
                    final long clientFd = Net.accept(fd);
                    try {
                        barrier.await();
                    } catch (InterruptedException | BrokenBarrierException e) {
                        e.printStackTrace();
                    }

                    while (true) {
                        try {
                            wireParser.recv(clientFd);
                        } catch (PeerDisconnectedException e) {
                            break;
                        } catch (PeerIsSlowToReadException ignored) {
                        }
                    }
                }).start();

                barrier.await();

                long clientFd = Net.socketTcp(true);
                long sockAddress = Net.sockaddr(0, 9120);
                Assert.assertEquals(0, Net.connect(clientFd, sockAddress));

                // this is a HEX encoded bytes of the same script as 'testSimple' sends using postgres jdbc driver
                String script = ">00073030075736572078797a0646174616261736506e6162755f6170700636c69656e745f656e636f64696e670555446380446174655374796c65049534f054696d655a6f6e6504575726f70652f4c6f6e646f6e065787472615f666c6f61745f64696769747303200\n" +
                        "<5200080003\n" +
                        ">7000076f680\n" +
                        "<5200080000530001154696d655a6f6e650474d540530001d6170706c69636174696f6e5f6e616d650517565737444420530001e7365727665725f76657273696f6e5f6e756d031303030303005300019696e74656765725f6461746574696d657306f6e05a000549\n" +
                        ">500002205345542065787472615f666c6f61745f646967697473203d203300042000c0000000045000900001530004\n" +
                        "<3100045a000549\n" +
                        ">>42000c0000000045000900001530004\n" +
                        ">>45000900001530004\n" +
                        ">>530004\n" +
                        ">50000370534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a444243204472697665722700042000c0000000045000900001530004\n" +
                        "<3100045a000549\n" +
                        ">>42000c0000000045000900001530004\n" +
                        ">>45000900001530004\n" +
                        ">>530004\n" +
                        ">50001a2073656c65637420726e645f73747228342c342c342920732c20726e645f696e7428302c203235362c20342920692c20726e645f646f75626c6528342920642c2074696d657374616d705f73657175656e636528746f5f74696d657374616d702830292c31303030302920742c20726e645f666c6f617428342920662c20726e645f73686f72742829205f73686f72742c20726e645f6c6f6e6728302c2031303030303030302c203529206c2c20726e645f74696d657374616d7028746f5f74696d657374616d70282732303135272c277979797927292c746f5f74696d657374616d70282732303136272c277979797927292c3229207473322c20726e645f6279746528302c313237292062622c20726e645f626f6f6c65616e282920622c20726e645f73796d626f6c28342c342c342c32292c20726e645f6461746528746f5f64617465282732303135272c20277979797927292c20746f5f64617465282732303136272c20277979797927292c2032292c726e645f62696e2831302c32302c32292066726f6d206c6f6e675f73657175656e63652835302900042000c0000000044000650045000900000530004\n" +
                        ">>42000c0000000044000650045000900000530004\n" +
                        "<54001280d73000000000413000000006900000000001700000000640000000002bd00000000740000000004a000000000660000000002bc000000005f73686f7274000000000015000000006c000000000014000000007473320000000004a0000000006262000000000015000000006200000000001000000000726e645f73796d626f6c00000000041300000000726e645f6461746500000000043a00000000726e645f62696e00000000001100000001\n" +
                        ">>44000650045000900000530004\n" +
                        "<440008c0dffffffff000235370005302e3632350001a313937302d30312d30312030303a30303a30302e3030303030300005302e34363200052d31353933000733343235323332ffffffff000331323100016600045045484e000a323031352d30332d3137000e19c49594365349b4597e3b8a11e44000ae0d00045859534200033134320005302e3537390001a313937302d30312d30312030303a30303a30302e3031303030300005302e393639000532303038380007313531373439300001a323031352d30312d31372032303a34313a31392e343830363835000331303000017400045045484e000a323031352d30362d323000011795f8b812b934d1a8e78b5b91153d0fb6444000a70d00044f5a5a5600033231390005302e3136340001a313937302d30312d30312030303a30303a30302e3032303030300005302e36353900062d31323330330007393438393530380001a323031352d30382d31332031373a31303a31392e373532353231000136000166ffffffff000a323031352d30352d3230000f2b4d5ff64690c3b3598ee5612f64e44000970d00044f4c5958000233300005302e3731330001a313937302d30312d30312030303a30303a30302e3033303030300005302e3635350004363631300007363530343432380001a323031352d30382d30382030303a34323a32342e3534353633390003313233000166ffffffff000a323031352d30312d3033ffffffff440009f0d000454495142000234320005302e3638310001a313937302d30312d30312030303a30303a30302e3034303030300005302e36323600052d313630350007383831343038360001a323031352d30372d32382031353a30383a35332e34363234393500023238000174000443505357ffffffff000e3ba6dc3b7d2be392fe6938e1779a44000950d00044c544f5600033133370005302e3736330001a313937302d30312d30312030303a30303a30302e3035303030300005302e383832000439303534ffffffff0001a323031352d30342d32302030353a30393a30332e353830353734000331303600016600045045484e000a323031352d30312d3039ffffffff44000930d00045a494d4e0003313235ffffffff0001a313937302d30312d30312030303a30303a30302e303630303030ffffffff000531313532340007383333353236310001a323031352d31302d32362030323a31303a35302e363838333934000331313100017400045045484e000a323031352d30382d3231ffffffff44000930d00044f504a4f00033136380005302e3130350001a313937302d30312d30312030303a30303a30302e3037303030300005302e35333500052d353932300007373038303730340001a323031352d30372d31312030393a31353a33382e3334323731370003313033000166000456544a57ffffffffffffffff440009c0d0004474c554f00033134350005302e3533390001a313937302d30312d30312030303a30303a30302e3038303030300005302e373637000531343234320007323439393932320001a323031352d31312d30322030393a30313a33312e3331323830340002383400016600045045484e000a323031352d31312d3134ffffffff44000a90d00045a56514500033130330005302e3637330001a313937302d30312d30312030303a30303a30302e303930303030ffffffff000531333732370007373837353834360001a323031352d31322d31322031333a31363a32362e3133343536320002323200017400045045484e000a323031352d30312d323000012143380c9eba3677a1a79e435e43adc5c65ff440009a0d00044c49475900033139390005302e3238340001a313937302d30312d30312030303a30303a30302e313030303030ffffffff000533303432360007333231353536320001a323031352d30382d32312031343a35353a30372e30353537323200023131000166000456544a57ffffffff000dff703ac78ab314cd47bc3912440008d0d00044d514e54000234330005302e3538360001a313937302d30312d30312030303a30303a30302e3131303030300005302e33333500053237303139ffffffffffffffff0002323700017400045045484e000a323031352d30372d31320001326fb2e42faf56e8f80e354b87b13257ff9aef44000ae0d00045757434300033231330005302e3736370001a313937302d30312d30312030303a30303a30302e3132303030300005302e353830000531333634300007343132313932330001a323031352d30382d30362030323a32373a33302e3436393736320002373300016600045045484e000a323031352d30342d33300001271a7d5af1196378dd98ef54882aa2ade7d444000a20d00045646475000033132300005302e3834300001a313937302d30312d30312030303a30303a30302e3133303030300005302e3737330004373232330007373234313432330001a323031352d31322d31382030373a33323a31382e34353630323500023433000166000456544a57ffffffff00011244e44a8dfe27ec53135db215e7b83567440009c0d0004524d444700033133340005302e3131300001a313937302d30312d30312030303a30303a30302e3134303030300005302e303433000532313232370007373135353730380001a323031352d30372d30332030343a31323a34352e37373432383100023432000174000443505357000a323031352d30322d3234ffffffff44000980d000457464f510003323535ffffffff0001a313937302d30312d30312030303a30303a30302e3135303030300005302e313136000533313536390007363638383237370001a323031352d30352d31392030333a33303a34352e373739393939000331323600017400045045484e000a323031352d31322d3039ffffffff440007e0d00044d58444b000235360005312e3030300001a313937302d30312d30312030303a30303a30302e3136303030300005302e35323300062d3332333732000736383834313332ffffffff00023538000166ffffffff000a323031352d30312d3230ffffffff44000a10d0004584d4b4a00033133390005302e3834310001a313937302d30312d30312030303a30303a30302e3137303030300005302e33303600053235383536ffffffff0001a323031352d30352d31382030333a35303a32322e373331343337000132000174000456544a57000a323031352d30362d3235000d07cfb119caf2bf845a6f383544000a20d000456494844ffffffffffffffff0001a313937302d30312d30312030303a30303a30302e3138303030300005302e353530000532323238300007393130393834320001a323031352d30312d32352031333a35313a33382e32373035383300023934000166000443505357000a323031352d31302d3237000e2d16f389a38364ded6fdc45bc4e944000a30d000457504e58ffffffff0005302e3934370001a313937302d30312d30312030303a30303a30302e3139303030300005302e34313500062d313739333300063637343236310001a323031352d30332d30342031353a34333a31352e32313336383600023433000174000448595258000a323031352d31322d3138000ab34ce8ff1cc560b7d144000a30d000459504f56000233360005302e3637340001a313937302d30312d30312030303a30303a30302e3230303030300005302e30333100052d353838380007313337353432330001a323031352d31322d31302032303a35303a33352e383636363134000133000174ffffffff000a323031352d30372d3233000dd4abbe30fa8dac3d98a0ad9a5d44000ac0d00044e55484effffffff0005302e3639340001a313937302d30312d30312030303a30303a30302e3231303030300005302e33333900062d32353232360007333532343734380001a323031352d30352d30372030343a30373a31382e31353239363800023339000174000456544a57000a323031352d30342d303400012b8bef8a146872892a39be3cbc2648ab035d8440008e0d0004424f534500033234300005302e3036300001a313937302d30312d30312030303a30303a30302e3232303030300005302e333739000532333930340007393036393333390001a323031352d30332d32312030333a34323a34322e36343331383600023834000174ffffffffffffffffffffffff44000ab0d0004494e4b4700033132340005302e3836320001a313937302d30312d30312030303a30303a30302e3233303030300005302e34303400062d33303338330007373233333534320001a323031352d30372d32312031363a34323a34372e30313231343800023939000166ffffffff000a323031352d30382d32370001287fc9283fc88f3322770c81b0dcc93a5b7e44000970d000446555843000235320005302e3734330001a313937302d30312d30312030303a30303a30302e323430303030ffffffff00062d31343732390007313034323036340001a323031352d30382d32312030323a31303a35382e39343936373400023238000174000443505357000a323031352d30382d3239ffffffff44000a40d0004554e5951000237310005302e3434320001a313937302d30312d30312030303a30303a30302e3235303030300005302e35333900062d3232363131ffffffff0001a323031352d31322d32332031383a34313a34322e3331393835390002393800017400045045484e000a323031352d30312d3236000f28ed9799d877333fb267da984747bf44000960d00044b424d51ffffffff0005302e3238300001a313937302d30312d30312030303a30303a30302e323630303030ffffffff00053132323430ffffffff0001a323031352d30382d31362030313a30323a35352e37363636323200023231000166ffffffff000a323031352d30352d3139000d6ade464d381e7a21622353b1c44000840d00044a534f4c0003323433ffffffff0001a313937302d30312d30312030303a30303a30302e3237303030300005302e30363800062d3137343638ffffffffffffffff00023230000174ffffffff000a323031352d30362d3139000113de02d486e7ca2998769ca5bd6cf969440007f0d0004484e53530003313530ffffffff0001a313937302d30312d30312030303a30303a30302e3238303030300005302e31343800053134383431000735393932343433ffffffff0002323500016600045045484effffffff000c14d6fcee32281b86c46af44000a70d0004505a504200033130310005302e3036320001a313937302d30312d30312030303a30303a30302e323930303030ffffffff000531323233370007393837383137390001a323031352d30392d30332032323a31333a31382e38353234363500023739000166000456544a57000a323031352d31322d31370001012613a9aad982e7552ad62878845b99d44000a90d00044f594e4e000232350005302e3333390001a313937302d30312d30312030303a30303a30302e3330303030300005302e363238000532323431320007343733363337380001a323031352d31302d31302031323a31393a34322e3532383232340003313036000174000443505357000a323031352d30372d3031000d54133fffb67ecd427669489db44000760dffffffff00033131370005302e3536340001a313937302d30312d30312030303a30303a30302e333130303030ffffffff00052d35363034000736333533303138ffffffff00023834000166ffffffffffffffff000b2bad257db6244336e08e440007e0d00044856524900033233330005302e3232340001a313937302d30312d30312030303a30303a30302e3332303030300005302e34323500053130343639000731373135323133ffffffff00023836000166ffffffff000a323031352d30322d3032ffffffff440009c0d00044f59544f000239360005302e3734310001a313937302d30312d30312030303a30303a30302e3333303030300005302e35323800062d31323233390007333439393632300001a323031352d30322d30372032323a33353a30332e3231323236380002313700016600045045484e000a323031352d30332d3239ffffffff440008b0d00044c464359000236330005302e3732320001a313937302d30312d30312030303a30303a30302e333430303030ffffffff00053233333434000739353233393832ffffffff0003313233000166000443505357000a323031352d30352d3138000e5e5c04eccd6e37b34cd1535bba444000a70d000447484c5800033134380005302e3330360001a313937302d30312d30312030303a30303a30302e3335303030300005302e36333600062d33313435370007323332323333370001a323031352d31302d32322031323a30363a30352e35343437303100023931000174000448595258000a323031352d30352d3231000a571d9172304b72cb344000970d00045954535a0003313233ffffffff0001a313937302d30312d30312030303a30303a30302e3336303030300005302e353139000532323533340007343434363233360001a323031352d30372d32372030373a32333a33372e32333337313100023533000166000443505357000a323031352d30312d3133ffffffff44000960d000453574c550003323531ffffffff0001a313937302d30312d30312030303a30303a30302e3337303030300005302e3137390004373733340007343038323437350001a323031352d31302d32312031383a32343a33342e3430303334350002363900016600045045484e000a323031352d30342d3031ffffffff44000a40d000454514a4c0003323435ffffffff0001a313937302d30312d30312030303a30303a30302e3338303030300005302e38363500043935313600063932393334300001a323031352d30352d32382030343a31383a31382e36343035363700023639000166000456544a57000a323031352d30362d3132000f6c3e51d7ebb1771321faf404e8c4744000910d00045245494a00023934ffffffff0001a313937302d30312d30312030303a30303a30302e3339303030300005302e31333000062d3239393234ffffffff0001a323031352d30332d32302032323a31343a34362e3230343731380003313133000174000448595258000a323031352d31322d3139ffffffff44000a80d000448444851000239340005302e3732330001a313937302d30312d30312030303a30303a30302e3430303030300005302e3733300005313939373000063635343133310001a323031352d30312d31302032323a35363a30382e34383034353000023834000174ffffffff000a323031352d30332d3035000124f566b65a45338e9cdc1a7ee8675ada52d49440009c0d0004554d4555000234300005302e3030380001a313937302d30312d30312030303a30303a30302e3431303030300005302e38303500062d31313632330007343539393836320001a323031352d31312d32302030343a30323a34342e3333353934370002373600016600045045484e000a323031352d30352d3137ffffffff44000a00d0004594a49480003313834ffffffff0001a313937302d30312d30312030303a30303a30302e3432303030300005302e333833000531373631340007333130313637310001a323031352d30312d32382031323a30353a34362e3638333030310003313035000174ffffffff000a323031352d31322d3037000cec69cd73bb9bc595db6191ce44000890d000443595847000232370005302e3239320001a313937302d30312d30312030303a30303a30302e3433303030300005302e3935330004333934340006323439313635ffffffff00023637000174ffffffff000a323031352d30332d3032000e148153ec7f3f8fe4b5ab34212944000990d00044d52544700033134330005302e3032360001a313937302d30312d30312030303a30303a30302e3434303030300005302e39343300062d32373332300007313636373834320001a323031352d30312d32342031393a35363a31352e39373331303900023131000166ffffffff000a323031352d30312d3234ffffffff44000aa0d0004444f4e5000033234360005302e3635340001a313937302d30312d30312030303a30303a30302e3435303030300005302e353536000532373437370007343136303031380001a323031352d31322d31342030333a34303a30352e3931313833390002323000017400045045484e000a323031352d31302d3239000e7921f56aa131cdcbc2a2b48e9944000a90d00044951585300033233320005302e3233310001a313937302d30312d30312030303a30303a30302e3436303030300005302e30343900062d31383131330007343030353232380001a323031352d30362d31312031333a30303a30372e323438313838000138000174000443505357000a323031352d30382d3136000dfa1f9224b1b867658b7f841044000a40dffffffff0003313738ffffffff0001a313937302d30312d30312030303a30303a30302e3437303030300005302e39303300062d31343632360007323933343537300001a323031352d30342d30342030383a35313a35342e30363831353400023838000174ffffffff000a323031352d30372d303100014843625632b6361431c477db646babb98ca8bea444000970d00044855575a000239340005302e3131300001a313937302d30312d30312030303a30303a30302e3438303030300005302e34323000052d333733360007353638373531340001a323031352d30312d30322031373a31383a30352e36323736333300023734000166ffffffff000a323031352d30332d3239ffffffff440009d0d000453524544000236360005302e3131330001a313937302d30312d30312030303a30303a30302e3439303030300005302e30363000062d31303534330007333636393337370001a323031352d31302d32322030323a35333a30322e3338313335310002373700017400045045484effffffff000b7c3fd6883a93ef24a5e2bc\n" +
                        "<43000f53454c4543542030203005a000549\n" +
                        ">>45000900000530004\n" +
                        ">>530004\n" +
                        ">580004\n";

                final int N = 1024 * 1024;
                long sendBuf = Unsafe.malloc(N);
                long recvBuf = Unsafe.malloc(N);

                long sendPtr = sendBuf;
                long recvPtr = recvBuf;

                int mode = 0;
                int n = script.length();
                int i = 0;
                while (i < n) {
                    char c1 = script.charAt(i);
                    switch (c1) {
                        case '<':
                            Net.recv(clientFd, recvPtr, (int) (N - (recvPtr - recvBuf)));
                            mode = 1;
                            i++;
                            continue;
                        case '>':
                            Net.send(clientFd, sendBuf, (int) (sendPtr - sendBuf));
                            mode = 0;
                            i++;
                            continue;
                        case '\n':
                            i++;
                            continue;
                        default:

                            char c2 = script.charAt(i + 1);
                            byte b = (byte) ((Numbers.hexToDecimal(c1) << 4) | Numbers.hexToDecimal(c2));

                            if (mode == 0) {
                                // we are sending this
                                Unsafe.getUnsafe().putByte(sendPtr++, b);
                            }
                            break;
                    }
                }
            } else {
                throw NetworkError.instance(Os.errno()).couldNotBindSocket();
            }
        } finally {
            Net.close(fd);
        }
    }

    @Test
    public void testPreparedStatement() throws SQLException, IOException {
        // start simple server

        long fd = Net.socketTcp(true);
        try {

            WireParser wireParser = new WireParser(new WireParserConfiguration() {
                @Override
                public NetworkFacade getNetworkFacade() {
                    return NetworkFacadeImpl.INSTANCE;
                }

                @Override
                public int getRecvBufferSize() {
                    return 1024 * 1024;
                }

                @Override
                public int getSendBufferSize() {
                    return 1024 * 1024;
                }

                @Override
                public int getIdleSendCountBeforeGivingUp() {
                    return 10_000;
                }
            }, engine);

            Net.setReusePort(fd);

            if (Net.bindTcp(fd, 0, 9120)) {
                Net.listen(fd, 128);

                new Thread(() -> {
                    SharedRandom.RANDOM.set(new Rnd());
                    final long clientFd = Net.accept(fd);
                    while (true) {
                        try {
                            wireParser.recv(clientFd);
                        } catch (PeerDisconnectedException e) {
                            break;
                        } catch (PeerIsSlowToReadException ignored) {
                        }
                    }
                }).start();

                Properties properties = new Properties();
                properties.setProperty("user", "xyz");
                properties.setProperty("password", "oh");
                properties.setProperty("sslmode", "disable");

                final Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:9120/nabu_app", properties);
                PreparedStatement statement = connection.prepareStatement("select 1,2,3 from long_sequence(1)");
                Statement statement1 = connection.createStatement();

                final String expected = "1[INTEGER],2[INTEGER],3[INTEGER]\n" +
                        "1,2,3\n";

                StringSink sink = new StringSink();
                for (int i = 0; i < 10; i++) {
                    sink.clear();
                    ResultSet rs = statement.executeQuery();

                    statement1.executeQuery("select 1 from long_sequence(2)");
                    assertResultSet(expected, sink, rs);
                    rs.close();
                }

                connection.close();
            } else {
                throw NetworkError.instance(Os.errno()).couldNotBindSocket();
            }
        } finally {
            Net.close(fd);
        }
    }

    @Test
    public void testDDL() throws SQLException {
        // start simple server

        long fd = Net.socketTcp(true);
        try {

            WireParser wireParser = new WireParser(new WireParserConfiguration() {
                @Override
                public NetworkFacade getNetworkFacade() {
                    return NetworkFacadeImpl.INSTANCE;
                }

                @Override
                public int getRecvBufferSize() {
                    return 1024 * 1024;
                }

                @Override
                public int getSendBufferSize() {
                    return 1024 * 1024;
                }

                @Override
                public int getIdleSendCountBeforeGivingUp() {
                    return 10_000;
                }
            }, engine);

            Net.setReusePort(fd);

            if (Net.bindTcp(fd, 0, 9120)) {
                Net.listen(fd, 128);

                new Thread(() -> {
                    SharedRandom.RANDOM.set(new Rnd());
                    final long clientFd = Net.accept(fd);
                    while (true) {
                        try {
                            wireParser.recv(clientFd);
                        } catch (PeerDisconnectedException e) {
                            break;
                        } catch (PeerIsSlowToReadException ignored) {
                        }
                    }
                }).start();

                Properties properties = new Properties();
                properties.setProperty("user", "xyz");
                properties.setProperty("password", "oh");
                properties.setProperty("sslmode", "disable");

                final Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:9120/nabu_app", properties);
                PreparedStatement statement = connection.prepareStatement("create table x (a int)");
                statement.execute();
                connection.close();
            } else {
                throw NetworkError.instance(Os.errno()).couldNotBindSocket();
            }
        } finally {
            Net.close(fd);
        }
    }

    @Test
    @Ignore
    public void testSqlSyntaxError() throws SQLException {
        // start simple server

        long fd = Net.socketTcp(true);

        WireParser wireParser = new WireParser(new WireParserConfiguration() {
            @Override
            public NetworkFacade getNetworkFacade() {
                return NetworkFacadeImpl.INSTANCE;
            }

            @Override
            public int getRecvBufferSize() {
                return 1024 * 1024;
            }

            @Override
            public int getSendBufferSize() {
                return 1024 * 1024;
            }

            @Override
            public int getIdleSendCountBeforeGivingUp() {
                return 10_000;
            }
        }, engine);

        Net.setReusePort(fd);

        if (Net.bindTcp(fd, 0, 9120)) {
            Net.listen(fd, 128);

            new Thread(() -> {
                SharedRandom.RANDOM.set(new Rnd());
                final long clientFd = Net.accept(fd);
                while (true) {
                    try {
                        wireParser.recv(clientFd);
                    } catch (PeerDisconnectedException e) {
                        break;
                    } catch (PeerIsSlowToReadException ignored) {
                    }
                }
            }).start();

            Properties properties = new Properties();
            properties.setProperty("user", "xyz");
            properties.setProperty("password", "oh");
            properties.setProperty("sslmode", "disable");

            final Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:9120/nabu_app", properties);
            PreparedStatement statement = connection.prepareStatement("create table x");
            statement.execute();
            statement.close();
            connection.close();
        } else {
            throw NetworkError.instance(Os.errno()).couldNotBindSocket();
        }
    }

    private void assertResultSet(String expected, StringSink sink, ResultSet rs) throws SQLException, IOException {
        // dump metadata
        ResultSetMetaData metaData = rs.getMetaData();
        final int columnCount = metaData.getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            if (i > 0) {
                sink.put(',');
            }

            sink.put(metaData.getColumnName(i + 1));
            sink.put('[').put(JDBCType.valueOf(metaData.getColumnType(i + 1)).name()).put(']');
        }
        sink.put('\n');

        while (rs.next()) {
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) {
                    sink.put(',');
                }
                switch (JDBCType.valueOf(metaData.getColumnType(i))) {
                    case VARCHAR:
                        String stringValue = rs.getString(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(stringValue);
                        }
                        break;
                    case INTEGER:
                        int intValue = rs.getInt(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(intValue);
                        }
                        break;
                    case DOUBLE:
                        double doubleValue = rs.getDouble(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(doubleValue, 8);
                        }
                        break;
                    case TIMESTAMP:
                        Timestamp timestamp = rs.getTimestamp(i);
                        if (timestamp == null) {
                            sink.put("null");
                        } else {
                            sink.put(timestamp.toString());
                        }
                        break;
                    case REAL:
                        double floatValue = rs.getFloat(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(floatValue, 4);
                        }
                        break;
                    case SMALLINT:
                        sink.put(rs.getShort(i));
                        break;
                    case BIGINT:
                        long longValue = rs.getLong(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(longValue);
                        }
                        break;
                    case BIT:
                        sink.put(rs.getBoolean(i));
                        break;
                    case DATE:
                        Date date = rs.getDate(i);
                        if (date == null) {
                            sink.put("null");
                        } else {
                            sink.put(date.toString());
                        }
                        break;
                    case BINARY:
                        InputStream stream = rs.getBinaryStream(i);
                        if (stream == null) {
                            sink.put("null");
                        } else {
                            toSink(stream, sink);
                        }
                        break;
                }
            }
            sink.put('\n');
        }

        TestUtils.assertEquals(expected, sink);
    }

}