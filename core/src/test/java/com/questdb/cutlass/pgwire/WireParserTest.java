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
import com.questdb.std.Numbers;
import com.questdb.std.Os;
import com.questdb.std.Rnd;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.StringSink;
import com.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

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
                    "null,57,0.62500000,1970-01-01 00:00:00.0,0.4620,-1593,3425232,null,121,false,PEHN,,2015-03-17,00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\n" +
                    "XYSB,142,0.57900000,1970-01-01 00:00:00.01,0.9690,20088,1517490,2015-01-17 20:41:19.480685,100,true,PEHN,,2015-06-20,00000000 79 5f 8b 81 2b 93 4d 1a 8e 78 b5 b9 11 53 d0 fb\n" +
                    "00000010 64\n" +
                    "OZZV,219,0.16400000,1970-01-01 00:00:00.02,0.6590,-12303,9489508,2015-08-13 17:10:19.752521,6,false,null,2015-05-20,00000000 2b 4d 5f f6 46 90 c3 b3 59 8e e5 61 2f 64 0e\n" +
                    "OLYX,30,0.71300000,1970-01-01 00:00:00.03,0.6550,6610,6504428,2015-08-08 00:42:24.545639,123,false,null,2015-01-03,null\n" +
                    "TIQB,42,0.68100000,1970-01-01 00:00:00.04,0.6260,-1605,8814086,2015-07-28 15:08:53.462495,28,true,CPSW,,null,00000000 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                    "LTOV,137,0.76300000,1970-01-01 00:00:00.05,0.8820,9054,null,2015-04-20 05:09:03.580574,106,false,PEHN,,2015-01-09,null\n" +
                    "ZIMN,125,null,1970-01-01 00:00:00.06,null,11524,8335261,2015-10-26 02:10:50.688394,111,true,PEHN,,2015-08-21,null\n" +
                    "OPJO,168,0.10500000,1970-01-01 00:00:00.07,0.5350,-5920,7080704,2015-07-11 09:15:38.342717,103,false,VTJW,,null,null\n" +
                    "GLUO,145,0.53900000,1970-01-01 00:00:00.08,0.7670,14242,2499922,2015-11-02 09:01:31.312804,84,false,PEHN,,2015-11-14,null\n" +
                    "ZVQE,103,0.67300000,1970-01-01 00:00:00.09,null,13727,7875846,2015-12-12 13:16:26.134562,22,true,PEHN,,2015-01-20,00000000 14 33 80 c9 eb a3 67 7a 1a 79 e4 35 e4 3a dc 5c\n" +
                    "00000010 65 ff\n" +
                    "LIGY,199,0.28400000,1970-01-01 00:00:00.1,null,30426,3215562,2015-08-21 14:55:07.055722,11,false,VTJW,,null,00000000 ff 70 3a c7 8a b3 14 cd 47 0b 0c 39 12\n" +
                    "MQNT,43,0.58600000,1970-01-01 00:00:00.11,0.3350,27019,null,null,27,true,PEHN,,2015-07-12,00000000 26 fb 2e 42 fa f5 6e 8f 80 e3 54 b8 07 b1 32 57\n" +
                    "00000010 ff 9a ef\n" +
                    "WWCC,213,0.76700000,1970-01-01 00:00:00.12,0.5800,13640,4121923,2015-08-06 02:27:30.469762,73,false,PEHN,,2015-04-30,00000000 71 a7 d5 af 11 96 37 08 dd 98 ef 54 88 2a a2 ad\n" +
                    "00000010 e7 d4\n" +
                    "VFGP,120,0.84000000,1970-01-01 00:00:00.13,0.7730,7223,7241423,2015-12-18 07:32:18.456025,43,false,VTJW,,null,00000000 24 4e 44 a8 0d fe 27 ec 53 13 5d b2 15 e7 b8 35\n" +
                    "00000010 67\n" +
                    "RMDG,134,0.11000000,1970-01-01 00:00:00.14,0.0430,21227,7155708,2015-07-03 04:12:45.774281,42,true,CPSW,,2015-02-24,null\n" +
                    "WFOQ,255,null,1970-01-01 00:00:00.15,0.1160,31569,6688277,2015-05-19 03:30:45.779999,126,true,PEHN,,2015-12-09,null\n" +
                    "MXDK,56,1.00000000,1970-01-01 00:00:00.16,0.5230,-32372,6884132,null,58,false,null,2015-01-20,null\n" +
                    "XMKJ,139,0.84100000,1970-01-01 00:00:00.17,0.3060,25856,null,2015-05-18 03:50:22.731437,2,true,VTJW,,2015-06-25,00000000 00 7c fb 01 19 ca f2 bf 84 5a 6f 38 35\n" +
                    "VIHD,null,null,1970-01-01 00:00:00.18,0.5500,22280,9109842,2015-01-25 13:51:38.270583,94,false,CPSW,,2015-10-27,00000000 2d 16 f3 89 a3 83 64 de d6 fd c4 5b c4 e9\n" +
                    "WPNX,null,0.94700000,1970-01-01 00:00:00.19,0.4150,-17933,674261,2015-03-04 15:43:15.213686,43,true,HYRX,,2015-12-18,00000000 b3 4c 0e 8f f1 0c c5 60 b7 d1\n" +
                    "YPOV,36,0.67400000,1970-01-01 00:00:00.2,0.0310,-5888,1375423,2015-12-10 20:50:35.866614,3,true,null,2015-07-23,00000000 d4 ab be 30 fa 8d ac 3d 98 a0 ad 9a 5d\n" +
                    "NUHN,null,0.69400000,1970-01-01 00:00:00.21,0.3390,-25226,3524748,2015-05-07 04:07:18.152968,39,true,VTJW,,2015-04-04,00000000 b8 be f8 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0\n" +
                    "00000010 35 d8\n" +
                    "BOSE,240,0.06000000,1970-01-01 00:00:00.22,0.3790,23904,9069339,2015-03-21 03:42:42.643186,84,true,null,null,null\n" +
                    "INKG,124,0.86200000,1970-01-01 00:00:00.23,0.4040,-30383,7233542,2015-07-21 16:42:47.012148,99,false,null,2015-08-27,00000000 87 fc 92 83 fc 88 f3 32 27 70 c8 01 b0 dc c9 3a\n" +
                    "00000010 5b 7e\n" +
                    "FUXC,52,0.74300000,1970-01-01 00:00:00.24,null,-14729,1042064,2015-08-21 02:10:58.949674,28,true,CPSW,,2015-08-29,null\n" +
                    "UNYQ,71,0.44200000,1970-01-01 00:00:00.25,0.5390,-22611,null,2015-12-23 18:41:42.319859,98,true,PEHN,,2015-01-26,00000000 28 ed 97 99 d8 77 33 3f b2 67 da 98 47 47 bf\n" +
                    "KBMQ,null,0.28000000,1970-01-01 00:00:00.26,null,12240,null,2015-08-16 01:02:55.766622,21,false,null,2015-05-19,00000000 6a de 46 04 d3 81 e7 a2 16 22 35 3b 1c\n" +
                    "JSOL,243,null,1970-01-01 00:00:00.27,0.0680,-17468,null,null,20,true,null,2015-06-19,00000000 3d e0 2d 04 86 e7 ca 29 98 07 69 ca 5b d6 cf 09\n" +
                    "00000010 69\n" +
                    "HNSS,150,null,1970-01-01 00:00:00.28,0.1480,14841,5992443,null,25,false,PEHN,,null,00000000 14 d6 fc ee 03 22 81 b8 06 c4 06 af\n" +
                    "PZPB,101,0.06200000,1970-01-01 00:00:00.29,null,12237,9878179,2015-09-03 22:13:18.852465,79,false,VTJW,,2015-12-17,00000000 12 61 3a 9a ad 98 2e 75 52 ad 62 87 88 45 b9 9d\n" +
                    "OYNN,25,0.33900000,1970-01-01 00:00:00.3,0.6280,22412,4736378,2015-10-10 12:19:42.528224,106,true,CPSW,,2015-07-01,00000000 54 13 3f ff b6 7e cd 04 27 66 94 89 db\n" +
                    "null,117,0.56400000,1970-01-01 00:00:00.31,null,-5604,6353018,null,84,false,null,null,00000000 2b ad 25 07 db 62 44 33 6e 00 8e\n" +
                    "HVRI,233,0.22400000,1970-01-01 00:00:00.32,0.4250,10469,1715213,null,86,false,null,2015-02-02,null\n" +
                    "OYTO,96,0.74100000,1970-01-01 00:00:00.33,0.5280,-12239,3499620,2015-02-07 22:35:03.212268,17,false,PEHN,,2015-03-29,null\n" +
                    "LFCY,63,0.72200000,1970-01-01 00:00:00.34,null,23344,9523982,null,123,false,CPSW,,2015-05-18,00000000 05 e5 c0 4e cc d6 e3 7b 34 cd 15 35 bb a4\n" +
                    "GHLX,148,0.30600000,1970-01-01 00:00:00.35,0.6360,-31457,2322337,2015-10-22 12:06:05.544701,91,true,HYRX,,2015-05-21,00000000 57 1d 91 72 30 04 b7 02 cb 03\n" +
                    "YTSZ,123,null,1970-01-01 00:00:00.36,0.5190,22534,4446236,2015-07-27 07:23:37.233711,53,false,CPSW,,2015-01-13,null\n" +
                    "SWLU,251,null,1970-01-01 00:00:00.37,0.1790,7734,4082475,2015-10-21 18:24:34.400345,69,false,PEHN,,2015-04-01,null\n" +
                    "TQJL,245,null,1970-01-01 00:00:00.38,0.8650,9516,929340,2015-05-28 04:18:18.640567,69,false,VTJW,,2015-06-12,00000000 6c 3e 51 d7 eb b1 07 71 32 1f af 40 4e 8c 47\n" +
                    "REIJ,94,null,1970-01-01 00:00:00.39,0.1300,-29924,null,2015-03-20 22:14:46.204718,113,true,HYRX,,2015-12-19,null\n" +
                    "HDHQ,94,0.72300000,1970-01-01 00:00:00.4,0.7300,19970,654131,2015-01-10 22:56:08.48045,84,true,null,2015-03-05,00000000 4f 56 6b 65 a4 53 38 e9 cd c1 a7 ee 86 75 ad a5\n" +
                    "00000010 2d 49\n" +
                    "UMEU,40,0.00800000,1970-01-01 00:00:00.41,0.8050,-11623,4599862,2015-11-20 04:02:44.335947,76,false,PEHN,,2015-05-17,null\n" +
                    "YJIH,184,null,1970-01-01 00:00:00.42,0.3830,17614,3101671,2015-01-28 12:05:46.683001,105,true,null,2015-12-07,00000000 ec 69 cd 73 bb 9b c5 95 db 61 91 ce\n" +
                    "CYXG,27,0.29200000,1970-01-01 00:00:00.43,0.9530,3944,249165,null,67,true,null,2015-03-02,00000000 01 48 15 3e 0c 7f 3f 8f e4 b5 ab 34 21 29\n" +
                    "MRTG,143,0.02600000,1970-01-01 00:00:00.44,0.9430,-27320,1667842,2015-01-24 19:56:15.973109,11,false,null,2015-01-24,null\n" +
                    "DONP,246,0.65400000,1970-01-01 00:00:00.45,0.5560,27477,4160018,2015-12-14 03:40:05.911839,20,true,PEHN,,2015-10-29,00000000 07 92 01 f5 6a a1 31 cd cb c2 a2 b4 8e 99\n" +
                    "IQXS,232,0.23100000,1970-01-01 00:00:00.46,0.0490,-18113,4005228,2015-06-11 13:00:07.248188,8,true,CPSW,,2015-08-16,00000000 fa 1f 92 24 b1 b8 67 65 08 b7 f8 41 00\n" +
                    "null,178,null,1970-01-01 00:00:00.47,0.9030,-14626,2934570,2015-04-04 08:51:54.068154,88,true,null,2015-07-01,00000000 84 36 25 63 2b 63 61 43 1c 47 7d b6 46 ba bb 98\n" +
                    "00000010 ca 08 be a4\n" +
                    "HUWZ,94,0.11000000,1970-01-01 00:00:00.48,0.4200,-3736,5687514,2015-01-02 17:18:05.627633,74,false,null,2015-03-29,null\n" +
                    "SRED,66,0.11300000,1970-01-01 00:00:00.49,0.0600,-10543,3669377,2015-10-22 02:53:02.381351,77,true,PEHN,,null,00000000 7c 3f d6 88 3a 93 ef 24 a5 e2 bc\n";

            StringSink sink = new StringSink();

            // dump metadata
            ResultSetMetaData metaData = rs.getMetaData();
            for (int i = 0, n = metaData.getColumnCount(); i < n; i++) {
                if (i > 0) {
                    sink.put(',');
                }

                sink.put(metaData.getColumnName(i + 1));
                sink.put('[').put(JDBCType.valueOf(metaData.getColumnType(i + 1)).name()).put(']');
            }
            sink.put('\n');

            while (rs.next()) {
                String stringValue = rs.getString(1);
                if (rs.wasNull()) {
                    sink.put("null");
                } else {
                    sink.put(stringValue);
                }

                sink.put(',');
                int intValue = rs.getInt(2);
                if (rs.wasNull()) {
                    sink.put("null");
                } else {
                    sink.put(intValue);
                }

                sink.put(',');
                double doubleValue = rs.getDouble(3);
                if (rs.wasNull()) {
                    sink.put("null");
                } else {
                    sink.put(doubleValue, 8);
                }

                sink.put(',');
                sink.put(rs.getTimestamp(4).toString());

                sink.put(',');
                double floatValue = rs.getFloat(5);
                if (rs.wasNull()) {
                    sink.put("null");
                } else {
                    sink.put(floatValue, 4);
                }

                sink.put(',');
                sink.put(rs.getShort(6));

                sink.put(',');
                long longValue = rs.getLong(7);
                if (rs.wasNull()) {
                    sink.put("null");
                } else {
                    sink.put(longValue);
                }

                sink.put(',');
                Timestamp timestamp = rs.getTimestamp(8);
                if (timestamp == null) {
                    sink.put("null");
                } else {
                    sink.put(timestamp.toString());
                }

                sink.put(',');
                sink.put(rs.getByte(9));

                sink.put(',');
                sink.put(rs.getBoolean(10));

                sink.put(',');
                stringValue = rs.getString(11);
                if (rs.wasNull()) {
                    sink.put("null");
                } else {
                    sink.put(stringValue).put(',');
                }

                sink.put(',');
                Date date = rs.getDate(12);
                if (date == null) {
                    sink.put("null");
                } else {
                    sink.put(date.toString());
                }

                sink.put(',');
                InputStream stream = rs.getBinaryStream(13);
                if (stream == null) {
                    sink.put("null");
                } else {
                    toSink(stream, sink);
                }

                sink.put('\n');
            }

            TestUtils.assertEquals(expected, sink);
            connection.close();
        } else {
            throw NetworkError.instance(Os.errno()).couldNotBindSocket();
        }
    }
}