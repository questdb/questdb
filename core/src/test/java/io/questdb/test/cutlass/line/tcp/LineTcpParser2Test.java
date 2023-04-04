/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.cutlass.line.LineProtoException;
import io.questdb.cutlass.line.tcp.LineTcpParser;
import io.questdb.cutlass.line.tcp.LineTcpParser.ParseResult;
import io.questdb.cutlass.line.tcp.LineTcpParser.ProtoEntity;
import io.questdb.test.cutlass.line.udp.LineUdpLexerTest;
import io.questdb.std.*;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


public class LineTcpParser2Test extends LineUdpLexerTest {
    private final LineTcpParser lineTcpParser = new LineTcpParser(false, false);
    private boolean onErrorLine;
    private long startOfLineAddr;

    @BeforeClass
    public static void init() {
        Os.init();
    }

    @Override
    public void testDanglingCommaOnTag() {
        assertThat(
                "measurement,tag=value field=\"x\" 10000\n",
                "measurement,tag=value, field=\"x\" 10000\n"
        );
    }

    @Test
    public void testDoubleScientificNotation() {
        assertThat(
                "measurement,tag=value,tag2=value field=1.123E-03,field2=9.19097E10,field3=10.097E+3 100000\n",
                "measurement,tag=value,tag2=value field=1.123E-03,field2=9.19097E10,field3=10.097E+3 100000\n"
        );
    }

    @Test
    public void testInvalidMeasurementNameDot1() {
        assertThat(
                ".measurement,tag=value,tag2=value field=10000i\n",
                ".measurement,tag=value,tag2=value field=10000i\n"
        );
    }

    @Test
    public void testInvalidMeasurementNameEnd1() {
        assertThat(
                "measurement\\\\,tag=value,tag2=value field=10000i--ERROR=INVALID_TABLE_NAME--",
                "measurement\\\\,tag=value,tag2=value field=10000i\n"
        );
    }

    @Test
    public void testInvalidMeasurementNameEnd3() {
        assertThat(
                "measurement/,tag=value,tag2=value field=10000i--ERROR=INVALID_TABLE_NAME--",
                "measurement/,tag=value,tag2=value field=10000i\n"
        );
    }

    @Test
    public void testInvalidMeasurementNameEnd4() {
        assertThat(
                "measurement\0,tag=value,tag2=value field=10000i--ERROR=INVALID_TABLE_NAME--",
                "measurement\0,tag=value,tag2=value field=10000i\n"
        );
    }

    @Test
    public void testInvalidMeasurementNameMid2() {
        assertThat(
                "mea/surement,tag=value,tag2=value field=10000i--ERROR=INVALID_TABLE_NAME--",
                "mea/surement,tag=value,tag2=value field=10000i\n"
        );
    }

    @Test
    public void testInvalidMeasurementNameMid3() {
        assertThat(
                "mea\0surement,tag=value,tag2=value field=10000i--ERROR=INVALID_TABLE_NAME--",
                "mea\0surement,tag=value,tag2=value field=10000i\n"
        );
    }

    @Test
    public void testInvalidMeasurementNameMid4() {
        assertThat(
                "mea\\\\surement,tag=value,tag2=value field=10000i--ERROR=INVALID_TABLE_NAME--",
                "mea\\\\surement,tag=value,tag2=value field=10000i\n"
        );
    }

    @Test
    public void testInvalidMeasurementNamePrefix1() {
        assertThat(
                "../measurement,tag=value,tag2=value field=10000i--ERROR=INVALID_TABLE_NAME--",
                "../measurement,tag=value,tag2=value field=10000i\n"
        );
    }

    @Test
    public void testInvalidMeasurementNamePrefix2() {
        assertThat(
                "\0measurement,tag=value,tag2=value field=10000i--ERROR=INVALID_TABLE_NAME--",
                "\0measurement,tag=value,tag2=value field=10000i\n"
        );
    }

    @Test
    public void testInvalidMeasurementNamePrefix3() {
        assertThat(
                "\\\\measurement,tag=value,tag2=value field=10000i--ERROR=INVALID_TABLE_NAME--",
                "\\\\measurement,tag=value,tag2=value field=10000i\n"
        );
    }

    @Test
    public void testMangledMeasurementNameFromBothEnds() {
        assertThat(
                "\0\0\0,tag=value,tag2=value field=10000i--ERROR=INVALID_TABLE_NAME--",
                "\0\0\0,tag=value,tag2=value field=10000i\n"
        );
    }

    @Override
    public void testNoFieldValue2() {
        assertThat(
                "measurement,tag=x f= 10000\n",
                "measurement,tag=x f= 10000\n"
        );
    }

    @Override
    public void testNoFieldValue3() {
        assertThat(
                "measurement,tag=x f= 10000\n",
                "measurement,tag=x f=, 10000\n"
        );
    }

    @Test
    public void testNoFields() {
        // Single space char between last tag and timestamp
        assertThat(
                "measurement,tag=x 10000\n",
                "measurement,tag=x 10000\n"
        );

        // Single space char after last tag and invalid timestamp
        assertThat(
                "measurement,tag=x 10000i--ERROR=INVALID_TIMESTAMP--", "measurement,tag=x 10000i\n"
        );

        // Double space char between last tag and timestamp
        assertThat(
                "measurement,tag=x 10000\n",
                "measurement,tag=x  10000\n"
        );

        // Double space char between last tag and invalid timestamp
        assertThat(
                "measurement,tag=x  10000i--ERROR=INVALID_TIMESTAMP--", "measurement,tag=x  10000i\n"
        );

    }

    @Test
    public void testNoFieldsAndNotTags() {
        assertThat("measurement 10000--ERROR=INCOMPLETE_FIELD--", "measurement 10000\n"); // One space char
        assertThat("measurement  10000--ERROR=NO_FIELDS--", "measurement  10000\n"); // Two space chars
    }

    @Override
    public void testNoTagValue1() {
        assertThat(
                "measurement,tag= field=\"x\" 10000\n",
                "measurement,tag= field=\"x\" 10000\n"
        );
    }

    @Override
    public void testNoTagValue2() {
        assertThat(
                "measurement,tag= field=\"x\" 10000\n",
                "measurement,tag=, field=\"x\" 10000\n"
        );
    }

    @Override
    public void testNoTagValue3() {
        assertThat(
                "measurement,tag=\n",
                "measurement,tag="
        );
    }

    @Override
    public void testNoTagValue4() {
        assertThat(
                "measurement,tag=\n",
                "measurement,tag=\n"
        );
    }

    @Test
    public void testNoTimestamp() {
        assertThat("measurement,a=10 v=11\n", "measurement,a=10 v=11\n"); // No trailing space
    }

    @Test
    public void testNonAscii() {
        assertThat(
                "weather1 terület=\"europeI\",temperature=80.0,humidity=24.0,hőmérséklet=18.0,notes=5072.0,ветер=63.0 1465839830102351000--non ascii--\n",
                "weather1 terület=\"europeI\",temperature=80.0,humidity=24.0,hőmérséklet=18.0,notes=5072.0,ветер=63.0 1465839830102351000\n"
        );
    }

    @Override
    public void testSkipLine() {
        assertThat(
                "measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n" +
                        "measurement,tag=value3,tag2=value2 field=,field2=\"ok\"\n" +
                        "measurement,tag=value4,tag2=value4 field=200i,field2=\"super\"\n",
                "measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n" +
                        "measurement,tag=value3,tag2=value2 field=,field2=\"ok\"\n" +
                        "measurement,tag=value4,tag2=value4 field=200i,field2=\"super\"\n"
        );
    }

    @Test
    public void testSpaceInMeasurementName() {
        assertThat(
                "tab ble,tag= 1 field=2 123\n",
                "tab\\ ble,tag=\\ 1 field=2 123\n"
        );
    }

    @Test
    public void testSupportsUtf8Chars() {
        assertThat(
                "लаблअца,символ=значение1 поле=\"значение2\",поле2=\"значение3\" 123--non ascii--\n",
                "लаблअца,символ=значение1 поле=\"значение2\",поле2=\"значение3\" 123\n"
        );

        assertThat(
                "लаблअца,символ=значение2 161--non ascii--\n",
                "लаблअца,символ=значение2  161\n"
        );


        assertThat(
                "table,tag=ok field=\"значение2 non ascii quoted\" 161--non ascii--\n",
                "table,tag=ok field=\"значение2 non ascii quoted\" 161\n"
        );
    }

    @Test
    public void testTrailingSpace() {
        assertThat("measurement,a=10\n", "measurement,a=10 \n"); // Trailing space
    }

    @Test
    public void testTrailingSpace2() {
        assertThat("measurement,a=10 v=11\n", "measurement,a=10 v=11 \n"); // Trailing space after fields
    }

    @Test
    public void testTrailingSpace3() {
        assertThat("measurement v=11\n", "measurement v=11 \n"); // Trailing space
    }

    @Test
    public void testValidMeasurementNameDot2() {
        assertThat(
                "meas.urement,tag=value,tag2=value field=10000i\n",
                "meas.urement,tag=value,tag2=value field=10000i\n"
        );
    }

    @Test
    public void testValidMeasurementNameDot3() {
        assertThat(
                "measurement.,tag=value,tag2=value field=10000i\n",
                "measurement.,tag=value,tag2=value field=10000i\n"
        );
    }

    @Test
    public void testValidMeasurementNameEnd2() {
        assertThat(
                "measurement..,tag=value,tag2=value field=10000i\n",
                "measurement..,tag=value,tag2=value field=10000i\n"
        );
    }

    @Test
    public void testValidMeasurementNameMid1() {
        assertThat(
                "mea..surement,tag=value,tag2=value field=10000i\n",
                "mea..surement,tag=value,tag2=value field=10000i\n"
        );
    }

    @Test
    public void testValidMeasurementNamePrefix4() {
        assertThat(
                "..measurement,tag=value,tag2=value field=10000i\n",
                "..measurement,tag=value,tag2=value field=10000i\n"
        );
    }

    @Test
    public void testWithEscapedKeys() {
        assertThat(
                "measurement,t ag=value with space,tag2=value field=10000i,field 2=\"str=special,end\" 100000\n",
                "measurement,t\\ ag=value\\ with\\ space,tag2=value field=10000i,field\\ 2=\"str=special,end\" 100000\n"
        );

        assertThat(
                "measurement,t\"ag=value with space,tag2=value field=10000i,field 2=\"str=special,end\" 100000\n",
                "measurement,t\\\"ag=value\\ with\\ space,tag2=value field=10000i,field\\ 2=\"str=special,end\" 100000\n"
        );
    }

    @Test
    public void testWithEscapedTagValues() {
        assertThat(
                "measurement,tag=value with space,tag2=value field=10000i,field2=\"str=special,end\" 100000\n",
                "measurement,tag=value\\ with\\ space,tag2=value field=10000i,field2=\"str=special,end\" 100000\n"
        );

        assertThat(
                "measurement,tag=value\\with\\slash,tag2=value field=10000i,field2=\"str=special,end\\ \" 100000\n",
                "measurement,tag=value\\\\with\\\\slash,tag2=value field=10000i,field2=\"str=special,end\\\\ \" 100000\n"
        );
    }

    @Test
    public void testWithQuotedStringsWithEscapedQuotes() {
        assertThat(
                "measurement,tag=value,tag2=value field=10000i,field2=\"str\" escaped\\ end\" 100000\n",
                "measurement,tag=value,tag2=value field=10000i,field2=\"str\\\" escaped\\\\ end\" 100000\n"
        );

        assertThat(
                "measurement field2=\"double escaped \\ \" and quoted\" 100000\n",
                "measurement field2=\"double escaped \\\\ \\\" and quoted\" 100000\n"
        );

        assertThat(
                "measurement field2=\"double escaped \\\" and quoted2\" 100000\n",
                "measurement field2=\"double escaped \\\\\\\" and quoted2\" 100000\n"
        );

        assertThat(
                "measurement,tag=value,tag2=value field=10000i,field2=\"str=special,end\" 100000\n",
                "measurement,tag=value,tag2=value field=10000i,field2=\"str=special,end\" 100000\n"
        );

        assertThat(
                "measurement,tag=value,tag2=value field=10000i,field2=\"str=special,end\",field3=34 100000\n",
                "measurement,tag=value,tag2=value field=10000i,field2=\"str=special,end\",field3=34 100000\n"
        );
    }

    @Test
    public void testWithQuotedStringsWithEscapedQuotesUnsuccessful() {
        assertThat(
                "measurement,tag=value,tag2=value field=10000i,field2=\"str=special,lineend--ERROR=INVALID_FIELD_VALUE--",
                "measurement,tag=value,tag2=value field=10000i,field2=\"str=special,lineend\n"
        );
    }

    @Test
    public void testWithQuotedStringsWithSpaces() {
        assertThat(
                "measurement,tag=value,tag2=value field=10000i,field2=\"longstring\",fld3=\"short string\" 100000\n",
                "measurement,tag=value,tag2=value field=10000i,field2=\"longstring\",fld3=\"short string\" 100000\n"
        );
    }

    @Test
    public void testWithQuotedStringsWithSpaces2() {
        // Good test, but runs 5s to check all combinations because message is quite long
//        assertThat(
//                "md_msgs ts_nsec=1634886503004129476i,pkt_size=1111i,pcap_file=\"_______________________________________________________\",pcap_msg=1111111i,raw_msg=\"__\"________\"___,\"_______\"___,\"______\"________,\"__________\"__\"___________\"____,\"_____\"_____,\"_________\"___,\"______\"________,\",Length=11i,MsgSeqNum=111111i,MsgType=11i,src_ip=\"______________\",dst_ip=\"_____________\",src_port=11111i,dst_port=11111i,first_dir=T 1634886503004129476\n" +
//                        "md_msgs ts_nsec=1634886503004129476i,pkt_size=1111i,pcap_file=\"_______________________________________________________\",pcap_msg=1111111i,raw_msg=\"__\"________\"___,\"_______\"___,\"______\"________,\"__________\"__\"___________\"____,\"_____\"_____,\"_________\"___,\"______\"________,\"________\"________\",Length=11i,MsgSeqNum=111111i,MsgType=11i,src_ip=\"______________\",dst_ip=\"_____________\",src_port=11111i,dst_port=11111i,first_dir=T 1634886503004129476\n",
//                "md_msgs ts_nsec=1634886503004129476i,pkt_size=1111i,pcap_file=\"_______________________________________________________\",pcap_msg=1111111i," +
//                        "raw_msg=\"__\\\"________\\\"___,\\\"_______\\\"___,\\\"______\\\"________,\\\"__________\\\"__\\\"___________\\\"____,\\\"_____\\\"_____,\\\"_________\\\"___,\\\"______\\\"________,\"," +
//                        "Length=11i,MsgSeqNum=111111i,MsgType=11i,src_ip=\"______________\",dst_ip=\"_____________\",src_port=11111i,dst_port=11111i,first_dir=T 1634886503004129476\r\n" +
//                        "md_msgs ts_nsec=1634886503004129476i,pkt_size=1111i,pcap_file=\"_______________________________________________________\"," +
//                        "pcap_msg=1111111i,raw_msg=\"__\\\"________\\\"___,\\\"_______\\\"___,\\\"______\\\"________,\\\"__________\\\"__\\\"___________\\\"____,\\\"_____\\\"_____,\\\"_________\\\"___,\\\"______\\\"________,\\\"________\\\"________\"," +
//                        "Length=11i,MsgSeqNum=111111i,MsgType=11i,src_ip=\"______________\",dst_ip=\"_____________\",src_port=11111i,dst_port=11111i,first_dir=T 1634886503004129476\r"
//        );

        // Shorter version
        assertThat(
                "md_msgs ts_nsec=1634886503004129476i,pcap_msg=1111111i,raw_msg=\"__\"____\"___,\"_______\"___,\"___\"________,\",Length=11i,MsgSeqNum=111111i,MsgType=11i,first_dir=T 1634886503004129476\n" +
                        "md_msgs ts_nsec=1634886503004129476i,pkt_size=1111i,pcap_file=\"_______________________________________________________\",raw_msg=\"__\"___________,\"________\"________\",Length=11i,first_dir=T 1634886503004129476\n",
                "md_msgs ts_nsec=1634886503004129476i,pcap_msg=1111111i,raw_msg=\"__\\\"____\\\"___,\\\"_______\\\"___,\\\"___\\\"________,\",Length=11i,MsgSeqNum=111111i,MsgType=11i,first_dir=T 1634886503004129476\r\n" +
                        "md_msgs ts_nsec=1634886503004129476i,pkt_size=1111i,pcap_file=\"_______________________________________________________\",raw_msg=\"__\\\"___________,\\\"________\\\"________\",Length=11i,first_dir=T 1634886503004129476\r"
        );
    }

    private void assembleLine() {
        int nEntities = lineTcpParser.getEntityCount();
        Chars.utf8toUtf16(lineTcpParser.getMeasurementName().getLo(), lineTcpParser.getMeasurementName().getHi(), sink);
        int n = 0;
        boolean tagsComplete = false;
        while (n < nEntities) {
            ProtoEntity entity = lineTcpParser.getEntity(n++);
            if (!tagsComplete && entity.getType() != LineTcpParser.ENTITY_TYPE_TAG) {
                tagsComplete = true;
                sink.put(' ');
            } else {
                sink.put(',');
            }
            Chars.utf8toUtf16(entity.getName().getLo(), entity.getName().getHi(), sink);
            sink.put('=');
            switch (entity.getType()) {
                case LineTcpParser.ENTITY_TYPE_STRING:
                    sink.put('"');
                    Chars.utf8toUtf16(entity.getValue().getLo(), entity.getValue().getHi(), sink);
                    sink.put('"');
                    break;
                case LineTcpParser.ENTITY_TYPE_INTEGER:
                case LineTcpParser.ENTITY_TYPE_LONG256:
                    sink.put(entity.getValue()).put('i');
                    break;
                default:
                    Chars.utf8toUtf16(entity.getValue().getLo(), entity.getValue().getHi(), sink);
                    break;
            }
        }

        if (lineTcpParser.hasTimestamp()) {
            sink.put(' ');
            Numbers.append(sink, lineTcpParser.getTimestamp());
        }

        if (lineTcpParser.hasNonAsciiChars()) {
            sink.put("--non ascii--");
        }
        sink.put('\n');
    }

    private boolean parseMeasurement(long bufHi) {
        while (lineTcpParser.getBufferAddress() < bufHi) {
            ParseResult rc;
            if (!onErrorLine) {
                rc = lineTcpParser.parseMeasurement(bufHi);
            } else {
                rc = lineTcpParser.skipMeasurement(bufHi);
            }
            switch (rc) {
                case MEASUREMENT_COMPLETE:
                    if (!onErrorLine) {
                        assembleLine();
                    } else {
                        final StringSink tmpSink = new StringSink();
                        if (Chars.utf8toUtf16(startOfLineAddr, lineTcpParser.getBufferAddress(), tmpSink)) {
                            sink.put(tmpSink.toString());
                        }
                        sink.put("--ERROR=");
                        sink.put(lineTcpParser.getErrorCode().toString());
                        sink.put("--");
                        onErrorLine = false;
                    }
                    startOfLineAddr = lineTcpParser.getBufferAddress() + 1;
                    lineTcpParser.startNextMeasurement();
                    break;
                case BUFFER_UNDERFLOW:
                    return false;
                case ERROR:
                    Assert.assertFalse(onErrorLine);
                    onErrorLine = true;
                    break;
            }
        }
        return true;
    }

    private boolean parseMeasurement(long fullBuffer, long parseBuffer, long buffersLen, long parseLen, long prevParseLen) {
        long shl = parseLen - prevParseLen;

        // This will copy ILP data from fullBuffer to parseBuffer so that the data ends at the end of the buffer
        long parseHi = parseBuffer + buffersLen;
        Vect.memmove(parseHi - parseLen, parseHi - prevParseLen, prevParseLen);
        Vect.memcpy(parseHi - shl, fullBuffer + prevParseLen, shl);

        // bufHi always the same, data always ends at the end of the buffer
        // the only difference from iteration to iteration is where the data starts, which is set in shl
        lineTcpParser.shl(shl);
        startOfLineAddr -= shl;
        return parseMeasurement(parseHi);
    }

    private void resetParser(long mem) {
        onErrorLine = false;
        startOfLineAddr = mem;
        lineTcpParser.of(mem);
    }

    protected void assertThat(CharSequence expected, String lineStr) throws LineProtoException {
        assertThat(expected, lineStr, 1);
    }

    protected void assertThat(CharSequence expected, String lineStr, int start) throws LineProtoException {
        byte[] line = lineStr.getBytes(Files.UTF_8);
        final int len = line.length;
        final boolean endWithEOL = line[len - 1] == '\n' || line[len - 1] == '\r';
        int fullLen = endWithEOL ? line.length : line.length + 1;
        long memFull = Unsafe.malloc(fullLen, MemoryTag.NATIVE_DEFAULT);
        long mem = Unsafe.malloc(fullLen, MemoryTag.NATIVE_DEFAULT);
        for (int j = 0; j < len; j++) {
            Unsafe.getUnsafe().putByte(memFull + j, line[j]);
        }
        if (!endWithEOL) {
            Unsafe.getUnsafe().putByte(memFull + len, (byte) '\n');
        }

        try {
            for (int i = start; i < len; i++) {
                for (int nextBreak = 0; nextBreak < len - i; nextBreak++) {
                    sink.clear();
                    resetParser(mem + fullLen);
                    parseMeasurement(memFull, mem, fullLen, i, 0);
                    if (nextBreak > 0) {
                        parseMeasurement(memFull, mem, fullLen, i + nextBreak, i);
                    }
                    boolean complete;
                    complete = parseMeasurement(memFull, mem, fullLen, fullLen, i + nextBreak);
                    if (!complete || !Chars.equals(expected, sink)) {
                        System.out.println(lineStr.substring(0, i));
                        if (nextBreak > 0) {
                            System.out.println(lineStr.substring(i, i + nextBreak));
                        }
                        System.out.println(lineStr.substring(i + nextBreak));
                        TestUtils.assertEquals("parse split " + i, expected, sink);
                    }
                    Assert.assertTrue(complete);
                }
            }
        } finally {
            Unsafe.free(mem, fullLen, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(memFull, fullLen, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
