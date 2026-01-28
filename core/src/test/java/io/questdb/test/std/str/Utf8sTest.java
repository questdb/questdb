/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.std.str;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.std.BitSet;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.GcUtf8String;
import io.questdb.std.str.MutableUtf8Sink;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf16Sink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static io.questdb.test.AbstractCairoTest.utf8;

public class Utf8sTest {

    @Test
    public void tesConvertInvalidUTF8UTF16Exception() {
        byte[] data = new byte[]{
                79, 112, 116, 46, 80, 118, 80, 110, 108, 32, 115, 116, 97, 114, 116, 95, 116, 105, 109, 101,
                61, 49, 55, 53, 55, 53, 54, 56, 54, 48, 48, 48, 48, 48, 48, 48, 48, 116, 44, 100,
                117, 114, 97, 116, 105, 111, 110, 95, 109, 115, 61, 61, 16, 0, 0, 0, 0, 64, 119,
                43, 65, 44, 112, 110, 108, 95, 105, 100, 61, 34, 105, 110, 118, 101, 115, 116, 109, 101, 110,
                116, 115, 45, 101, 118, 97, 124, 99, 97, 108, 97, 100, 97, 110, 95, 116, 105, 97, 95, 117,
                115, 100, 116, 95, 99, 95, 51, 46, 56, 53, 95, 50, 48, 50, 53, 48, 57, 48, 56, 34,
                44, 108, 101, 103, 95, 105, 100, 120, 61, 61, 16, 0, 0, 0, 0, 0, (byte) 132, (byte) 146, 64, 44,
                98, 97, 115, 101, 95, 105, 100, 120, 61, 61, 16, 0, 0, 0, 0, 0, 0, 36, 64, 44,
                112, 97, 114, 101, 110, 116, 95, 105, 100, 61, 61, 16, 0, 0, 0, 0, 0, 0, (byte) 240, (byte) 191,
                44, 116, 121, 112, 101, 61, 61, 16, 0, 0, 0, 0, 0, 0, (byte) 240, 63, 44, 110, 116, 108,
                61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 97, 118, 103, 95, 117, 115, 100, 61,
                61, 16, 0, 0, 0, 0, 0, 0, (byte) 248, 127, 44, 109, 97, 114, 107, 95, 117, 115, 100, 61,
                61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 109, 111, 100, 101, 108, 95, 117, 115, 100,
                61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 112, 118, 95, 117, 115, 100, 61, 61,
                16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 112, 118, 95, 109, 97, 114, 107, 61, 61, 16,
                0, 0, 0, 0, 0, 0, 0, 0, 44, 117, 110, 114, 101, 97, 108, 95, 117, 115, 100, 61,
                61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 117, 110, 114, 101, 97, 108, 95, 109, 97,
                114, 107, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 105, 118, 61, 61, 16, 0,
                0, 0, 0, 0, 0, 0, 0, 44, 102, 61, 61, 16, 83, 89, (byte) 235, (byte) 246, (byte) 131, 52, (byte) 252, 63,
                44, 114, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 97, 116, 109, 61, 61, 16,
                20, 82, (byte) 235, (byte) 135, 55, 88, (byte) 228, 63, 44, 116, 116, 109, 61, 61, 16, 11, 84, (byte) 241, 120, 45,
                3, 97, 62, 44, 116, 104, 101, 116, 97, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0,
                44, 100, 101, 108, 116, 97, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 100, 101,
                108, 116, 97, 95, 116, 49, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 103, 97,
                109, 109, 97, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 114, 104, 111, 61, 61,
                16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 101, 112, 115, 61, 61, 16, 0, 0, 0, 0,
                0, 0, 0, 0, 44, 118, 101, 103, 97, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0,
                44, 118, 97, 110, 110, 97, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 118, 111,
                108, 103, 97, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 97, 116, 109, 103, 97,
                61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 119, 118, 101, 103, 97, 61, 61, 16,
                0, 0, 0, 0, 0, 0, 0, 0, 44, 119, 118, 111, 108, 103, 97, 61, 61, 16, 0, 0,
                0, 0, 0, 0, 0, 0, 44, 119, 118, 97, 110, 110, 97, 61, 61, 16, 0, 0, 0, 0,
                0, 0, 0, 0, 44, 119, 97, 116, 109, 103, 97, 61, 61, 16, 0, 0, 0, 0, 0, 0,
                0, 0, 44, 116, 104, 101, 116, 97, 95, 112, 110, 108, 61, 61, 16, 0, 0, 0, 0, 0,
                0, 0, 0, 44, 100, 101, 108, 116, 97, 95, 112, 110, 108, 61, 61, 16, 0, 0, 0, 0,
                0, 0, 0, 0, 44, 97, 116, 109, 95, 112, 110, 108, 61, 61, 16, 0, 0, 0, 0, 0,
                0, 0, 0, 44, 114, 101, 103, 97, 95, 112, 110, 108, 61, 61, 16, 0, 0, 0, 0, 0,
                0, 0, 0, 44, 98, 117, 102, 103, 97, 95, 112, 110, 108, 61, 61, 16, 0, 0, 0, 0,
                0, 0, 0, 0, 44, 102, 119, 100, 114, 95, 112, 110, 108, 61, 61, 16, 0, 0, 0, 0,
                0, 0, 0, 0, 44, 102, 117, 110, 100, 95, 112, 110, 108, 61, 61, 16, 0, 0, 0, 0,
                0, 0, 0, 0, 44, 118, 111, 108, 95, 112, 110, 108, 61, 61, 16, 0, 0, 0, 0, 0,
                0, 0, 0, 44, 116, 114, 100, 95, 112, 110, 108, 61, 61, 16, 0, 0, 0, 0, 0, 0,
                0, 0, 44, 109, 97, 114, 107, 95, 100, 105, 102, 102, 61, 61, 16, 0, 0, 0, 0, 0,
                0, 0, 0, 44, 97, 99, 99, 111, 117, 110, 116, 61, 34, 105, 110, 118, 101, 115, 116, 109,
                101, 110, 116, 115, 45, 101, 118, 97, 34, 44, 105, 110, 115, 116, 114, 117, 109, 101, 110, 116,
                61, 34, 99, 97, 108, 97, 100, 97, 110, 95, 116, 105, 97, 95, 117, 115, 100, 116, 95, 99,
                95, 51, 46, 56, 53, 95, 50, 48, 50, 53, 48, 57, 48, 56, 34, 44, 101, 120, 112, 61,
                34, 50, 48, 50, 53, 48, 57, 48, 56, 34, 44, 98, 117, 99, 107, 101, 116, 61, 34, 50,
                48, 50, 53, 48, 57, 48, 56, 34, 44, 98, 97, 115, 101, 61, 34, 116, 105, 97, 34, 44,
                112, 111, 114, 116, 102, 111, 108, 105, 111, 61, 34, 105, 110, 118, 101, 115, 116, 109, 101, 110,
                116, 115, 45, 101, 118, 97, 34, 44, 115, 116, 97, 114, 116, 95, 100, 105, 102, 102, 61, 61,
                16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 115, 116, 97, 114, 116, 95, 112, 118, 61, 61,
                16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 116, 114, 97, 99, 107, 105, 110, 103, 95, 100,
                105, 102, 102, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 116, 114, 97, 99, 107,
                101, 100, 95, 112, 110, 108, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 112, 118,
                95, 100, 105, 102, 102, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 114, 101, 115,
                105, 100, 117, 97, 108, 115, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 114, 101,
                103, 97, 50, 53, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 114, 101, 103, 97,
                49, 48, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 98, 117, 102, 103, 97, 50,
                53, 61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 98, 117, 102, 103, 97, 49, 48,
                61, 61, 16, 0, 0, 0, 0, 0, 0, 0, 0, 44, 107, 101, 121, 61, 34, 105, 110, 118,
                101, 115, 116, 109, 101, 110, 116, 115, 45, 101, 118, 97, 124, 99, 97, 108, 97, 100, 97, 110,
                95, 116, 105, 97, 95, 117, 115, 100, 116, 95, 99, 95, 51, 46, 56, 53, 95, 50, 48, 50,
                53, 48, 57, 48, 56, 34, 32, 49, 55, 53, 55, 53, 54, 56, 54, 48, 48, 48, 48, 48,
                48, 48, 48, 48, 48, 48, 10
        };
        final int len = data.length;
        long mem = Unsafe.malloc(data.length, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < data.length; i++) {
            Unsafe.getUnsafe().putByte(mem + i, data[i]);
        }
        try {
            Utf8s.stringFromUtf8Bytes(mem, mem + len);
            Assert.fail();
        } catch (CairoException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), "cannot convert invalid UTF-8 sequence " +
                    "to UTF-16 [seq=Opt.PvPnl start_time=1757568600000000t,duration_ms==\\x10\\x00\\x00\\x00\\x00@w+A," +
                    "pnl_id=\"investments-eva|caladan_tia_usdt_c_3.85_20250908\",leg_idx==\\x10\\x00\\x00\\x00\\x00\\x00\\x84\\x92@," +
                    "base_idx==\\x10\\x00\\x00\\x00\\x00\\x00\\x00$@,parent_id==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\xF0\\xBF," +
                    "type==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\xF0?,ntl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "avg_usd==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\xF8\\x7F,mark_usd==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "model_usd==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,pv_usd==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "pv_mark==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,unreal_usd==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "unreal_mark==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,iv==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "f==\\x10SY\\xEB\\xF6\\x834\\xFC?,r==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,atm==\\x10\\x14R\\xEB\\x877X\\xE4?," +
                    "ttm==\\x10\\x0BT\\xF1x-\\x03a>,theta==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,delta==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "delta_t1==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,gamma==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "rho==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,eps==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "vega==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,vanna==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "volga==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,atmga==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "wvega==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,wvolga==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "wvanna==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,watmga==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "theta_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,delta_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "atm_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,rega_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "bufga_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,fwdr_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "fund_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,vol_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "trd_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,mark_diff==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "account=\"investments-eva\",instrument=\"caladan_tia_usdt_c_3.85_20250908\",exp=\"20250908\",bucket=\"20250908\"," +
                    "base=\"tia\",portfolio=\"investments-eva\",start_diff==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "start_pv==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,tracking_diff==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "tracked_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,pv_diff==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "residuals==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,rega25==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "rega10==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,bufga25==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                    "bufga10==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,key=\"investments-eva|caladan_tia_usdt_c_3.85_20250908\" 1757568600000000000\\x0A]");

            try {
                DirectUtf8String sequence = new DirectUtf8String().of(mem, mem + len);
                Utf8s.stringFromUtf8Bytes(sequence);
                Assert.fail();
            } catch (CairoException ex1) {
                TestUtils.assertContains(ex1.getFlyweightMessage(), "cannot convert invalid UTF-8 sequence " +
                        "to UTF-16 [seq=Opt.PvPnl start_time=1757568600000000t,duration_ms==\\x10\\x00\\x00\\x00\\x00@w+A," +
                        "pnl_id=\"investments-eva|caladan_tia_usdt_c_3.85_20250908\",leg_idx==\\x10\\x00\\x00\\x00\\x00\\x00\\x84\\x92@," +
                        "base_idx==\\x10\\x00\\x00\\x00\\x00\\x00\\x00$@,parent_id==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\xF0\\xBF," +
                        "type==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\xF0?,ntl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "avg_usd==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\xF8\\x7F,mark_usd==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "model_usd==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,pv_usd==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "pv_mark==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,unreal_usd==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "unreal_mark==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,iv==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "f==\\x10SY\\xEB\\xF6\\x834\\xFC?,r==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,atm==\\x10\\x14R\\xEB\\x877X\\xE4?," +
                        "ttm==\\x10\\x0BT\\xF1x-\\x03a>,theta==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,delta==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "delta_t1==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,gamma==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "rho==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,eps==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "vega==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,vanna==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "volga==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,atmga==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "wvega==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,wvolga==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "wvanna==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,watmga==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "theta_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,delta_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "atm_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,rega_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "bufga_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,fwdr_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "fund_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,vol_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "trd_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,mark_diff==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "account=\"investments-eva\",instrument=\"caladan_tia_usdt_c_3.85_20250908\",exp=\"20250908\",bucket=\"20250908\"," +
                        "base=\"tia\",portfolio=\"investments-eva\",start_diff==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "start_pv==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,tracking_diff==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "tracked_pnl==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,pv_diff==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "residuals==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,rega25==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "rega10==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,bufga25==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00," +
                        "bufga10==\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00,key=\"investments-eva|caladan_tia_usdt_c_3.85_20250908\" 1757568600000000000\\x0A]");
            }
        } finally {
            Unsafe.free(mem, data.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testCompare() {
        // Null handling
        Assert.assertEquals(0, Utf8s.compare(null, null));
        Assert.assertTrue(Utf8s.compare(null, utf8("a")) < 0);
        Assert.assertTrue(Utf8s.compare(utf8("a"), null) > 0);

        // Identity
        Utf8Sequence s = utf8("test");
        Assert.assertEquals(0, Utf8s.compare(s, s));

        // Empty strings
        Assert.assertEquals(0, Utf8s.compare(utf8(""), utf8("")));
        Assert.assertTrue(Utf8s.compare(utf8(""), utf8("a")) < 0);
        Assert.assertTrue(Utf8s.compare(utf8("a"), utf8("")) > 0);

        // Short strings (< 6 bytes) - fully handled by prefix comparison
        Assert.assertEquals(0, Utf8s.compare(utf8("abc"), utf8("abc")));
        Assert.assertTrue(Utf8s.compare(utf8("abc"), utf8("abd")) < 0);
        Assert.assertTrue(Utf8s.compare(utf8("abd"), utf8("abc")) > 0);
        Assert.assertTrue(Utf8s.compare(utf8("ab"), utf8("abc")) < 0);
        Assert.assertTrue(Utf8s.compare(utf8("abc"), utf8("ab")) > 0);

        // Strings exactly 6 bytes (VARCHAR_INLINED_PREFIX_BYTES)
        Assert.assertEquals(0, Utf8s.compare(utf8("abcdef"), utf8("abcdef")));
        Assert.assertTrue(Utf8s.compare(utf8("abcdef"), utf8("abcdeg")) < 0);
        Assert.assertTrue(Utf8s.compare(utf8("abcdeg"), utf8("abcdef")) > 0);

        // Long strings (> 6 bytes) with different prefixes - no data vector access needed
        Assert.assertTrue(Utf8s.compare(utf8("abcdefghij"), utf8("abcdegghij")) < 0);
        Assert.assertTrue(Utf8s.compare(utf8("abcdegghij"), utf8("abcdefghij")) > 0);

        // Long strings (> 6 bytes) with same prefix but different suffixes - requires data vector access
        Assert.assertEquals(0, Utf8s.compare(utf8("abcdefghij"), utf8("abcdefghij")));
        Assert.assertTrue(Utf8s.compare(utf8("abcdefghij"), utf8("abcdefghik")) < 0);
        Assert.assertTrue(Utf8s.compare(utf8("abcdefghik"), utf8("abcdefghij")) > 0);

        // Different lengths with same prefix (> 6 bytes)
        Assert.assertTrue(Utf8s.compare(utf8("abcdefgh"), utf8("abcdefghi")) < 0);
        Assert.assertTrue(Utf8s.compare(utf8("abcdefghi"), utf8("abcdefgh")) > 0);

        // Difference at byte position 6 (first byte in data vector for long strings)
        Assert.assertTrue(Utf8s.compare(utf8("abcdefXhij"), utf8("abcdefYhij")) < 0);
        Assert.assertTrue(Utf8s.compare(utf8("abcdefYhij"), utf8("abcdefXhij")) > 0);

        // Unicode characters (multi-byte UTF-8)
        Assert.assertEquals(0, Utf8s.compare(utf8("привет"), utf8("привет")));
        Assert.assertTrue(Utf8s.compare(utf8("привет"), utf8("приве|")) != 0);

        // Mixed ASCII and Unicode
        Assert.assertEquals(0, Utf8s.compare(utf8("hello мир"), utf8("hello мир")));
        Assert.assertTrue(Utf8s.compare(utf8("hello мир"), utf8("hello мис")) != 0);

        // Unsigned byte comparison (bytes > 127 should be treated as positive)
        Assert.assertTrue(Utf8s.compare(utf8("a"), utf8("ÿ")) < 0); // 'a' (0x61) < 0xFF

        // Long strings (14+ bytes) - exercises the longAt loop (bytes 6-13)
        // "abcdef" (6) + "ghijklmn" (8) = 14 bytes
        Assert.assertEquals(0, Utf8s.compare(utf8("abcdefghijklmn"), utf8("abcdefghijklmn")));
        Assert.assertTrue(Utf8s.compare(utf8("abcdefghijklmn"), utf8("abcdefghijklmo")) < 0);
        Assert.assertTrue(Utf8s.compare(utf8("abcdefghijklmo"), utf8("abcdefghijklmn")) > 0);
        // Difference at byte 6 (first byte of first long)
        Assert.assertTrue(Utf8s.compare(utf8("abcdefXhijklmn"), utf8("abcdefYhijklmn")) < 0);
        // Difference at byte 13 (last byte of first long)
        Assert.assertTrue(Utf8s.compare(utf8("abcdefghijklmX"), utf8("abcdefghijklmY")) < 0);

        // Very long strings (22+ bytes) - exercises multiple longAt iterations
        // "abcdef" (6) + "ghijklmn" (8) + "opqrstuv" (8) = 22 bytes
        Assert.assertEquals(0, Utf8s.compare(utf8("abcdefghijklmnopqrstuv"), utf8("abcdefghijklmnopqrstuv")));
        Assert.assertTrue(Utf8s.compare(utf8("abcdefghijklmnopqrstuv"), utf8("abcdefghijklmnopqrstuw")) < 0);
        // Difference in second long (byte 14)
        Assert.assertTrue(Utf8s.compare(utf8("abcdefghijklmnXpqrstuv"), utf8("abcdefghijklmnYpqrstuv")) < 0);

        // Strings where longs match but trailing bytes differ (exercises byte loop after long loop)
        // 17 bytes = 6 prefix + 8 long + 3 trailing bytes
        Assert.assertEquals(0, Utf8s.compare(utf8("abcdefghijklmnopq"), utf8("abcdefghijklmnopq")));
        Assert.assertTrue(Utf8s.compare(utf8("abcdefghijklmnopq"), utf8("abcdefghijklmnopr")) < 0);
        Assert.assertTrue(Utf8s.compare(utf8("abcdefghijklmnopr"), utf8("abcdefghijklmnopq")) > 0);
    }

    @Test
    public void testCompareFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);
        final int n = 1_000;
        final int maxLen = 25;
        Utf8StringSink[] utf8Sinks = new Utf8StringSink[n];
        String[] strings = new String[n];
        for (int i = 0; i < n; i++) {
            int len = rnd.nextPositiveInt() % maxLen;
            rnd.nextUtf8Str(len, utf8Sinks[i] = new Utf8StringSink());
            strings[i] = utf8Sinks[i].toString();
        }

        // custom comparator to sort strings by codepoint values
        Arrays.sort(strings, (l, r) -> {
            int len = Math.min(l.length(), r.length());
            for (int i = 0; i < len; i++) {
                int lCodepoint = l.codePointAt(i);
                int rCodepoint = r.codePointAt(i);
                int diff = lCodepoint - rCodepoint;
                if (diff != 0) {
                    return diff;
                }
            }
            return l.length() - r.length();
        });
        Arrays.sort(utf8Sinks, Utf8s::compare);

        for (int i = 0; i < n; i++) {
            Assert.assertEquals("error at iteration " + i, strings[i], utf8Sinks[i].toString());
        }
    }

    @Test
    public void testContains() {
        Assert.assertTrue(Utf8s.contains(utf8("аз съм грут"), utf8("грут")));
        Assert.assertTrue(Utf8s.contains(utf8("foo bar baz"), utf8("bar")));
        Assert.assertFalse(Utf8s.contains(utf8("foo bar baz"), utf8("buz")));
        Assert.assertTrue(Utf8s.contains(utf8("foo bar baz"), Utf8String.EMPTY));
        Assert.assertFalse(Utf8s.contains(Utf8String.EMPTY, utf8("foobar")));
    }

    @Test
    public void testContainsAscii() {
        Assert.assertTrue(Utf8s.containsAscii(utf8("foo bar baz"), "bar"));
        Assert.assertFalse(Utf8s.containsAscii(utf8("foo bar baz"), "buz"));
        Assert.assertTrue(Utf8s.containsAscii(utf8("foo bar baz"), ""));
        Assert.assertFalse(Utf8s.containsAscii(Utf8String.EMPTY, "foobar"));
    }

    @Test
    public void testContainsLowerCaseAscii() {
        Assert.assertTrue(Utf8s.containsLowerCaseAscii(utf8("аз съм грут foo bar baz"), utf8("bar")));
        Assert.assertTrue(Utf8s.containsLowerCaseAscii(utf8("аз съм грут FoO BaR BaZ"), utf8("bar")));
        Assert.assertTrue(Utf8s.containsLowerCaseAscii(utf8("foo bar baz"), utf8("foo")));
        Assert.assertTrue(Utf8s.containsLowerCaseAscii(utf8("FOO bar baz"), utf8("foo")));
        Assert.assertTrue(Utf8s.containsLowerCaseAscii(utf8("foo bar baz"), utf8("baz")));
        Assert.assertTrue(Utf8s.containsLowerCaseAscii(utf8("FOO BAR BAZ"), utf8("baz")));
        Assert.assertFalse(Utf8s.containsLowerCaseAscii(utf8("foo bar baz"), utf8("buz")));
        Assert.assertTrue(Utf8s.containsLowerCaseAscii(utf8("foo bar baz"), Utf8String.EMPTY));
        Assert.assertFalse(Utf8s.containsLowerCaseAscii(Utf8String.EMPTY, utf8("foobar")));
    }

    @Test
    public void testDoubleQuotedTextBySingleQuoteParsing() {
        StringSink query = new StringSink();

        String text = "select count(*) from \"\"file.csv\"\" abcd";
        Assert.assertTrue(copyToSinkWithTextUtil(query, text, false));

        Assert.assertEquals(text, query.toString());
    }

    @Test
    public void testDoubleQuotedTextParsing() {
        StringSink query = new StringSink();

        String text = "select count(*) from \"\"file.csv\"\" abcd";
        Assert.assertTrue(copyToSinkWithTextUtil(query, text, true));

        Assert.assertEquals(text.replace("\"\"", "\""), query.toString());
    }

    @Test
    public void testEncodeUtf16WithLimit() {
        Utf8StringSink sink = new Utf8StringSink();
        // one byte
        Assert.assertFalse(Utf8s.encodeUtf16WithLimit(sink, "foobar", 0));
        TestUtils.assertEquals("", sink);

        sink.clear();
        Assert.assertTrue(Utf8s.encodeUtf16WithLimit(sink, "foobar", 42));
        TestUtils.assertEquals("foobar", sink);

        sink.clear();
        Assert.assertFalse(Utf8s.encodeUtf16WithLimit(sink, "foobar", 3));
        TestUtils.assertEquals("foo", sink);

        // two bytes
        sink.clear();
        Assert.assertTrue(Utf8s.encodeUtf16WithLimit(sink, "фубар", 10));
        TestUtils.assertEquals("фубар", sink);

        sink.clear();
        Assert.assertFalse(Utf8s.encodeUtf16WithLimit(sink, "фубар", 4));
        TestUtils.assertEquals("фу", sink);

        sink.clear();
        Assert.assertFalse(Utf8s.encodeUtf16WithLimit(sink, "фубар", 3));
        TestUtils.assertEquals("ф", sink);

        // three bytes
        sink.clear();
        Assert.assertTrue(Utf8s.encodeUtf16WithLimit(sink, "∆", 3));
        TestUtils.assertEquals("∆", sink);

        sink.clear();
        Assert.assertFalse(Utf8s.encodeUtf16WithLimit(sink, "∆", 2));
        TestUtils.assertEquals("", sink);

        // four bytes
        sink.clear();
        Assert.assertTrue(Utf8s.encodeUtf16WithLimit(sink, "\uD83D\uDE00", 4));
        TestUtils.assertEquals("\uD83D\uDE00", sink);

        sink.clear();
        Assert.assertFalse(Utf8s.encodeUtf16WithLimit(sink, "\uD83D\uDE00", 3));
        TestUtils.assertEquals("", sink);
    }

    @Test
    public void testEndsWith() {
        Assert.assertTrue(Utf8s.endsWith(utf8("фу бар баз"), utf8("баз")));
        Assert.assertTrue(Utf8s.endsWith(utf8("foo bar baz"), utf8("oo bar baz")));
        Assert.assertFalse(Utf8s.endsWith(utf8("foo bar baz"), utf8("oo bar bax")));
        Assert.assertTrue(Utf8s.endsWith(utf8("foo bar baz"), utf8("baz")));
        Assert.assertFalse(Utf8s.endsWith(utf8("foo bar baz"), utf8("bar")));
        Assert.assertTrue(Utf8s.endsWith(utf8("foo bar baz"), Utf8String.EMPTY));
        Assert.assertFalse(Utf8s.endsWith(Utf8String.EMPTY, utf8("foo")));
    }

    @Test
    public void testEndsWithAscii() {
        Assert.assertTrue(Utf8s.endsWithAscii(utf8("foo bar baz"), "baz"));
        Assert.assertFalse(Utf8s.endsWithAscii(utf8("foo bar baz"), "bar"));
        Assert.assertTrue(Utf8s.endsWithAscii(utf8("foo bar baz"), ""));
        Assert.assertFalse(Utf8s.endsWithAscii(Utf8String.EMPTY, "foo"));
    }

    @Test
    public void testEndsWithAsciiChar() {
        Assert.assertTrue(Utf8s.endsWithAscii(utf8("foo bar baz"), 'z'));
        Assert.assertFalse(Utf8s.endsWithAscii(utf8("foo bar baz"), 'f'));
        Assert.assertFalse(Utf8s.endsWithAscii(utf8("foo bar baz"), (char) 0));
        Assert.assertFalse(Utf8s.endsWithAscii(Utf8String.EMPTY, ' '));
    }

    @Test
    public void testEndsWithLowerCaseAscii() {
        Assert.assertTrue(Utf8s.endsWithLowerCaseAscii(utf8("FOO BAR BAZ"), utf8("baz")));
        Assert.assertTrue(Utf8s.endsWithLowerCaseAscii(utf8("foo bar baz"), utf8("baz")));
        Assert.assertFalse(Utf8s.endsWithLowerCaseAscii(utf8("foo bar baz"), utf8("bar")));
        Assert.assertTrue(Utf8s.endsWithLowerCaseAscii(utf8("foo bar baz"), Utf8String.EMPTY));
        Assert.assertFalse(Utf8s.endsWithLowerCaseAscii(Utf8String.EMPTY, utf8("foo")));
    }

    @Test
    public void testEquals() {
        String test1 = "test1";
        String test2 = "test2";
        String longerString = "a_longer_string";

        final DirectUtf8Sequence str1a = new GcUtf8String(test1);
        final DirectUtf8Sequence str1b = new GcUtf8String(test1);
        final DirectUtf8Sequence str2 = new GcUtf8String(test2);
        final DirectUtf8Sequence str3 = new GcUtf8String(longerString);
        Assert.assertNotEquals(str1a.ptr(), str1b.ptr());

        Assert.assertTrue(Utf8s.equals(str1a, str1a));
        Assert.assertTrue(Utf8s.equals(str1a, str1b));
        Assert.assertFalse(Utf8s.equals(str1a, str2));
        Assert.assertFalse(Utf8s.equals(str2, str3));

        final Utf8String onHeapStr1a = utf8(test1);
        final Utf8String onHeapStr1b = utf8(test1);
        final Utf8String onHeapStr2 = utf8(test2);
        final Utf8String onHeapStr3 = utf8(longerString);

        Assert.assertTrue(Utf8s.equals(onHeapStr1a, onHeapStr1a));
        Assert.assertTrue(Utf8s.equals(onHeapStr1a, onHeapStr1b));
        Assert.assertFalse(Utf8s.equals(onHeapStr1a, onHeapStr2));
        Assert.assertFalse(Utf8s.equals(onHeapStr2, onHeapStr3));

        final DirectUtf8String directStr1a = new DirectUtf8String().of(str1a.lo(), str1a.hi());
        final DirectUtf8String directStr2 = new DirectUtf8String().of(str2.lo(), str2.hi());

        Assert.assertTrue(Utf8s.equals(directStr1a, onHeapStr1a));
        Assert.assertTrue(Utf8s.equals(directStr1a, onHeapStr1b));
        Assert.assertFalse(Utf8s.equals(directStr1a, onHeapStr2));
        Assert.assertFalse(Utf8s.equals(directStr2, onHeapStr3));

        Assert.assertTrue(Utf8s.equals(directStr1a, 0, 3, onHeapStr1a, 0, 3));
        Assert.assertFalse(Utf8s.equals(directStr1a, 0, 3, onHeapStr3, 0, 3));
    }

    @Test
    public void testEqualsAscii() {
        final Utf8String str = utf8("test1");

        Assert.assertTrue(Utf8s.equalsAscii("test1", str));
        Assert.assertFalse(Utf8s.equalsAscii("test2", str));
        Assert.assertFalse(Utf8s.equalsAscii("a_longer_string", str));

        Assert.assertTrue(Utf8s.equalsAscii("test1", 0, 3, str, 0, 3));
        Assert.assertFalse(Utf8s.equalsAscii("a_longer_string", 0, 3, str, 0, 3));
    }

    @Test
    public void testEqualsIgnoreCaseAscii() {
        final Utf8String str = utf8("test1");

        Assert.assertTrue(Utf8s.equalsIgnoreCaseAscii("test1", str));
        Assert.assertTrue(Utf8s.equalsIgnoreCaseAscii("TeSt1", str));
        Assert.assertTrue(Utf8s.equalsIgnoreCaseAscii("TEST1", str));
        Assert.assertFalse(Utf8s.equalsIgnoreCaseAscii("test2", str));
        Assert.assertFalse(Utf8s.equalsIgnoreCaseAscii("a_longer_string", str));

        Assert.assertTrue(Utf8s.equalsIgnoreCaseAscii(utf8("test1"), str));
        Assert.assertTrue(Utf8s.equalsIgnoreCaseAscii(utf8("TeSt1"), str));
        Assert.assertTrue(Utf8s.equalsIgnoreCaseAscii(utf8("TEST1"), str));
        Assert.assertFalse(Utf8s.equalsIgnoreCaseAscii(utf8("test2"), str));
        Assert.assertFalse(Utf8s.equalsIgnoreCaseAscii(utf8("a_longer_string"), str));

        Assert.assertTrue(Utf8s.equalsIgnoreCaseAscii(utf8("test1"), 0, 5, str, 0, 5));
        Assert.assertTrue(Utf8s.equalsIgnoreCaseAscii(utf8("TeSt1"), 0, 5, str, 0, 5));
        Assert.assertTrue(Utf8s.equalsIgnoreCaseAscii(utf8("TEST1"), 0, 5, str, 0, 5));
        Assert.assertFalse(Utf8s.equalsIgnoreCaseAscii(utf8("test2"), 0, 5, str, 0, 5));
        Assert.assertTrue(Utf8s.equalsIgnoreCaseAscii(utf8("test2"), 0, 4, str, 0, 4));
    }

    @Test
    public void testEqualsNcAscii() {
        final Utf8String str = utf8("test1");

        Assert.assertTrue(Utf8s.equalsNcAscii("test1", str));
        Assert.assertFalse(Utf8s.equalsNcAscii("test2", str));
        Assert.assertFalse(Utf8s.equalsNcAscii("a_longer_string", str));

        Assert.assertFalse(Utf8s.equalsNcAscii("test1", null));
    }

    @Test
    public void testGetUtf8SequenceType() {
        try (DirectUtf8Sink sink = new DirectUtf8Sink(16)) {
            Assert.assertEquals(0, Utf8s.getUtf8SequenceType(sink.lo(), sink.hi()));
            sink.put("abc");
            Assert.assertEquals(0, Utf8s.getUtf8SequenceType(sink.lo(), sink.hi()));
            sink.put("привет мир");
            Assert.assertEquals(1, Utf8s.getUtf8SequenceType(sink.lo(), sink.hi()));
            // invalid UTF-8
            sink.clear();
            sink.put((byte) 0x80);
            Assert.assertEquals(-1, Utf8s.getUtf8SequenceType(sink.lo(), sink.hi()));
        }
    }

    @Test
    public void testHasDots() {
        try (DirectUtf8Sink sink = new DirectUtf8Sink(16)) {
            Assert.assertFalse(Utf8s.hasDots(sink));
            sink.put(' ');
            Assert.assertFalse(Utf8s.hasDots(sink));
            sink.clear();
            sink.put('.');
            Assert.assertTrue(Utf8s.hasDots(sink));

            sink.clear();
            sink.put("12345678");
            Assert.assertFalse(Utf8s.hasDots(sink));
            sink.clear();
            sink.put("123456.8");
            Assert.assertTrue(Utf8s.hasDots(sink));

            sink.clear();
            sink.put("foobarfoobar");
            Assert.assertFalse(Utf8s.hasDots(sink));
            sink.clear();
            sink.put("foobarfoobar.2");
            Assert.assertTrue(Utf8s.hasDots(sink));

            sink.clear();
            sink.put("фубар");
            Assert.assertFalse(Utf8s.hasDots(sink));
            sink.clear();
            sink.put("фуба.р");
            Assert.assertTrue(Utf8s.hasDots(sink));

            for (int i = 1; i < 50; i++) {
                sink.clear();
                for (int j = 0; j < i; j++) {
                    sink.put('a');
                }
                Assert.assertFalse(Utf8s.hasDots(sink));

                sink.put('.');
                Assert.assertTrue(Utf8s.hasDots(sink));

                for (int j = 0; j < i; j++) {
                    sink.put('a');
                }
                Assert.assertTrue(Utf8s.hasDots(sink));
            }
        }
    }

    @Test
    public void testHashCode() {
        final int size = 64;
        StringSink charSink = new StringSink();
        Utf8StringSink utf8Sink = new Utf8StringSink();
        for (int i = 0; i < size; i++) {
            charSink.putAscii('A');
            utf8Sink.putAscii('A');

            Assert.assertEquals(Chars.hashCode(charSink), Utf8s.hashCode(utf8Sink));

            if (i > 0) {
                Assert.assertEquals(Chars.hashCode(charSink, 0, i - 1), Utf8s.hashCode(utf8Sink, 0, i - 1));
            }
        }
    }

    @Test
    public void testIndexOf() {
        Assert.assertEquals(1, Utf8s.indexOf(utf8("foo bar baz"), 0, 7, utf8("oo")));
        Assert.assertEquals(-1, Utf8s.indexOf(utf8("foo bar baz"), 2, 4, utf8("y")));
        Assert.assertEquals(-1, Utf8s.indexOf(Utf8String.EMPTY, 0, 0, utf8("byz")));
    }

    @Test
    public void testIndexOfAscii() {
        Assert.assertEquals(1, Utf8s.indexOfAscii(utf8("foo bar baz"), 0, 7, "oo"));
        Assert.assertEquals(1, Utf8s.indexOfAscii(utf8("foo bar baz"), 0, 7, "oo", -1));
        Assert.assertEquals(-1, Utf8s.indexOfAscii(utf8("foo bar baz"), 2, 4, "y"));
        Assert.assertEquals(-1, Utf8s.indexOfAscii(Utf8String.EMPTY, 0, 0, "byz"));
    }

    @Test
    public void testIndexOfAsciiChar() {
        Assert.assertEquals(1, Utf8s.indexOfAscii(utf8("foo bar baz"), 'o'));
        Assert.assertEquals(-1, Utf8s.indexOfAscii(utf8("foo bar baz"), 'y'));
        Assert.assertEquals(-1, Utf8s.indexOfAscii(Utf8String.EMPTY, 'y'));

        Assert.assertEquals(2, Utf8s.indexOfAscii(utf8("foo bar baz"), 2, 'o'));
        Assert.assertEquals(-1, Utf8s.indexOfAscii(utf8("foo bar baz"), 2, 'y'));
        Assert.assertEquals(-1, Utf8s.indexOfAscii(Utf8String.EMPTY, 0, 'y'));

        Assert.assertEquals(2, Utf8s.indexOfAscii(utf8("foo bar baz"), 2, 4, 'o'));
        Assert.assertEquals(2, Utf8s.indexOfAscii(utf8("foo bar baz"), 0, 4, 'o', -1));
        Assert.assertEquals(-1, Utf8s.indexOfAscii(utf8("foo bar baz"), 2, 4, 'y'));
        Assert.assertEquals(-1, Utf8s.indexOfAscii(Utf8String.EMPTY, 0, 0, 'y'));
    }

    @Test
    public void testIndexOfLowerCaseAscii() {
        Assert.assertEquals(20, Utf8s.indexOfLowerCaseAscii(utf8("фу бар баз FOO BAR BAZ"), 0, 30, utf8("oo")));
        Assert.assertEquals(1, Utf8s.indexOfLowerCaseAscii(utf8("FOO BAR BAZ"), 0, 7, utf8("oo")));
        Assert.assertEquals(1, Utf8s.indexOfLowerCaseAscii(utf8("foo bar baz"), 0, 7, utf8("oo")));
        Assert.assertEquals(-1, Utf8s.indexOfLowerCaseAscii(utf8("foo bar baz"), 2, 4, utf8("y")));
        Assert.assertEquals(-1, Utf8s.indexOfLowerCaseAscii(Utf8String.EMPTY, 0, 0, utf8("byz")));
    }

    @Test
    public void testIsAscii() {
        try (DirectUtf8Sink sink = new DirectUtf8Sink(16)) {
            sink.put("foobar");
            Assert.assertTrue(Utf8s.isAscii(sink));
            Assert.assertTrue(Utf8s.isAscii(sink.ptr(), sink.size()));
            sink.clear();
            sink.put("foobarfoobarfoobarfoobarfoobarfoobarfoobar");
            Assert.assertTrue(Utf8s.isAscii(sink));
            Assert.assertTrue(Utf8s.isAscii(sink.ptr(), sink.size()));
            sink.clear();
            sink.put("фубар");
            Assert.assertFalse(Utf8s.isAscii(sink));
            Assert.assertFalse(Utf8s.isAscii(sink.ptr(), sink.size()));
            sink.clear();
            sink.put("foobarfoobarfoobarfoobarфубарфубарфубарфубар");
            Assert.assertFalse(Utf8s.isAscii(sink));
            Assert.assertFalse(Utf8s.isAscii(sink.ptr(), sink.size()));
            sink.clear();
            sink.put("12345678ы87654321");
            Assert.assertTrue(Utf8s.isAscii(sink.longAt(0)));
            for (int i = 1; i < 10; i++) {
                Assert.assertFalse(Utf8s.isAscii(sink.longAt(i)));
            }
            Assert.assertTrue(Utf8s.isAscii(sink.longAt(10)));
            Assert.assertFalse(Utf8s.isAscii(sink));
            Assert.assertFalse(Utf8s.isAscii(sink.ptr(), sink.size()));
        }
    }

    @Test
    public void testLastIndexOfAscii() {
        Assert.assertEquals(2, Utf8s.lastIndexOfAscii(utf8("foo bar baz"), 'o'));
        Assert.assertEquals(10, Utf8s.lastIndexOfAscii(utf8("foo bar baz"), 'z'));
        Assert.assertEquals(-1, Utf8s.lastIndexOfAscii(utf8("foo bar baz"), 'y'));
        Assert.assertEquals(-1, Utf8s.lastIndexOfAscii(Utf8String.EMPTY, 'y'));
    }

    @Test
    public void testLowerCaseAsciiHashCode() {
        final int size = 64;
        StringSink charSink = new StringSink();
        Utf8StringSink utf8Sink = new Utf8StringSink();
        for (int i = 0; i < size; i++) {
            charSink.putAscii('a');
            utf8Sink.putAscii('A');

            Assert.assertEquals(Chars.hashCode(charSink), Utf8s.lowerCaseAsciiHashCode(utf8Sink));

            if (i > 0) {
                Assert.assertEquals(Chars.hashCode(charSink, 0, i - 1), Utf8s.lowerCaseAsciiHashCode(utf8Sink, 0, i - 1));
            }
        }
    }

    @Test
    public void testPutSafeInvalid() {
        final Utf8StringSink source = new Utf8StringSink();
        final Utf8StringSink sink = new Utf8StringSink();
        final byte[][] testBufs = {
                {b(0b1101_0101)},
                {b(0b1101_0101), b(0b0011_1100)},
                {b(0b1100_0100), b(0b1100_1101)},

                {b(0b1110_0100), b(0b1011_1101)},
                {b(0b1110_0100), b(0b1011_1101), b(0b1110_0000)},
                {b(0b1110_0100), b(0b1011_1101), b(0b0110_0000)},
                {b(0b1110_0100), b(0b0110_0000), b(0b1011_1101)},

                {b(0b1111_0000)},
                {b(0b1111_0000), b(0b1001_1111)},
                {b(0b1111_0000), b(0b1001_1111), b(0b1001_0010)},

                {b(0b1111_0000), b(0b0101_1111), b(0b1001_0010), b(0b1010_1001)},
                {b(0b1111_0000), b(0b1001_1111), b(0b0101_0010), b(0b1010_1001)},
                {b(0b1111_0000), b(0b1001_1111), b(0b1001_0010), b(0b0110_1001)},

                {b(0b1111_1000)},
                {b(0b1111_1000), b(0b1000_0000)},
                {b(0b1111_1000), b(0b1000_0000), b(0b1000_0001)},
                {b(0b1111_1000), b(0b1000_0000), b(0b1000_0001), b(0b1000_0010)},
                {b(0b1111_1000), b(0b1000_0000), b(0b1000_0001), b(0b1000_0010), b(0b1000_0011)},
                {b(0b1111_1110), b(0b1100_0000)},
                {b(0b1111_1000), b(0b1100_0000)},
                {b(0b1111_1000), b(0b1111_0000)},
                {b(0b1111_1000), b(0b1111_1111)},
        };
        final String[] expectedStrs = {
                "\\xD5",
                "\\xD5<",
                "\\xC4\\xCD",

                "\\xE4\\xBD",
                "\\xE4\\xBD\\xE0",
                "\\xE4\\xBD`",
                "\\xE4`\\xBD",

                "\\xF0",
                "\\xF0\\x9F",
                "\\xF0\\x9F\\x92",

                "\\xF0_\\x92\\xA9",
                "\\xF0\\x9FR\\xA9",
                "\\xF0\\x9F\\x92i",

                "\\xF8",
                "\\xF8\\x80",
                "\\xF8\\x80\\x81",
                "\\xF8\\x80\\x81\\x82",
                "\\xF8\\x80\\x81\\x82\\x83",
                "\\xFE\\xC0",
                "\\xF8\\xC0",
                "\\xF8\\xF0",
                "\\xF8\\xFF",
        };
        final long buf = Unsafe.malloc(128, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int n = testBufs.length, i = 0; i < n; i++) {
                byte[] bytes = testBufs[i];

                long hi = copyBytes(buf, bytes);
                sink.clear();
                Utf8s.putSafe(buf, hi, sink);
                Assert.assertEquals(expectedStrs[i], sink.toString());

                source.clear();
                for (byte b : bytes) {
                    source.putAny(b);
                }
                sink.clear();
                Utf8s.putSafe(source, sink);
                Assert.assertEquals(expectedStrs[i], sink.toString());
            }
        } finally {
            Unsafe.free(buf, 128, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testPutSafeValid() {
        final Utf8StringSink sink = new Utf8StringSink();
        final String[] testStrs = {
                "abc",
                "čćžšđ",
                "ČĆŽŠĐ",
                "фубар",
                "ФУБАР",
                "你好世界",
        };
        final long buf = Unsafe.malloc(128, MemoryTag.NATIVE_DEFAULT);
        try {
            for (String testStr : testStrs) {
                byte[] bytes = testStr.getBytes(StandardCharsets.UTF_8);
                long hi = copyBytes(buf, bytes);
                sink.clear();
                Utf8s.putSafe(buf, hi, sink);
                String actual = sink.toString();
                for (long ptr = buf; ptr < hi; ptr++) {
                    int b = Unsafe.getUnsafe().getByte(ptr) & 0xFF;
                }
                Assert.assertEquals(testStr, actual);
            }
        } finally {
            Unsafe.free(buf, 128, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testQuotedTextParsing() {
        StringSink query = new StringSink();

        String text = "select count(*) from \"file.csv\" abcd";
        Assert.assertTrue(copyToSinkWithTextUtil(query, text, false));

        Assert.assertEquals(text, query.toString());
    }

    @Test
    public void testReadWriteVarchar() {
        try (
                MemoryCARW auxMem = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                MemoryCARW dataMem = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
        ) {
            final Rnd rnd = TestUtils.generateRandom(null);
            final Utf8StringSink utf8Sink = new Utf8StringSink();
            int n = rnd.nextInt(10000);
            ObjList<String> expectedValues = new ObjList<>(n);
            BitSet asciiBitSet = new BitSet();
            LongList expectedOffsets = new LongList();
            for (int i = 0; i < n; i++) {
                boolean ascii = rnd.nextBoolean();
                if (rnd.nextInt(10) == 0) {
                    VarcharTypeDriver.appendValue(auxMem, dataMem, null);
                    expectedValues.add(null);
                } else {
                    utf8Sink.clear();
                    int len = Math.max(1, rnd.nextInt(25));
                    if (ascii) {
                        rnd.nextUtf8AsciiStr(len, utf8Sink);
                        Assert.assertTrue(utf8Sink.isAscii());
                    } else {
                        rnd.nextUtf8Str(len, utf8Sink);
                    }
                    if (utf8Sink.isAscii()) {
                        asciiBitSet.set(i);
                    }
                    expectedValues.add(utf8Sink.toString());
                    VarcharTypeDriver.appendValue(auxMem, dataMem, utf8Sink);
                }
                expectedOffsets.add(dataMem.getAppendOffset());
            }

            for (int i = 0; i < n; i++) {
                Utf8Sequence varchar = VarcharTypeDriver.getSplitValue(auxMem, dataMem, i, rnd.nextBoolean() ? 1 : 2);
                Assert.assertEquals(expectedOffsets.getQuick(i), VarcharTypeDriver.getDataVectorSize(auxMem, i * 16L));
                String expectedValue = expectedValues.getQuick(i);
                if (expectedValue == null) {
                    Assert.assertNull(varchar);
                } else {
                    Assert.assertNotNull(varchar);
                    Assert.assertEquals(expectedValue, varchar.toString());
                    Assert.assertEquals(asciiBitSet.get(i), varchar.isAscii());
                }
            }
        }
    }

    @Test
    public void testReadWriteVarcharOver2GB() {
        try (
                MemoryCARW auxMem = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                MemoryCARW dataMem = Vm.getCARWInstance(16 * 1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
        ) {
            final Utf8StringSink utf8Sink = new Utf8StringSink();
            int len = 1024;
            int n = Integer.MAX_VALUE / len + len;
            LongList expectedOffsets = new LongList(n);
            for (int i = 0; i < n; i++) {
                utf8Sink.clear();
                utf8Sink.repeat('a', len);
                VarcharTypeDriver.appendValue(auxMem, dataMem, utf8Sink);
                expectedOffsets.add(dataMem.getAppendOffset());
            }

            utf8Sink.clear();
            utf8Sink.repeat('a', len);
            String expectedStr = utf8Sink.toString();
            for (int i = 0; i < n; i++) {
                Utf8Sequence varchar = VarcharTypeDriver.getSplitValue(auxMem, dataMem, i, 1);
                Assert.assertEquals(expectedOffsets.getQuick(i), VarcharTypeDriver.getDataVectorSize(auxMem, i * 16L));
                Assert.assertNotNull(varchar);
                TestUtils.assertEquals(expectedStr, varchar.asAsciiCharSequence());
                Assert.assertTrue(varchar.isAscii());
            }
        }
    }

    @Test
    public void testRndUtf8toUtf16Equality() {
        Rnd rnd = TestUtils.generateRandom(null);
        StringSink sink = new StringSink();
        try (DirectUtf8Sink utf8Sink = new DirectUtf8Sink(4)) {
            for (int i = 0; i < 1000; i++) {
                utf8Sink.clear();
                rnd.nextUtf8Str(100, utf8Sink);

                sink.clear();
                Utf8s.directUtf8ToUtf16(utf8Sink, sink);

                if (!Utf8s.equalsUtf16(sink, utf8Sink)) {
                    Assert.fail("iteration " + i + ", expected equals: " + sink);
                }
            }
        }
    }

    @Test
    public void testRndUtf8toUtf16EqualityShortStr() {
        Rnd rnd = TestUtils.generateRandom(null);
        StringSink sink = new StringSink();
        try (DirectUtf8Sink utf8Sink = new DirectUtf8Sink(4)) {
            for (int i = 0; i < 1000; i++) {
                utf8Sink.clear();
                rnd.nextUtf8Str(100, utf8Sink);

                sink.clear();
                Utf8s.directUtf8ToUtf16(utf8Sink, sink);

                // remove the last character
                if (Utf8s.equalsUtf16(sink, 0, sink.length() - 1, utf8Sink, 0, utf8Sink.size())) {
                    Assert.fail("iteration " + i + ", expected non-equals: " + sink);
                }

                // remove the last character
                if (Utf8s.equalsUtf16(sink, 0, sink.length(), utf8Sink, 0, utf8Sink.size() - 1)) {
                    Assert.fail("iteration " + i + ", expected non-equals: " + sink);
                }

                if (sink.length() > 0) {
                    // compare to empty
                    if (Utf8s.equalsUtf16(sink, 0, 0, utf8Sink, 0, utf8Sink.size() - 1)) {
                        Assert.fail("iteration " + i + ", expected non-equals: " + sink);
                    }

                    if (Utf8s.equalsUtf16(sink, 0, sink.length(), utf8Sink, 0, 0)) {
                        Assert.fail("iteration " + i + ", expected non-equals: " + sink);
                    }

                    long address = utf8Sink.ptr() + rnd.nextInt(utf8Sink.size());
                    byte b = Unsafe.getUnsafe().getByte(address);
                    Unsafe.getUnsafe().putByte(address, (byte) (b + 1));
                    if (Utf8s.equalsUtf16(sink, utf8Sink)) {
                        Assert.fail("iteration " + i + ", expected non-equals: " + sink);
                    }
                }
            }
        }
    }

    @Test
    public void testStartsWith() {
        String asciiShort = "abcdef";
        String asciiMid = "abcdefgh";
        String asciiLong = "abcdefghijk";
        Assert.assertTrue(Utf8s.startsWith(utf8(asciiShort), utf8("ab")));
        Assert.assertTrue(Utf8s.startsWith(utf8(asciiShort), utf8(asciiShort)));
        Assert.assertFalse(Utf8s.startsWith(utf8(asciiShort), utf8(asciiMid)));
        Assert.assertFalse(Utf8s.startsWith(utf8(asciiShort), utf8(asciiLong)));
        Assert.assertFalse(Utf8s.startsWith(utf8(asciiShort), utf8("abcdex")));

        Assert.assertTrue(Utf8s.startsWith(utf8(asciiMid), utf8(asciiMid)));
        Assert.assertFalse(Utf8s.startsWith(utf8(asciiMid), utf8(asciiLong)));
        Assert.assertFalse(Utf8s.startsWith(utf8(asciiMid), utf8("abcdefgx")));
        Assert.assertFalse(Utf8s.startsWith(utf8(asciiMid), utf8("xabcde")));

        Assert.assertTrue(Utf8s.startsWith(utf8(asciiLong), utf8(asciiShort)));
        Assert.assertTrue(Utf8s.startsWith(utf8(asciiLong), utf8(asciiMid)));
        Assert.assertFalse(Utf8s.startsWith(utf8(asciiLong), utf8("xabcdefghijk")));
        Assert.assertFalse(Utf8s.startsWith(utf8(asciiLong), utf8("abcdefghijkl")));
        Assert.assertFalse(Utf8s.startsWith(utf8(asciiLong), utf8("x")));

        String nonAsciiLong = "фу бар баз";
        Assert.assertTrue(Utf8s.startsWith(utf8(nonAsciiLong), utf8("фу")));
        Assert.assertFalse(Utf8s.startsWith(utf8(nonAsciiLong), utf8("бар")));
        Assert.assertTrue(Utf8s.startsWith(utf8(nonAsciiLong), Utf8String.EMPTY));
        Assert.assertFalse(Utf8s.startsWith(Utf8String.EMPTY, utf8("фу-фу-фу")));
    }

    @Test
    public void testStartsWithAscii() {
        Assert.assertTrue(Utf8s.startsWithAscii(utf8("foo bar baz"), "foo"));
        Assert.assertFalse(Utf8s.startsWithAscii(utf8("foo bar baz"), "bar"));
        Assert.assertTrue(Utf8s.startsWithAscii(utf8("foo bar baz"), ""));
        Assert.assertFalse(Utf8s.startsWithAscii(Utf8String.EMPTY, "foo"));
    }

    @Test
    public void testStartsWithLowerCaseAscii() {
        Assert.assertTrue(Utf8s.startsWithLowerCaseAscii(utf8("FOO BAR BAZ"), utf8("foo")));
        Assert.assertTrue(Utf8s.startsWithLowerCaseAscii(utf8("foo bar baz"), utf8("foo")));
        Assert.assertFalse(Utf8s.startsWithLowerCaseAscii(utf8("foo bar baz"), utf8("bar")));
        Assert.assertTrue(Utf8s.startsWithLowerCaseAscii(utf8("foo bar baz"), Utf8String.EMPTY));
        Assert.assertFalse(Utf8s.startsWithLowerCaseAscii(Utf8String.EMPTY, utf8("foo")));
    }

    @Test
    public void testStartsWithSixPrefix() {
        Utf8String asciiShort = utf8("abcdef");
        Utf8String asciiMid = utf8("abcdefgh");
        Utf8String asciiLong = utf8("abcdefghijk");

        Assert.assertEquals(Utf8s.zeroPaddedSixPrefix(asciiShort), asciiShort.zeroPaddedSixPrefix());
        Assert.assertEquals(Utf8s.zeroPaddedSixPrefix(asciiMid), asciiMid.zeroPaddedSixPrefix());
        Assert.assertEquals(Utf8s.zeroPaddedSixPrefix(asciiLong), asciiLong.zeroPaddedSixPrefix());

        long sixPrefixShort = asciiShort.zeroPaddedSixPrefix();
        long sixPrefixMid = asciiMid.zeroPaddedSixPrefix();
        long sixPrefixLong = asciiLong.zeroPaddedSixPrefix();

        Utf8String ab = utf8("ab");
        Utf8String abcdex = utf8("abcdex");
        Assert.assertTrue(Utf8s.startsWith(asciiShort, sixPrefixShort, ab, ab.zeroPaddedSixPrefix()));
        Assert.assertTrue(Utf8s.startsWith(asciiShort, sixPrefixShort, asciiShort, sixPrefixShort));
        Assert.assertFalse(Utf8s.startsWith(asciiShort, sixPrefixShort, asciiMid, sixPrefixMid));
        Assert.assertFalse(Utf8s.startsWith(asciiShort, sixPrefixShort, asciiLong, sixPrefixLong));
        Assert.assertFalse(Utf8s.startsWith(asciiShort, sixPrefixShort, abcdex, abcdex.zeroPaddedSixPrefix()));

        Utf8String abcdefgx = utf8("abcdefgx");
        Utf8String xabcde = utf8("xabcde");
        Assert.assertTrue(Utf8s.startsWith(asciiMid, sixPrefixMid, asciiMid, sixPrefixMid));
        Assert.assertFalse(Utf8s.startsWith(asciiMid, sixPrefixMid, asciiLong, sixPrefixLong));
        Assert.assertFalse(Utf8s.startsWith(asciiMid, sixPrefixMid, abcdefgx, abcdefgx.zeroPaddedSixPrefix()));
        Assert.assertFalse(Utf8s.startsWith(asciiMid, sixPrefixMid, xabcde, xabcde.zeroPaddedSixPrefix()));

        Utf8String xabcdefghijk = utf8("xabcdefghijk");
        Utf8String abcdefghijkl = utf8("abcdefghijkl");
        Utf8String x = utf8("x");
        Assert.assertTrue(Utf8s.startsWith(asciiLong, sixPrefixLong, asciiShort, sixPrefixShort));
        Assert.assertTrue(Utf8s.startsWith(asciiLong, sixPrefixLong, asciiMid, sixPrefixMid));
        Assert.assertFalse(Utf8s.startsWith(asciiLong, sixPrefixLong, xabcdefghijk, xabcdefghijk.zeroPaddedSixPrefix()));
        Assert.assertFalse(Utf8s.startsWith(asciiLong, sixPrefixLong, abcdefghijkl, abcdefghijkl.zeroPaddedSixPrefix()));
        Assert.assertFalse(Utf8s.startsWith(asciiLong, sixPrefixLong, x, x.zeroPaddedSixPrefix()));

        Utf8String nonAsciiLong = utf8("фу бар баз");
        Utf8String fu = utf8("фу");
        Utf8String bar = utf8("бар");
        Utf8String fufufu = utf8("фу-фу-фу");
        long sixPrefixNaLong = nonAsciiLong.zeroPaddedSixPrefix();
        Assert.assertTrue(Utf8s.startsWith(nonAsciiLong, sixPrefixNaLong, fu, fu.zeroPaddedSixPrefix()));
        Assert.assertFalse(Utf8s.startsWith(nonAsciiLong, sixPrefixNaLong, bar, bar.zeroPaddedSixPrefix()));
        Assert.assertTrue(Utf8s.startsWith(nonAsciiLong, sixPrefixNaLong, Utf8String.EMPTY, 0L));
        Assert.assertFalse(Utf8s.startsWith(Utf8String.EMPTY, 0L, fufufu, fufufu.zeroPaddedSixPrefix()));
    }

    @Test
    public void testStrCpy() {
        final int size = 32;
        long mem = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        DirectUtf8String str = new DirectUtf8String();
        str.of(mem, mem + size);
        try {
            Utf8StringSink strSink = new Utf8StringSink();
            strSink.repeat("a", size);

            Utf8s.strCpy(strSink, size, mem);
            TestUtils.assertEquals(strSink, str);

            // overwrite the sink contents
            strSink.clear();
            strSink.repeat("b", size);
            strSink.clear();

            Utf8s.strCpy(mem, mem + size, strSink);
            TestUtils.assertEquals(strSink, str);

            // test with DirectUtf8Sink too
            DirectUtf8Sink directSink = new DirectUtf8Sink(size);
            Utf8s.strCpy(mem, mem + size, directSink);
            TestUtils.assertEquals(strSink, directSink);
        } finally {
            Unsafe.free(mem, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testStrCpyAscii() {
        final int size = 32;
        long mem = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        DirectUtf8String actualString = new DirectUtf8String();
        actualString.of(mem, mem + size);
        try {
            StringSink expectedSink = new StringSink();
            expectedSink.repeat("a", size);
            expectedSink.putAscii("foobar"); // this should get ignored by Utf8s.strCpyAscii()
            Utf8s.strCpyAscii(expectedSink.toString().toCharArray(), 0, size, mem);

            expectedSink.clear(size);
            TestUtils.assertEquals(expectedSink, actualString);

            expectedSink.clear();
            expectedSink.repeat("b", size);
            Utf8s.strCpyAscii(expectedSink, mem);

            expectedSink.clear(size);
            TestUtils.assertEquals(expectedSink, actualString);

            actualString.of(mem, mem + (size / 2));

            expectedSink.clear();
            expectedSink.repeat("c", size);
            Utf8s.strCpyAscii(expectedSink, size / 2, mem);

            expectedSink.clear(size / 2);
            TestUtils.assertEquals(expectedSink, actualString);

            expectedSink.clear();
            expectedSink.repeat("d", size);
            Utf8s.strCpyAscii(expectedSink, 0, size / 2, mem);

            expectedSink.clear(size / 2);
            TestUtils.assertEquals(expectedSink, actualString);
        } finally {
            Unsafe.free(mem, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testStrCpySubstring() {
        final int len = 3;
        Utf8StringSink srcSink = new Utf8StringSink();
        Utf8StringSink destSink = new Utf8StringSink();

        // ASCII
        srcSink.repeat("a", len);

        destSink.clear();
        Assert.assertEquals(0, Utf8s.strCpy(srcSink, 0, 0, destSink));
        TestUtils.assertEquals(Utf8String.EMPTY, destSink);

        destSink.clear();
        Assert.assertEquals(len, Utf8s.strCpy(srcSink, 0, len, destSink));
        TestUtils.assertEquals(srcSink, destSink);

        for (int i = 0; i < len - 1; i++) {
            destSink.clear();
            Assert.assertEquals(1, Utf8s.strCpy(srcSink, i, i + 1, destSink));
            TestUtils.assertEquals(utf8("a"), destSink);
        }

        // non-ASCII
        srcSink.clear();
        srcSink.repeat("ы", len);

        destSink.clear();
        Assert.assertEquals(0, Utf8s.strCpy(srcSink, 0, 0, destSink));
        TestUtils.assertEquals(Utf8String.EMPTY, destSink);

        destSink.clear();
        Assert.assertEquals(2 * len, Utf8s.strCpy(srcSink, 0, len, destSink));
        TestUtils.assertEquals(srcSink, destSink);

        for (int i = 0; i < len - 1; i++) {
            destSink.clear();
            Assert.assertEquals(2, Utf8s.strCpy(srcSink, i, i + 1, destSink));
            TestUtils.assertEquals(utf8("ы"), destSink);
        }
    }

    @Test
    public void testUtf8CharDecode() {
        try (DirectUtf8Sink sink = new DirectUtf8Sink(8)) {
            testUtf8Char("A", sink, false); // 1 byte
            testUtf8Char("Ч", sink, false); // 2 bytes
            testUtf8Char("∆", sink, false); // 3 bytes
            testUtf8Char("\uD83D\uDE00\"", sink, true); // fail, cannot store it as one char
        }
    }

    @Test
    public void testUtf8CharMalformedDecode() {
        try (DirectUtf8Sink sink = new DirectUtf8Sink(8)) {

            // empty
            Assert.assertEquals(0, Utf8s.utf8CharDecode(sink));
            // one byte
            sink.put((byte) 0xFF);
            Assert.assertEquals(0, Utf8s.utf8CharDecode(sink));

            sink.clear();
            sink.put((byte) 0xC0);
            Assert.assertEquals(0, Utf8s.utf8CharDecode(sink));

            sink.clear();
            sink.put((byte) 0x80);
            Assert.assertEquals(0, Utf8s.utf8CharDecode(sink));

            // two bytes
            sink.clear();
            sink.put((byte) 0xC0);
            sink.put((byte) 0x80);
            Assert.assertEquals(0, Utf8s.utf8CharDecode(sink));

            sink.clear();
            sink.put((byte) 0xC1);
            sink.put((byte) 0xBF);
            Assert.assertEquals(0, Utf8s.utf8CharDecode(sink));

            sink.clear();
            sink.put((byte) 0xC2);
            sink.putAscii((char) 0x00);
            Assert.assertEquals(0, Utf8s.utf8CharDecode(sink));

            sink.clear();
            sink.put((byte) 0xE0);
            sink.put((byte) 0x80);
            sink.put((byte) 0xC0);
            Assert.assertEquals(0, Utf8s.utf8CharDecode(sink));

            sink.clear();
            sink.put((byte) 0xE0);
            sink.put((byte) 0xC0);
            sink.put((byte) 0xBF);
            Assert.assertEquals(0, Utf8s.utf8CharDecode(sink));

            sink.clear();
            sink.put((byte) 0xED);
            sink.put((byte) 0xA0);
            sink.putAscii((char) 0x7F);
            Assert.assertEquals(0, Utf8s.utf8CharDecode(sink));

            sink.clear();
            sink.put((byte) 0xED);
            sink.put((byte) 0xAE);
            sink.put((byte) 0x80);
            Assert.assertEquals(0, Utf8s.utf8CharDecode(sink));
        }
    }

    @Test
    public void testUtf8Support() {
        StringBuilder expected = new StringBuilder();
        for (int i = 0; i < 0xD800; i++) {
            expected.append((char) i);
        }

        String in = expected.toString();
        long p = Unsafe.malloc(8 * 0xffff, MemoryTag.NATIVE_DEFAULT);
        try {
            byte[] bytes = in.getBytes(Files.UTF_8);
            for (int i = 0, n = bytes.length; i < n; i++) {
                Unsafe.getUnsafe().putByte(p + i, bytes[i]);
            }
            Utf16Sink b = new StringSink();
            Utf8s.utf8ToUtf16(p, p + bytes.length, b);
            TestUtils.assertEquals(in, b.toString());
        } finally {
            Unsafe.free(p, 8 * 0xffff, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUtf8SupportZ() {
        final int nChars = 128;
        final StringBuilder expected = new StringBuilder();
        for (int i = 0; i < nChars; i++) {
            expected.append(i);
        }

        String in = expected.toString();
        byte[] bytes = in.getBytes(StandardCharsets.UTF_8);
        final int nBytes = bytes.length + 1; // +1 byte for the NULL terminator

        long mem = Unsafe.malloc(nBytes, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < nBytes - 1; i++) {
                Unsafe.getUnsafe().putByte(mem + i, bytes[i]);
            }
            Unsafe.getUnsafe().putByte(mem + nBytes - 1, (byte) 0);

            StringSink b = new StringSink();
            Utf8s.utf8ToUtf16Z(mem, b);
            TestUtils.assertEquals(in, b.toString());
        } finally {
            Unsafe.free(mem, nBytes, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUtf8toUtf16() {
        StringSink utf16Sink = new StringSink();
        String empty = "";
        String ascii = "abc";
        String cyrillic = "абв";
        String chinese = "你好";
        String emoji = "😀";
        String mixed = "abcабв你好😀";
        String[] strings = {empty, ascii, cyrillic, chinese, emoji, mixed};
        byte[] terminators = {':', '-', ' ', '\0'};
        try (DirectUtf8Sink utf8Sink = new DirectUtf8Sink(4)) {
            for (String left : strings) {
                for (String right : strings) {
                    for (byte terminator : terminators) {
                        // test with terminator (left + terminator + right)
                        String input = left + (char) terminator + right;
                        int expectedUtf8ByteRead = left.getBytes(StandardCharsets.UTF_8).length;
                        assertUtf8ToUtf16WithTerminator(utf8Sink, utf16Sink, input, left, terminator, expectedUtf8ByteRead);
                    }
                    for (byte terminator : terminators) {
                        // test without terminator (left + right)
                        String input = left + right;
                        int expectedUtf8ByteRead = input.getBytes(StandardCharsets.UTF_8).length;
                        assertUtf8ToUtf16WithTerminator(utf8Sink, utf16Sink, input, input, terminator, expectedUtf8ByteRead);
                    }
                }
            }
        }
    }

    @Test
    public void testUtf8toUtf16Equality() {
        String empty = "";
        String ascii = "abc";
        String cyrillic = "абв";
        String chinese = "你好";
        String emoji = "😀";
        String mixed = "abcабв你好😀";
        String[] strings = {empty, ascii, cyrillic, chinese, emoji, mixed};

        try (DirectUtf8Sink utf8Sink = new DirectUtf8Sink(4)) {
            for (String left : strings) {
                for (String right : strings) {
                    utf8Sink.clear();
                    utf8Sink.put(right);

                    if (left.equals(right)) {
                        Assert.assertTrue("expected equals " + right, Utf8s.equalsUtf16Nc(left, utf8Sink));
                    } else {
                        Assert.assertFalse("expected not equals " + right, Utf8s.equalsUtf16Nc(left, utf8Sink));
                    }
                }
            }
        }
    }

    @Test
    public void testValidateUtf8() {
        Assert.assertEquals(0, Utf8s.validateUtf8(Utf8String.EMPTY));
        Assert.assertEquals(3, Utf8s.validateUtf8(utf8("abc")));
        Assert.assertEquals(10, Utf8s.validateUtf8(utf8("привет мир")));
        // invalid UTF-8
        Assert.assertEquals(-1, Utf8s.validateUtf8(new Utf8String(new byte[]{(byte) 0x80}, false)));
    }

    private static void assertUtf8ToUtf16WithTerminator(
            DirectUtf8Sink utf8Sink,
            StringSink utf16Sink,
            String inputString,
            String expectedDecodedString,
            byte terminator,
            int expectedUtf8ByteRead
    ) {
        utf8Sink.clear();
        utf16Sink.clear();

        utf8Sink.put(inputString);
        int n = Utf8s.utf8ToUtf16(utf8Sink, utf16Sink, terminator);
        Assert.assertEquals(inputString, expectedUtf8ByteRead, n);
        TestUtils.assertEquals(expectedDecodedString, utf16Sink);

        Assert.assertEquals(inputString, Utf8s.stringFromUtf8Bytes(utf8Sink));
        Assert.assertEquals(inputString, Utf8s.stringFromUtf8Bytes(utf8Sink.lo(), utf8Sink.hi()));

        Assert.assertEquals(inputString, utf8Sink.toString());
    }

    private static byte b(int n) {
        return (byte) n;
    }

    private static long copyBytes(long buf, byte[] bytes) {
        for (int n = bytes.length, i = 0; i < n; i++) {
            Unsafe.getUnsafe().putByte(buf + i, bytes[i]);
        }
        return buf + bytes.length;
    }

    private static void testUtf8Char(String x, MutableUtf8Sink sink, boolean failExpected) {
        sink.clear();
        byte[] bytes = x.getBytes(Files.UTF_8);
        for (int i = 0, n = Math.min(bytes.length, 8); i < n; i++) {
            byte b = bytes[i];
            if (b < 0) {
                sink.put(b);
            } else {
                sink.putAscii((char) b);
            }
        }
        int res = Utf8s.utf8CharDecode(sink);
        boolean eq = x.charAt(0) == (char) Numbers.decodeHighShort(res);
        Assert.assertTrue(failExpected != eq);
    }

    private boolean copyToSinkWithTextUtil(StringSink query, String text, boolean doubleQuoteParse) {
        byte[] bytes = text.getBytes(Files.UTF_8);
        long ptr = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < bytes.length; i++) {
            Unsafe.getUnsafe().putByte(ptr + i, bytes[i]);
        }

        boolean res;
        if (doubleQuoteParse) {
            res = Utf8s.utf8ToUtf16EscConsecutiveQuotes(ptr, ptr + bytes.length, query);
        } else {
            res = Utf8s.utf8ToUtf16(ptr, ptr + bytes.length, query);
        }
        Unsafe.free(ptr, bytes.length, MemoryTag.NATIVE_DEFAULT);
        return res;
    }
}
