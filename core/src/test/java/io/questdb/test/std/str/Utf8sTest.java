/*+*****************************************************************************
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
import io.questdb.griffin.engine.functions.str.TrimType;
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
    public void testDirectUtf8ToUtf16AsciiPath() {
        try (DirectUtf8Sink utf8Sink = new DirectUtf8Sink(16)) {
            utf8Sink.put("hello");
            Assert.assertTrue(utf8Sink.isAscii());
            StringSink utf16Sink = new StringSink();
            CharSequence result = Utf8s.directUtf8ToUtf16(utf8Sink, utf16Sink);
            Assert.assertEquals("hello", result.toString());
            // ASCII path returns the asAsciiCharSequence directly
            Assert.assertSame(utf8Sink.asAsciiCharSequence(), result);
        }
    }

    @Test
    public void testDoubleQuotedTextBySingleQuoteParsing() {
        StringSink query = new StringSink();

        String text = "select count(*) from \"\"file.csv\"\" abcd";
        Assert.assertTrue(copyToSinkWithTextUtil(query, text, false));

        Assert.assertEquals(text, query.toString());
    }

    @Test
    public void testDoubleQuotedTextBySingleQuoteParsingWithMultiByte() {
        // Exercises utf8ToUtf16EscConsecutiveQuotes with non-ASCII (multi-byte) input
        StringSink query = new StringSink();
        String text = "select * from \"\"фубар\"\" abcd";
        byte[] bytes = text.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        long ptr = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < bytes.length; i++) {
            Unsafe.getUnsafe().putByte(ptr + i, bytes[i]);
        }
        try {
            Assert.assertTrue(Utf8s.utf8ToUtf16EscConsecutiveQuotes(ptr, ptr + bytes.length, query));
            Assert.assertEquals(text.replace("\"\"", "\""), query.toString());
        } finally {
            Unsafe.free(ptr, bytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDoubleQuotedTextParsing() {
        StringSink query = new StringSink();

        String text = "select count(*) from \"\"file.csv\"\" abcd";
        Assert.assertTrue(copyToSinkWithTextUtil(query, text, true));

        Assert.assertEquals(text.replace("\"\"", "\""), query.toString());
    }

    @Test
    public void testEncodeUtf16CharSurrogateErrors() {
        Utf8StringSink sink = new Utf8StringSink();

        // Lone high surrogate at end of input => '?'
        // encodeUtf16Char is called with i already past the current char
        sink.clear();
        int pos = Utf8s.encodeUtf16Char(sink, "\uD83D", 1, 1, '\uD83D');
        Assert.assertEquals(1, pos);
        Assert.assertEquals("?", sink.toString());

        // High surrogate followed by non-low surrogate => '?'
        sink.clear();
        pos = Utf8s.encodeUtf16Char(sink, "\uD83Dx", 2, 1, '\uD83D');
        Assert.assertEquals(2, pos); // consumed up to position 2 (read 'x' at pos=1, rejected it)
        Assert.assertEquals("?", sink.toString());

        // Lone low surrogate => '?'
        sink.clear();
        pos = Utf8s.encodeUtf16Char(sink, "\uDE00b", 2, 1, '\uDE00');
        Assert.assertEquals(1, pos);
        Assert.assertEquals("?", sink.toString());
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

        // Lone high surrogate at end of string => '?'
        sink.clear();
        Assert.assertTrue(Utf8s.encodeUtf16WithLimit(sink, "a\uD83D", 10));
        TestUtils.assertEquals("a?", sink);

        // High surrogate followed by non-low surrogate => '?'
        // Note: the non-low surrogate char is consumed but not encoded
        sink.clear();
        Assert.assertTrue(Utf8s.encodeUtf16WithLimit(sink, "\uD83Dx", 10));
        TestUtils.assertEquals("?", sink);

        // Lone low surrogate => '?'
        sink.clear();
        Assert.assertTrue(Utf8s.encodeUtf16WithLimit(sink, "\uDE00b", 10));
        TestUtils.assertEquals("?b", sink);

        // Invalid surrogate with limited budget for the '?'
        sink.clear();
        Assert.assertFalse(Utf8s.encodeUtf16WithLimit(sink, "a\uD83D", 1));
        TestUtils.assertEquals("a", sink);
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

        // Long suffix (>= 8 bytes) that differs — exercises equalSuffixBytes longAt mismatch
        Assert.assertFalse(Utf8s.endsWith(utf8("abcdefghijklmnop"), utf8("ijklmnox")));
        Assert.assertTrue(Utf8s.endsWith(utf8("abcdefghijklmnop"), utf8("ijklmnop")));
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
    public void testEqualsAsciiPointer() {
        byte[] data = "hello".getBytes(StandardCharsets.UTF_8);
        long mem = Unsafe.malloc(data.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < data.length; i++) {
                Unsafe.getUnsafe().putByte(mem + i, data[i]);
            }
            Assert.assertTrue(Utf8s.equalsAscii("hello", mem, mem + data.length));
            Assert.assertFalse(Utf8s.equalsAscii("world", mem, mem + data.length));
            Assert.assertFalse(Utf8s.equalsAscii("hell", mem, mem + data.length));
            Assert.assertFalse(Utf8s.equalsAscii("helloo", mem, mem + data.length));
        } finally {
            Unsafe.free(mem, data.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEqualsDirectSink() {
        try (DirectUtf8Sink sink = new DirectUtf8Sink(16)) {
            sink.put("hello");
            byte[] data = "hello".getBytes(StandardCharsets.UTF_8);
            long mem = Unsafe.malloc(data.length, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < data.length; i++) {
                    Unsafe.getUnsafe().putByte(mem + i, data[i]);
                }
                Assert.assertTrue(Utf8s.equals(sink, mem, data.length));
                Assert.assertFalse(Utf8s.equals(sink, mem, data.length - 1));
            } finally {
                Unsafe.free(mem, data.length, MemoryTag.NATIVE_DEFAULT);
            }
        }
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
    public void testEqualsIgnoreCaseAsciiRangeEdgeCases() {
        Utf8Sequence s = utf8("abcdef");
        // Identity check
        Assert.assertTrue(Utf8s.equalsIgnoreCaseAscii(s, 0, 3, s, 0, 3));
        // Length mismatch
        Utf8Sequence t = utf8("abc");
        Assert.assertFalse(Utf8s.equalsIgnoreCaseAscii(s, 0, 3, t, 0, 2));
    }

    @Test
    public void testEqualsLongStrings() {
        // Exercises dataEquals long-at-a-time loop and int/short/byte fallback
        // 14 bytes = 6 prefix + 8 data (one full long comparison)
        Assert.assertTrue(Utf8s.equals(utf8("abcdefghijklmn"), utf8("abcdefghijklmn")));
        Assert.assertFalse(Utf8s.equals(utf8("abcdefghijklmn"), utf8("abcdefghijklmo")));

        // 22 bytes = 6 prefix + 16 data (two long comparisons)
        Assert.assertTrue(Utf8s.equals(utf8("abcdefghijklmnopqrstuv"), utf8("abcdefghijklmnopqrstuv")));
        Assert.assertFalse(Utf8s.equals(utf8("abcdefghijklmnopqrstuv"), utf8("abcdefghijklmnopqrstuw")));

        // 10 bytes = 6 prefix + 4 data (int comparison, no full long)
        Assert.assertTrue(Utf8s.equals(utf8("abcdefghij"), utf8("abcdefghij")));
        Assert.assertFalse(Utf8s.equals(utf8("abcdefghij"), utf8("abcdefghik")));

        // 8 bytes = 6 prefix + 2 data (short comparison)
        Assert.assertTrue(Utf8s.equals(utf8("abcdefgh"), utf8("abcdefgh")));
        Assert.assertFalse(Utf8s.equals(utf8("abcdefgh"), utf8("abcdefgi")));

        // 7 bytes = 6 prefix + 1 data (byte comparison)
        Assert.assertTrue(Utf8s.equals(utf8("abcdefg"), utf8("abcdefg")));
        Assert.assertFalse(Utf8s.equals(utf8("abcdefg"), utf8("abcdefh")));
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
    public void testEqualsNullCases() {
        Utf8Sequence abc = utf8("abc");
        // Both null
        Assert.assertTrue(Utf8s.equals(null, (@org.jetbrains.annotations.Nullable Utf8Sequence) null));
        // One null
        Assert.assertFalse(Utf8s.equals(abc, null));
        Assert.assertFalse(Utf8s.equals(null, abc));
    }

    @Test
    public void testEqualsRangeEdgeCases() {
        Utf8Sequence s = utf8("abcdef");
        Utf8Sequence t = utf8("abc");
        // Identity check
        Assert.assertTrue(Utf8s.equals(s, 0, 3, s, 0, 3));
        // Length mismatch
        Assert.assertFalse(Utf8s.equals(s, 0, 3, t, 0, 2));
    }

    @Test
    public void testEqualsRangeLengthMismatch() {
        Utf8Sequence s = utf8("abc");
        // Length mismatch via equalsAscii(CharSequence, Utf8Sequence, rLo, rHi)
        Assert.assertFalse(Utf8s.equalsAscii("ab", s, 0, 3));
        // Length mismatch via equalsAscii(CharSequence, lLo, lHi, Utf8Sequence, rLo, rHi)
        Assert.assertFalse(Utf8s.equalsAscii("abcdef", 0, 2, s, 0, 3));
    }

    @Test
    public void testEqualsSixPrefix() {
        Utf8String a = utf8("abcdefgh");
        Utf8String b = utf8("abcdefgh");
        Utf8String c = utf8("abcdexgh");
        long aPrefix = a.zeroPaddedSixPrefix();
        long bPrefix = b.zeroPaddedSixPrefix();
        long cPrefix = c.zeroPaddedSixPrefix();

        Assert.assertTrue(Utf8s.equals(a, aPrefix, b, bPrefix));
        Assert.assertFalse(Utf8s.equals(a, aPrefix, c, cPrefix));
    }

    @Test
    public void testEqualsUtf16NcNull() {
        Assert.assertTrue(Utf8s.equalsUtf16Nc(null, null));
        Assert.assertFalse(Utf8s.equalsUtf16Nc("abc", null));
        Assert.assertFalse(Utf8s.equalsUtf16Nc(null, utf8("abc")));
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
    public void testGreaterThan() {
        // Null cases
        Assert.assertFalse(Utf8s.greaterThan(null, null));
        Assert.assertFalse(Utf8s.greaterThan(null, utf8("a")));
        Assert.assertFalse(Utf8s.greaterThan(utf8("a"), null));

        // Equal strings
        Assert.assertFalse(Utf8s.greaterThan(utf8("abc"), utf8("abc")));

        // Greater by byte value
        Assert.assertTrue(Utf8s.greaterThan(utf8("abd"), utf8("abc")));
        Assert.assertFalse(Utf8s.greaterThan(utf8("abc"), utf8("abd")));

        // Greater by length
        Assert.assertTrue(Utf8s.greaterThan(utf8("abcd"), utf8("abc")));
        Assert.assertFalse(Utf8s.greaterThan(utf8("abc"), utf8("abcd")));

        // Empty strings
        Assert.assertFalse(Utf8s.greaterThan(utf8(""), utf8("")));
        Assert.assertTrue(Utf8s.greaterThan(utf8("a"), utf8("")));
        Assert.assertFalse(Utf8s.greaterThan(utf8(""), utf8("a")));
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
    public void testHashCodeEmpty() {
        Assert.assertEquals(0, Utf8s.hashCode(Utf8String.EMPTY));
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
    public void testIndexOfAsciiCharOccurrence() {
        Utf8String seq = utf8("abcabcabc");
        // occurrence = 0 returns -1
        Assert.assertEquals(-1, Utf8s.indexOfAscii(seq, 0, seq.size(), 'a', 0));

        // Forward search: 1st, 2nd, 3rd occurrence of 'a'
        Assert.assertEquals(0, Utf8s.indexOfAscii(seq, 0, seq.size(), 'a', 1));
        Assert.assertEquals(3, Utf8s.indexOfAscii(seq, 0, seq.size(), 'a', 2));
        Assert.assertEquals(6, Utf8s.indexOfAscii(seq, 0, seq.size(), 'a', 3));
        // 4th occurrence doesn't exist
        Assert.assertEquals(-1, Utf8s.indexOfAscii(seq, 0, seq.size(), 'a', 4));

        // Reverse search: -1 = last, -2 = second-to-last
        Assert.assertEquals(6, Utf8s.indexOfAscii(seq, 0, seq.size(), 'a', -1));
        Assert.assertEquals(3, Utf8s.indexOfAscii(seq, 0, seq.size(), 'a', -2));
        Assert.assertEquals(0, Utf8s.indexOfAscii(seq, 0, seq.size(), 'a', -3));
        Assert.assertEquals(-1, Utf8s.indexOfAscii(seq, 0, seq.size(), 'a', -4));

        // Char not found
        Assert.assertEquals(-1, Utf8s.indexOfAscii(seq, 0, seq.size(), 'z', 1));
        Assert.assertEquals(-1, Utf8s.indexOfAscii(seq, 0, seq.size(), 'z', -1));
    }

    @Test
    public void testIndexOfAsciiTermOccurrence() {
        Utf8String seq = utf8("foo bar foo baz foo");

        // occurrence = 0 returns -1
        Assert.assertEquals(-1, Utf8s.indexOfAscii(seq, 0, seq.size(), "foo", 0));

        // Forward: 1st, 2nd, 3rd
        Assert.assertEquals(0, Utf8s.indexOfAscii(seq, 0, seq.size(), "foo", 1));
        Assert.assertEquals(8, Utf8s.indexOfAscii(seq, 0, seq.size(), "foo", 2));
        Assert.assertEquals(16, Utf8s.indexOfAscii(seq, 0, seq.size(), "foo", 3));
        Assert.assertEquals(-1, Utf8s.indexOfAscii(seq, 0, seq.size(), "foo", 4));

        // Reverse: -1 = last, -2 = second-to-last
        Assert.assertEquals(16, Utf8s.indexOfAscii(seq, 0, seq.size(), "foo", -1));
        Assert.assertEquals(8, Utf8s.indexOfAscii(seq, 0, seq.size(), "foo", -2));
        Assert.assertEquals(0, Utf8s.indexOfAscii(seq, 0, seq.size(), "foo", -3));
        Assert.assertEquals(-1, Utf8s.indexOfAscii(seq, 0, seq.size(), "foo", -4));

        // Empty term returns -1 (not 0, unlike the non-occurrence overload)
        Assert.assertEquals(-1, Utf8s.indexOfAscii(seq, 0, seq.size(), "", 1));

        // Term not found
        Assert.assertEquals(-1, Utf8s.indexOfAscii(seq, 0, seq.size(), "xyz", 1));
        Assert.assertEquals(-1, Utf8s.indexOfAscii(seq, 0, seq.size(), "xyz", -1));

        // Term longer than remaining sequence
        Assert.assertEquals(-1, Utf8s.indexOfAscii(seq, 17, seq.size(), "foo", 1));
        Assert.assertEquals(-1, Utf8s.indexOfAscii(seq, 0, 2, "foo", -1));

        // Partial match then mismatch in forward search — exercises the backtracking branch
        // "abYabX": matching "abX", first "ab" matches but 'Y' != 'X' triggers backtrack
        Utf8String seq2 = utf8("abYabX");
        Assert.assertEquals(3, Utf8s.indexOfAscii(seq2, 0, seq2.size(), "abX", 1));

        // Partial match then mismatch in reverse search
        Utf8String seq3 = utf8("abXabY");
        Assert.assertEquals(0, Utf8s.indexOfAscii(seq3, 0, seq3.size(), "abX", -1));
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
    public void testIndexOfOccurrence() {
        Utf8String seq = utf8("foo bar foo baz foo");

        // occurrence = 0 returns -1
        Assert.assertEquals(-1, Utf8s.indexOf(seq, 0, seq.size(), utf8("foo"), 0));

        // Forward search
        Assert.assertEquals(0, Utf8s.indexOf(seq, 0, seq.size(), utf8("foo"), 1));
        Assert.assertEquals(8, Utf8s.indexOf(seq, 0, seq.size(), utf8("foo"), 2));
        Assert.assertEquals(16, Utf8s.indexOf(seq, 0, seq.size(), utf8("foo"), 3));
        Assert.assertEquals(-1, Utf8s.indexOf(seq, 0, seq.size(), utf8("foo"), 4));

        // Reverse search
        Assert.assertEquals(16, Utf8s.indexOf(seq, 0, seq.size(), utf8("foo"), -1));
        Assert.assertEquals(8, Utf8s.indexOf(seq, 0, seq.size(), utf8("foo"), -2));
        Assert.assertEquals(0, Utf8s.indexOf(seq, 0, seq.size(), utf8("foo"), -3));
        Assert.assertEquals(-1, Utf8s.indexOf(seq, 0, seq.size(), utf8("foo"), -4));

        // Empty term returns 0
        Assert.assertEquals(0, Utf8s.indexOf(seq, 0, seq.size(), Utf8String.EMPTY, 1));

        // Not found
        Assert.assertEquals(-1, Utf8s.indexOf(seq, 0, seq.size(), utf8("xyz"), 1));
        Assert.assertEquals(-1, Utf8s.indexOf(seq, 0, seq.size(), utf8("xyz"), -1));
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
    public void testIsAsciiTrailingNonAscii() {
        // Non-ASCII byte in the trailing bytes after the 8-byte loop
        // 9 bytes: 8 ASCII + 1 non-ASCII in the tail
        try (DirectUtf8Sink sink = new DirectUtf8Sink(16)) {
            sink.put("12345678");
            sink.put((byte) 0x80);
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
    public void testLength() {
        // Null
        Assert.assertEquals(-1, Utf8s.length(null));

        // Empty
        Assert.assertEquals(0, Utf8s.length(Utf8String.EMPTY));

        // Pure ASCII (fast path)
        Assert.assertEquals(5, Utf8s.length(utf8("hello")));

        // 2-byte chars (Cyrillic)
        Assert.assertEquals(6, Utf8s.length(utf8("привет")));

        // 3-byte chars (CJK)
        Assert.assertEquals(2, Utf8s.length(utf8("你好")));

        // 4-byte chars (emoji, surrogate pair)
        Utf8StringSink emojiSink = new Utf8StringSink();
        emojiSink.put("\uD83D\uDE00"); // 4 UTF-8 bytes, 1 codepoint
        Assert.assertEquals(1, Utf8s.length(emojiSink));

        // Mixed: "Aé世" = 3 codepoints (1 + 1 + 1)
        Assert.assertEquals(3, Utf8s.length(utf8("Aé世")));

        // Long string to exercise the 8-byte SWAR loop
        // 20 Cyrillic chars = 40 UTF-8 bytes, 20 codepoints
        Assert.assertEquals(20, Utf8s.length(utf8("абвгдежзиклмнопрстуф")));
    }

    @Test
    public void testLessThan() {
        // Null cases
        Assert.assertFalse(Utf8s.lessThan(null, null));
        Assert.assertFalse(Utf8s.lessThan(null, utf8("a")));
        Assert.assertFalse(Utf8s.lessThan(utf8("a"), null));

        // Equal
        Assert.assertFalse(Utf8s.lessThan(utf8("abc"), utf8("abc")));

        // Less by byte value
        Assert.assertTrue(Utf8s.lessThan(utf8("abc"), utf8("abd")));
        Assert.assertFalse(Utf8s.lessThan(utf8("abd"), utf8("abc")));

        // Less by length
        Assert.assertTrue(Utf8s.lessThan(utf8("abc"), utf8("abcd")));
        Assert.assertFalse(Utf8s.lessThan(utf8("abcd"), utf8("abc")));

        // Empty
        Assert.assertFalse(Utf8s.lessThan(utf8(""), utf8("")));
        Assert.assertTrue(Utf8s.lessThan(utf8(""), utf8("a")));
        Assert.assertFalse(Utf8s.lessThan(utf8("a"), utf8("")));
    }

    @Test
    public void testLessThanNegated() {
        // negated=false => strict less-than
        Assert.assertTrue(Utf8s.lessThan(utf8("abc"), utf8("abd"), false));
        Assert.assertFalse(Utf8s.lessThan(utf8("abc"), utf8("abc"), false));
        Assert.assertFalse(Utf8s.lessThan(utf8("abd"), utf8("abc"), false));

        // negated=true => greater-than-or-equal (>=)
        Assert.assertFalse(Utf8s.lessThan(utf8("abc"), utf8("abd"), true));
        Assert.assertTrue(Utf8s.lessThan(utf8("abc"), utf8("abc"), true)); // equal => true
        Assert.assertTrue(Utf8s.lessThan(utf8("abd"), utf8("abc"), true)); // greater => true
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
    public void testLowerCaseAsciiHashCodeEmpty() {
        Assert.assertEquals(0, Utf8s.lowerCaseAsciiHashCode(Utf8String.EMPTY));
    }

    @Test
    public void testPutQuotedEscapedStr() {
        try (DirectUtf8Sink sink = new DirectUtf8Sink(64)) {
            sink.putQuotedEscapedStr("hello");
            Assert.assertEquals("\"hello\"", sink.toString());

            sink.clear();
            sink.putQuotedEscapedStr("say \"hi\"");
            Assert.assertEquals("\"say \\\"hi\\\"\"", sink.toString());

            sink.clear();
            sink.putQuotedEscapedStr("back\\slash");
            Assert.assertEquals("\"back\\\\slash\"", sink.toString());

            sink.clear();
            sink.putQuotedEscapedStr("tab\there");
            Assert.assertEquals("\"tab\\there\"", sink.toString());

            sink.clear();
            sink.putQuotedEscapedStr("new\nline");
            Assert.assertEquals("\"new\\nline\"", sink.toString());

            sink.clear();
            sink.putQuotedEscapedStr("");
            Assert.assertEquals("\"\"", sink.toString());

            sink.clear();
            sink.putQuotedEscapedStr("table~1");
            Assert.assertEquals("\"table~1\"", sink.toString());
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
    public void testPutSafeInvalidFourPlusBytesFromSequence() {
        // Invalid leading byte (0xF8 = 5-byte, not valid UTF-8) followed by 3 continuation bytes
        // Exercises putInvalidBytes(Utf8Sequence) with 4 invalid bytes and also the
        // putUpTo4BytesSafe fallback with hi - lo > 3 for the Utf8Sequence path
        Utf8StringSink source = new Utf8StringSink();
        source.putAny((byte) 0xF8);
        source.putAny((byte) 0x80);
        source.putAny((byte) 0x80);
        source.putAny((byte) 0x80);
        source.putAny((byte) 0x80);

        Utf8StringSink sink = new Utf8StringSink();
        Utf8s.putSafe(source, sink);
        Assert.assertEquals("\\xF8\\x80\\x80\\x80\\x80", sink.toString());
    }

    @Test
    public void testPutSafeTruncatedLeadBytes() {
        // Lone 3-byte lead byte (0xE4) at end of input — hi - lo == 1
        // Exercises putUpTo3BytesSafe returning 1 (both pointer and sequence paths)
        Utf8StringSink source = new Utf8StringSink();
        Utf8StringSink sink = new Utf8StringSink();
        source.putAny((byte) 0xE4);
        Utf8s.putSafe(source, sink);
        Assert.assertEquals("\\xE4", sink.toString());

        long buf = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(buf, (byte) 0xE4);
            sink.clear();
            Utf8s.putSafe(buf, buf + 1, sink);
            Assert.assertEquals("\\xE4", sink.toString());
        } finally {
            Unsafe.free(buf, 4, MemoryTag.NATIVE_DEFAULT);
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
                Assert.assertEquals(testStr, actual);
            }
        } finally {
            Unsafe.free(buf, 128, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testPutSafeValidFourByteSequence() {
        // Valid 4-byte UTF-8 sequence (emoji) via putSafe - exercises put4ByteSafe valid path.
        // put4ByteSafe emits highSurrogate/lowSurrogate chars, but Utf8Sink.put(char) replaces
        // lone surrogates with '?', so the output is "??" — this is expected behavior.
        try (DirectUtf8Sink source = new DirectUtf8Sink(8)) {
            source.put("\uD83D\uDE00"); // U+1F600 grinning face: F0 9F 98 80

            Utf8StringSink sink = new Utf8StringSink();
            Utf8s.putSafe(source, sink);
            Assert.assertEquals("??", sink.toString());

            // Also via pointer path
            sink.clear();
            Utf8s.putSafe(source.lo(), source.hi(), sink);
            Assert.assertEquals("??", sink.toString());
        }
    }

    @Test
    public void testPutSafeValidFromSequence() {
        // Exercises putSafe(Utf8Sequence, Utf8Sink) with valid printable ASCII
        // (covers the !Character.isISOControl(c) branch for the Utf8Sequence overload)
        final Utf8StringSink source = new Utf8StringSink();
        final Utf8StringSink sink = new Utf8StringSink();

        source.put("hello");
        Utf8s.putSafe(source, sink);
        Assert.assertEquals("hello", sink.toString());

        // Mixed printable ASCII and valid multi-byte
        source.clear();
        sink.clear();
        source.put("abcфубар");
        Utf8s.putSafe(source, sink);
        Assert.assertEquals("abcфубар", sink.toString());
    }

    @Test
    public void testPutSafeWithAsciiAfterInvalidByte() {
        // Invalid leading byte (0xFE doesn't match any valid 2/3/4-byte lead pattern)
        // followed by a continuation byte then an ASCII byte.
        // Exercises putInvalidBytes(Utf8Sequence) early-break-on-positive-byte branch.
        Utf8StringSink source = new Utf8StringSink();
        Utf8StringSink sink = new Utf8StringSink();

        source.putAny((byte) 0xFE);
        source.putAny((byte) 0x80); // continuation byte
        source.putAscii('A');       // ASCII byte terminates the invalid run
        Utf8s.putSafe(source, sink);
        Assert.assertEquals("\\xFE\\x80A", sink.toString());
    }

    @Test
    public void testPutSafeWithControlCharacters() {
        // Test that ASCII control characters (e.g., \0, \t, \n) are rendered as hex
        final Utf8StringSink source = new Utf8StringSink();
        final Utf8StringSink sink = new Utf8StringSink();

        // Control char via Utf8Sequence path
        source.putAny((byte) 0x01); // SOH control character
        Utf8s.putSafe(source, sink);
        Assert.assertEquals("\\x01", sink.toString());

        // Control char via pointer path
        long buf = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(buf, (byte) 0x01);
            sink.clear();
            Utf8s.putSafe(buf, buf + 1, sink);
            Assert.assertEquals("\\x01", sink.toString());
        } finally {
            Unsafe.free(buf, 4, MemoryTag.NATIVE_DEFAULT);
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

                if (!sink.isEmpty()) {
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
    public void testStrCpySubstringInvalidUtf8() {
        // Invalid multi-byte in non-ASCII sequence — exercises strCpyNonAscii error return
        Utf8StringSink src = new Utf8StringSink();
        src.putAny((byte) 0xC0); // overlong lead
        src.putAny((byte) 0x80);
        Assert.assertFalse(src.isAscii());

        Utf8StringSink dest = new Utf8StringSink();
        Assert.assertEquals(-1, Utf8s.strCpy(src, 0, 1, dest));
    }

    @Test
    public void testStrCpySubstringNonAsciiWithAsciiBytes() {
        // Mixed sequence where isAscii()=false but has ASCII bytes,
        // exercises strCpyNonAscii's ASCII byte handling path
        Utf8StringSink src = new Utf8StringSink();
        src.put("aы"); // 'a' (1 byte) + 'ы' (2 bytes), isAscii=false
        Assert.assertFalse(src.isAscii());

        Utf8StringSink dest = new Utf8StringSink();
        // Copy first character only (the ASCII 'a')
        Assert.assertEquals(1, Utf8s.strCpy(src, 0, 1, dest));
        Assert.assertEquals("a", dest.toString());

        // Skip first char, copy second — exercises charPos < charLo for ASCII byte path
        dest.clear();
        Assert.assertEquals(2, Utf8s.strCpy(src, 1, 2, dest));
        Assert.assertEquals("ы", dest.toString());
    }

    @Test
    public void testStrCpyUtf8() {
        final int bufSize = 64;
        long mem = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
        try {
            // empty string
            Assert.assertEquals(0, Utf8s.strCpyUtf8("", mem, bufSize));

            // maxBytes = 0
            Assert.assertEquals(0, Utf8s.strCpyUtf8("hello", mem, 0));

            // pure ASCII
            int n = Utf8s.strCpyUtf8("hello", mem, bufSize);
            Assert.assertEquals(5, n);
            Assert.assertEquals("hello", readUtf8(mem, n));

            // 2-byte UTF-8 (Latin, Cyrillic, etc.)
            n = Utf8s.strCpyUtf8("éü", mem, bufSize); // é ü
            Assert.assertEquals(4, n);
            Assert.assertEquals("éü", readUtf8(mem, n));

            // 3-byte UTF-8 (CJK, etc.)
            n = Utf8s.strCpyUtf8("世界", mem, bufSize); // 世界
            Assert.assertEquals(6, n);
            Assert.assertEquals("世界", readUtf8(mem, n));

            // 4-byte UTF-8 (surrogate pair: emoji U+1F600)
            String emoji = "\uD83D\uDE00";
            n = Utf8s.strCpyUtf8(emoji, mem, bufSize);
            Assert.assertEquals(4, n);
            Assert.assertEquals(emoji, readUtf8(mem, n));

            // lone high surrogate replaced with '?'
            n = Utf8s.strCpyUtf8("\uD83Da", mem, bufSize);
            Assert.assertEquals(2, n);
            Assert.assertEquals("?a", readUtf8(mem, n));

            // lone low surrogate replaced with '?'
            n = Utf8s.strCpyUtf8("\uDE00b", mem, bufSize);
            Assert.assertEquals(2, n);
            Assert.assertEquals("?b", readUtf8(mem, n));

            // mixed: ASCII + 2-byte + 3-byte + 4-byte
            String mixed = "Aé世\uD83D\uDE00";
            n = Utf8s.strCpyUtf8(mixed, mem, bufSize);
            Assert.assertEquals(1 + 2 + 3 + 4, n);
            Assert.assertEquals(mixed, readUtf8(mem, n));

            // truncation: ASCII at boundary
            n = Utf8s.strCpyUtf8("abcdef", mem, 3);
            Assert.assertEquals(3, n);
            Assert.assertEquals("abc", readUtf8(mem, n));

            // truncation: 2-byte char doesn't fit
            n = Utf8s.strCpyUtf8("aéb", mem, 2);
            Assert.assertEquals(1, n);
            Assert.assertEquals("a", readUtf8(mem, n));

            // truncation: 3-byte char doesn't fit
            n = Utf8s.strCpyUtf8("a世b", mem, 3);
            Assert.assertEquals(1, n);
            Assert.assertEquals("a", readUtf8(mem, n));

            // truncation: 4-byte char (surrogate pair) doesn't fit
            n = Utf8s.strCpyUtf8("a\uD83D\uDE00b", mem, 4);
            Assert.assertEquals(1, n);
            Assert.assertEquals("a", readUtf8(mem, n));

            // truncation: 2-byte char fits exactly
            n = Utf8s.strCpyUtf8("aé", mem, 3);
            Assert.assertEquals(3, n);
            Assert.assertEquals("aé", readUtf8(mem, n));

            // truncation: 3-byte char fits exactly
            n = Utf8s.strCpyUtf8("a世", mem, 4);
            Assert.assertEquals(4, n);
            Assert.assertEquals("a世", readUtf8(mem, n));

            // truncation: 4-byte char fits exactly
            n = Utf8s.strCpyUtf8("a\uD83D\uDE00", mem, 5);
            Assert.assertEquals(5, n);
            Assert.assertEquals("a\uD83D\uDE00", readUtf8(mem, n));
        } finally {
            Unsafe.free(mem, bufSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testStringFromUtf8BytesInvalidNonDirect() {
        // Invalid UTF-8 in a non-DirectUtf8Sequence (Utf8StringSink is heap-based)
        // This covers the else branch at line 1256 of Utf8s.java
        Utf8StringSink invalid = new Utf8StringSink();
        invalid.putAny((byte) 0xC0);
        invalid.putAny((byte) 0x80); // overlong 2-byte => invalid
        try {
            Utf8s.stringFromUtf8Bytes(invalid);
            Assert.fail("expected CairoException");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "cannot convert invalid UTF-8 sequence to UTF-16");
        }
    }

    @Test
    public void testStringFromUtf8BytesSafe() {
        // Empty
        Assert.assertEquals("", Utf8s.stringFromUtf8BytesSafe(Utf8String.EMPTY));

        // Valid ASCII
        Assert.assertEquals("hello", Utf8s.stringFromUtf8BytesSafe(utf8("hello")));

        // Valid non-ASCII
        Assert.assertEquals("привет", Utf8s.stringFromUtf8BytesSafe(utf8("привет")));

        // Invalid UTF-8 doesn't throw (unlike stringFromUtf8Bytes)
        Utf8StringSink invalid = new Utf8StringSink();
        invalid.putAny((byte) 0x80);
        invalid.putAny((byte) 0xFF);
        // Should not throw; the result is best-effort
        Utf8s.stringFromUtf8BytesSafe(invalid);
    }

    @Test
    public void testStrpos() {
        // Empty needle => 1
        Assert.assertEquals(1, Utf8s.strpos(utf8("hello"), Utf8String.EMPTY));
        Assert.assertEquals(1, Utf8s.strpos(Utf8String.EMPTY, Utf8String.EMPTY));

        // Empty haystack with non-empty needle => 0
        Assert.assertEquals(0, Utf8s.strpos(Utf8String.EMPTY, utf8("a")));

        // ASCII: 1-based position
        Assert.assertEquals(1, Utf8s.strpos(utf8("hello"), utf8("h")));
        Assert.assertEquals(2, Utf8s.strpos(utf8("hello"), utf8("ell")));
        Assert.assertEquals(5, Utf8s.strpos(utf8("hello"), utf8("o")));
        Assert.assertEquals(0, Utf8s.strpos(utf8("hello"), utf8("xyz")));

        // Multi-byte: position counts codepoints, not bytes
        // "привет" = 6 codepoints (each 2 UTF-8 bytes)
        Assert.assertEquals(1, Utf8s.strpos(utf8("привет"), utf8("п")));
        Assert.assertEquals(4, Utf8s.strpos(utf8("привет"), utf8("в")));
        Assert.assertEquals(0, Utf8s.strpos(utf8("привет"), utf8("ы")));
    }

    @Test
    public void testToString() {
        Assert.assertNull(Utf8s.toString(null));
        Assert.assertEquals("hello", Utf8s.toString(utf8("hello")));
        Assert.assertEquals("", Utf8s.toString(Utf8String.EMPTY));
    }

    @Test
    public void testToStringUnescape() {
        // toString(seq, start, end, unescapeAscii) removes doubled ASCII chars
        Utf8String seq = utf8("he''llo");
        String result = Utf8s.toString(seq, 0, seq.size(), (byte) '\'');
        Assert.assertEquals("he'llo", result);

        // No doubled chars
        Utf8String seq2 = utf8("hello");
        Assert.assertEquals("hello", Utf8s.toString(seq2, 0, seq2.size(), (byte) '\''));

        // Doubled at end
        Utf8String seq3 = utf8("hello''");
        Assert.assertEquals("hello'", Utf8s.toString(seq3, 0, seq3.size(), (byte) '\''));

        // Subrange
        Utf8String seq4 = utf8("xx''yy");
        Assert.assertEquals("'yy", Utf8s.toString(seq4, 2, seq4.size(), (byte) '\''));
    }

    @Test
    public void testToUtf8String() {
        Assert.assertNull(Utf8s.toUtf8String(null));
        Utf8String s = utf8("hello");
        Utf8String result = Utf8s.toUtf8String(s);
        Assert.assertNotNull(result);
        Assert.assertEquals("hello", result.toString());

        Utf8String cyrillic = utf8("мир");
        result = Utf8s.toUtf8String(cyrillic);
        Assert.assertNotNull(result);
        Assert.assertEquals("мир", result.toString());
    }

    @Test
    public void testTrim() {
        Utf8StringSink sink = new Utf8StringSink();

        // Null source
        Utf8s.trim(TrimType.TRIM, null, sink);
        Assert.assertEquals("", sink.toString());

        // Empty source
        sink.clear();
        Utf8s.trim(TrimType.TRIM, Utf8String.EMPTY, sink);
        Assert.assertEquals("", sink.toString());

        // TRIM (both sides)
        sink.clear();
        Utf8s.trim(TrimType.TRIM, utf8("  hello  "), sink);
        Assert.assertEquals("hello", sink.toString());

        // LTRIM (left only)
        sink.clear();
        Utf8s.trim(TrimType.LTRIM, utf8("  hello  "), sink);
        Assert.assertEquals("hello  ", sink.toString());

        // RTRIM (right only)
        sink.clear();
        Utf8s.trim(TrimType.RTRIM, utf8("  hello  "), sink);
        Assert.assertEquals("  hello", sink.toString());

        // No spaces to trim
        sink.clear();
        Utf8s.trim(TrimType.TRIM, utf8("hello"), sink);
        Assert.assertEquals("hello", sink.toString());

        // All spaces
        sink.clear();
        Utf8s.trim(TrimType.TRIM, utf8("   "), sink);
        Assert.assertEquals("", sink.toString());
    }

    @Test
    public void testUtf8Bytes() {
        // Pure ASCII
        Assert.assertEquals(5, Utf8s.utf8Bytes("hello"));

        // 2-byte chars (Cyrillic)
        Assert.assertEquals(12, Utf8s.utf8Bytes("привет"));

        // 3-byte chars (CJK)
        Assert.assertEquals(6, Utf8s.utf8Bytes("你好"));

        // 4-byte chars (surrogate pair)
        Assert.assertEquals(4, Utf8s.utf8Bytes("\uD83D\uDE00"));

        // Mixed
        Assert.assertEquals(1 + 2 + 3 + 4, Utf8s.utf8Bytes("Aé世\uD83D\uDE00"));

        // Lone high surrogate => 1 byte ('?')
        Assert.assertEquals(2, Utf8s.utf8Bytes("\uD83Da"));

        // Lone low surrogate => 1 byte ('?')
        Assert.assertEquals(2, Utf8s.utf8Bytes("\uDE00b"));

        // High surrogate at end of string => 1 byte ('?')
        Assert.assertEquals(1, Utf8s.utf8Bytes("\uD83D"));

        // Empty
        Assert.assertEquals(0, Utf8s.utf8Bytes(""));
    }

    @Test
    public void testUtf8BytesWithLimit() {
        // Fits entirely
        Assert.assertEquals(5, Utf8s.utf8Bytes("hello", 10));

        // Limit cuts ASCII
        Assert.assertEquals(3, Utf8s.utf8Bytes("hello", 3));

        // 2-byte char doesn't fit
        Assert.assertEquals(1, Utf8s.utf8Bytes("aé", 2));

        // 2-byte char fits exactly
        Assert.assertEquals(3, Utf8s.utf8Bytes("aé", 3));

        // 3-byte char doesn't fit
        Assert.assertEquals(1, Utf8s.utf8Bytes("a世", 3));

        // 3-byte char fits exactly
        Assert.assertEquals(4, Utf8s.utf8Bytes("a世", 4));

        // 4-byte surrogate pair doesn't fit
        Assert.assertEquals(1, Utf8s.utf8Bytes("a\uD83D\uDE00", 4));

        // 4-byte surrogate pair fits exactly
        Assert.assertEquals(5, Utf8s.utf8Bytes("a\uD83D\uDE00", 5));

        // Lone surrogate
        Assert.assertEquals(1, Utf8s.utf8Bytes("\uD83Da", 1));

        // Zero limit
        Assert.assertEquals(0, Utf8s.utf8Bytes("hello", 0));
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
    public void testUtf8SupportZFourByte() {
        // Test utf8ToUtf16Z with 4-byte UTF-8 (emoji) — exercises utf8Decode4BytesZ
        String input = "\uD83D\uDE00"; // U+1F600
        byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
        int nBytes = bytes.length + 1;
        long mem = Unsafe.malloc(nBytes, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.getUnsafe().putByte(mem + i, bytes[i]);
            }
            Unsafe.getUnsafe().putByte(mem + bytes.length, (byte) 0);

            StringSink b = new StringSink();
            Assert.assertTrue(Utf8s.utf8ToUtf16Z(mem, b));
            TestUtils.assertEquals(input, b.toString());
        } finally {
            Unsafe.free(mem, nBytes, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUtf8SupportZNonAscii() {
        // Test utf8ToUtf16Z with non-ASCII (Cyrillic) null-terminated string
        // Exercises utf8DecodeMultiByteZ / utf8Decode2BytesZ
        String input = "привет";
        byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
        int nBytes = bytes.length + 1;
        long mem = Unsafe.malloc(nBytes, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.getUnsafe().putByte(mem + i, bytes[i]);
            }
            Unsafe.getUnsafe().putByte(mem + bytes.length, (byte) 0);

            StringSink b = new StringSink();
            Assert.assertTrue(Utf8s.utf8ToUtf16Z(mem, b));
            TestUtils.assertEquals(input, b.toString());
        } finally {
            Unsafe.free(mem, nBytes, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUtf8SupportZThreeByte() {
        // Test utf8ToUtf16Z with 3-byte UTF-8 (CJK) — exercises utf8Decode3BytesZ
        String input = "你好世界";
        byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
        int nBytes = bytes.length + 1;
        long mem = Unsafe.malloc(nBytes, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.getUnsafe().putByte(mem + i, bytes[i]);
            }
            Unsafe.getUnsafe().putByte(mem + bytes.length, (byte) 0);

            StringSink b = new StringSink();
            Assert.assertTrue(Utf8s.utf8ToUtf16Z(mem, b));
            TestUtils.assertEquals(input, b.toString());
        } finally {
            Unsafe.free(mem, nBytes, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUtf8ToUtf16DecodeErrors() {
        StringSink utf16Sink = new StringSink();

        // --- Utf8Sequence path (through utf8ToUtf16(Utf8Sequence, Utf16Sink)) ---

        // Truncated 2-byte: lead byte only
        Utf8StringSink seq = new Utf8StringSink();
        seq.putAny((byte) 0xC2); // 2-byte lead, no continuation
        Assert.assertFalse(Utf8s.utf8ToUtf16(seq, utf16Sink));

        // Invalid continuation in 2-byte
        seq.clear();
        utf16Sink.clear();
        seq.putAny((byte) 0xC2);
        seq.putAny((byte) 0x00); // not a continuation byte
        Assert.assertFalse(Utf8s.utf8ToUtf16(seq, utf16Sink));

        // Truncated 3-byte: lead + 1 continuation
        seq.clear();
        utf16Sink.clear();
        seq.putAny((byte) 0xE4);
        seq.putAny((byte) 0xBD);
        Assert.assertFalse(Utf8s.utf8ToUtf16(seq, utf16Sink));

        // Malformed 3-byte: invalid continuation pattern
        seq.clear();
        utf16Sink.clear();
        seq.putAny((byte) 0xE0);
        seq.putAny((byte) 0x80); // overlong
        seq.putAny((byte) 0x80);
        Assert.assertFalse(Utf8s.utf8ToUtf16(seq, utf16Sink));

        // 3-byte encoding of surrogate code point (U+D800)
        seq.clear();
        utf16Sink.clear();
        seq.putAny((byte) 0xED);
        seq.putAny((byte) 0xA0);
        seq.putAny((byte) 0x80);
        Assert.assertFalse(Utf8s.utf8ToUtf16(seq, utf16Sink));

        // Malformed 4-byte: invalid continuation
        seq.clear();
        utf16Sink.clear();
        seq.putAny((byte) 0xF0);
        seq.putAny((byte) 0x28); // not a continuation byte
        seq.putAny((byte) 0x80);
        seq.putAny((byte) 0x80);
        Assert.assertFalse(Utf8s.utf8ToUtf16(seq, utf16Sink));

        // 4-byte encoding producing non-supplementary code point
        seq.clear();
        utf16Sink.clear();
        seq.putAny((byte) 0xF4);
        seq.putAny((byte) 0x90); // code point > U+10FFFF
        seq.putAny((byte) 0x80);
        seq.putAny((byte) 0x80);
        Assert.assertFalse(Utf8s.utf8ToUtf16(seq, utf16Sink));

        // --- Pointer path (through utf8ToUtf16(long, long, Utf16Sink)) ---
        long buf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        try {
            // Truncated 2-byte via pointer
            Unsafe.getUnsafe().putByte(buf, (byte) 0xC2);
            utf16Sink.clear();
            Assert.assertFalse(Utf8s.utf8ToUtf16(buf, buf + 1, utf16Sink));

            // Invalid continuation in 2-byte via pointer
            Unsafe.getUnsafe().putByte(buf, (byte) 0xC2);
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0x00);
            utf16Sink.clear();
            Assert.assertFalse(Utf8s.utf8ToUtf16(buf, buf + 2, utf16Sink));

            // Truncated 3-byte via pointer
            Unsafe.getUnsafe().putByte(buf, (byte) 0xE4);
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0xBD);
            utf16Sink.clear();
            Assert.assertFalse(Utf8s.utf8ToUtf16(buf, buf + 2, utf16Sink));
        } finally {
            Unsafe.free(buf, 8, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUtf8ToUtf16EscConsecutiveQuotesInvalid() {
        // Invalid multi-byte UTF-8 in utf8ToUtf16EscConsecutiveQuotes
        long buf = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(buf, (byte) 0xC0); // overlong
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0x80);
            StringSink sink = new StringSink();
            Assert.assertFalse(Utf8s.utf8ToUtf16EscConsecutiveQuotes(buf, buf + 2, sink));
        } finally {
            Unsafe.free(buf, 4, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUtf8ToUtf16UncheckedError() {
        // Invalid UTF-8 in DirectUtf8Sequence — exercises utf8ToUtf16Unchecked error path
        try (DirectUtf8Sink dirSink = new DirectUtf8Sink(8)) {
            dirSink.put((byte) 0xC0);
            dirSink.put((byte) 0x80);
            StringSink tempSink = new StringSink();
            try {
                Utf8s.utf8ToUtf16Unchecked(dirSink, tempSink);
                Assert.fail("expected CairoException");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid UTF8 in value for");
            }
        }
    }

    @Test
    public void testUtf8ToUtf16WithTerminatorError() {
        // Invalid UTF-8 before terminator — exercises utf8ToUtf16(seq, sink, terminator) error path
        Utf8StringSink seq = new Utf8StringSink();
        seq.putAny((byte) 0xC0); // overlong 2-byte lead (b & 30 == 0)
        seq.putAny((byte) 0x80);
        seq.putAscii(':');

        StringSink sink = new StringSink();
        Assert.assertEquals(-1, Utf8s.utf8ToUtf16(seq, sink, (byte) ':'));
    }

    @Test
    public void testUtf8ToUtf16ZDecodeErrors() {
        long buf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        try {
            StringSink sink = new StringSink();

            // 2-byte lead followed by null terminator
            Unsafe.getUnsafe().putByte(buf, (byte) 0xC2);
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0);
            Assert.assertFalse(Utf8s.utf8ToUtf16Z(buf, sink));

            // 2-byte lead followed by invalid continuation
            Unsafe.getUnsafe().putByte(buf, (byte) 0xC2);
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0x41); // 'A', not continuation
            Unsafe.getUnsafe().putByte(buf + 2, (byte) 0);
            sink.clear();
            Assert.assertFalse(Utf8s.utf8ToUtf16Z(buf, sink));

            // 3-byte: null after b2
            Unsafe.getUnsafe().putByte(buf, (byte) 0xE4);
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0);
            sink.clear();
            Assert.assertFalse(Utf8s.utf8ToUtf16Z(buf, sink));

            // 3-byte: null after b3
            Unsafe.getUnsafe().putByte(buf, (byte) 0xE4);
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0xBD);
            Unsafe.getUnsafe().putByte(buf + 2, (byte) 0);
            sink.clear();
            Assert.assertFalse(Utf8s.utf8ToUtf16Z(buf, sink));

            // 4-byte: invalid lead (not >> 3 == -2)
            Unsafe.getUnsafe().putByte(buf, (byte) 0xF8); // 11111000, >> 3 = -1
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0x80);
            Unsafe.getUnsafe().putByte(buf + 2, (byte) 0x80);
            Unsafe.getUnsafe().putByte(buf + 3, (byte) 0x80);
            Unsafe.getUnsafe().putByte(buf + 4, (byte) 0);
            sink.clear();
            Assert.assertFalse(Utf8s.utf8ToUtf16Z(buf, sink));

            // 4-byte: null after b2
            Unsafe.getUnsafe().putByte(buf, (byte) 0xF0);
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0);
            sink.clear();
            Assert.assertFalse(Utf8s.utf8ToUtf16Z(buf, sink));

            // 4-byte: null after b3
            Unsafe.getUnsafe().putByte(buf, (byte) 0xF0);
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0x9F);
            Unsafe.getUnsafe().putByte(buf + 2, (byte) 0);
            sink.clear();
            Assert.assertFalse(Utf8s.utf8ToUtf16Z(buf, sink));

            // 4-byte: null after b4
            Unsafe.getUnsafe().putByte(buf, (byte) 0xF0);
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0x9F);
            Unsafe.getUnsafe().putByte(buf + 2, (byte) 0x98);
            Unsafe.getUnsafe().putByte(buf + 3, (byte) 0);
            sink.clear();
            Assert.assertFalse(Utf8s.utf8ToUtf16Z(buf, sink));
        } finally {
            Unsafe.free(buf, 8, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUtf8ZCopy() {
        // ASCII null-terminated string
        String input = "hello";
        byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
        int nBytes = bytes.length + 1; // +1 for null terminator
        long mem = Unsafe.malloc(nBytes, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.getUnsafe().putByte(mem + i, bytes[i]);
            }
            Unsafe.getUnsafe().putByte(mem + bytes.length, (byte) 0);

            Utf8StringSink sink = new Utf8StringSink();
            Utf8s.utf8ZCopy(mem, sink);
            Assert.assertEquals("hello", sink.toString());

            // Non-ASCII
            sink.clear();
            String cyrillic = "привет";
            byte[] cyrBytes = cyrillic.getBytes(StandardCharsets.UTF_8);
            long mem2 = Unsafe.malloc(cyrBytes.length + 1, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < cyrBytes.length; i++) {
                    Unsafe.getUnsafe().putByte(mem2 + i, cyrBytes[i]);
                }
                Unsafe.getUnsafe().putByte(mem2 + cyrBytes.length, (byte) 0);
                Utf8s.utf8ZCopy(mem2, sink);
                Assert.assertEquals("привет", sink.toString());
            } finally {
                Unsafe.free(mem2, cyrBytes.length + 1, MemoryTag.NATIVE_DEFAULT);
            }

            // Empty string (just null terminator)
            sink.clear();
            Unsafe.getUnsafe().putByte(mem, (byte) 0);
            Utf8s.utf8ZCopy(mem, sink);
            Assert.assertEquals("", sink.toString());
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

        // 3-byte chars (CJK) — exercises validateUtf8Decode3Bytes(Utf8Sequence)
        Assert.assertEquals(2, Utf8s.validateUtf8(utf8("你好")));

        // 4-byte chars (emoji) — exercises validateUtf8Decode4Bytes(Utf8Sequence)
        Utf8StringSink emojiSink = new Utf8StringSink();
        emojiSink.put("\uD83D\uDE00");
        Assert.assertEquals(1, Utf8s.validateUtf8(emojiSink));

        // Invalid 3-byte: truncated
        Assert.assertEquals(-1, Utf8s.validateUtf8(new Utf8String(new byte[]{(byte) 0xE0, (byte) 0xA0}, false)));

        // Invalid 3-byte: surrogate range (U+D800..U+DFFF)
        Assert.assertEquals(-1, Utf8s.validateUtf8(new Utf8String(new byte[]{(byte) 0xED, (byte) 0xA0, (byte) 0x80}, false)));

        // Invalid 3-byte: malformed continuation
        Assert.assertEquals(-1, Utf8s.validateUtf8(new Utf8String(new byte[]{(byte) 0xE0, (byte) 0x80, (byte) 0x80}, false)));

        // Invalid 4-byte: truncated
        Assert.assertEquals(-1, Utf8s.validateUtf8(new Utf8String(new byte[]{(byte) 0xF0, (byte) 0x9F, (byte) 0x98}, false)));

        // Invalid 4-byte: malformed continuation
        Assert.assertEquals(-1, Utf8s.validateUtf8(new Utf8String(new byte[]{(byte) 0xF0, (byte) 0x80, (byte) 0x80, (byte) 0x80}, false)));

        // Invalid 4-byte: not a supplementary code point
        Assert.assertEquals(-1, Utf8s.validateUtf8(new Utf8String(new byte[]{(byte) 0xF4, (byte) 0x90, (byte) 0x80, (byte) 0x80}, false)));

        // Truncated 2-byte: lead only
        Assert.assertEquals(-1, Utf8s.validateUtf8(new Utf8String(new byte[]{(byte) 0xC2}, false)));

        // Invalid continuation in 2-byte
        Assert.assertEquals(-1, Utf8s.validateUtf8(new Utf8String(new byte[]{(byte) 0xC2, (byte) 0x00}, false)));

        // Malformed 4-byte: valid lead and length but bad continuation bytes
        Assert.assertEquals(-1, Utf8s.validateUtf8(new Utf8String(new byte[]{(byte) 0xF0, (byte) 0x28, (byte) 0x80, (byte) 0x80}, false)));
    }

    @Test
    public void testValidateUtf8PointerPaths() {
        // Exercises validateUtf8MultiByte(long, long, byte) and its 3-byte/4-byte branches
        long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            // Valid 3-byte (CJK 你 = E4 BD A0)
            Unsafe.getUnsafe().putByte(buf, (byte) 0xE4);
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0xBD);
            Unsafe.getUnsafe().putByte(buf + 2, (byte) 0xA0);
            Assert.assertEquals(3, Utf8s.validateUtf8MultiByte(buf, buf + 3, (byte) 0xE4));

            // Valid 4-byte (emoji U+1F600 = F0 9F 98 80)
            Unsafe.getUnsafe().putByte(buf, (byte) 0xF0);
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0x9F);
            Unsafe.getUnsafe().putByte(buf + 2, (byte) 0x98);
            Unsafe.getUnsafe().putByte(buf + 3, (byte) 0x80);
            Assert.assertEquals(4, Utf8s.validateUtf8MultiByte(buf, buf + 4, (byte) 0xF0));

            // Invalid 3-byte: truncated
            Assert.assertEquals(-1, Utf8s.validateUtf8MultiByte(buf, buf + 2, (byte) 0xE4));

            // Invalid 4-byte: truncated
            Assert.assertEquals(-1, Utf8s.validateUtf8MultiByte(buf, buf + 3, (byte) 0xF0));

            // Invalid 3-byte: surrogate range
            Unsafe.getUnsafe().putByte(buf, (byte) 0xED);
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0xA0);
            Unsafe.getUnsafe().putByte(buf + 2, (byte) 0x80);
            Assert.assertEquals(-1, Utf8s.validateUtf8MultiByte(buf, buf + 3, (byte) 0xED));

            // Invalid 4-byte: not supplementary
            Unsafe.getUnsafe().putByte(buf, (byte) 0xF4);
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0x90);
            Unsafe.getUnsafe().putByte(buf + 2, (byte) 0x80);
            Unsafe.getUnsafe().putByte(buf + 3, (byte) 0x80);
            Assert.assertEquals(-1, Utf8s.validateUtf8MultiByte(buf, buf + 4, (byte) 0xF4));

            // Valid 2-byte
            Unsafe.getUnsafe().putByte(buf, (byte) 0xC2);
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0xA9);
            Assert.assertEquals(2, Utf8s.validateUtf8MultiByte(buf, buf + 2, (byte) 0xC2));

            // Truncated 2-byte
            Assert.assertEquals(-1, Utf8s.validateUtf8MultiByte(buf, buf + 1, (byte) 0xC2));

            // Invalid continuation in 2-byte
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0x00);
            Assert.assertEquals(-1, Utf8s.validateUtf8MultiByte(buf, buf + 2, (byte) 0xC2));

            // Malformed 3-byte: overlong (E0 80 80)
            Unsafe.getUnsafe().putByte(buf, (byte) 0xE0);
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0x80);
            Unsafe.getUnsafe().putByte(buf + 2, (byte) 0x80);
            Assert.assertEquals(-1, Utf8s.validateUtf8MultiByte(buf, buf + 3, (byte) 0xE0));

            // Malformed 4-byte: invalid continuation bytes
            Unsafe.getUnsafe().putByte(buf, (byte) 0xF0);
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0x28);
            Unsafe.getUnsafe().putByte(buf + 2, (byte) 0x80);
            Unsafe.getUnsafe().putByte(buf + 3, (byte) 0x80);
            Assert.assertEquals(-1, Utf8s.validateUtf8MultiByte(buf, buf + 4, (byte) 0xF0));
        } finally {
            Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
        }
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

    private static String readUtf8(long addr, int len) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = Unsafe.getUnsafe().getByte(addr + i);
        }
        return new String(bytes, StandardCharsets.UTF_8);
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
