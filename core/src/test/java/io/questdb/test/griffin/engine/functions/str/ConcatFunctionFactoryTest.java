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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.tools.BindVariableTestTuple;
import org.junit.Test;

public class ConcatFunctionFactoryTest extends AbstractCairoTest {
    @Test
    public void testAllTypesWithTimestampMicros() throws Exception {
        testAllTypes(ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testAllTypesWithTimestampNanos() throws Exception {
        testAllTypes(ColumnType.TIMESTAMP_NANO);
    }

    @Test
    public void testBindVarMixed() throws Exception {
        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "mixed",
                "concat\n" +
                        "hihiA10hohohoABC77\n",
                bindVariableService -> {
                    bindVariableService.setChar(0, 'A');
                    bindVariableService.setStr(1, "hohoho");
                    bindVariableService.setVarchar(2, new Utf8String("ABC77"));
                }
        ));

        assertSql("select concat('hihi', null, $1, 10, $2, $3)", tuples);
    }

    @Test
    public void testBindVarTypeChange() throws Exception {
        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "all varchar",
                "concat\n" +
                        "1hopparea51\n",
                bindVariableService -> {
                    bindVariableService.setVarchar(0, new Utf8String("1"));
                    bindVariableService.setVarchar(1, new Utf8String("hopp"));
                    bindVariableService.setVarchar(2, new Utf8String("area51"));
                }
        ));

        tuples.add(new BindVariableTestTuple(
                "type change",
                "concat\n" +
                        "AhohohoABC77\n",
                bindVariableService -> {
                    bindVariableService.setChar(0, 'A');
                    bindVariableService.setStr(1, "hohoho");
                    bindVariableService.setVarchar(2, new Utf8String("ABC77"));
                }
        ));

        assertSql("select concat($1, $2, $3)", tuples);
    }

    @Test
    public void testColumn() throws Exception {
        execute("create table test as (select cast(x as varchar) a, timestamp_sequence(0, 1000000) ts from long_sequence(5))");

        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "column",
                "concat\n" +
                        "hihi2A10hohohoABC77\n" +
                        "hihi3A10hohohoABC77\n" +
                        "hihi4A10hohohoABC77\n" +
                        "hihi5A10hohohoABC77\n" +
                        "hihi6A10hohohoABC77\n",
                bindVariableService -> {
                    bindVariableService.setChar(0, 'A');
                    bindVariableService.setStr(1, "hohoho");
                    bindVariableService.setVarchar(2, new Utf8String("ABC77"));
                }
        ));

        assertSql("select concat('hihi', a::int + 1, $1, 10, $2, $3) from test", tuples);
    }

    @Test
    public void testCursor() throws Exception {
        assertException(
                "select concat('hehe', select max(a) from test), concat('hoho', 'haha')",
                "create table test as (select cast(x as varchar) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))",
                22,
                "unsupported type: CURSOR"
        );
    }

    @Test
    public void testDecimals() throws Exception {
        CreateTableTestUtils.createDecimalsTable(engine, PartitionBy.NONE, ColumnType.TIMESTAMP_MICRO);

        execute(
                "insert into decimals values" +
                        "(null, null, null, null, null, null, now())," +
                        "(1.2m, 34.56m, 789.012m, 3456.7891m, 23456.78901m, 234567.890123m, now())"
        );

        assertSql(
                "concat\n" +
                        "nullnullnullnullnullnull\n" +
                        "1.234.56789.0123456.789123456.78901234567.890123\n",
                "select concat(dec8, dec16, dec32, dec64, dec128, dec256) from decimals"
        );
    }

    @Test
    public void testNoArgs() throws Exception {
        assertException(
                "select concat();",
                7,
                "no arguments provided"
        );
    }

    @Test
    public void testNull() throws Exception {
        assertSql(
                "concat\n" +
                        "foo1.2\n",
                "select concat('foo', null, 1.2)"
        );
    }

    @Test
    public void testNullStr() throws Exception {
        assertQuery(
                "concat\n" +
                        "\n",
                "select concat(sym, str, v) from x",
                "create table x as (select rnd_symbol(null) sym, rnd_str(null) str, rnd_varchar(null) v from long_sequence(1))",
                null,
                true,
                true
        );
    }

    private void testAllTypes(int timestampType) throws Exception {
        assertMemoryLeak(() -> {
            CreateTableTestUtils.createAllTableWithNewTypes(engine, PartitionBy.NONE, timestampType);

            execute(
                    "insert into all2 select * from (" +
                            "select" +
                            " rnd_int()," +
                            " rnd_short()," +
                            " rnd_byte()," +
                            " rnd_double()," +
                            " rnd_float()," +
                            " rnd_long()," +
                            " rnd_str(2,3,0)," +
                            " rnd_symbol('A','D')," +
                            " rnd_boolean()," +
                            " rnd_bin()," +
                            " rnd_date()," +
                            " rnd_long256()," +
                            " rnd_char()," +
                            " rnd_uuid4()," +
                            " rnd_ipv4()," +
                            " rnd_varchar(2,3,0)," +
                            " timestamp_sequence(0L, 10L) ts from long_sequence(10)) timestamp(ts)"
            );
            if (ColumnType.isTimestampMicro(timestampType)) {
                assertSql(
                        "concat\n" +
                                "-1148479920/24814/27/0.12966659791573354/0.28455776/-7611843578141082998/YR/A/false/[]/2827518/0x63eb3740c80f661e9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b/D/cec82869-edec-421b-8259-3f82b430328d/235.156.195.158/jF/1970-01-01T00:00:00.000000Z\n" +
                                "-1309308188/10549/84/0.706473302224657/0.5913874/788901813531436389/LIG/A/false/[]/7841424/0x69e00a3e8d5fdfe360add80ba563dff19c7a8bc4087de26d94ccc98dfff078f4/M/34570a2b-ee44-4735-92c3-95ffb8982d58/128.124.238.97/ŗ\uDB47\uDD9C\uDA96\uDF8F/1970-01-01T00:00:00.000050Z\n" +
                                "-1383560599/-31176/33/0.3679848625908545/0.82312495/5398991075259361292/VL/D/true/[]/5276379/0x2accfc7ab9ae2e0b5825a545d3d3e2bdd095456a4d3d5993fdb12ef0d2c74218/O/3c84de8f-7bd9-435d-abaf-9eca859915f5/130.88.42.208/ Ԡ阷/1970-01-01T00:00:00.000030Z\n" +
                                "-1587018368/29923/36/0.30028279396280155/0.6721404/-1905597357123382478/HGZ/D/true/[]/1053841/0x568a2ffdcaf345a26a44a113d18bd82a6784e4a783247d88546c26b247358a54/Z/35974aa0-62e3-44d4-8289-60f4997837e7/77.235.175.98/\uDA50\uDCC4^/1970-01-01T00:00:00.000060Z\n" +
                                "1124960428/-21502/126/0.933609514582851/0.5475429/5917909491180292756/MX/A/false/[]/2900504/0x5a6e970fb8b80abcc4129ae493cc6076935c8f50476768716ce7a5e0d217d9fe/G/455b1f46-fe7f-40cd-a337-f7e6b82ebc24/178.94.169.19/&{/1970-01-01T00:00:00.000080Z\n" +
                                "1278547815/17250/108/0.7340656260730631/0.05024612/-8371487291073160693/UO/A/true/[]/6158348/0x84a7624f383eb28be6924ee0e0eb76eac0e2f557a78a4b4e460f10d774f587ea/K/6c3493fc-b2d0-472d-a046-e5d137dd8f0f/226.138.190.179/ˣgX/1970-01-01T00:00:00.000040Z\n" +
                                "1289699549/-12476/53/0.05133515566281188/0.34257197/-5701911565963471026/ZN/D/false/[]/4978635/0xb23ff8774a5db433b19ddb7ff5abcafec82c35a389f834dababcd0482f05618f/Q/46065f74-1cb0-4a85-9508-d3fad3f12f80/105.159.220.32/]>U/1970-01-01T00:00:00.000070Z\n" +
                                "1965091786/-3598/47/0.0032519916115479885/0.21458226/1704407071711577912/VIH/D/true/[]/5579405/0x85e70b46349799fe49f783d5343dd7bc3d3fe1302cd3371137fccdabf181b5ad/U/30ec2498-d018-4fdd-a7bf-677cfe82f252/56.8.217.77/-?y/1970-01-01T00:00:00.000090Z\n" +
                                "2085282008/-1379/44/0.12026122412833129/0.6761935/8325936937764905778/QU/D/true/[]/395663/0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e/I/a5f80be4-b45b-4437-8929-90e1a29afcac/184.92.27.200/kV/1970-01-01T00:00:00.000010Z\n" +
                                "532665695/-4874/54/0.7588175403454873/0.54067093/-8081265393416742311/YCT/A/false/[]/1179064/0xd25adf928386cdd2d992946a26184664ba453d761efcf9bb7ee6a03f4f930fa3/S/af44c40a-67ef-4e1c-9b3e-f21223ee8849/130.40.224.242/軦۽㒾/1970-01-01T00:00:00.000020Z\n",
                        "select concat(int, '/', short, '/', byte, '/', double, '/', float, '/', long, '/', str, '/', sym, '/', bool, '/', bin, '/', date, '/', long256, '/', chr, '/', uuid, '/', ipv4, '/', varchar, '/', timestamp) from all2 order by 1"
                );
            } else {
                assertSql(
                        "concat\n" +
                                "-1148479920/24814/27/0.12966659791573354/0.28455776/-7611843578141082998/YR/A/false/[]/2827518/0x63eb3740c80f661e9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b/D/cec82869-edec-421b-8259-3f82b430328d/235.156.195.158/jF/1970-01-01T00:00:00.000000000Z\n" +
                                "-1309308188/10549/84/0.706473302224657/0.5913874/788901813531436389/LIG/A/false/[]/7841424/0x69e00a3e8d5fdfe360add80ba563dff19c7a8bc4087de26d94ccc98dfff078f4/M/34570a2b-ee44-4735-92c3-95ffb8982d58/128.124.238.97/ŗ\uDB47\uDD9C\uDA96\uDF8F/1970-01-01T00:00:00.000050000Z\n" +
                                "-1383560599/-31176/33/0.3679848625908545/0.82312495/5398991075259361292/VL/D/true/[]/5276379/0x2accfc7ab9ae2e0b5825a545d3d3e2bdd095456a4d3d5993fdb12ef0d2c74218/O/3c84de8f-7bd9-435d-abaf-9eca859915f5/130.88.42.208/ Ԡ阷/1970-01-01T00:00:00.000030000Z\n" +
                                "-1587018368/29923/36/0.30028279396280155/0.6721404/-1905597357123382478/HGZ/D/true/[]/1053841/0x568a2ffdcaf345a26a44a113d18bd82a6784e4a783247d88546c26b247358a54/Z/35974aa0-62e3-44d4-8289-60f4997837e7/77.235.175.98/\uDA50\uDCC4^/1970-01-01T00:00:00.000060000Z\n" +
                                "1124960428/-21502/126/0.933609514582851/0.5475429/5917909491180292756/MX/A/false/[]/2900504/0x5a6e970fb8b80abcc4129ae493cc6076935c8f50476768716ce7a5e0d217d9fe/G/455b1f46-fe7f-40cd-a337-f7e6b82ebc24/178.94.169.19/&{/1970-01-01T00:00:00.000080000Z\n" +
                                "1278547815/17250/108/0.7340656260730631/0.05024612/-8371487291073160693/UO/A/true/[]/6158348/0x84a7624f383eb28be6924ee0e0eb76eac0e2f557a78a4b4e460f10d774f587ea/K/6c3493fc-b2d0-472d-a046-e5d137dd8f0f/226.138.190.179/ˣgX/1970-01-01T00:00:00.000040000Z\n" +
                                "1289699549/-12476/53/0.05133515566281188/0.34257197/-5701911565963471026/ZN/D/false/[]/4978635/0xb23ff8774a5db433b19ddb7ff5abcafec82c35a389f834dababcd0482f05618f/Q/46065f74-1cb0-4a85-9508-d3fad3f12f80/105.159.220.32/]>U/1970-01-01T00:00:00.000070000Z\n" +
                                "1965091786/-3598/47/0.0032519916115479885/0.21458226/1704407071711577912/VIH/D/true/[]/5579405/0x85e70b46349799fe49f783d5343dd7bc3d3fe1302cd3371137fccdabf181b5ad/U/30ec2498-d018-4fdd-a7bf-677cfe82f252/56.8.217.77/-?y/1970-01-01T00:00:00.000090000Z\n" +
                                "2085282008/-1379/44/0.12026122412833129/0.6761935/8325936937764905778/QU/D/true/[]/395663/0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e/I/a5f80be4-b45b-4437-8929-90e1a29afcac/184.92.27.200/kV/1970-01-01T00:00:00.000010000Z\n" +
                                "532665695/-4874/54/0.7588175403454873/0.54067093/-8081265393416742311/YCT/A/false/[]/1179064/0xd25adf928386cdd2d992946a26184664ba453d761efcf9bb7ee6a03f4f930fa3/S/af44c40a-67ef-4e1c-9b3e-f21223ee8849/130.40.224.242/軦۽㒾/1970-01-01T00:00:00.000020000Z\n",
                        "select concat(int, '/', short, '/', byte, '/', double, '/', float, '/', long, '/', str, '/', sym, '/', bool, '/', bin, '/', date, '/', long256, '/', chr, '/', uuid, '/', ipv4, '/', varchar, '/', timestamp) from all2 order by 1"
                );
            }
        });
    }
}
