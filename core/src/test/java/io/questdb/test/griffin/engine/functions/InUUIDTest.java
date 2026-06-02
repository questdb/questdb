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

package io.questdb.test.griffin.engine.functions;

import io.questdb.griffin.SqlException;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Uuid;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVariableTestTuple;
import org.junit.Test;

public class InUUIDTest extends AbstractCairoTest {

    @Test
    public void testBindVarConstants() throws SqlException {
        execute("""
                create table MovementLog(
                ts timestamp,
                initParticipantId long,
                initParticipantIdType symbol,
                movementBusinessDate date,
                slotId uuid
                ) timestamp(ts) partition by day wal
                """);

        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "constants",
                "participantId\tparticipantIdType\n",
                bindVariableService -> bindVariableService.setDate(0, 1000L)
        ));

        assertSql("""
                SELECT DISTINCT initParticipantId AS participantId, initParticipantIdType AS participantIdType
                FROM 'MovementLog'
                WHERE movementBusinessDate=$1 AND slotId IN ('aa7dc4ec-cb68-446f-b37f-1ec82752c7d7', \
                'b5b2159a-2356-4217-965d-4c984f0ffa8a', '4cd64b0b-0a34-4f8e-a698-c6c186b7571a')
                ORDER BY participantId
                LIMIT 0,6""", tuples);
    }

    @Test
    public void testBindVarRuntimeConstants() throws SqlException {
        execute("""
                create table MovementLog(
                ts timestamp,
                initParticipantId long,
                initParticipantIdType symbol,
                movementBusinessDate date,
                slotId uuid
                ) timestamp(ts) partition by day wal
                """);

        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "runtime constants",
                "participantId\tparticipantIdType\n",
                bindVariableService -> {
                    bindVariableService.setDate(0, 1000L);
                    bindVariableService.setStr(1, "aa7dc4ec-cb68-446f-b37f-1ec82752c7d7");
                    bindVariableService.setStr(2, "b5b2159a-2356-4217-965d-4c984f0ffa8a");
                    bindVariableService.setStr(3, "4cd64b0b-0a34-4f8e-a698-c6c186b7571a");
                }
        ));

        assertSql("""
                SELECT DISTINCT initParticipantId AS participantId, initParticipantIdType AS participantIdType
                FROM 'MovementLog'
                WHERE movementBusinessDate=$1 AND slotId IN ($2, $3, $4)
                ORDER BY participantId
                LIMIT 0,6""", tuples);
    }

    @Test
    public void testBindVarTypeChange() throws SqlException {
        execute("create table test as (select x, rnd_uuid4(1) a from long_sequence(100))");

        // when more than one argument supplied, the function will match exact values from the list
        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "simple",
                """
                        x\ta
                        5\tb5b2159a-2356-4217-965d-4c984f0ffa8a
                        54\t5277ee62-a5a6-49fb-9ff9-7d73fc0c62d0
                        70\ta7a8f4e5-4999-4e46-916e-1efd8bbcecf6
                        """,
                bindVariableService -> {
                    try {
                        bindVariableService.setStr(0, "b5b2159a-2356-4217-965d-4c984f0ffa8a");
                        bindVariableService.setUuid(1, Uuid.parseLo("5277ee62-a5a6-49fb-9ff9-7d73fc0c62d0"), Uuid.parseHi("5277ee62-a5a6-49fb-9ff9-7d73fc0c62d0"));
                        bindVariableService.setStr(2, "a7a8f4e5-4999-4e46-916e-1efd8bbcecf6");
                    } catch (NumericException e) {
                        throw SqlException.$(0, "dnag, invalid uuid?");
                    }
                }
        ));

        tuples.add(new BindVariableTestTuple(
                "undefined bind variable",
                "undefined bind variable: 1",
                bindVariableService -> {
                    bindVariableService.setStr(0, "a1b56c3d-802c-4735-9290-f9bc187b0cd2");
                    bindVariableService.setStr(2, "4cd64b0b-0a34-4f8e-a698-c6c186b7571a");
                },
                20
        ));

        tuples.add(new BindVariableTestTuple(
                "bad type",
                "inconvertible types: INT -> UUID [from=INT, to=UUID]",
                bindVariableService -> {
                    bindVariableService.setStr(0, "5277ee62-a5a6-49fb-9ff9-7d73fc0c62d0");
                    bindVariableService.setInt(1, 30);
                    bindVariableService.setStr(2, "30d46a3a-4749-441d-ba90-2c77fa1a889c");
                },
                20
        ));

        tuples.add(new BindVariableTestTuple(
                "with nulls",
                """
                        x\ta
                        2\t
                        4\t
                        11\t
                        14\t
                        18\t
                        19\t
                        22\t
                        25\t
                        26\t
                        34\t
                        36\t
                        44\t
                        45\t
                        46\t
                        47\t
                        48\t
                        53\t
                        68\t
                        69\t
                        72\t
                        74\t
                        75\t
                        76\t
                        79\taa7dc4ec-cb68-446f-b37f-1ec82752c7d7
                        82\t
                        84\taa1896d0-ad34-49d2-910a-a7b6d58506dc
                        85\t
                        86\t
                        88\t
                        92\t
                        94\t
                        98\t
                        """,
                bindVariableService -> {
                    bindVariableService.setStr(0, "aa7dc4ec-cb68-446f-b37f-1ec82752c7d7");
                    bindVariableService.setStr(1, null);
                    bindVariableService.setStr(2, "aa1896d0-ad34-49d2-910a-a7b6d58506dc");
                }
        ));
        assertSql("test where a in ($1,$2,$3)", tuples);
    }

    @Test
    public void testConstAndBindVariableMix() throws SqlException {
        execute("create table test as (select x, rnd_uuid4(1) a from long_sequence(100))");

        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "mix",
                """
                        x\ta
                        79\taa7dc4ec-cb68-446f-b37f-1ec82752c7d7
                        84\taa1896d0-ad34-49d2-910a-a7b6d58506dc
                        """,
                bindVariableService -> bindVariableService.setStr(0, "aa1896d0-ad34-49d2-910a-a7b6d58506dc")
        ));

        assertSql("test where a in ('aa7dc4ec-cb68-446f-b37f-1ec82752c7d7', $1)", tuples);
    }

    @Test
    public void testConstAndBindVariableVarcharMix() throws SqlException {
        execute("create table test as (select x, rnd_uuid4(1) a from long_sequence(100))");

        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "mix",
                """
                        x\ta
                        2\t
                        4\t
                        11\t
                        14\t
                        18\t
                        19\t
                        22\t
                        25\t
                        26\t
                        34\t
                        36\t
                        44\t
                        45\t
                        46\t
                        47\t
                        48\t
                        53\t
                        68\t
                        69\t
                        72\t
                        74\t
                        75\t
                        76\t
                        79\taa7dc4ec-cb68-446f-b37f-1ec82752c7d7
                        82\t
                        84\taa1896d0-ad34-49d2-910a-a7b6d58506dc
                        85\t
                        86\t
                        88\t
                        92\t
                        94\t
                        98\t
                        """,
                bindVariableService -> bindVariableService.setVarchar(0, new Utf8String("aa1896d0-ad34-49d2-910a-a7b6d58506dc"))
        ));

        assertSql("test where a in ('aa7dc4ec-cb68-446f-b37f-1ec82752c7d7'::varchar, $1, null::varchar)", tuples);
    }

    @Test
    public void testConstBadUUID() throws Exception {
        execute("create table test as (select x, rnd_uuid4(1) a from long_sequence(100))");
        assertQuery("test where a in ('9/12')")
                .fails(17, "invalid UUID value [9/12]");
    }

    @Test
    public void testConstInvalidType() throws Exception {
        execute("create table test as (select x, rnd_uuid4(1) a from long_sequence(100))");
        assertQuery("test where a in (0.1323)")
                .fails(17, "cannot compare UUID with type DOUBLE");
    }
}
