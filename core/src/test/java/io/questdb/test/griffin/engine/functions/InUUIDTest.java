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
        execute("create table MovementLog(\n" +
                "ts timestamp,\n" +
                "initParticipantId long,\n" +
                "initParticipantIdType symbol,\n" +
                "movementBusinessDate date,\n" +
                "slotId uuid\n" +
                ") timestamp(ts) partition by day wal\n");

        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "constants",
                "participantId\tparticipantIdType\n",
                bindVariableService -> bindVariableService.setDate(0, 1000L)
        ));

        assertSql("SELECT DISTINCT initParticipantId AS participantId, initParticipantIdType AS participantIdType\n" +
                "FROM 'MovementLog'\n" +
                "WHERE movementBusinessDate=$1 AND slotId IN ('aa7dc4ec-cb68-446f-b37f-1ec82752c7d7', " +
                "'b5b2159a-2356-4217-965d-4c984f0ffa8a', '4cd64b0b-0a34-4f8e-a698-c6c186b7571a')\n" +
                "ORDER BY participantId\n" +
                "LIMIT 0,6", tuples);
    }

    @Test
    public void testBindVarRuntimeConstants() throws SqlException {
        execute("create table MovementLog(\n" +
                "ts timestamp,\n" +
                "initParticipantId long,\n" +
                "initParticipantIdType symbol,\n" +
                "movementBusinessDate date,\n" +
                "slotId uuid\n" +
                ") timestamp(ts) partition by day wal\n");

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

        assertSql("SELECT DISTINCT initParticipantId AS participantId, initParticipantIdType AS participantIdType\n" +
                "FROM 'MovementLog'\n" +
                "WHERE movementBusinessDate=$1 AND slotId IN ($2, $3, $4)\n" +
                "ORDER BY participantId\n" +
                "LIMIT 0,6", tuples);
    }

    @Test
    public void testBindVarTypeChange() throws SqlException {
        execute("create table test as (select x, rnd_uuid4(1) a from long_sequence(100))");

        // when more than one argument supplied, the function will match exact values from the list
        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "simple",
                "x\ta\n" +
                        "5\tb5b2159a-2356-4217-965d-4c984f0ffa8a\n" +
                        "54\t5277ee62-a5a6-49fb-9ff9-7d73fc0c62d0\n" +
                        "70\ta7a8f4e5-4999-4e46-916e-1efd8bbcecf6\n",
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
                "x\ta\n" +
                        "2\t\n" +
                        "4\t\n" +
                        "11\t\n" +
                        "14\t\n" +
                        "18\t\n" +
                        "19\t\n" +
                        "22\t\n" +
                        "25\t\n" +
                        "26\t\n" +
                        "34\t\n" +
                        "36\t\n" +
                        "44\t\n" +
                        "45\t\n" +
                        "46\t\n" +
                        "47\t\n" +
                        "48\t\n" +
                        "53\t\n" +
                        "68\t\n" +
                        "69\t\n" +
                        "72\t\n" +
                        "74\t\n" +
                        "75\t\n" +
                        "76\t\n" +
                        "79\taa7dc4ec-cb68-446f-b37f-1ec82752c7d7\n" +
                        "82\t\n" +
                        "84\taa1896d0-ad34-49d2-910a-a7b6d58506dc\n" +
                        "85\t\n" +
                        "86\t\n" +
                        "88\t\n" +
                        "92\t\n" +
                        "94\t\n" +
                        "98\t\n",
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
                "x\ta\n" +
                        "79\taa7dc4ec-cb68-446f-b37f-1ec82752c7d7\n" +
                        "84\taa1896d0-ad34-49d2-910a-a7b6d58506dc\n",
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
                "x\ta\n" +
                        "2\t\n" +
                        "4\t\n" +
                        "11\t\n" +
                        "14\t\n" +
                        "18\t\n" +
                        "19\t\n" +
                        "22\t\n" +
                        "25\t\n" +
                        "26\t\n" +
                        "34\t\n" +
                        "36\t\n" +
                        "44\t\n" +
                        "45\t\n" +
                        "46\t\n" +
                        "47\t\n" +
                        "48\t\n" +
                        "53\t\n" +
                        "68\t\n" +
                        "69\t\n" +
                        "72\t\n" +
                        "74\t\n" +
                        "75\t\n" +
                        "76\t\n" +
                        "79\taa7dc4ec-cb68-446f-b37f-1ec82752c7d7\n" +
                        "82\t\n" +
                        "84\taa1896d0-ad34-49d2-910a-a7b6d58506dc\n" +
                        "85\t\n" +
                        "86\t\n" +
                        "88\t\n" +
                        "92\t\n" +
                        "94\t\n" +
                        "98\t\n",
                bindVariableService -> bindVariableService.setVarchar(0, new Utf8String("aa1896d0-ad34-49d2-910a-a7b6d58506dc"))
        ));

        assertSql("test where a in ('aa7dc4ec-cb68-446f-b37f-1ec82752c7d7'::varchar, $1, null::varchar)", tuples);
    }

    @Test
    public void testConstBadUUID() throws Exception {
        execute("create table test as (select x, rnd_uuid4(1) a from long_sequence(100))");
        assertException(
                "test where a in ('9/12')",
                17,
                "invalid UUID value [9/12]"
        );
    }

    @Test
    public void testConstInvalidType() throws Exception {
        execute("create table test as (select x, rnd_uuid4(1) a from long_sequence(100))");
        assertException(
                "test where a in (0.1323)",
                17,
                "cannot compare UUID with type DOUBLE"
        );
    }
}
