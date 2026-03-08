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

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVariableTestTuple;
import org.junit.Test;

public class InDoubleTest extends AbstractCairoTest {


    @Test
    public void testBindVarConstants() throws SqlException {
        execute("create table MovementLog(\n" +
                "ts timestamp,\n" +
                "initParticipantId long,\n" +
                "initParticipantIdType symbol,\n" +
                "movementBusinessDate date,\n" +
                "slotId double\n" +
                ") timestamp(ts) partition by day wal\n");

        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "constants",
                "participantId\tparticipantIdType\n",
                bindVariableService -> bindVariableService.setDate(0, 1000L)
        ));

        assertSql("SELECT DISTINCT initParticipantId AS participantId, initParticipantIdType AS participantIdType\n" +
                "FROM 'MovementLog'\n" +
                "WHERE movementBusinessDate=$1 AND slotId IN (1.1, 2.1, 3.52)\n" +
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
                "slotId double\n" +
                ") timestamp(ts) partition by day wal\n");

        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "runtime constants",
                "participantId\tparticipantIdType\n",
                bindVariableService -> {
                    bindVariableService.setDate(0, 1000L);
                    bindVariableService.setDouble(1, 1.5);
                    bindVariableService.setDouble(2, 22.3);
                    bindVariableService.setDouble(3, 3.46);
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
        execute("create table test as (select x, rnd_double() a from long_sequence(100))");

        // when more than one argument supplied, the function will match exact values from the list
        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "simple",
                "x\ta\n" +
                        "58\t0.6821660861001273\n" +
                        "89\t0.3045253310626277\n" +
                        "90\t0.3901731258748704\n",
                bindVariableService -> {
                    bindVariableService.setStr(0, "0.6821660861001273");
                    bindVariableService.setDouble(1, 0.3901731258748704);
                    bindVariableService.setStr(2, "0.3045253310626277");
                }
        ));

        tuples.add(new BindVariableTestTuple(
                "undefined bind variable",
                "undefined bind variable: 1",
                bindVariableService -> {
                    bindVariableService.setDouble(0, 0.7763904674818695);
                    bindVariableService.setDouble(2, 0.26369335635512836);
                },
                20
        ));

        tuples.add(new BindVariableTestTuple(
                "bad type",
                "inconvertible types: GEOHASH(4c) -> DOUBLE [from=GEOHASH(4c), to=DOUBLE]",
                bindVariableService -> {
                    bindVariableService.setStr(0, "0.42281342727402726");
                    bindVariableService.setGeoHash(1, 30, ColumnType.getGeoHashTypeWithBits(20));
                    bindVariableService.setDouble(2, 0.8940917126581895);
                },
                20
        ));

        tuples.add(new BindVariableTestTuple(
                "bad double",
                "invalid DOUBLE value [hello]",
                bindVariableService -> {
                    bindVariableService.setDouble(0, 0.8940917126581895);
                    bindVariableService.setStr(2, "hello");
                    bindVariableService.setStr(1, "0.42281342727402726");
                },
                23
        ));

        assertSql("test where a in ($1,$2,$3)", tuples);
    }

    @Test
    public void testConstAndBindVariableMix() throws SqlException {
        execute("create table test as (select x, rnd_double() a from long_sequence(100))");

        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "mix",
                "x\ta\n" +
                        "58\t0.6821660861001273\n" +
                        "90\t0.3901731258748704\n",
                bindVariableService -> bindVariableService.setStr(0, "0.6821660861001273")
        ));

        assertSql("test where a in (0.3901731258748704, $1)", tuples);
    }

    @Test
    public void testNulls() throws SqlException {
        execute("create table test as (select x, rnd_double(1) a from long_sequence(100))");

        // when more than one argument supplied, the function will match exact values from the list
        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "simple",
                "x\ta\n" +
                        "8\tnull\n" +
                        "17\tnull\n" +
                        "26\tnull\n" +
                        "29\tnull\n" +
                        "30\tnull\n" +
                        "35\tnull\n" +
                        "39\t0.6590341607692226\n" +
                        "41\tnull\n" +
                        "44\tnull\n" +
                        "45\t0.45659895188239796\n" +
                        "49\tnull\n" +
                        "54\tnull\n" +
                        "56\tnull\n" +
                        "57\tnull\n" +
                        "60\tnull\n" +
                        "65\tnull\n" +
                        "67\tnull\n" +
                        "69\tnull\n" +
                        "70\tnull\n" +
                        "73\tnull\n" +
                        "74\tnull\n" +
                        "75\tnull\n" +
                        "80\tnull\n" +
                        "81\tnull\n" +
                        "84\tnull\n" +
                        "96\tnull\n" +
                        "99\tnull\n",
                bindVariableService -> {
                    bindVariableService.setStr(0, "0.6590341607692226");
                    bindVariableService.setDouble(1, 0.45659895188239796);
                }
        ));

        tuples.add(new BindVariableTestTuple(
                "bad type",
                "x\ta\n" +
                        "8\tnull\n" +
                        "17\tnull\n" +
                        "26\tnull\n" +
                        "29\tnull\n" +
                        "30\tnull\n" +
                        "35\tnull\n" +
                        "39\t0.6590341607692226\n" +
                        "41\tnull\n" +
                        "44\tnull\n" +
                        "49\tnull\n" +
                        "54\tnull\n" +
                        "56\tnull\n" +
                        "57\tnull\n" +
                        "60\tnull\n" +
                        "65\tnull\n" +
                        "67\tnull\n" +
                        "69\tnull\n" +
                        "70\tnull\n" +
                        "73\tnull\n" +
                        "74\tnull\n" +
                        "75\tnull\n" +
                        "80\tnull\n" +
                        "81\tnull\n" +
                        "84\tnull\n" +
                        "96\tnull\n" +
                        "99\tnull\n",
                bindVariableService -> {
                    bindVariableService.setStr(0, null);
                    bindVariableService.setDouble(1, 0.6590341607692226);
                }
        ));
        assertSql("test where a in ($1,null,$2)", tuples);
    }

    @Test
    public void testUnsupportedConstType() throws Exception {
        execute("create table test as (select x, rnd_double(1) a from long_sequence(100))");
        assertException(
                "test where a in (0.234, cast ('1CB' as geohash(1b)))",
                24,
                "cannot compare DOUBLE with type GEOHASH(1b)"
        );
    }
}
