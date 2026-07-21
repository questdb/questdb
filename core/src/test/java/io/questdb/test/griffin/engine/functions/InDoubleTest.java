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

import io.questdb.cairo.ColumnType;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVarTuple;
import org.junit.Test;

public class InDoubleTest extends AbstractCairoTest {


    @Test
    public void testBindVarConstants() throws Exception {
        final ObjList<BindVarTuple> cases = new ObjList<>();
        cases.add(BindVarTuple.ok(
                "constants",
                "participantId\tparticipantIdType\n",
                bindVariableService -> bindVariableService.setDate(0, 1000L)
        ));

        assertQuery("""
                SELECT DISTINCT initParticipantId AS participantId, initParticipantIdType AS participantIdType
                FROM 'MovementLog'
                WHERE movementBusinessDate=$1 AND slotId IN (1.1, 2.1, 3.52)
                ORDER BY participantId
                LIMIT 0,6""")
                .ddl("""
                        create table MovementLog(
                        ts timestamp,
                        initParticipantId long,
                        initParticipantIdType symbol,
                        movementBusinessDate date,
                        slotId double
                        ) timestamp(ts) partition by day wal
                        """)
                .assertBinds(cases);
    }

    @Test
    public void testBindVarRuntimeConstants() throws Exception {
        final ObjList<BindVarTuple> cases = new ObjList<>();
        cases.add(BindVarTuple.ok(
                "runtime constants",
                "participantId\tparticipantIdType\n",
                bindVariableService -> {
                    bindVariableService.setDate(0, 1000L);
                    bindVariableService.setDouble(1, 1.5);
                    bindVariableService.setDouble(2, 22.3);
                    bindVariableService.setDouble(3, 3.46);
                }
        ));

        assertQuery("""
                SELECT DISTINCT initParticipantId AS participantId, initParticipantIdType AS participantIdType
                FROM 'MovementLog'
                WHERE movementBusinessDate=$1 AND slotId IN ($2, $3, $4)
                ORDER BY participantId
                LIMIT 0,6""")
                .ddl("""
                        create table MovementLog(
                        ts timestamp,
                        initParticipantId long,
                        initParticipantIdType symbol,
                        movementBusinessDate date,
                        slotId double
                        ) timestamp(ts) partition by day wal
                        """)
                .assertBinds(cases);
    }


    @Test
    public void testBindVarTypeChange() throws Exception {
        // when more than one argument supplied, the function will match exact values from the list
        final ObjList<BindVarTuple> cases = new ObjList<>();
        cases.add(BindVarTuple.ok(
                "simple",
                """
                        x\ta
                        58\t0.6821660861001273
                        89\t0.3045253310626277
                        90\t0.3901731258748704
                        """,
                bindVariableService -> {
                    bindVariableService.setStr(0, "0.6821660861001273");
                    bindVariableService.setDouble(1, 0.3901731258748704);
                    bindVariableService.setStr(2, "0.3045253310626277");
                }
        ));

        cases.add(BindVarTuple.fails(
                "undefined bind variable",
                20,
                "undefined bind variable: 1",
                bindVariableService -> {
                    bindVariableService.setDouble(0, 0.7763904674818695);
                    bindVariableService.setDouble(2, 0.26369335635512836);
                }
        ));

        cases.add(BindVarTuple.fails(
                "bad type",
                20,
                "inconvertible types: GEOHASH(4c) -> DOUBLE [from=GEOHASH(4c), to=DOUBLE]",
                bindVariableService -> {
                    bindVariableService.setStr(0, "0.42281342727402726");
                    bindVariableService.setGeoHash(1, 30, ColumnType.getGeoHashTypeWithBits(20));
                    bindVariableService.setDouble(2, 0.8940917126581895);
                }
        ));

        cases.add(BindVarTuple.fails(
                "bad double",
                23,
                "invalid DOUBLE value [hello]",
                bindVariableService -> {
                    bindVariableService.setDouble(0, 0.8940917126581895);
                    bindVariableService.setStr(2, "hello");
                    bindVariableService.setStr(1, "0.42281342727402726");
                }
        ));

        assertQuery("test where a in ($1,$2,$3)")
                .ddl("create table test as (select x, rnd_double() a from long_sequence(100))")
                .assertBinds(cases);
    }

    @Test
    public void testConstAndBindVariableMix() throws Exception {
        final ObjList<BindVarTuple> cases = new ObjList<>();
        cases.add(BindVarTuple.ok(
                "mix",
                """
                        x\ta
                        58\t0.6821660861001273
                        90\t0.3901731258748704
                        """,
                bindVariableService -> bindVariableService.setStr(0, "0.6821660861001273")
        ));

        assertQuery("test where a in (0.3901731258748704, $1)")
                .ddl("create table test as (select x, rnd_double() a from long_sequence(100))")
                .assertBinds(cases);
    }

    @Test
    public void testNulls() throws Exception {
        // when more than one argument supplied, the function will match exact values from the list
        final ObjList<BindVarTuple> cases = new ObjList<>();
        cases.add(BindVarTuple.ok(
                "simple",
                """
                        x\ta
                        8\tnull
                        17\tnull
                        26\tnull
                        29\tnull
                        30\tnull
                        35\tnull
                        39\t0.6590341607692226
                        41\tnull
                        44\tnull
                        45\t0.45659895188239796
                        49\tnull
                        54\tnull
                        56\tnull
                        57\tnull
                        60\tnull
                        65\tnull
                        67\tnull
                        69\tnull
                        70\tnull
                        73\tnull
                        74\tnull
                        75\tnull
                        80\tnull
                        81\tnull
                        84\tnull
                        96\tnull
                        99\tnull
                        """,
                bindVariableService -> {
                    bindVariableService.setStr(0, "0.6590341607692226");
                    bindVariableService.setDouble(1, 0.45659895188239796);
                }
        ));

        cases.add(BindVarTuple.ok(
                "bad type",
                """
                        x\ta
                        8\tnull
                        17\tnull
                        26\tnull
                        29\tnull
                        30\tnull
                        35\tnull
                        39\t0.6590341607692226
                        41\tnull
                        44\tnull
                        49\tnull
                        54\tnull
                        56\tnull
                        57\tnull
                        60\tnull
                        65\tnull
                        67\tnull
                        69\tnull
                        70\tnull
                        73\tnull
                        74\tnull
                        75\tnull
                        80\tnull
                        81\tnull
                        84\tnull
                        96\tnull
                        99\tnull
                        """,
                bindVariableService -> {
                    bindVariableService.setStr(0, null);
                    bindVariableService.setDouble(1, 0.6590341607692226);
                }
        ));
        assertQuery("test where a in ($1,null,$2)")
                .ddl("create table test as (select x, rnd_double(1) a from long_sequence(100))")
                .assertBinds(cases);
    }

    @Test
    public void testUnsupportedConstType() throws Exception {
        execute("create table test as (select x, rnd_double(1) a from long_sequence(100))");
        assertQuery("test where a in (0.234, cast ('1CB' as geohash(1b)))")
                .fails(24, "cannot compare DOUBLE with type GEOHASH(1b)");
    }
}
