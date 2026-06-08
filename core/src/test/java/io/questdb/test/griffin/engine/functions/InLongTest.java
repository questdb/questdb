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
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVariableTestTuple;
import org.junit.Test;

public class InLongTest extends AbstractCairoTest {

    @Test
    public void testBindVarConstants() throws SqlException {
        execute("""
                create table MovementLog(
                ts timestamp,
                initParticipantId long,
                initParticipantIdType symbol,
                movementBusinessDate date,
                slotId long
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
                WHERE movementBusinessDate=$1 AND slotId IN (1, 2, 3)
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
                slotId long
                ) timestamp(ts) partition by day wal
                """);

        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "runtime constants",
                "participantId\tparticipantIdType\n",
                bindVariableService -> {
                    bindVariableService.setDate(0, 1000L);
                    bindVariableService.setLong(1, 1L);
                    bindVariableService.setLong(2, 2L);
                    bindVariableService.setLong(3, 3L);
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
        execute("create table test as (select x, rnd_long(2991, 2989892, 1) a from long_sequence(100))");

        // when more than one argument supplied, the function will match exact values from the list
        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "simple",
                """
                        x\ta
                        30\t1605084
                        38\t223221
                        69\t2433166
                        """,
                bindVariableService -> {
                    bindVariableService.setStr(0, "1605084");
                    bindVariableService.setInt(1, 223221);
                    bindVariableService.setLong(2, 2433166);
                }
        ));

        tuples.add(new BindVariableTestTuple(
                "undefined bind variable",
                "undefined bind variable: 1",
                bindVariableService -> {
                    bindVariableService.setLong(0, 402266);
                    bindVariableService.setLong(2, 55333);
                },
                20
        ));

        tuples.add(new BindVariableTestTuple(
                "bad type",
                "inconvertible types: GEOHASH(4c) -> LONG [from=GEOHASH(4c), to=LONG]",
                bindVariableService -> {
                    bindVariableService.setStr(0, "402266");
                    bindVariableService.setGeoHash(1, 30, ColumnType.getGeoHashTypeWithBits(20));
                    bindVariableService.setLong(2, 55333);
                },
                20
        ));

        tuples.add(new BindVariableTestTuple(
                "bad long",
                "invalid LONG value [not a long]",
                bindVariableService -> {
                    bindVariableService.setStr(0, "402266");
                    bindVariableService.setLong(1, 55333);
                    bindVariableService.setStr(2, "not a long");
                },
                23
        ));

        assertSql("test where a in ($1,$2,$3)", tuples);
    }

    @Test
    public void testConstAndBindVariableMix() throws SqlException {
        execute("create table test as (select x, rnd_long() a from long_sequence(100))");

        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "mix",
                """
                        x\ta
                        18\t8173439391403617681
                        39\t8416773233910814357
                        """,
                bindVariableService -> bindVariableService.setLong(0, 8173439391403617681L)
        ));

        assertSql("test where a in (8416773233910814357L, $1)", tuples);
    }

    @Test
    public void testNulls() throws SqlException {
        execute("create table test as (select x, rnd_double(1) a from long_sequence(100))");

        // when more than one argument supplied, the function will match exact values from the list
        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
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

        tuples.add(new BindVariableTestTuple(
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

        assertSql("test where a in ($1,null,$2)", tuples);
    }
}
