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

package io.questdb.test.griffin.engine.functions;

import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVariableTestTuple;
import org.junit.Test;

public class InTimestampTimestampTest extends AbstractCairoTest {

    @Test
    public void testBindVarRuntimeConstantsWithConstant() throws SqlException {
        ddl("create table MovementLog(\n" +
                "ts timestamp,\n" +
                "initParticipantId long,\n" +
                "initParticipantIdType symbol,\n" +
                "movementBusinessDate date,\n" +
                "slotId timestamp\n" +
                ") timestamp(ts) partition by day wal\n");

        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "runtime constants",
                "participantId\tparticipantIdType\n",
                bindVariableService -> {
                    bindVariableService.setDate(0, 1000L);
                    bindVariableService.setTimestamp(1, 2000L);
                    bindVariableService.setTimestamp(2, 3000L);
                }
        ));

        assertSql("SELECT DISTINCT initParticipantId AS participantId, initParticipantIdType AS participantIdType\n" +
                "FROM 'MovementLog'\n" +
                "WHERE movementBusinessDate=$1 AND slotId IN ($2, '1970-01-01T00:00:00.005000Z', $3)\n" +
                "ORDER BY initParticipantId\n" +
                "LIMIT 0,6", tuples);
    }

    @Test
    public void testBindVarTypeChange() throws SqlException {
        ddl("create table test as (select rnd_int() a, timestamp_sequence(0, 1000) ts from long_sequence(100))");

        // when more than one argument supplied, the function will match exact values from the list
        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "simple",
                "a\tts\n" +
                        "-1148479920\t1970-01-01T00:00:00.000000Z\n" +
                        "315515118\t1970-01-01T00:00:00.001000Z\n" +
                        "-948263339\t1970-01-01T00:00:00.005000Z\n",
                bindVariableService -> {
                    bindVariableService.setInt(0, 0);
                    bindVariableService.setInt(1, 1000);
                    bindVariableService.setStr(2, "1970-01-01T00:00:00.005000Z");
                }
        ));

        tuples.add(new BindVariableTestTuple(
                "type change",
                "a\tts\n" +
                        "1326447242\t1970-01-01T00:00:00.006000Z\n" +
                        "592859671\t1970-01-01T00:00:00.007000Z\n" +
                        "-1191262516\t1970-01-01T00:00:00.010000Z\n",
                bindVariableService -> {
                    bindVariableService.setLong(0, 6000);
                    bindVariableService.setStr(1, "1970-01-01T00:00:00.007000Z");
                    bindVariableService.setInt(2, 10_000);
                }
        ));

        tuples.add(new BindVariableTestTuple(
                "type change with varchar",
                "a\tts\n" +
                        "1326447242\t1970-01-01T00:00:00.006000Z\n" +
                        "-1191262516\t1970-01-01T00:00:00.010000Z\n" +
                        "-2041844972\t1970-01-01T00:00:00.011000Z\n",
                bindVariableService -> {
                    bindVariableService.setLong(0, 6000);
                    bindVariableService.setVarchar(1, new Utf8String("1970-01-01T00:00:00.011000Z"));
                    bindVariableService.setInt(2, 10_000);
                }
        ));

        assertSql("test where ts in ($1,$2,$3)", tuples);
    }

    @Test
    public void testIntervalBindVariable() throws SqlException {
        ddl("create table test as (select rnd_int() a, timestamp_sequence(0, 1000) ts from long_sequence(10000))");

        // baseline
        assertSql(
                "timestamp_floor\tcount\n" +
                        "1970-01-01T00:00:02.000000Z\t1000\n",
                "select timestamp_floor('1s', ts), count() from test where ts in '1970-01-01T00:00:02'"
        );

        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "2s",
                "timestamp_floor\tcount\n" +
                        "1970-01-01T00:00:02.000000Z\t1000\n",
                bindVariableService -> bindVariableService.setStr(0, "1970-01-01T00:00:02")
        ));

        tuples.add(new BindVariableTestTuple(
                "int interval",
                "unsupported bind variable type [INT] expected one of [STRING or VARCHAR]",
                bindVariableService -> bindVariableService.setInt(0, 10),
                64
        ));

        tuples.add(new BindVariableTestTuple(
                "2s",
                "timestamp_floor\tcount\n" +
                        "1970-01-01T00:00:03.000000Z\t1000\n",
                bindVariableService -> bindVariableService.setStr(0, "1970-01-01T00:00:03")
        ));

        assertSql(
                "select timestamp_floor('1s', ts), count() from test where ts in $1",
                tuples
        );
    }

    @Test
    public void testListOfTimestamps() throws SqlException {
        ddl("create table test as (select rnd_int() a, timestamp_sequence(0, 1000) ts from long_sequence(100))");

        assertSql(
                "a\tts\n" +
                        "-1148479920\t1970-01-01T00:00:00.000000Z\n" +
                        "-2144581835\t1970-01-01T00:00:00.070000Z\n" +
                        "-296610933\t1970-01-01T00:00:00.077000Z\n",
                "test where ts in ('1970-01-01T00:00:00.070000Z', 77000, '1970-01-01'::date)"
        );
    }

    @Test
    public void testListOfTimestampsInvalidInput() throws Exception {
        ddl("create table test as (select rnd_int() a, timestamp_sequence(0, 1000) ts from long_sequence(100))");

        assertException(
                "test where ts in ('1970-01-01T00:00:0.070000Z', 'abc')",
                18,
                "Invalid date [str=1970-01-01T00:00:0.070000Z]"
        );
    }

    @Test
    public void testListOfTimestampsUnsupportedType() throws Exception {
        ddl("create table test as (select rnd_int() a, timestamp_sequence(0, 1000) ts from long_sequence(100))");

        assertException(
                "test where ts in ('1970-01-01T00:00:00.070000Z', true)",
                49,
                "cannot compare TIMESTAMP with type BOOLEAN"
        );
    }
}
