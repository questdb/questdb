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

package io.questdb.test.griffin.engine.functions.conditional;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class SwitchFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBindVar() throws Exception {
        assertException(
                """
                        select\s
                            a,
                            case a
                                when '1' then $1
                                when '2' then $2
                                else $3
                            end k
                        from test""",
                "create table test as (select cast(x as varchar) a, timestamp_sequence(0, 1000000) ts from long_sequence(5))",
                48,
                "CASE values cannot be bind variables"
        );
    }

    @Test
    public void testBindVarAsKey() throws Exception {
        assertException(
                "SELECT CASE $1 WHEN 'a' THEN b ELSE c END FROM test",
                "CREATE TABLE test AS (SELECT rnd_str('a', 'b') b, rnd_str('c', 'd') c FROM long_sequence(5))",
                12,
                "bind variable is not supported here, please use column instead"
        );
    }

    @Test
    public void testBooleanDuplicateFalse() throws Exception {
        assertException(
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when false then 'HELLO'
                                when false then 'HELLO2'
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_boolean() x," +
                        " rnd_str() a," +
                        " rnd_str() b," +
                        " rnd_str() c" +
                        " from long_sequence(20)" +
                        ")",
                92,
                "duplicate branch"
        );
    }

    @Test
    public void testBooleanDuplicateTrue() throws Exception {
        assertException(
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when true then 'HELLO'
                                when true then 'HELLO2'
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_boolean() x," +
                        " rnd_str() a," +
                        " rnd_str() b," +
                        " rnd_str() c" +
                        " from long_sequence(20)" +
                        ")",
                91,
                "duplicate branch"
        );
    }

    @Test
    public void testBooleanDuplicateWayTooManyBranches() throws Exception {
        assertException(
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when false then 'HELLO'
                                when true then 'HELLO2'
                                when false then 'HELLO3'
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_boolean() x," +
                        " rnd_str() a," +
                        " rnd_str() b," +
                        " rnd_str() c" +
                        " from long_sequence(20)" +
                        ")",
                124,
                "too many branches"
        );
    }

    @Test
    public void testBooleanToStrOrElse() throws Exception {
        assertQuery(
                """
                        x\ta\tb\tc\tk
                        false\tWCPS\tYRXPE\tRXG\tRXG
                        false\tUXIBBT\tGWFFYUD\tYQEHBH\tYQEHBH
                        false\tLPD\tSBEOUOJS\tUED\tUED
                        true\tULOFJGE\tRSZSRYRF\tTMHGOOZZVD\tHELLO
                        true\tYICCXZOUIC\tKGH\tVSDOTS\tHELLO
                        true\tYCTGQO\tXWCK\tSUWDSWU\tHELLO
                        false\tOLNVTI\tZXIOVI\tSMSSUQ\tSMSSUQ
                        false\tTKVV\tOJIPHZ\tIHVL\tIHVL
                        true\tLJU\tGLHMLLEOYP\tIPZIMNZZR\tHELLO
                        true\tBEZGHWVD\tLOPJOX\tRGIIHYH\tHELLO
                        true\tMYSSMPGLUO\tZHZSQLDGL\tIFOUSZM\tHELLO
                        true\tEBNDCQ\tHNOMVELLKK\tWNWIFFLR\tHELLO
                        true\tMNXKUIZ\tIGYV\tFKWZ\tHELLO
                        true\tGXHFVWSWSR\tONFCLTJCKF\tNTO\tHELLO
                        false\tUKL\tXSLUQD\tPHNIMYF\tPHNIMYF
                        true\tNPH\tPBNH\tWWC\tHELLO
                        false\tTNLE\tUHH\tGGLN\tGGLN
                        false\tLCBDMIGQ\tKHT\tZSLQVFGPP\tZSLQVFGPP
                        true\tXBHYSBQYMI\tSVTNPIW\tFKPEV\tHELLO
                        false\tFNWG\tDGGI\tDVRVNGS\tDVRVNGS
                        """,
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when true then 'HELLO'
                                else c
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_boolean() x," +
                        " rnd_str() a," +
                        " rnd_str() b," +
                        " rnd_str() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testBooleanToStrOrElseReversed() throws Exception {
        assertQuery(
                """
                        x\ta\tb\tc\tk
                        false\tWCPS\tYRXPE\tRXG\tRXG
                        false\tUXIBBT\tGWFFYUD\tYQEHBH\tYQEHBH
                        false\tLPD\tSBEOUOJS\tUED\tUED
                        true\tULOFJGE\tRSZSRYRF\tTMHGOOZZVD\tHELLO
                        true\tYICCXZOUIC\tKGH\tVSDOTS\tHELLO
                        true\tYCTGQO\tXWCK\tSUWDSWU\tHELLO
                        false\tOLNVTI\tZXIOVI\tSMSSUQ\tSMSSUQ
                        false\tTKVV\tOJIPHZ\tIHVL\tIHVL
                        true\tLJU\tGLHMLLEOYP\tIPZIMNZZR\tHELLO
                        true\tBEZGHWVD\tLOPJOX\tRGIIHYH\tHELLO
                        true\tMYSSMPGLUO\tZHZSQLDGL\tIFOUSZM\tHELLO
                        true\tEBNDCQ\tHNOMVELLKK\tWNWIFFLR\tHELLO
                        true\tMNXKUIZ\tIGYV\tFKWZ\tHELLO
                        true\tGXHFVWSWSR\tONFCLTJCKF\tNTO\tHELLO
                        false\tUKL\tXSLUQD\tPHNIMYF\tPHNIMYF
                        true\tNPH\tPBNH\tWWC\tHELLO
                        false\tTNLE\tUHH\tGGLN\tGGLN
                        false\tLCBDMIGQ\tKHT\tZSLQVFGPP\tZSLQVFGPP
                        true\tXBHYSBQYMI\tSVTNPIW\tFKPEV\tHELLO
                        false\tFNWG\tDGGI\tDVRVNGS\tDVRVNGS
                        """,
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when false then c
                                else 'HELLO'
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_boolean() x," +
                        " rnd_str() a," +
                        " rnd_str() b," +
                        " rnd_str() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testBooleanToStrOrMoreBranches() throws Exception {
        assertQuery(
                """
                        x\ta\tb\tc\tk
                        false\tWCPS\tYRXPE\tRXG\tRXG
                        false\tUXIBBT\tGWFFYUD\tYQEHBH\tYQEHBH
                        false\tLPD\tSBEOUOJS\tUED\tUED
                        true\tULOFJGE\tRSZSRYRF\tTMHGOOZZVD\tHELLO
                        true\tYICCXZOUIC\tKGH\tVSDOTS\tHELLO
                        true\tYCTGQO\tXWCK\tSUWDSWU\tHELLO
                        false\tOLNVTI\tZXIOVI\tSMSSUQ\tSMSSUQ
                        false\tTKVV\tOJIPHZ\tIHVL\tIHVL
                        true\tLJU\tGLHMLLEOYP\tIPZIMNZZR\tHELLO
                        true\tBEZGHWVD\tLOPJOX\tRGIIHYH\tHELLO
                        true\tMYSSMPGLUO\tZHZSQLDGL\tIFOUSZM\tHELLO
                        true\tEBNDCQ\tHNOMVELLKK\tWNWIFFLR\tHELLO
                        true\tMNXKUIZ\tIGYV\tFKWZ\tHELLO
                        true\tGXHFVWSWSR\tONFCLTJCKF\tNTO\tHELLO
                        false\tUKL\tXSLUQD\tPHNIMYF\tPHNIMYF
                        true\tNPH\tPBNH\tWWC\tHELLO
                        false\tTNLE\tUHH\tGGLN\tGGLN
                        false\tLCBDMIGQ\tKHT\tZSLQVFGPP\tZSLQVFGPP
                        true\tXBHYSBQYMI\tSVTNPIW\tFKPEV\tHELLO
                        false\tFNWG\tDGGI\tDVRVNGS\tDVRVNGS
                        """,
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when true then 'HELLO'
                                when false then c
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_boolean() x," +
                        " rnd_str() a," +
                        " rnd_str() b," +
                        " rnd_str() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testBooleanToStrOrMoreBranchesReversed() throws Exception {
        assertQuery(
                """
                        x\ta\tb\tc\tk
                        false\tWCPS\tYRXPE\tRXG\tRXG
                        false\tUXIBBT\tGWFFYUD\tYQEHBH\tYQEHBH
                        false\tLPD\tSBEOUOJS\tUED\tUED
                        true\tULOFJGE\tRSZSRYRF\tTMHGOOZZVD\tHELLO
                        true\tYICCXZOUIC\tKGH\tVSDOTS\tHELLO
                        true\tYCTGQO\tXWCK\tSUWDSWU\tHELLO
                        false\tOLNVTI\tZXIOVI\tSMSSUQ\tSMSSUQ
                        false\tTKVV\tOJIPHZ\tIHVL\tIHVL
                        true\tLJU\tGLHMLLEOYP\tIPZIMNZZR\tHELLO
                        true\tBEZGHWVD\tLOPJOX\tRGIIHYH\tHELLO
                        true\tMYSSMPGLUO\tZHZSQLDGL\tIFOUSZM\tHELLO
                        true\tEBNDCQ\tHNOMVELLKK\tWNWIFFLR\tHELLO
                        true\tMNXKUIZ\tIGYV\tFKWZ\tHELLO
                        true\tGXHFVWSWSR\tONFCLTJCKF\tNTO\tHELLO
                        false\tUKL\tXSLUQD\tPHNIMYF\tPHNIMYF
                        true\tNPH\tPBNH\tWWC\tHELLO
                        false\tTNLE\tUHH\tGGLN\tGGLN
                        false\tLCBDMIGQ\tKHT\tZSLQVFGPP\tZSLQVFGPP
                        true\tXBHYSBQYMI\tSVTNPIW\tFKPEV\tHELLO
                        false\tFNWG\tDGGI\tDVRVNGS\tDVRVNGS
                        """,
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when false then c
                                when true then 'HELLO'
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_boolean() x," +
                        " rnd_str() a," +
                        " rnd_str() b," +
                        " rnd_str() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testBooleanTooManyBranchesIgnoreElse() throws Exception {
        assertQuery(
                """
                        x\ta\tb\tc\tk
                        false\tWCPS\tYRXPE\tRXG\tHELLO2
                        false\tUXIBBT\tGWFFYUD\tYQEHBH\tHELLO2
                        false\tLPD\tSBEOUOJS\tUED\tHELLO2
                        true\tULOFJGE\tRSZSRYRF\tTMHGOOZZVD\tHELLO
                        true\tYICCXZOUIC\tKGH\tVSDOTS\tHELLO
                        true\tYCTGQO\tXWCK\tSUWDSWU\tHELLO
                        false\tOLNVTI\tZXIOVI\tSMSSUQ\tHELLO2
                        false\tTKVV\tOJIPHZ\tIHVL\tHELLO2
                        true\tLJU\tGLHMLLEOYP\tIPZIMNZZR\tHELLO
                        true\tBEZGHWVD\tLOPJOX\tRGIIHYH\tHELLO
                        true\tMYSSMPGLUO\tZHZSQLDGL\tIFOUSZM\tHELLO
                        true\tEBNDCQ\tHNOMVELLKK\tWNWIFFLR\tHELLO
                        true\tMNXKUIZ\tIGYV\tFKWZ\tHELLO
                        true\tGXHFVWSWSR\tONFCLTJCKF\tNTO\tHELLO
                        false\tUKL\tXSLUQD\tPHNIMYF\tHELLO2
                        true\tNPH\tPBNH\tWWC\tHELLO
                        false\tTNLE\tUHH\tGGLN\tHELLO2
                        false\tLCBDMIGQ\tKHT\tZSLQVFGPP\tHELLO2
                        true\tXBHYSBQYMI\tSVTNPIW\tFKPEV\tHELLO
                        false\tFNWG\tDGGI\tDVRVNGS\tHELLO2
                        """,
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when true then 'HELLO'
                                when false then 'HELLO2'
                                else c
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_boolean() x," +
                        " rnd_str() a," +
                        " rnd_str() b," +
                        " rnd_str() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testBranchTypeMismatch() throws Exception {
        assertException(
                "SELECT CASE u WHEN 123 THEN 'a' ELSE 'b' END FROM test",
                "CREATE TABLE test AS (SELECT rnd_uuid4() u FROM long_sequence(5))",
                19,
                "type mismatch [expected=UUID, actual=INT]"
        );
    }

    @Test
    public void testByteOrElse() throws Exception {
        assertQuery(
                """
                        x\ta\tb\tc\tk
                        76\tT\tJ\tW\tJ
                        79\tP\tS\tW\tS
                        90\tY\tR\tX\tY
                        74\tE\tH\tN\tH
                        32\tX\tG\tZ\tG
                        77\tX\tU\tX\tU
                        101\tB\tB\tT\tB
                        89\tP\tG\tW\tG
                        112\tF\tY\tU\tY
                        117\tE\tY\tY\tY
                        86\tE\tH\tB\tH
                        65\tF\tO\tW\tO
                        73\tP\tD\tX\tD
                        119\tS\tB\tE\tB
                        57\tU\tO\tJ\tJ
                        103\tH\tR\tU\tR
                        58\tD\tR\tQ\tR
                        20\tU\tL\tO\tL
                        54\tJ\tG\tE\tG
                        31\tJ\tR\tS\tZ
                        """,
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when cast(90 as byte) then a
                                when cast(57 as byte) then c
                                when cast(31 as byte) then 'Z'
                                else b
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_byte() x," +
                        " rnd_char() a," +
                        " rnd_char() b," +
                        " rnd_char() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testCastValueToIPv4_1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select x, rnd_ipv4('54.23.11.87/8', 2) ip from long_sequence(5))");
            assertSql(
                    """
                            x\tip\tk
                            1\t54.206.96.238\t54.206.96.238
                            2\t\t
                            3\t54.98.173.21\t127.0.0.1
                            4\t54.15.250.138\t127.0.0.1
                            5\t\t127.0.0.1
                            """,
                    """
                            select\s
                                x,
                                ip,
                                case x
                                    when 1 then ip
                                    when 2 then null
                                    else '127.0.0.1'
                                end k
                            from x"""
            );
        });
    }

    @Test
    public void testCastValueToIPv4_2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select x, rnd_ipv4('54.23.11.87/8', 2) ip from long_sequence(5))");
            assertSql(
                    """
                            x\tip\tk
                            1\t54.206.96.238\t192.168.1.1
                            2\t\t
                            3\t54.98.173.21\t54.98.173.21
                            4\t54.15.250.138\t54.15.250.138
                            5\t\t
                            """,
                    """
                            select\s
                                x,
                                ip,
                                case x
                                    when 1 then '192.168.1.1'
                                    else ip
                                end k
                            from x"""
            );
        });
    }

    @Test
    public void testCastValueToLong256() throws Exception {
        execute(
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_int() a," +
                        " rnd_long() b," +
                        " rnd_long256() c" +
                        " from long_sequence(20)" +
                        ")"
        );

        assertException(
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when -920 then a
                                when 701 then c
                                when -714 then 350
                            end k
                        from tanc""",
                94,
                "inconvertible types: LONG256 -> INT [from=LONG256, to=INT]"
        );
    }

    @Test
    public void testCastValueToUuid1() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table tanc as (" +
                            "select rnd_int() % 1000 x," +
                            " rnd_int() a," +
                            " rnd_long() b," +
                            " rnd_long256() c, " +
                            " rnd_uuid4() d" +
                            " from long_sequence(20)" +
                            ")"
            );
            assertExceptionNoLeakCheck(
                    """
                            select\s
                                x,
                                a,
                                b,
                                c,
                                d,
                                case x
                                    when -920 then a
                                    when -405 then 350
                                    when 968 then d
                                end k
                            from tanc""",
                    128,
                    "inconvertible types: UUID -> INT [from=UUID, to=INT]"
            );
        });
    }

    @Test
    public void testCastValueToUuid2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select x, rnd_uuid4() u from long_sequence(5))");
            assertSql(
                    """
                            x\tu\tk
                            1\t0010cde8-12ce-40ee-8010-a928bb8b9650\t0010cde8-12ce-40ee-8010-a928bb8b9650
                            2\t9f9b2131-d49f-4d1d-ab81-39815c50d341\tb5b2159a-2356-4217-965d-4c984f0ffa8a
                            3\t7bcd48d8-c77a-4655-b2a2-15ba0462ad15\t
                            4\tb5b2159a-2356-4217-965d-4c984f0ffa8a\t00000000-0000-0000-0000-000000000000
                            5\te8beef38-cd7b-43d8-9b2d-34586f6275fa\t00000000-0000-0000-0000-000000000000
                            """,
                    """
                            select\s
                                x,
                                u,
                                case x
                                    when 1 then u
                                    when 2 then 'b5b2159a-2356-4217-965d-4c984f0ffa8a'
                                    when 3 then null
                                    else '00000000-0000-0000-0000-000000000000'
                                end k
                            from x"""
            );
        });
    }

    @Test
    public void testCastValueToUuid3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select x, rnd_uuid4() u from long_sequence(5))");
            assertSql(
                    """
                            x\tu\tk
                            1\t0010cde8-12ce-40ee-8010-a928bb8b9650\t00000000-0000-0000-0000-000000000000
                            2\t9f9b2131-d49f-4d1d-ab81-39815c50d341\t9f9b2131-d49f-4d1d-ab81-39815c50d341
                            3\t7bcd48d8-c77a-4655-b2a2-15ba0462ad15\t7bcd48d8-c77a-4655-b2a2-15ba0462ad15
                            4\tb5b2159a-2356-4217-965d-4c984f0ffa8a\tb5b2159a-2356-4217-965d-4c984f0ffa8a
                            5\te8beef38-cd7b-43d8-9b2d-34586f6275fa\te8beef38-cd7b-43d8-9b2d-34586f6275fa
                            """,
                    """
                            select\s
                                x,
                                u,
                                case x
                                    when 1 then '00000000-0000-0000-0000-000000000000'
                                    else u
                                end k
                            from x"""
            );
        });
    }

    @Test
    public void testCastValueToUuid4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select x, rnd_uuid4() u from long_sequence(5))");
            assertSql(
                    """
                            x\tu\tk
                            1\t0010cde8-12ce-40ee-8010-a928bb8b9650\t
                            2\t9f9b2131-d49f-4d1d-ab81-39815c50d341\t9f9b2131-d49f-4d1d-ab81-39815c50d341
                            3\t7bcd48d8-c77a-4655-b2a2-15ba0462ad15\t7bcd48d8-c77a-4655-b2a2-15ba0462ad15
                            4\tb5b2159a-2356-4217-965d-4c984f0ffa8a\tb5b2159a-2356-4217-965d-4c984f0ffa8a
                            5\te8beef38-cd7b-43d8-9b2d-34586f6275fa\te8beef38-cd7b-43d8-9b2d-34586f6275fa
                            """,
                    """
                            select\s
                                x,
                                u,
                                case x
                                    when 1 then null
                                    else u
                                end k
                            from x"""
            );
        });
    }

    @Test
    public void testCharOrElse() throws Exception {
        assertQuery(
                """
                        x\ta\tb\tc\tk
                        V\tT\tJ\tW\tJ
                        C\tP\tS\tW\tS
                        H\tY\tR\tX\tR
                        P\tE\tH\tN\tH
                        R\tX\tG\tZ\tG
                        S\tX\tU\tX\tU
                        I\tB\tB\tT\tB
                        G\tP\tG\tW\tG
                        F\tF\tY\tU\tY
                        D\tE\tY\tY\tY
                        Q\tE\tH\tB\tH
                        H\tF\tO\tW\tO
                        L\tP\tD\tX\tP
                        Y\tS\tB\tE\tB
                        O\tU\tO\tJ\tJ
                        S\tH\tR\tU\tR
                        E\tD\tR\tQ\tR
                        Q\tU\tL\tO\tL
                        F\tJ\tG\tE\tG
                        T\tJ\tR\tS\tZ
                        """,
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when 'L' then a
                                when 'O' then c
                                when 'T' then 'Z'
                                else b
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_char() x," +
                        " rnd_char() a," +
                        " rnd_char() b," +
                        " rnd_char() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testDateOrElse() throws Exception {
        assertQuery(
                """
                        x\ta\tb\tc\tk
                        1970-01-01T02:07:23.856Z\tT\tJ\tW\tJ
                        1970-01-01T00:43:07.029Z\tP\tS\tW\tZ
                        1970-01-01T00:14:24.006Z\tY\tR\tX\tR
                        1970-01-01T00:32:57.934Z\tE\tH\tN\tH
                        1970-01-01T01:00:00.060Z\tX\tG\tZ\tG
                        1970-01-01T01:52:00.859Z\tX\tU\tX\tU
                        1970-01-01T02:37:52.057Z\tB\tB\tT\tB
                        1970-01-01T02:03:42.727Z\tP\tG\tW\tG
                        1970-01-01T02:45:57.016Z\tF\tY\tU\tY
                        1970-01-01T02:30:11.353Z\tE\tY\tY\tY
                        1970-01-01T00:55:56.086Z\tE\tH\tB\tE
                        1970-01-01T01:24:20.057Z\tF\tO\tW\tO
                        1970-01-01T01:04:57.951Z\tP\tD\tX\tD
                        1970-01-01T01:38:37.157Z\tS\tB\tE\tB
                        1970-01-01T02:37:52.839Z\tU\tO\tJ\tO
                        1970-01-01T00:38:26.717Z\tH\tR\tU\tR
                        1970-01-01T00:48:12.010Z\tD\tR\tQ\tQ
                        1970-01-01T01:53:35.364Z\tU\tL\tO\tL
                        1970-01-01T00:08:55.106Z\tJ\tG\tE\tG
                        1970-01-01T02:04:44.767Z\tJ\tR\tS\tR
                        """,
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when cast('1970-01-01T00:55:56.086Z' as date) then a
                                when cast('1970-01-01T00:48:12.010Z' as date) then c
                                when cast('1970-01-01T00:43:07.029Z' as date) then 'Z'
                                else b
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_date() x," +
                        " rnd_char() a," +
                        " rnd_char() b," +
                        " rnd_char() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testDouble() throws Exception {
        assertQuery(
                """
                        x\ta\tb\tc\tk
                        322.0\t1548800833\t-727724771\t73575701\t1548800833
                        -431.0\t592859671\t1868723706\t-847531048\t-847531048
                        302.0\t-1436881714\t-1575378703\t806715481\tnull
                        -616.0\t1573662097\t-409854405\t339631474\t350
                        251.0\t-1532328444\t-1458132197\t1125579207\tnull
                        -156.0\t426455968\t-85170055\t-1792928964\tnull
                        -522.0\t-1101822104\t-1153445279\t1404198\tnull
                        419.0\t1631244228\t-1975183723\t-1252906348\tnull
                        760.0\t-2119387831\t-212807500\t1699553881\tnull
                        381.0\t-113506296\t-422941535\t-938514914\tnull
                        243.0\t-303295973\t-342047842\t-2132716300\tnull
                        -618.0\t-27395319\t264240638\t2085282008\tnull
                        808.0\t1890602616\t-1272693194\t68265578\tnull
                        340.0\t44173540\t458818940\t410717394\tnull
                        -154.0\t-1418341054\t-1162267908\t2031014705\tnull
                        null\t-530317703\t-1575135393\t-296610933\t1
                        null\t936627841\t326010667\t-667031149\t2
                        null\t-1870444467\t-2034804966\t171200398\t3
                        0.0\t1637847416\t-419093579\t-1819240775\t4
                        -0.0\t-1533414895\t-1787109293\t-66297136\t5
                        """,
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when  322.0d then a
                                when -431.0d then c
                                when -616.0d then 350
                                when null then 1
                                when 'Infinity' then 2
                                when '-Infinity' then 3
                                when 0.0 then 4
                                when -0.0 then 5
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select round(rnd_double() * 2000 - 1000) x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(15)" +
                        "union all " +
                        "select " +
                        "case x when 1 then null::double " +
                        "       when 2 then 'Infinity'::double " +
                        "       when 3 then '-Infinity'::double " +
                        "       when 4 then  0.0::double " +
                        "       when 5 then -0.0::double end x, " +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(5) )",
                null,
                true,
                true
        );
    }

    @Test
    public void testDoubleDuplicateBranch() throws Exception {
        assertException(
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when -920.0d then a
                                when 701.0d then c
                                when -714.0d then 350
                                when 701.0d then c
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select round(rnd_double() * 2000 - 1000) x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(20)" +
                        ")",
                145,
                "duplicate branch"
        );
    }

    @Test
    public void testDoubleOrElse() throws Exception {
        assertQuery(
                """
                        x\ta\tb\tc\tk
                        322.0\t1548800833\t-727724771\t73575701\t1548800833
                        -431.0\t592859671\t1868723706\t-847531048\t-847531048
                        302.0\t-1436881714\t-1575378703\t806715481\t-1575378703
                        -616.0\t1573662097\t-409854405\t339631474\t350
                        251.0\t-1532328444\t-1458132197\t1125579207\t-1458132197
                        -156.0\t426455968\t-85170055\t-1792928964\t-85170055
                        -522.0\t-1101822104\t-1153445279\t1404198\t-1153445279
                        419.0\t1631244228\t-1975183723\t-1252906348\t-1975183723
                        760.0\t-2119387831\t-212807500\t1699553881\t-212807500
                        381.0\t-113506296\t-422941535\t-938514914\t-422941535
                        243.0\t-303295973\t-342047842\t-2132716300\t-342047842
                        -618.0\t-27395319\t264240638\t2085282008\t264240638
                        808.0\t1890602616\t-1272693194\t68265578\t-1272693194
                        340.0\t44173540\t458818940\t410717394\t458818940
                        -154.0\t-1418341054\t-1162267908\t2031014705\t-1162267908
                        null\t-530317703\t-1575135393\t-296610933\t1
                        null\t936627841\t326010667\t-667031149\t2
                        null\t-1870444467\t-2034804966\t171200398\t3
                        0.0\t1637847416\t-419093579\t-1819240775\t4
                        -0.0\t-1533414895\t-1787109293\t-66297136\t5
                        """,
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when  322.0d then a
                                when -431.0 then c
                                when -616.0 then 350
                                when null then 1
                                when 'Infinity' then 2
                                when '-Infinity' then 3
                                when 0.0 then 4
                                when -0.0 then 5
                                else b
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select round(rnd_double() * 2000 - 1000) x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(15)" +
                        "union all " +
                        "select " +
                        "case x when 1 then null::double " +
                        "       when 2 then 'Infinity'::double " +
                        "       when 3 then '-Infinity'::double " +
                        "       when 4 then  0.0::double " +
                        "       when 5 then -0.0::double end x, " +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(5) )",
                null,
                true,
                true
        );
    }

    @Test
    public void testDuplicateBranchStringToLongCast() throws Exception {
        assertException(
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when -920 then a
                                when 701 then c
                                when -714 then 350
                                when '701' then c
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_long() % 1000 x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(20)" +
                        ")",
                136,
                "duplicate branch"
        );
    }

    @Test
    public void testFloat() throws Exception {
        assertQuery(
                """
                        x\ta\tb\tc\tk
                        322.0\t315515118\t1548800833\t-727724771\t315515118
                        -830.0\t-948263339\t1326447242\t592859671\t592859671
                        -591.0\t-847531048\t-1191262516\t-2041844972\tnull
                        685.0\t-1575378703\t806715481\t1545253512\t350
                        -551.0\t1573662097\t-409854405\t339631474\tnull
                        251.0\t1904508147\t-1532328444\t-1458132197\tnull
                        49.0\t-1849627000\t-1432278050\t426455968\tnull
                        -926.0\t-1792928964\t-1844391305\t-1520872171\tnull
                        -155.0\t-1153445279\t1404198\t-1715058769\tnull
                        -380.0\t1631244228\t-1975183723\t-1252906348\tnull
                        760.0\t-761275053\t-2119387831\t-212807500\tnull
                        -342.0\t1110979454\t1253890363\t-113506296\tnull
                        954.0\t-938514914\t-547127752\t-1271909747\tnull
                        -684.0\t-342047842\t-2132716300\t2006313928\tnull
                        -195.0\t-27395319\t264240638\t2085282008\tnull
                        null\t-483853667\t2137969456\t1890602616\t1
                        null\t-1272693194\t68265578\t1036510002\t2
                        null\t-2002373666\t44173540\t458818940\t3
                        0.0\t410717394\t-2144581835\t1978144263\t4
                        -0.0\t-1418341054\t-1162267908\t2031014705\t5
                        """,
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when 322.0f then a
                                when -830.0f then c
                                when 685.0f then 350
                                when cast(null as float) then 1
                                when cast('Infinity' as float) then 2
                                when cast('-Infinity' as float) then 3
                                when cast(0.0 as float) then 4
                                when cast(-0.0 as float) then 5
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select cast(round(rnd_float() * 2000 - 1000) as float) x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(15)" +
                        "union all " +
                        "select " +
                        "case x when 1 then cast(null as float) " +
                        "       when 2 then cast('Infinity' as float) " +
                        "       when 3 then cast('-Infinity' as float) " +
                        "       when 4 then 0.0f " +
                        "       when 5 then -0.0f end x, " +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(5) )",
                null,
                true,
                true
        );
    }

    @Test
    public void testFloatDuplicateBranch() throws Exception {
        assertException(
                "SELECT CASE x WHEN 1.0 THEN a WHEN 2.0 THEN b WHEN 1.0 THEN c END k FROM tanc",
                "CREATE TABLE tanc AS (" +
                        "SELECT rnd_float() x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " FROM long_sequence(5)" +
                        ")",
                51,
                "duplicate branch"
        );
    }

    @Test
    public void testFloatOrElse() throws Exception {
        assertQuery(
                """
                        x\ta\tb\tc\tk
                        322.0\t315515118\t1548800833\t-727724771\t315515118
                        -830.0\t-948263339\t1326447242\t592859671\t592859671
                        -591.0\t-847531048\t-1191262516\t-2041844972\t-1191262516
                        685.0\t-1575378703\t806715481\t1545253512\t350
                        -551.0\t1573662097\t-409854405\t339631474\t-409854405
                        251.0\t1904508147\t-1532328444\t-1458132197\t-1532328444
                        49.0\t-1849627000\t-1432278050\t426455968\t-1432278050
                        -926.0\t-1792928964\t-1844391305\t-1520872171\t-1844391305
                        -155.0\t-1153445279\t1404198\t-1715058769\t1404198
                        -380.0\t1631244228\t-1975183723\t-1252906348\t-1975183723
                        760.0\t-761275053\t-2119387831\t-212807500\t-2119387831
                        -342.0\t1110979454\t1253890363\t-113506296\t1253890363
                        954.0\t-938514914\t-547127752\t-1271909747\t-547127752
                        -684.0\t-342047842\t-2132716300\t2006313928\t-2132716300
                        -195.0\t-27395319\t264240638\t2085282008\t264240638
                        null\t-483853667\t2137969456\t1890602616\t1
                        null\t-1272693194\t68265578\t1036510002\t2
                        null\t-2002373666\t44173540\t458818940\t3
                        0.0\t410717394\t-2144581835\t1978144263\t4
                        -0.0\t-1418341054\t-1162267908\t2031014705\t5
                        """,
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when 322.0f then a
                                when -830.0f then c
                                when 685.0f then 350
                                when cast(null as float) then 1
                                when cast('Infinity' as float) then 2
                                when cast('-Infinity' as float) then 3
                                when cast(0.0 as float) then 4
                                when cast(-0.0 as float) then 5
                                else b \
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select cast(round(rnd_float() * 2000 - 1000) as float) x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(15)" +
                        "union all " +
                        "select " +
                        "case x when 1 then cast(null as float) " +
                        "       when 2 then cast('Infinity' as float) " +
                        "       when 3 then cast('-Infinity' as float) " +
                        "       when 4 then 0.0f " +
                        "       when 5 then -0.0f end x, " +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(5) )",
                null,
                true,
                true
        );
    }

    @Test
    public void testInt() throws Exception {
        assertQuery(
                """
                        x\ta\tb\tc\tk
                        -920\t315515118\t1548800833\t-727724771\t315515118
                        701\t-948263339\t1326447242\t592859671\t592859671
                        706\t-847531048\t-1191262516\t-2041844972\tnull
                        -714\t-1575378703\t806715481\t1545253512\t350
                        116\t1573662097\t-409854405\t339631474\tnull
                        67\t1904508147\t-1532328444\t-1458132197\tnull
                        207\t-1849627000\t-1432278050\t426455968\tnull
                        -55\t-1792928964\t-1844391305\t-1520872171\tnull
                        -104\t-1153445279\t1404198\t-1715058769\tnull
                        -127\t1631244228\t-1975183723\t-1252906348\tnull
                        790\t-761275053\t-2119387831\t-212807500\tnull
                        881\t1110979454\t1253890363\t-113506296\tnull
                        -535\t-938514914\t-547127752\t-1271909747\tnull
                        -973\t-342047842\t-2132716300\t2006313928\tnull
                        -463\t-27395319\t264240638\t2085282008\tnull
                        -667\t2137969456\t1890602616\t-1272693194\tnull
                        578\t1036510002\t-2002373666\t44173540\tnull
                        940\t410717394\t-2144581835\t1978144263\tnull
                        -54\t-1162267908\t2031014705\t-530317703\tnull
                        -393\t-296610933\t936627841\t326010667\tnull
                        """,
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when -920 then a
                                when 701 then c
                                when -714 then 350
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntDuplicateBranch() throws Exception {
        assertException(
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when -920 then a
                                when 701 then c
                                when -714 then 350
                                when 701 then c
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(20)" +
                        ")",
                136,
                "duplicate branch"
        );
    }

    @Test
    public void testIntOrElse() throws Exception {
        assertQuery(
                """
                        x\ta\tb\tc\tk
                        -920\t315515118\t1548800833\t-727724771\t315515118
                        701\t-948263339\t1326447242\t592859671\t592859671
                        706\t-847531048\t-1191262516\t-2041844972\t-1191262516
                        -714\t-1575378703\t806715481\t1545253512\t350
                        116\t1573662097\t-409854405\t339631474\t-409854405
                        67\t1904508147\t-1532328444\t-1458132197\t-1532328444
                        207\t-1849627000\t-1432278050\t426455968\t-1432278050
                        -55\t-1792928964\t-1844391305\t-1520872171\t-1844391305
                        -104\t-1153445279\t1404198\t-1715058769\t1404198
                        -127\t1631244228\t-1975183723\t-1252906348\t-1975183723
                        790\t-761275053\t-2119387831\t-212807500\t-2119387831
                        881\t1110979454\t1253890363\t-113506296\t1253890363
                        -535\t-938514914\t-547127752\t-1271909747\t-547127752
                        -973\t-342047842\t-2132716300\t2006313928\t-2132716300
                        -463\t-27395319\t264240638\t2085282008\t264240638
                        -667\t2137969456\t1890602616\t-1272693194\t1890602616
                        578\t1036510002\t-2002373666\t44173540\t-2002373666
                        940\t410717394\t-2144581835\t1978144263\t-2144581835
                        -54\t-1162267908\t2031014705\t-530317703\t2031014705
                        -393\t-296610933\t936627841\t326010667\t936627841
                        """,
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when -920 then a
                                when 701 then c
                                when -714 then 350
                                else b
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntOrElseBinValue() throws Exception {
        assertQuery(
                """
                        x\ta\tb\tc\tk
                        -920\t00000000 ee 41 1d 15 55 8a 17 fa d8 cc 14 ce f1 59 88 c4
                        00000010 91 3b 72 db f3 04 1b c7 88 de a0 79 3c 77 15 68\t00000000 61 26 af 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1
                        00000010 1e 38 8d 1b 9e f4 c8 39 09 fe d8 9d 30 78 36 6a\t00000000 32 de e4 7c d2 35 07 42 fc 31 79 5f 8b 81 2b 93
                        00000010 4d 1a 8e 78 b5 b9 11 53 d0 fb 64 bb 1a d4 f0 2d\t00000000 ee 41 1d 15 55 8a 17 fa d8 cc 14 ce f1 59 88 c4
                        00000010 91 3b 72 db f3 04 1b c7 88 de a0 79 3c 77 15 68
                        -352\t00000000 e2 4b b1 3e e3 f1 f1 1e ca 9c 1d 06 ac 37 c8 cd
                        00000010 82 89 2b 4d 5f f6 46 90 c3 b3 59 8e e5 61 2f 64\t00000000 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47 dc d2 85 7f
                        00000010 a5 b8 7b 4a 9d 46 7c 8d dd 93 e6 d0 b3 2b 07 98\t00000000 cc 76 48 a3 bb 64 d2 ad 49 1c f2 3c ed 39 ac a8
                        00000010 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a e7 0c\t00000000 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47 dc d2 85 7f
                        00000010 a5 b8 7b 4a 9d 46 7c 8d dd 93 e6 d0 b3 2b 07 98
                        -743\t00000000 14 58 63 b7 c2 9f 29 8e 29 5e 69 c6 eb ea c3 c9
                        00000010 73 93 46 fe c2 d3 68 79 8b 43 1d 57 34 04 23 8d\t00000000 d8 57 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb 67
                        00000010 44 a7 6a 71 34 e0 b0 e9 98 f7 67 62 28 60 b0 ec\t00000000 0b 92 58 7d 24 bc 2e 60 6a 1c 0b 20 a2 86 89 37
                        00000010 11 2c 14 0c 2d 20 84 52 d9 6f 04 ab 27 47 8f 23\t00000000 d8 57 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb 67
                        00000010 44 a7 6a 71 34 e0 b0 e9 98 f7 67 62 28 60 b0 ec
                        -601\t00000000 ae 7c 9f 77 04 e9 0c ea 4e ea 8b f5 0f 2d b3 14
                        00000010 33 80 c9 eb a3 67 7a 1a 79 e4 35 e4 3a dc 5c 65\t00000000 ff 27 67 77 12 54 52 d0 29 26 c5 aa da 18 ce 5f
                        00000010 b2 8b 5c 54 90 25 c2 20 ff 70 3a c7 8a b3 14 cd\t00000000 47 0b 0c 39 12 f7 05 10 f4 6d f1 e3 ee 58 35 61
                        00000010 52 8b 0b 93 e5 57 a5 db a1 76 1c 1c 26 fb 2e 42\t00000000 ff 27 67 77 12 54 52 d0 29 26 c5 aa da 18 ce 5f
                        00000010 b2 8b 5c 54 90 25 c2 20 ff 70 3a c7 8a b3 14 cd
                        -398\t00000000 f5 6e 8f 80 e3 54 b8 07 b1 32 57 ff 9a ef 88 cb
                        00000010 4b a1 cf cf 41 7d a6 d1 3e b4 48 d4 41 9d fb 49\t00000000 40 44 49 96 cf 2b b3 71 a7 d5 af 11 96 37 08 dd
                        00000010 98 ef 54 88 2a a2 ad e7 d4 62 e1 4e d6 b2 57 5b\t00000000 e3 71 3d 20 e2 37 f2 64 43 84 55 a0 dd 44 11 e2
                        00000010 a3 24 4e 44 a8 0d fe 27 ec 53 13 5d b2 15 e7 b8\t00000000 40 44 49 96 cf 2b b3 71 a7 d5 af 11 96 37 08 dd
                        00000010 98 ef 54 88 2a a2 ad e7 d4 62 e1 4e d6 b2 57 5b
                        437\t00000000 67 9c 94 b9 8e 28 b6 a9 17 ec 0e 01 c4 eb 9f 13
                        00000010 8f bb 2a 4b af 8f 89 df 35 8f da fe 33 98 80 85\t00000000 20 53 3b 51 9d 5d 28 ac 02 2e fe 05 3b 94 5f ec
                        00000010 d3 dc f8 43 b2 e3 75 62 60 af 6d 8c d8 ac c8 46\t00000000 3b 47 3c e1 72 3b 9d ef c4 4a c9 cf fb 9d 63 ca
                        00000010 94 00 6b dd 18 fe 71 76 bc 45 24 cd 13 00 7c fb\t00000000 20 53 3b 51 9d 5d 28 ac 02 2e fe 05 3b 94 5f ec
                        00000010 d3 dc f8 43 b2 e3 75 62 60 af 6d 8c d8 ac c8 46
                        -231\t00000000 19 ca f2 bf 84 5a 6f 38 35 15 29 83 1f c3 2f ed
                        00000010 b0 ba 08 e0 2c ee 41 de b6 81 df b7 6c 4b fb 2d\t00000000 16 f3 89 a3 83 64 de d6 fd c4 5b c4 e9 19 47 8d
                        00000010 ad 11 bc fe b9 52 dd 4d f3 f9 76 f6 85 ab a3 ab\t00000000 ee 6d 54 75 10 b3 4c 0e 8f f1 0c c5 60 b7 d1 5a
                        00000010 0c e9 db 51 13 4d 59 20 c9 37 a1 00 f8 42 23 37\t00000000 16 f3 89 a3 83 64 de d6 fd c4 5b c4 e9 19 47 8d
                        00000010 ad 11 bc fe b9 52 dd 4d f3 f9 76 f6 85 ab a3 ab
                        19\t00000000 a1 8c 47 64 59 1a d4 ab be 30 fa 8d ac 3d 98 a0
                        00000010 ad 9a 5d df dc 72 d7 97 cb f6 2c 23 45 a3 76 60\t00000000 15 c1 8c d9 11 69 94 3f 7d ef 3b b8 be f8 a1 46
                        00000010 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8 ab 3f a1\t00000000 f5 4b ea 01 c9 63 b4 fc 92 60 1f df 41 ec 2c 38
                        00000010 88 88 e7 59 40 10 20 81 c6 3d bc b5 05 2b 73 51\t00000000 15 c1 8c d9 11 69 94 3f 7d ef 3b b8 be f8 a1 46
                        00000010 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8 ab 3f a1
                        215\t00000000 c3 7e c0 1d 6c a9 65 81 ad 79 87 fc 92 83 fc 88
                        00000010 f3 32 27 70 c8 01 b0 dc c9 3a 5b 7e 0e 98 0a 8a\t00000000 0b 1e c4 fd a2 9e b3 77 f8 f6 78 09 1c 5d 88 f5
                        00000010 52 fd 36 02 50 d9 a0 b5 90 6c 9c 23 22 89 99 ad\t00000000 f7 fe 9a 9e 1b fd a9 d7 0e 39 5a 28 ed 97 99 d8
                        00000010 77 33 3f b2 67 da 98 47 47 bf 4f ea 5f 48 ed f6\t00000000 0b 1e c4 fd a2 9e b3 77 f8 f6 78 09 1c 5d 88 f5
                        00000010 52 fd 36 02 50 d9 a0 b5 90 6c 9c 23 22 89 99 ad
                        819\t00000000 28 a2 3c d0 65 5e b7 95 2e 4a af c6 d0 19 6a de
                        00000010 46 04 d3 81 e7 a2 16 22 35 3b 1c 9c 1d 5c c1 5d\t00000000 2d 44 ea 00 81 c4 19 a1 ec 74 f8 10 fc 6e 23 3d
                        00000010 e0 2d 04 86 e7 ca 29 98 07 69 ca 5b d6 cf 09 69\t00000000 01 b1 55 38 ad b2 4a 4e 7d 85 f9 39 25 42 67 78
                        00000010 47 b3 80 69 b9 14 d6 fc ee 03 22 81 b8 06 c4 06\t00000000 2d 44 ea 00 81 c4 19 a1 ec 74 f8 10 fc 6e 23 3d
                        00000010 e0 2d 04 86 e7 ca 29 98 07 69 ca 5b d6 cf 09 69
                        15\t00000000 38 71 1f e1 e4 91 7d e9 5d 4b 6a cd 4e f9 17 9e
                        00000010 cf 6a 34 2c 37 a3 6f 2a 12 61 3a 9a ad 98 2e 75\t00000000 52 ad 62 87 88 45 b9 9d 20 13 51 c0 e0 b7 a4 24
                        00000010 40 4d 50 b1 8c 4d 66 e8 32 6a 9b cd bb 2e 74 cd\t00000000 44 54 13 3f ff b6 7e cd 04 27 66 94 89 db 3c 1a
                        00000010 23 f3 88 83 73 1c 04 63 f9 ac 3d 61 6b 04 33 2b\t00000000 52 ad 62 87 88 45 b9 9d 20 13 51 c0 e0 b7 a4 24
                        00000010 40 4d 50 b1 8c 4d 66 e8 32 6a 9b cd bb 2e 74 cd
                        -307\t00000000 25 07 db 62 44 33 6e 00 8e 93 bd 27 42 f8 25 2a
                        00000010 42 71 a3 7a 58 e5 78 b8 1c d6 fc 7a ac 4c 11 9e\t00000000 60 de 1d 43 8c 7e f3 04 4a 73 f0 31 3e 55 3e 3b
                        00000010 6f 93 3f ab ab ac 21 61 99 be 2d f5 30 78 6d 5a\t00000000 3b 2b 30 3c 8b 88 7b 6e a6 6b 91 c7 5f 78 05 e5
                        00000010 c0 4e cc d6 e3 7b 34 cd 15 35 bb a4 a3 c8 66 0c\t00000000 60 de 1d 43 8c 7e f3 04 4a 73 f0 31 3e 55 3e 3b
                        00000010 6f 93 3f ab ab ac 21 61 99 be 2d f5 30 78 6d 5a
                        -272\t00000000 71 ea 20 7e 43 97 27 1f 5c d9 ee 04 5b 9c 17 f2
                        00000010 8c bf 95 30 57 1d 91 72 30 04 b7 02 cb 03 23 61\t00000000 b4 bf 04 03 5b 11 44 83 06 63 2b 58 3b 4b b7 e2
                        00000010 7f ab 6e 23 03 dd c7 d6 65 29 89 d3 f8 ca 36 84\t00000000 41 fd 48 c5 c5 d7 77 a7 2f 9f 56 79 35 d8 a2 8f
                        00000010 f9 64 ed e3 2c 97 0b f5 ef 3b be 85 7c 11 f7 34\t00000000 b4 bf 04 03 5b 11 44 83 06 63 2b 58 3b 4b b7 e2
                        00000010 7f ab 6e 23 03 dd c7 d6 65 29 89 d3 f8 ca 36 84
                        -559\t00000000 6c 3e 51 d7 eb b1 07 71 32 1f af 40 4e 8c 47 84
                        00000010 e9 c0 55 12 44 dc 4b c0 d9 1c 71 cf 5a 8f 21 06\t00000000 b2 3f 0e 41 93 89 27 ca 10 2f 60 ce 59 1c 79 dd
                        00000010 02 5e 87 d7 fe ac 8a a3 83 d5 7d e1 4f 56 6b 65\t00000000 a4 53 38 e9 cd c1 a7 ee 86 75 ad a5 2d 49 48 68
                        00000010 36 f0 35 dc 45 21 95 01 ef 2d 99 66 3d db c1 cc\t00000000 b2 3f 0e 41 93 89 27 ca 10 2f 60 ce 59 1c 79 dd
                        00000010 02 5e 87 d7 fe ac 8a a3 83 d5 7d e1 4f 56 6b 65
                        560\t00000000 82 3d ec f3 66 5e 70 38 5e bc e0 8f 10 c3 50 ce
                        00000010 4a 20 0f 7f 97 2b 04 b0 97 a4 0f ec 69 cd 73 bb\t00000000 9b c5 95 db 61 91 ce 60 fe 01 a0 ba a5 d1 63 ca
                        00000010 32 e5 0d 68 52 c6 94 c3 18 c9 7c 70 9f dc 01 48\t00000000 15 3e 0c 7f 3f 8f e4 b5 ab 34 21 29 23 e8 17 ca
                        00000010 f4 c0 8e e1 15 9c aa 69 48 c3 a0 d6 14 8b 7f 03\t00000000 9b c5 95 db 61 91 ce 60 fe 01 a0 ba a5 d1 63 ca
                        00000010 32 e5 0d 68 52 c6 94 c3 18 c9 7c 70 9f dc 01 48
                        687\t00000000 1a 54 1d 18 c6 1a 8a eb 14 37 55 fd 05 0e 55 76
                        00000010 37 bb c3 ec 4b 97 27 df cd 7a 14 07 92 01 f5 6a\t00000000 a1 31 cd cb c2 a2 b4 8e 99 a8 2b 8d 35 c5 85 9a
                        00000010 00 4a a1 06 7e 3f 4e 27 42 f2 f8 5e 29 d3 b9 67\t00000000 75 95 fa 1f 92 24 b1 b8 67 65 08 b7 f8 41 00 33
                        00000010 ac 30 77 91 b2 de 58 45 d0 1b 58 be 33 92 cd 5c\t00000000 a1 31 cd cb c2 a2 b4 8e 99 a8 2b 8d 35 c5 85 9a
                        00000010 00 4a a1 06 7e 3f 4e 27 42 f2 f8 5e 29 d3 b9 67
                        629\t00000000 84 36 25 63 2b 63 61 43 1c 47 7d b6 46 ba bb 98
                        00000010 ca 08 be a4 96 01 07 cf 1f e7 da eb 6d 5e 37 e4\t00000000 68 2a 96 06 46 b6 aa 36 92 43 c7 e9 ce 9c 54 02
                        00000010 9f c2 37 98 60 bb 38 d1 36 18 32 90 33 b5 de f9\t00000000 49 10 e7 7c 3f d6 88 3a 93 ef 24 a5 e2 bc 86 f9
                        00000010 92 a3 f1 92 08 f1 96 7f a0 cf 00 74 7c 32 16 38\t00000000 68 2a 96 06 46 b6 aa 36 92 43 c7 e9 ce 9c 54 02
                        00000010 9f c2 37 98 60 bb 38 d1 36 18 32 90 33 b5 de f9
                        -592\t00000000 7d f4 03 ed c9 2a 4e 91 c5 e4 39 b2 dd 0d a7 bb
                        00000010 d5 71 72 ba 9c ac 89 76 dd e7 1f eb 30 58 15 38\t00000000 83 18 dd 1a 16 b2 d8 83 f5 95 7c 95 fd 52 bb 50
                        00000010 c9 02 4d 40 ef 1f b8 17 f7 41 ff c1 a7 5c c3 31\t00000000 17 dd 8d c1 cf 5c 24 32 1e 3d ba 5f e3 91 7b ae
                        00000010 d1 be 82 5f 87 9e 79 1c c4 fa b8 a3 f7 ba 94 d7\t00000000 83 18 dd 1a 16 b2 d8 83 f5 95 7c 95 fd 52 bb 50
                        00000010 c9 02 4d 40 ef 1f b8 17 f7 41 ff c1 a7 5c c3 31
                        -228\t00000000 1c dd fc d2 8e 79 ec 02 b2 31 9c 69 be 74 9a ad
                        00000010 cc cf b8 e4 d1 7a 4f fb 16 fa 19 a2 df 43 81 a2\t00000000 93 38 ef 38 75 01 cb 8b 64 50 48 10 64 65 32 e1
                        00000010 a2 d4 70 b2 53 92 83 24 53 60 4d 04 c2 f0 7a 07\t00000000 d4 a3 d1 5f 0d fe 63 10 0d 8f 53 7d a0 9d a0 50
                        00000010 db b2 18 66 ca 85 56 e2 44 db b8 e9 93 fc d9 cb\t00000000 93 38 ef 38 75 01 cb 8b 64 50 48 10 64 65 32 e1
                        00000010 a2 d4 70 b2 53 92 83 24 53 60 4d 04 c2 f0 7a 07
                        625\t00000000 9c 48 24 83 dc 35 1b b9 0f 97 f5 77 7e a3 2d ce
                        00000010 fe eb cd 47 06 53 61 97 40 33 a8 22 95 14 45 fc\t00000000 5c 27 c7 97 9b 8b f8 04 6f d6 af 3f 2f 84 d5 12
                        00000010 fb 71 99 34 03 82 08 fb e7 94 3a 32 5d 8a 66 0b\t00000000 e4 85 f1 13 06 f2 27 0f 0c ae 8c 49 a1 ce bf 46
                        00000010 36 0d 5b 7f 48 92 ff 37 63 be 5f b7 70 a0 07 8f\t00000000 5c 27 c7 97 9b 8b f8 04 6f d6 af 3f 2f 84 d5 12
                        00000010 fb 71 99 34 03 82 08 fb e7 94 3a 32 5d 8a 66 0b
                        """,
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when -920 then a
                                when 701 then c
                                else b
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_bin() a," +
                        " rnd_bin() b," +
                        " rnd_bin() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testLong() throws Exception {
        assertQuery(
                """
                        x\ta\tb\tc\tk
                        -920\t4729996258992366\t1548800833\t-727724771\t4729996258992366
                        701\t8920866532787660373\t1326447242\t592859671\t592859671
                        706\t-1675638984090602536\t-1191262516\t-2041844972\tnull
                        -714\t-7489826605295361807\t806715481\t1545253512\t350
                        116\t8173439391403617681\t-409854405\t339631474\tnull
                        67\t-8968886490993754893\t-1532328444\t-1458132197\tnull
                        207\t-8284534269888369016\t-1432278050\t426455968\tnull
                        -55\t5539350449504785212\t-1844391305\t-1520872171\tnull
                        -104\t-4100339045953973663\t1404198\t-1715058769\tnull
                        -127\t2811900023577169860\t-1975183723\t-1252906348\tnull
                        790\t7700030475747712339\t-2119387831\t-212807500\tnull
                        881\t9194293009132827518\t1253890363\t-113506296\tnull
                        -535\t7199909180655756830\t-547127752\t-1271909747\tnull
                        -973\t6404066507400987550\t-2132716300\t2006313928\tnull
                        -463\t8573481508564499209\t264240638\t2085282008\tnull
                        -667\t-8480005421611953360\t1890602616\t-1272693194\tnull
                        578\t8325936937764905778\t-2002373666\t44173540\tnull
                        940\t-7885528361265853230\t-2144581835\t1978144263\tnull
                        -54\t3152466304308949756\t2031014705\t-530317703\tnull
                        -393\t6179044593759294347\t936627841\t326010667\tnull
                        """,
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when -920 then a
                                when 701 then c
                                when -714 then 350
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_long() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testLong256OrElse() throws Exception {
        assertException(
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when cast('0x00' as long256) then a
                                when cast('0x00' as long256) then c
                                when cast('0x00' as long256) then 350
                                else b
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_long256() x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(20)" +
                        ")",
                45,
                "type LONG256 is not supported in 'switch' type of 'case' statement"
        );
    }

    @Test
    public void testLongDuplicateBranch() throws Exception {
        assertException(
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when -920 then a
                                when 701 then c
                                when -714 then 350
                                when 701 then c
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_long() % 1000 x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(20)" +
                        ")",
                136,
                "duplicate branch"
        );
    }

    @Test
    public void testLongOrElse() throws Exception {
        assertQuery(
                """
                        x\ta\tb\tc\tk
                        856\t315515118\t1548800833\t-727724771\t1548800833
                        29\t-948263339\t1326447242\t592859671\t1326447242
                        -6\t-847531048\t-1191262516\t-2041844972\t-1191262516
                        934\t-1575378703\t806715481\t1545253512\t806715481
                        -60\t1573662097\t-409854405\t339631474\t-409854405
                        859\t1904508147\t-1532328444\t-1458132197\t-1532328444
                        -57\t-1849627000\t-1432278050\t426455968\t-1432278050
                        -727\t-1792928964\t-1844391305\t-1520872171\t-1844391305
                        -16\t-1153445279\t1404198\t-1715058769\t-1715058769
                        353\t1631244228\t-1975183723\t-1252906348\t-1975183723
                        86\t-761275053\t-2119387831\t-212807500\t-2119387831
                        57\t1110979454\t1253890363\t-113506296\t350
                        -951\t-938514914\t-547127752\t-1271909747\t-547127752
                        -157\t-342047842\t-2132716300\t2006313928\t-2132716300
                        -839\t-27395319\t264240638\t2085282008\t-27395319
                        717\t2137969456\t1890602616\t-1272693194\t1890602616
                        10\t1036510002\t-2002373666\t44173540\t-2002373666
                        -364\t410717394\t-2144581835\t1978144263\t-2144581835
                        106\t-1162267908\t2031014705\t-530317703\t2031014705
                        767\t-296610933\t936627841\t326010667\t936627841
                        """,
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when -839 then a
                                when -16 then c
                                when 57 then 350
                                else b
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_long() % 1000 x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testLongVariableKeyError() throws Exception {
        assertException(
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when -920 then a
                                when 701 then c
                                when c then 350
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_long() % 1000 x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(20)" +
                        ")",
                109,
                "constant expected"
        );
    }

    @Test
    public void testShort() throws Exception {
        assertQuery(
                """
                        a\tk
                        -27056\t0
                        -13027\t0
                        -1398\t0
                        -19496\t0
                        -4914\t0
                        -19832\t0
                        7739\t7739
                        31987\t0
                        -1593\t0
                        13216\t0
                        -11657\t0
                        -11679\t0
                        18457\t0
                        10900\t21558
                        -19127\t0
                        13182\t0
                        27809\t0
                        12941\t0
                        21748\t0
                        -1271\t0
                        """,
                """
                        select\s
                            a,
                            case a
                                when cast(7739 as short) then a
                                when cast(10900 as short) then b
                            end k\s
                        from tanc""",
                "create table tanc as (" +
                        "select " +
                        " rnd_short() a," +
                        " rnd_short() b," +
                        " rnd_short() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testShortOrElse() throws Exception {
        assertQuery(
                """
                        x\ta\tb\tc\tk
                        -27056\t315515118\t1548800833\t-727724771\t1548800833
                        -21227\t-948263339\t1326447242\t592859671\t1326447242
                        30202\t-847531048\t-1191262516\t-2041844972\t-1191262516
                        -4914\t-1575378703\t806715481\t1545253512\t806715481
                        -31548\t1573662097\t-409854405\t339631474\t-409854405
                        -24357\t1904508147\t-1532328444\t-1458132197\t-1532328444
                        -1593\t-1849627000\t-1432278050\t426455968\t-1432278050
                        26745\t-1792928964\t-1844391305\t-1520872171\t-1792928964
                        -30872\t-1153445279\t1404198\t-1715058769\t1404198
                        18457\t1631244228\t-1975183723\t-1252906348\t-1975183723
                        21558\t-761275053\t-2119387831\t-212807500\t-2119387831
                        8793\t1110979454\t1253890363\t-113506296\t1253890363
                        27809\t-938514914\t-547127752\t-1271909747\t-547127752
                        4635\t-342047842\t-2132716300\t2006313928\t2006313928
                        24121\t-27395319\t264240638\t2085282008\t264240638
                        -1379\t2137969456\t1890602616\t-1272693194\t1890602616
                        -22934\t1036510002\t-2002373666\t44173540\t-2002373666
                        1404\t410717394\t-2144581835\t1978144263\t350
                        -10942\t-1162267908\t2031014705\t-530317703\t2031014705
                        22367\t-296610933\t936627841\t326010667\t936627841
                        """,
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when cast(26745 as short) then a
                                when cast(4635 as short) then c
                                when cast(1404 as short) then 350
                                else b
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_short() x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testStrAndNullKeyOrElse() throws Exception {
        // string key column with nulls but no WHEN null branch;
        // null key falls through to else (L256 false branch)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tanc (x STRING, a INT, b INT)");
            execute("INSERT INTO tanc VALUES ('X', 1, 10)," +
                    " (null, 2, 20)," +
                    " ('Y', 3, 30)");
            assertQueryNoLeakCheck(
                    """
                            x\ta\tb\tk
                            X\t1\t10\t1
                            \t2\t20\t20
                            Y\t3\t30\t30
                            """,
                    "SELECT x, a, b," +
                            " CASE x" +
                            " WHEN 'X' THEN a" +
                            " ELSE b" +
                            " END k" +
                            " FROM tanc",
                    null, null, true, true
            );
        });
    }

    @Test
    public void testStrAndNullOrElse() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tanc (x STRING, a INT, b INT)");
            execute("INSERT INTO tanc VALUES ('X', 1, 10)," +
                    " ('Y', 2, 20)," +
                    " (null, 3, 30)," +
                    " ('Z', 4, 40)");
            assertQueryNoLeakCheck(
                    """
                            x\ta\tb\tk
                            X\t1\t10\t1
                            Y\t2\t20\t20
                            \t3\t30\t-1
                            Z\t4\t40\t40
                            """,
                    "SELECT x, a, b," +
                            " CASE x" +
                            " WHEN 'X' THEN a" +
                            " WHEN null THEN -1" +
                            " ELSE b" +
                            " END k" +
                            " FROM tanc",
                    null, null, true, true
            );
        });
    }

    @Test
    public void testStrCharOrElse() throws Exception {
        assertQuery(
                """
                        x\ta\tb\tc\tk
                        A\t315515118\t1548800833\t-727724771\tthis is A
                        B\t-948263339\t1326447242\t592859671\tthis is B
                        C\t-847531048\t-1191262516\t-2041844972\tthis is something else
                        C\t-1575378703\t806715481\t1545253512\tthis is something else
                        A\t1573662097\t-409854405\t339631474\tthis is A
                        D\t1904508147\t-1532328444\t-1458132197\tthis is D
                        D\t-1849627000\t-1432278050\t426455968\tthis is D
                        D\t-1792928964\t-1844391305\t-1520872171\tthis is D
                        A\t-1153445279\t1404198\t-1715058769\tthis is A
                        D\t1631244228\t-1975183723\t-1252906348\tthis is D
                        C\t-761275053\t-2119387831\t-212807500\tthis is something else
                        B\t1110979454\t1253890363\t-113506296\tthis is B
                        D\t-938514914\t-547127752\t-1271909747\tthis is D
                        B\t-342047842\t-2132716300\t2006313928\tthis is B
                        D\t-27395319\t264240638\t2085282008\tthis is D
                        D\t2137969456\t1890602616\t-1272693194\tthis is D
                        C\t1036510002\t-2002373666\t44173540\tthis is something else
                        A\t410717394\t-2144581835\t1978144263\tthis is A
                        C\t-1162267908\t2031014705\t-530317703\tthis is something else
                        B\t-296610933\t936627841\t326010667\tthis is B
                        """,
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when 'A' then 'this is A'
                                when 'B' then 'this is B'
                                when 'D' then 'this is D'
                                else 'this is something else'
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_str('A','B','C','D') x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testStrOrElse() throws Exception {
        assertQuery(
                """
                        x\ta\tb\tc\tk
                        JWCPSWHYR\t-2041844972\t-1436881714\t-1575378703\t-1436881714
                        RXG\t339631474\t1530831067\t1904508147\t1530831067
                        IBBTGPGW\t-1101822104\t-1153445279\t1404198\t-1153445279
                        EYYQEHBHFO\t-113506296\t-422941535\t-938514914\t-422941535
                        YSBEOU\t264240638\t2085282008\t-483853667\t264240638
                        UED\t-2002373666\t44173540\t458818940\t44173540
                        OFJGET\t-296610933\t936627841\t326010667\t936627841
                        RYRFBV\t-1787109293\t-66297136\t-1515787781\t-66297136
                        OZZVDZ\t-235358133\t-1299391311\t-1212175298\t-1299391311
                        CXZO\t1196016669\t-307026682\t-1566901076\t-1566901076
                        KGH\t-1582495445\t-1424048819\t532665695\t-1424048819
                        OTSEDYYCT\t-1794809330\t-1609750740\t-731466113\t350
                        XWCK\t-880943673\t-2075675260\t1254404167\t-2075675260
                        DSWUGSHOL\t1864113037\t-1966408995\t183633043\t-1966408995
                        BZX\t2124174232\t-2043803188\t544695670\t-2043803188
                        JSMSSU\t-2111250190\t462277692\t614536941\t462277692
                        KVVSJOJ\t1238491107\t-1056463214\t-636975106\t-1056463214
                        PIH\t1362833895\t576104460\t-805434743\t576104460
                        LJU\t454820511\t-246923735\t-514934130\t-246923735
                        MLLEO\t387510473\t1431425139\t-948252781\t1431425139
                        """,
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when 'YSBEOU' then a
                                when 'CXZO' then c
                                when 'OTSEDYYCT' then 350
                                else b
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_str() x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testStrToStrOrElse() throws Exception {
        assertQuery(
                """
                        x\ta\tb\tc\tk
                        JWCPSWHYR\tEHNRX\tSXUXI\tTGPGW\tSXUXI
                        YUDEYYQEHB\tOWLPDXYSB\tUOJSHRUEDR\tULOFJGE\tUOJSHRUEDR
                        RSZSRYRF\tTMHGOOZZVD\tMYICCXZO\tCWEKG\tMYICCXZO
                        UVSDOTSE\tYCTGQO\tXWCK\tSUWDSWU\tXWCK
                        HOLNV\tQBZXIOVIK\tMSSUQSR\tKVVSJOJ\tMSSUQSR
                        HZEPIHVLT\tLJU\tGLHMLLEOYP\tIPZIMNZZR\tGLHMLLEOYP
                        MBEZGHW\tKFL\tJOXPKR\tIHYH\tJOXPKR
                        QMYS\tPGLUOHN\tZSQLDGLOG\tOUSZMZV\tZSQLDGLOG
                        BNDCQCEHNO\tELLKK\tWNWIFFLR\tOMNXKUIZUL\tWNWIFFLR
                        YVFZF\tZLUOG\tFVWSWSR\tONFCLTJCKF\tFVWSWSR
                        NTO\tXUKLG\tSLUQDY\tHNIMYFF\tXUKLG
                        NPH\tPBNH\tWWC\tGTNLEGPUHH\tWWC
                        GGLN\tZLCBDMIG\tVKHTLQ\tLQVF\tVKHTLQ
                        PRGSXBHYS\tYMIZJS\tNPIWZNFK\tVMCGFN\tVMCGFN
                        RMDGGIJ\tVRVNG\tEQODRZEI\tOQKYH\tEQODRZEI
                        UWQOEE\tEBQQEMXDK\tJCTIZK\tLUHZQSN\tJCTIZK
                        MKJSMKIX\tVTUPDHH\tIWHPZRHH\tZJYYFLSVI\tIWHPZRHH
                        WWLEVM\tCJBEV\tHLIHYBT\tNCLNXFS\tHLIHYBT
                        PNXH\tTZODWKOCPF\tPVKNC\tLNLRH\tPVKNC
                        XYPO\tDBZWNI\tEHR\tPBMB\tWORKS!
                        """,
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when 'NTO' then a
                                when 'PRGSXBHYS' then c
                                when 'XYPO' then 'WORKS!'
                                else b
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_str() x," +
                        " rnd_str() a," +
                        " rnd_str() b," +
                        " rnd_str() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testStrToStrOrElseDuplicateBranch() throws Exception {
        assertException(
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when 'NTO' then a
                                when 'PRGSXBHYS' then c
                                when 'NTO' then 'WORKS!'
                                else b
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_str() x," +
                        " rnd_str() a," +
                        " rnd_str() b," +
                        " rnd_str() c" +
                        " from long_sequence(20)" +
                        ")",
                118,
                "duplicate branch"
        );
    }

    @Test
    public void testSymbolAndNullOrElse() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tanc AS (" +
                    "SELECT rnd_symbol('a1', 'b2', 'c3', 'd4', null) x," +
                    " rnd_char() a," +
                    " rnd_char() b," +
                    " rnd_char() c" +
                    " FROM long_sequence(20)" +
                    ")");
            String query = "SELECT x, a, b, c," +
                    " CASE x" +
                    " WHEN 'b2' THEN a" +
                    " WHEN 'd4' THEN c" +
                    " WHEN null THEN 'Z'" +
                    " ELSE b" +
                    " END k" +
                    " FROM tanc";
            assertQueryNoLeakCheck(
                    """
                            x\ta\tb\tc\tk
                            a1\tT\tJ\tW\tJ
                            b2\tP\tS\tW\tP
                            b2\tY\tR\tX\tY
                            \tE\tH\tN\tZ
                            b2\tX\tG\tZ\tX
                            c3\tX\tU\tX\tU
                            c3\tB\tB\tT\tB
                            a1\tP\tG\tW\tG
                            \tF\tY\tU\tZ
                            c3\tE\tY\tY\tY
                            a1\tE\tH\tB\tH
                            b2\tF\tO\tW\tF
                            a1\tP\tD\tX\tD
                            d4\tS\tB\tE\tE
                            d4\tU\tO\tJ\tJ
                            c3\tH\tR\tU\tR
                            d4\tD\tR\tQ\tQ
                            a1\tU\tL\tO\tL
                            \tJ\tG\tE\tZ
                            d4\tJ\tR\tS\tS
                            """,
                    query, null, null, true, true
            );
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [x,a,b,c,case([a,c,'Z',b,x,switch(x,'b2',a,'d4',c,null,'Z',b)])]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tanc
                            """
            );
        });
    }

    @Test
    public void testSymbolConstIntBasic() throws Exception {
        // single-branch CASE with constant int results triggers inlined
        // SymbolSwitchConstIntFunction: direct int comparison, no picker/CaseFunction
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (side SYMBOL, price DOUBLE)");
            execute("""
                    INSERT INTO t VALUES
                    ('BUY', 100.0),
                    ('SELL', 200.0),
                    ('BUY', 150.0),
                    ('SELL', 50.0)
                    """);
            String query = "SELECT side, CASE side WHEN 'BUY' THEN 1 ELSE -1 END AS dir FROM t";
            assertQueryNoLeakCheck(
                    """
                            side\tdir
                            BUY\t1
                            SELL\t-1
                            BUY\t1
                            SELL\t-1
                            """,
                    query, null, null, true, true
            );
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [side,switch(side,'BUY',1,-1)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testSymbolConstIntInDoubleContext() throws Exception {
        // inlined CASE used in arithmetic with doubles exercises
        // the getDouble() override that avoids Numbers.intToDouble() NULL check
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (side SYMBOL, price DOUBLE)");
            execute("""
                    INSERT INTO t VALUES
                    ('BUY', 100.0),
                    ('SELL', 200.0),
                    ('BUY', 150.0),
                    ('SELL', 50.0)
                    """);
            assertQueryNoLeakCheck(
                    """
                            side\tsigned_price
                            BUY\t100.0
                            SELL\t-200.0
                            BUY\t150.0
                            SELL\t-50.0
                            """,
                    "SELECT side, CASE side WHEN 'BUY' THEN 1 ELSE -1 END * price AS signed_price FROM t",
                    null, null, true, true
            );
        });
    }

    @Test
    public void testSymbolConstIntMissingKey() throws Exception {
        // WHEN value not in symbol table: resolvedKey becomes VALUE_NOT_FOUND (-2),
        // which never matches any record key, so every row gets the else value
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (side SYMBOL, price DOUBLE)");
            execute("""
                    INSERT INTO t VALUES
                    ('BUY', 100.0),
                    ('SELL', 200.0)
                    """);
            assertQueryNoLeakCheck(
                    """
                            side\tdir
                            BUY\t-1
                            SELL\t-1
                            """,
                    "SELECT side, CASE side WHEN 'ZZZ' THEN 1 ELSE -1 END AS dir FROM t",
                    null, null, true, true
            );
        });
    }

    @Test
    public void testSymbolConstIntWithNulls() throws Exception {
        // NULL symbol values: VALUE_IS_NULL (INT_NULL) never equals a valid
        // resolved key, so NULLs naturally map to the else value
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (side SYMBOL, price DOUBLE)");
            execute("""
                    INSERT INTO t VALUES
                    ('BUY', 100.0),
                    (NULL, 200.0),
                    ('SELL', 150.0),
                    (NULL, 50.0)
                    """);
            assertQueryNoLeakCheck(
                    """
                            side\tdir
                            BUY\t1
                            \t-1
                            SELL\t-1
                            \t-1
                            """,
                    "SELECT side, CASE side WHEN 'BUY' THEN 1 ELSE -1 END AS dir FROM t",
                    null, null, true, true
            );
        });
    }

    @Test
    public void testSymbolDualBranchMissingKeyOrElse() throws Exception {
        // dual-branch picker where one WHEN value doesn't exist in the symbol table;
        // 'b2' resolves normally, 'zz' becomes VALUE_NOT_FOUND and never matches
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tanc (x SYMBOL, a CHAR, b CHAR)");
            execute("INSERT INTO tanc VALUES ('a1', 'A', 'B')," +
                    " ('b2', 'C', 'D')," +
                    " ('c3', 'E', 'F')");
            assertQueryNoLeakCheck(
                    """
                            x\ta\tb\tk
                            a1\tA\tB\tB
                            b2\tC\tD\tC
                            c3\tE\tF\tF
                            """,
                    "SELECT x, a, b," +
                            " CASE x" +
                            " WHEN 'b2' THEN a" +
                            " WHEN 'zz' THEN 'Z'" +
                            " ELSE b" +
                            " END k" +
                            " FROM tanc",
                    null, null, true, true
            );
        });
    }

    @Test
    public void testSymbolDuplicateBranch() throws Exception {
        assertException(
                "SELECT CASE x WHEN 'a1' THEN a WHEN 'b2' THEN b WHEN 'a1' THEN c END k FROM tanc",
                "CREATE TABLE tanc AS (" +
                        "SELECT rnd_symbol('a1', 'b2', 'c3') x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " FROM long_sequence(5)" +
                        ")",
                53,
                "duplicate branch"
        );
    }

    @Test
    public void testSymbolNonStaticOrElse() throws Exception {
        // cast to symbol produces a non-static symbol table,
        // so the switch falls back to CharSequence-keyed comparison (L170)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tanc (x VARCHAR, a CHAR, b CHAR, c CHAR)");
            execute("INSERT INTO tanc VALUES ('b2', 'A', 'B', 'C')," +
                    " ('d4', 'D', 'E', 'F')," +
                    " ('a1', 'G', 'H', 'I')," +
                    " ('c3', 'J', 'K', 'L')");
            assertQueryNoLeakCheck(
                    """
                            x\ta\tb\tc\tk
                            b2\tA\tB\tC\tA
                            d4\tD\tE\tF\tF
                            a1\tG\tH\tI\tZ
                            c3\tJ\tK\tL\tK
                            """,
                    "SELECT x, a, b, c," +
                            " CASE x::symbol" +
                            " WHEN 'b2' THEN a" +
                            " WHEN 'd4' THEN c" +
                            " WHEN 'a1' THEN 'Z'" +
                            " ELSE b" +
                            " END k" +
                            " FROM tanc",
                    null, null, true, true
            );
        });
    }

    @Test
    public void testSymbolOrElse() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tanc AS (" +
                    "SELECT rnd_symbol('a1', 'b2', 'c3', 'd4', null) x," +
                    " rnd_char() a," +
                    " rnd_char() b," +
                    " rnd_char() c" +
                    " FROM long_sequence(20)" +
                    ")");
            String query = "SELECT x, a, b, c," +
                    " CASE x" +
                    " WHEN 'b2' THEN a" +
                    " WHEN 'd4' THEN c" +
                    " WHEN 'a1' THEN 'Z'" +
                    " ELSE b" +
                    " END k" +
                    " FROM tanc";
            assertQueryNoLeakCheck(
                    """
                            x\ta\tb\tc\tk
                            a1\tT\tJ\tW\tZ
                            b2\tP\tS\tW\tP
                            b2\tY\tR\tX\tY
                            \tE\tH\tN\tH
                            b2\tX\tG\tZ\tX
                            c3\tX\tU\tX\tU
                            c3\tB\tB\tT\tB
                            a1\tP\tG\tW\tZ
                            \tF\tY\tU\tY
                            c3\tE\tY\tY\tY
                            a1\tE\tH\tB\tZ
                            b2\tF\tO\tW\tF
                            a1\tP\tD\tX\tZ
                            d4\tS\tB\tE\tE
                            d4\tU\tO\tJ\tJ
                            c3\tH\tR\tU\tR
                            d4\tD\tR\tQ\tQ
                            a1\tU\tL\tO\tZ
                            \tJ\tG\tE\tG
                            d4\tJ\tR\tS\tS
                            """,
                    query, null, null, true, true
            );
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [x,a,b,c,case([a,c,'Z',b,x,switch(x,'b2',a,'d4',c,'a1','Z',b)])]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tanc
                            """
            );
        });
    }

    @Test
    public void testSymbolOrElseMissingKey() throws Exception {
        // one WHEN value ('zz') is not in the symbol table,
        // so symbolTable.keyOf() returns VALUE_NOT_FOUND (L721 false branch)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tanc AS (" +
                    "SELECT rnd_symbol('a1', 'b2', 'c3', 'd4', null) x," +
                    " rnd_char() a," +
                    " rnd_char() b," +
                    " rnd_char() c" +
                    " FROM long_sequence(20)" +
                    ")");
            assertQueryNoLeakCheck(
                    """
                            x\ta\tb\tc\tk
                            a1\tT\tJ\tW\tJ
                            b2\tP\tS\tW\tP
                            b2\tY\tR\tX\tY
                            \tE\tH\tN\tH
                            b2\tX\tG\tZ\tX
                            c3\tX\tU\tX\tU
                            c3\tB\tB\tT\tB
                            a1\tP\tG\tW\tG
                            \tF\tY\tU\tY
                            c3\tE\tY\tY\tY
                            a1\tE\tH\tB\tH
                            b2\tF\tO\tW\tF
                            a1\tP\tD\tX\tD
                            d4\tS\tB\tE\tB
                            d4\tU\tO\tJ\tO
                            c3\tH\tR\tU\tR
                            d4\tD\tR\tQ\tR
                            a1\tU\tL\tO\tL
                            \tJ\tG\tE\tG
                            d4\tJ\tR\tS\tR
                            """,
                    "SELECT x, a, b, c," +
                            " CASE x" +
                            " WHEN 'b2' THEN a" +
                            " WHEN 'zz' THEN c" +
                            " WHEN 'yy' THEN 'Z'" +
                            " ELSE b" +
                            " END k" +
                            " FROM tanc",
                    null, null, true, true
            );
        });
    }

    @Test
    public void testSymbolSingleBranchAndNullOrElse() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tanc AS (" +
                    "SELECT rnd_symbol('a1', 'b2', 'c3', 'd4', null) x," +
                    " rnd_char() a," +
                    " rnd_char() b," +
                    " rnd_char() c" +
                    " FROM long_sequence(20)" +
                    ")");
            String query = "SELECT x, a, b, c," +
                    " CASE x" +
                    " WHEN 'b2' THEN a" +
                    " WHEN null THEN 'Z'" +
                    " ELSE b" +
                    " END k" +
                    " FROM tanc";
            assertQueryNoLeakCheck(
                    """
                            x\ta\tb\tc\tk
                            a1\tT\tJ\tW\tJ
                            b2\tP\tS\tW\tP
                            b2\tY\tR\tX\tY
                            \tE\tH\tN\tZ
                            b2\tX\tG\tZ\tX
                            c3\tX\tU\tX\tU
                            c3\tB\tB\tT\tB
                            a1\tP\tG\tW\tG
                            \tF\tY\tU\tZ
                            c3\tE\tY\tY\tY
                            a1\tE\tH\tB\tH
                            b2\tF\tO\tW\tF
                            a1\tP\tD\tX\tD
                            d4\tS\tB\tE\tB
                            d4\tU\tO\tJ\tO
                            c3\tH\tR\tU\tR
                            d4\tD\tR\tQ\tR
                            a1\tU\tL\tO\tL
                            \tJ\tG\tE\tZ
                            d4\tJ\tR\tS\tR
                            """,
                    query, null, null, true, true
            );
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [x,a,b,c,case([a,'Z',b,x,switch(x,'b2',a,null,'Z',b)])]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tanc
                            """
            );
        });
    }

    @Test
    public void testSymbolSingleBranchMissingKeyOrElse() throws Exception {
        // single-branch picker where the WHEN value doesn't exist in the symbol table;
        // resolvedKey becomes VALUE_NOT_FOUND (-2), which never matches any record key,
        // so every row falls through to else
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tanc (x SYMBOL, a CHAR, b CHAR)");
            execute("INSERT INTO tanc VALUES ('a1', 'A', 'B')," +
                    " ('b2', 'C', 'D')," +
                    " ('c3', 'E', 'F')");
            assertQueryNoLeakCheck(
                    """
                            x\ta\tb\tk
                            a1\tA\tB\tB
                            b2\tC\tD\tD
                            c3\tE\tF\tF
                            """,
                    "SELECT x, a, b," +
                            " CASE x" +
                            " WHEN 'zz' THEN a" +
                            " ELSE b" +
                            " END k" +
                            " FROM tanc",
                    null, null, true, true
            );
        });
    }

    @Test
    public void testSymbolSingleBranchOrElse() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tanc AS (" +
                    "SELECT rnd_symbol('a1', 'b2', 'c3', 'd4', null) x," +
                    " rnd_char() a," +
                    " rnd_char() b," +
                    " rnd_char() c" +
                    " FROM long_sequence(20)" +
                    ")");
            String query = "SELECT x, a, b, c," +
                    " CASE x" +
                    " WHEN 'b2' THEN a" +
                    " ELSE b" +
                    " END k" +
                    " FROM tanc";
            assertQueryNoLeakCheck(
                    """
                            x\ta\tb\tc\tk
                            a1\tT\tJ\tW\tJ
                            b2\tP\tS\tW\tP
                            b2\tY\tR\tX\tY
                            \tE\tH\tN\tH
                            b2\tX\tG\tZ\tX
                            c3\tX\tU\tX\tU
                            c3\tB\tB\tT\tB
                            a1\tP\tG\tW\tG
                            \tF\tY\tU\tY
                            c3\tE\tY\tY\tY
                            a1\tE\tH\tB\tH
                            b2\tF\tO\tW\tF
                            a1\tP\tD\tX\tD
                            d4\tS\tB\tE\tB
                            d4\tU\tO\tJ\tO
                            c3\tH\tR\tU\tR
                            d4\tD\tR\tQ\tR
                            a1\tU\tL\tO\tL
                            \tJ\tG\tE\tG
                            d4\tJ\tR\tS\tR
                            """,
                    query, null, null, true, true
            );
            assertPlanNoLeakCheck(
                    query,
                    """
                            VirtualRecord
                              functions: [x,a,b,c,case([a,b,x,switch(x,'b2',a,b)])]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tanc
                            """
            );
        });
    }

    @Test
    public void testTimestampDuplicateBranch() throws Exception {
        assertException(
                "SELECT CASE ts WHEN '2020-01-01' THEN a WHEN '2020-01-02' THEN b WHEN '2020-01-01' THEN c END k FROM tanc",
                "CREATE TABLE tanc AS (" +
                        "SELECT timestamp_sequence('2020-01-01', 100000000) ts," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " FROM long_sequence(5)" +
                        ")",
                70,
                "duplicate branch"
        );
    }

    @Test
    public void testTimestampOrElse() throws Exception {
        assertQuery(
                """
                        x\ta\tb\tc\tk
                        1970-01-01T00:00:00.000000Z\tV\tT\tJ\tT
                        1970-01-01T00:00:00.100000Z\tW\tC\tP\tC
                        1970-01-01T00:00:00.200000Z\tS\tW\tH\tW
                        1970-01-01T00:00:00.300000Z\tY\tR\tX\tR
                        1970-01-01T00:00:00.400000Z\tP\tE\tH\tE
                        1970-01-01T00:00:00.500000Z\tN\tR\tX\tN
                        1970-01-01T00:00:00.600000Z\tG\tZ\tS\tZ
                        1970-01-01T00:00:00.700000Z\tX\tU\tX\tU
                        1970-01-01T00:00:00.800000Z\tI\tB\tB\tB
                        1970-01-01T00:00:00.900000Z\tT\tG\tP\tG
                        1970-01-01T00:00:01.000000Z\tG\tW\tF\tW
                        1970-01-01T00:00:01.100000Z\tF\tY\tU\tY
                        1970-01-01T00:00:01.200000Z\tD\tE\tY\tE
                        1970-01-01T00:00:01.300000Z\tY\tQ\tE\tQ
                        1970-01-01T00:00:01.400000Z\tH\tB\tH\tB
                        1970-01-01T00:00:01.500000Z\tF\tO\tW\tO
                        1970-01-01T00:00:01.600000Z\tL\tP\tD\tP
                        1970-01-01T00:00:01.700000Z\tX\tY\tS\tY
                        1970-01-01T00:00:01.800000Z\tB\tE\tO\tE
                        1970-01-01T00:00:01.900000Z\tU\tO\tJ\tZ
                        """,
                """
                        select\s
                            x,
                            a,
                            b,
                            c,
                            case x
                                when cast('1970-01-01T00:00:00.500Z' as date) then a
                                when cast('1970-01-01T00:48:12.010Z' as date) then c
                                when cast('1970-01-01T00:00:01.900Z' as date) then 'Z'
                                else b
                            end k
                        from tanc""",
                "create table tanc as (" +
                        "select timestamp_sequence(0, 100000L) x," +
                        " rnd_char() a," +
                        " rnd_char() b," +
                        " rnd_char() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }
}
