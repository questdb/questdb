/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.test.AbstractGriffinTest;
import org.junit.Test;

public class SwitchFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testBooleanDuplicateFalse() throws Exception {
        assertFailure("select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when false then 'HELLO'\n" +
                        "        when false then 'HELLO2'\n" +
                        "    end k\n" +
                        "from tanc",
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
        assertFailure("select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when true then 'HELLO'\n" +
                        "        when true then 'HELLO2'\n" +
                        "    end k\n" +
                        "from tanc",
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
        assertFailure("select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when false then 'HELLO'\n" +
                        "        when true then 'HELLO2'\n" +
                        "        when false then 'HELLO3'\n" +
                        "    end k\n" +
                        "from tanc",
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
                "x\ta\tb\tc\tk\n" +
                        "false\tWCPS\tYRXPE\tRXG\tRXG\n" +
                        "false\tUXIBBT\tGWFFYUD\tYQEHBH\tYQEHBH\n" +
                        "false\tLPD\tSBEOUOJS\tUED\tUED\n" +
                        "true\tULOFJGE\tRSZSRYRF\tTMHGOOZZVD\tHELLO\n" +
                        "true\tYICCXZOUIC\tKGH\tVSDOTS\tHELLO\n" +
                        "true\tYCTGQO\tXWCK\tSUWDSWU\tHELLO\n" +
                        "false\tOLNVTI\tZXIOVI\tSMSSUQ\tSMSSUQ\n" +
                        "false\tTKVV\tOJIPHZ\tIHVL\tIHVL\n" +
                        "true\tLJU\tGLHMLLEOYP\tIPZIMNZZR\tHELLO\n" +
                        "true\tBEZGHWVD\tLOPJOX\tRGIIHYH\tHELLO\n" +
                        "true\tMYSSMPGLUO\tZHZSQLDGL\tIFOUSZM\tHELLO\n" +
                        "true\tEBNDCQ\tHNOMVELLKK\tWNWIFFLR\tHELLO\n" +
                        "true\tMNXKUIZ\tIGYV\tFKWZ\tHELLO\n" +
                        "true\tGXHFVWSWSR\tONFCLTJCKF\tNTO\tHELLO\n" +
                        "false\tUKL\tXSLUQD\tPHNIMYF\tPHNIMYF\n" +
                        "true\tNPH\tPBNH\tWWC\tHELLO\n" +
                        "false\tTNLE\tUHH\tGGLN\tGGLN\n" +
                        "false\tLCBDMIGQ\tKHT\tZSLQVFGPP\tZSLQVFGPP\n" +
                        "true\tXBHYSBQYMI\tSVTNPIW\tFKPEV\tHELLO\n" +
                        "false\tFNWG\tDGGI\tDVRVNGS\tDVRVNGS\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when true then 'HELLO'\n" +
                        "        else c\n" +
                        "    end k\n" +
                        "from tanc",
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
                "x\ta\tb\tc\tk\n" +
                        "false\tWCPS\tYRXPE\tRXG\tRXG\n" +
                        "false\tUXIBBT\tGWFFYUD\tYQEHBH\tYQEHBH\n" +
                        "false\tLPD\tSBEOUOJS\tUED\tUED\n" +
                        "true\tULOFJGE\tRSZSRYRF\tTMHGOOZZVD\tHELLO\n" +
                        "true\tYICCXZOUIC\tKGH\tVSDOTS\tHELLO\n" +
                        "true\tYCTGQO\tXWCK\tSUWDSWU\tHELLO\n" +
                        "false\tOLNVTI\tZXIOVI\tSMSSUQ\tSMSSUQ\n" +
                        "false\tTKVV\tOJIPHZ\tIHVL\tIHVL\n" +
                        "true\tLJU\tGLHMLLEOYP\tIPZIMNZZR\tHELLO\n" +
                        "true\tBEZGHWVD\tLOPJOX\tRGIIHYH\tHELLO\n" +
                        "true\tMYSSMPGLUO\tZHZSQLDGL\tIFOUSZM\tHELLO\n" +
                        "true\tEBNDCQ\tHNOMVELLKK\tWNWIFFLR\tHELLO\n" +
                        "true\tMNXKUIZ\tIGYV\tFKWZ\tHELLO\n" +
                        "true\tGXHFVWSWSR\tONFCLTJCKF\tNTO\tHELLO\n" +
                        "false\tUKL\tXSLUQD\tPHNIMYF\tPHNIMYF\n" +
                        "true\tNPH\tPBNH\tWWC\tHELLO\n" +
                        "false\tTNLE\tUHH\tGGLN\tGGLN\n" +
                        "false\tLCBDMIGQ\tKHT\tZSLQVFGPP\tZSLQVFGPP\n" +
                        "true\tXBHYSBQYMI\tSVTNPIW\tFKPEV\tHELLO\n" +
                        "false\tFNWG\tDGGI\tDVRVNGS\tDVRVNGS\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when false then c\n" +
                        "        else 'HELLO'\n" +
                        "    end k\n" +
                        "from tanc",
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
                "x\ta\tb\tc\tk\n" +
                        "false\tWCPS\tYRXPE\tRXG\tRXG\n" +
                        "false\tUXIBBT\tGWFFYUD\tYQEHBH\tYQEHBH\n" +
                        "false\tLPD\tSBEOUOJS\tUED\tUED\n" +
                        "true\tULOFJGE\tRSZSRYRF\tTMHGOOZZVD\tHELLO\n" +
                        "true\tYICCXZOUIC\tKGH\tVSDOTS\tHELLO\n" +
                        "true\tYCTGQO\tXWCK\tSUWDSWU\tHELLO\n" +
                        "false\tOLNVTI\tZXIOVI\tSMSSUQ\tSMSSUQ\n" +
                        "false\tTKVV\tOJIPHZ\tIHVL\tIHVL\n" +
                        "true\tLJU\tGLHMLLEOYP\tIPZIMNZZR\tHELLO\n" +
                        "true\tBEZGHWVD\tLOPJOX\tRGIIHYH\tHELLO\n" +
                        "true\tMYSSMPGLUO\tZHZSQLDGL\tIFOUSZM\tHELLO\n" +
                        "true\tEBNDCQ\tHNOMVELLKK\tWNWIFFLR\tHELLO\n" +
                        "true\tMNXKUIZ\tIGYV\tFKWZ\tHELLO\n" +
                        "true\tGXHFVWSWSR\tONFCLTJCKF\tNTO\tHELLO\n" +
                        "false\tUKL\tXSLUQD\tPHNIMYF\tPHNIMYF\n" +
                        "true\tNPH\tPBNH\tWWC\tHELLO\n" +
                        "false\tTNLE\tUHH\tGGLN\tGGLN\n" +
                        "false\tLCBDMIGQ\tKHT\tZSLQVFGPP\tZSLQVFGPP\n" +
                        "true\tXBHYSBQYMI\tSVTNPIW\tFKPEV\tHELLO\n" +
                        "false\tFNWG\tDGGI\tDVRVNGS\tDVRVNGS\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when true then 'HELLO'\n" +
                        "        when false then c\n" +
                        "    end k\n" +
                        "from tanc",
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
                "x\ta\tb\tc\tk\n" +
                        "false\tWCPS\tYRXPE\tRXG\tRXG\n" +
                        "false\tUXIBBT\tGWFFYUD\tYQEHBH\tYQEHBH\n" +
                        "false\tLPD\tSBEOUOJS\tUED\tUED\n" +
                        "true\tULOFJGE\tRSZSRYRF\tTMHGOOZZVD\tHELLO\n" +
                        "true\tYICCXZOUIC\tKGH\tVSDOTS\tHELLO\n" +
                        "true\tYCTGQO\tXWCK\tSUWDSWU\tHELLO\n" +
                        "false\tOLNVTI\tZXIOVI\tSMSSUQ\tSMSSUQ\n" +
                        "false\tTKVV\tOJIPHZ\tIHVL\tIHVL\n" +
                        "true\tLJU\tGLHMLLEOYP\tIPZIMNZZR\tHELLO\n" +
                        "true\tBEZGHWVD\tLOPJOX\tRGIIHYH\tHELLO\n" +
                        "true\tMYSSMPGLUO\tZHZSQLDGL\tIFOUSZM\tHELLO\n" +
                        "true\tEBNDCQ\tHNOMVELLKK\tWNWIFFLR\tHELLO\n" +
                        "true\tMNXKUIZ\tIGYV\tFKWZ\tHELLO\n" +
                        "true\tGXHFVWSWSR\tONFCLTJCKF\tNTO\tHELLO\n" +
                        "false\tUKL\tXSLUQD\tPHNIMYF\tPHNIMYF\n" +
                        "true\tNPH\tPBNH\tWWC\tHELLO\n" +
                        "false\tTNLE\tUHH\tGGLN\tGGLN\n" +
                        "false\tLCBDMIGQ\tKHT\tZSLQVFGPP\tZSLQVFGPP\n" +
                        "true\tXBHYSBQYMI\tSVTNPIW\tFKPEV\tHELLO\n" +
                        "false\tFNWG\tDGGI\tDVRVNGS\tDVRVNGS\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when false then c\n" +
                        "        when true then 'HELLO'\n" +
                        "    end k\n" +
                        "from tanc",
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
        assertQuery("x\ta\tb\tc\tk\n" +
                        "false\tWCPS\tYRXPE\tRXG\tHELLO2\n" +
                        "false\tUXIBBT\tGWFFYUD\tYQEHBH\tHELLO2\n" +
                        "false\tLPD\tSBEOUOJS\tUED\tHELLO2\n" +
                        "true\tULOFJGE\tRSZSRYRF\tTMHGOOZZVD\tHELLO\n" +
                        "true\tYICCXZOUIC\tKGH\tVSDOTS\tHELLO\n" +
                        "true\tYCTGQO\tXWCK\tSUWDSWU\tHELLO\n" +
                        "false\tOLNVTI\tZXIOVI\tSMSSUQ\tHELLO2\n" +
                        "false\tTKVV\tOJIPHZ\tIHVL\tHELLO2\n" +
                        "true\tLJU\tGLHMLLEOYP\tIPZIMNZZR\tHELLO\n" +
                        "true\tBEZGHWVD\tLOPJOX\tRGIIHYH\tHELLO\n" +
                        "true\tMYSSMPGLUO\tZHZSQLDGL\tIFOUSZM\tHELLO\n" +
                        "true\tEBNDCQ\tHNOMVELLKK\tWNWIFFLR\tHELLO\n" +
                        "true\tMNXKUIZ\tIGYV\tFKWZ\tHELLO\n" +
                        "true\tGXHFVWSWSR\tONFCLTJCKF\tNTO\tHELLO\n" +
                        "false\tUKL\tXSLUQD\tPHNIMYF\tHELLO2\n" +
                        "true\tNPH\tPBNH\tWWC\tHELLO\n" +
                        "false\tTNLE\tUHH\tGGLN\tHELLO2\n" +
                        "false\tLCBDMIGQ\tKHT\tZSLQVFGPP\tHELLO2\n" +
                        "true\tXBHYSBQYMI\tSVTNPIW\tFKPEV\tHELLO\n" +
                        "false\tFNWG\tDGGI\tDVRVNGS\tHELLO2\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when true then 'HELLO'\n" +
                        "        when false then 'HELLO2'\n" +
                        "        else c\n" +
                        "    end k\n" +
                        "from tanc",
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
    public void testByteOrElse() throws Exception {
        assertQuery(
                "x\ta\tb\tc\tk\n" +
                        "76\tT\tJ\tW\tJ\n" +
                        "79\tP\tS\tW\tS\n" +
                        "90\tY\tR\tX\tY\n" +
                        "74\tE\tH\tN\tH\n" +
                        "32\tX\tG\tZ\tG\n" +
                        "77\tX\tU\tX\tU\n" +
                        "101\tB\tB\tT\tB\n" +
                        "89\tP\tG\tW\tG\n" +
                        "112\tF\tY\tU\tY\n" +
                        "117\tE\tY\tY\tY\n" +
                        "86\tE\tH\tB\tH\n" +
                        "65\tF\tO\tW\tO\n" +
                        "73\tP\tD\tX\tD\n" +
                        "119\tS\tB\tE\tB\n" +
                        "57\tU\tO\tJ\tJ\n" +
                        "103\tH\tR\tU\tR\n" +
                        "58\tD\tR\tQ\tR\n" +
                        "20\tU\tL\tO\tL\n" +
                        "54\tJ\tG\tE\tG\n" +
                        "31\tJ\tR\tS\tZ\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when cast(90 as byte) then a\n" +
                        "        when cast(57 as byte) then c\n" +
                        "        when cast(31 as byte) then 'Z'\n" +
                        "        else b\n" +
                        "    end k\n" +
                        "from tanc",
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
    public void testCastValueToLong256() throws Exception {
        assertQuery(
                "x\ta\tb\tc\tk\n" +
                        "-920\t315515118\t7746536061816329025\t0x965d4c984f0ffa8a7bcd48d8c77aa65572a215ba0462ad159f9b2131d49fcd1d\t0x12ce60ee\n" +
                        "671\t1868723706\t-1675638984090602536\t0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\t\n" +
                        "481\t1545253512\t-6943924477733600060\t0x6e60a01a5b3ea0db4b0f595f143e5d722f1a8266e7921e3b716de3d25dcc2d91\t\n" +
                        "147\t-1532328444\t8336855953317473051\t0xa0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88c8b1863d4316f9c7\t\n" +
                        "-55\t-1792928964\t4086802474270249591\t0x9840ad8800156d26c718ab5cbb3fd261c1bf6c24be53876861b1a0b0a5595515\t\n" +
                        "-769\t-1125169127\t2811900023577169860\t0x6adc00ebd29fdd5373dee145497c54365b9832d4b5522a9474ce62a98a451695\t\n" +
                        "-831\t-212807500\t3958193676455060057\t0x9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b7f98b0c74238337e\t\n" +
                        "-914\t-547127752\t-4442449726822927731\t0xbccb30ed7795ebc85f20a35e80e154f458dfd08eeb9cc39ecec82869edec121b\t\n" +
                        "-463\t-27395319\t5476540218465058302\t0x83b91ec970b04e788a50f7ff7f6ed3305705e75fe328fa9d6afe61bd7c4ae0d8\t\n" +
                        "-194\t68265578\t8325936937764905778\t0x9290f9bc187b0cd2bacd57f41b59057caa237cfb02a208e494cfe42988a633de\t\n" +
                        "-835\t1978144263\t6820495939660535106\t0x655f87a3a21d575f610f69efe063fe79336dc434790ed3312bbfcf66bab932fc\t\n" +
                        "-933\t936627841\t5334238747895433003\t0x4cd64b0b0a344f8e6698c6c186b7571a9cba3ef59083484d98c2d832d83de993\t\n" +
                        "416\t-419093579\t-5228148654835984711\t0x5277ee62a5a6e9fb9ff97d73fc0c62d069440048957ae05360802a2ca499f211\t\n" +
                        "380\t161592763\t-7316123607359392486\t0x9660300cea7db540954a62eca44acb2d71660a9b0890a2f06a0accd425e948d4\t\n" +
                        "-574\t-235358133\t-4692986177227268943\t0xd4e0ddcd6eb2cff1c736a8b67656c4f159d574d2ff5fb1e3687a84abb7bfac3e\t\n" +
                        "-722\t-443320374\t8831607763082315932\t0xa5f80be4b45bf437492990e1a29afcac07efe23cedb3250630d46a3a4749c41d\t\n" +
                        "-128\t-1386539059\t-4608960730952244094\t0x37b4f6e41fbfd55f587274e3ab1ebd4d6cecb916a1ad092b997918f622d62989\t\n" +
                        "-842\t1234796102\t-3214230645884399728\t0x87c4f865faa4218e8fd993f0e9bcda593c5d8a6969daa0b37d4f1da8fd48b2c3\t\n" +
                        "-123\t-998315423\t8155981915549526575\t0x84646fead466b67f39d5534da00d272c772c8b7f9505620ebbdfe8ff0cd60c64\t\n" +
                        "535\t-882371473\t-8425379692364264520\t0x14e2b6a0cb7dddc7781a7e89ba21f328a4099f7e23f05cae7ebaf6ca993f8fc9\t\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when -920 then a\n" +
                        "        when 701 then c\n" +
                        "        when -714 then 350\n" +
                        "    end k\n" +
                        "from tanc",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_int() a," +
                        " rnd_long() b," +
                        " rnd_long256() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testCastValueToUuid() throws Exception {
        assertQuery(
                "x\ta\tb\tc\td\tk\n" +
                        "-920\t315515118\t7746536061816329025\t0x965d4c984f0ffa8a7bcd48d8c77aa65572a215ba0462ad159f9b2131d49fcd1d\tdb2d3458-6f62-45fa-b5b2-159a23565217\t315515118\n" +
                        "-48\t-1191262516\t3614738589890112276\t0xc1e631285c1ab288c72bfc5230158059980eca62a219a0f16846d7a3aa5aecce\t716de3d2-5dcc-4d91-9fa2-397a5d8c84c4\t0xc1e631285c1ab288c72bfc5230158059980eca62a219a0f16846d7a3aa5aecce\n" +
                        "-405\t339631474\t7953532976996720859\t0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3\t7e828f56-aaa1-4bde-8d07-6bf991c0ee88\t350\n" +
                        "968\t-85170055\t5539350449504785212\t0xc718ab5cbb3fd261c1bf6c24be53876861b1a0b0a559551538b73d329210d277\t523eb59d-99c6-47af-9840-ad8800156d26\t523eb59d-99c6-47af-9840-ad8800156d26\n" +
                        "-127\t1631244228\t8416773233910814357\t0x6f06560981acb5496adc00ebd29fdd5373dee145497c54365b9832d4b5522a94\t36ee542d-654d-4259-8a53-8661f350d0b4\t\n" +
                        "454\t1253890363\t8942747579519338504\t0xc2593f82b430328d84a09f29df637e3863eb3740c80f661e9c8afa23e6ca6ca1\t58dfd08e-eb9c-439e-8ec8-2869edec121b\t\n" +
                        "-300\t2006313928\t-5986859522579472839\t0x5705e75fe328fa9d6afe61bd7c4ae0d84c0094500fbffdfe76fb2001fe5dfb09\t83b91ec9-70b0-4e78-8a50-f7ff7f6ed330\t\n" +
                        "-194\t68265578\t8325936937764905778\t0x9290f9bc187b0cd2bacd57f41b59057caa237cfb02a208e494cfe42988a633de\ta937c9ce-75e8-4607-a1b5-6c3d802c4735\t\n" +
                        "-54\t-1162267908\t3705833798044144433\t0x3ad08d6037d3ce8155c06051ee52138b655f87a3a21d575f610f69efe063fe79\t98c2d832-d83d-4993-8a07-05e1136e872b\t\n" +
                        "-467\t-2034804966\t5536695302686527374\t0x60802a2ca499f211b771e27f939096b9c356f99ae70523b585b80cec619f9178\t9ff97d73-fc0c-42d0-a944-0048957ae053\t\n" +
                        "-781\t1627393380\t9036423629723776443\t0x954a62eca44acb2d71660a9b0890a2f06a0accd425e948d49a77e857727e751a\t9b27eba5-e9cf-41e2-9660-300cea7db540\t\n" +
                        "-133\t-1299391311\t7528475600160271422\t0x68653a6cd896f81ed4e0ddcd6eb2cff1c736a8b67656c4f159d574d2ff5fb1e3\t7a902c77-fa1a-489c-9168-6790e59377ca\t\n" +
                        "669\t-307026682\t5271904137583983788\t0xc009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8a5f80be4b45bf437\t6cecb916-a1ad-492b-9979-18f622d62989\t\n" +
                        "-819\t532665695\t5867661438830308598\t0x3c5d8a6969daa0b37d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46\t87c4f865-faa4-418e-8fd9-93f0e9bcda59\t\n" +
                        "-123\t-998315423\t8155981915549526575\t0x84646fead466b67f39d5534da00d272c772c8b7f9505620ebbdfe8ff0cd60c64\taa7dc4ec-cb68-446f-b37f-1ec82752c7d7\t\n" +
                        "272\t-1723887671\t-6626590012581323602\t0x7c97a2cb4ac4b04722556b928447b58414e2b6a0cb7dddc7781a7e89ba21f328\taa1896d0-ad34-49d2-910a-a7b6d58506dc\t\n" +
                        "-227\t817130367\t-8408704077728333147\t0x0cb5f439cbc22e9d1f0481ab7acd1f4a77827c4f6b03027bc6dfacdd3f3c52b8\t4b0a72b3-339b-4c7c-9872-e79ea1032246\t\n" +
                        "37\t-1966408995\t9063592617902736531\t0x8fd449ba32592a2b5beb329042090bb3acb025f759cffbd0de9be4e331fe36e6\tb482cff5-7e9c-4398-ac09-f1b4db297f07\t\n" +
                        "-188\t544695670\t-7103100524321179064\t0xd25adf928386cdd2d992946a26184664ba453d761efcf9bb7ee6a03f4f930fa3\t5b3ef212-23ee-4849-a500-9e89eacf0aad\t\n" +
                        "444\t-2111250190\t7035958104135945276\t0x7b9d06dec7caf8a87ee54df55f49e9ac6ea837f54a4154397f3f9fef24a116ed\t5bee3da4-8400-45a6-a827-63e262d6903b\t\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    d,\n" +
                        "    case x\n" +
                        "        when -920 then a\n" +
                        "        when -48 then c\n" +
                        "        when -405 then 350\n" +
                        "        when 968 then d\n" +
                        "    end k\n" +
                        "from tanc",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_int() a," +
                        " rnd_long() b," +
                        " rnd_long256() c, " +
                        " rnd_uuid4() d" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testCharOrElse() throws Exception {
        assertQuery(
                "x\ta\tb\tc\tk\n" +
                        "V\tT\tJ\tW\tJ\n" +
                        "C\tP\tS\tW\tS\n" +
                        "H\tY\tR\tX\tR\n" +
                        "P\tE\tH\tN\tH\n" +
                        "R\tX\tG\tZ\tG\n" +
                        "S\tX\tU\tX\tU\n" +
                        "I\tB\tB\tT\tB\n" +
                        "G\tP\tG\tW\tG\n" +
                        "F\tF\tY\tU\tY\n" +
                        "D\tE\tY\tY\tY\n" +
                        "Q\tE\tH\tB\tH\n" +
                        "H\tF\tO\tW\tO\n" +
                        "L\tP\tD\tX\tP\n" +
                        "Y\tS\tB\tE\tB\n" +
                        "O\tU\tO\tJ\tJ\n" +
                        "S\tH\tR\tU\tR\n" +
                        "E\tD\tR\tQ\tR\n" +
                        "Q\tU\tL\tO\tL\n" +
                        "F\tJ\tG\tE\tG\n" +
                        "T\tJ\tR\tS\tZ\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when 'L' then a\n" +
                        "        when 'O' then c\n" +
                        "        when 'T' then 'Z'\n" +
                        "        else b\n" +
                        "    end k\n" +
                        "from tanc",
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
                "x\ta\tb\tc\tk\n" +
                        "1970-01-01T02:07:23.856Z\tT\tJ\tW\tJ\n" +
                        "1970-01-01T00:43:07.029Z\tP\tS\tW\tZ\n" +
                        "1970-01-01T00:14:24.006Z\tY\tR\tX\tR\n" +
                        "1970-01-01T00:32:57.934Z\tE\tH\tN\tH\n" +
                        "1970-01-01T01:00:00.060Z\tX\tG\tZ\tG\n" +
                        "1970-01-01T01:52:00.859Z\tX\tU\tX\tU\n" +
                        "1970-01-01T02:37:52.057Z\tB\tB\tT\tB\n" +
                        "1970-01-01T02:03:42.727Z\tP\tG\tW\tG\n" +
                        "1970-01-01T02:45:57.016Z\tF\tY\tU\tY\n" +
                        "1970-01-01T02:30:11.353Z\tE\tY\tY\tY\n" +
                        "1970-01-01T00:55:56.086Z\tE\tH\tB\tE\n" +
                        "1970-01-01T01:24:20.057Z\tF\tO\tW\tO\n" +
                        "1970-01-01T01:04:57.951Z\tP\tD\tX\tD\n" +
                        "1970-01-01T01:38:37.157Z\tS\tB\tE\tB\n" +
                        "1970-01-01T02:37:52.839Z\tU\tO\tJ\tO\n" +
                        "1970-01-01T00:38:26.717Z\tH\tR\tU\tR\n" +
                        "1970-01-01T00:48:12.010Z\tD\tR\tQ\tQ\n" +
                        "1970-01-01T01:53:35.364Z\tU\tL\tO\tL\n" +
                        "1970-01-01T00:08:55.106Z\tJ\tG\tE\tG\n" +
                        "1970-01-01T02:04:44.767Z\tJ\tR\tS\tR\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when cast('1970-01-01T00:55:56.086Z' as date) then a\n" +
                        "        when cast('1970-01-01T00:48:12.010Z' as date) then c\n" +
                        "        when cast('1970-01-01T00:43:07.029Z' as date) then 'Z'\n" +
                        "        else b\n" +
                        "    end k\n" +
                        "from tanc",
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
    public void testDuplicateBranchStringToLongCast() throws Exception {
        assertFailure("select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when -920 then a\n" +
                        "        when 701 then c\n" +
                        "        when -714 then 350\n" +
                        "        when '701' then c\n" +
                        "    end k\n" +
                        "from tanc",
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
    public void testInt() throws Exception {
        assertQuery(
                "x\ta\tb\tc\tk\n" +
                        "-920\t315515118\t1548800833\t-727724771\t315515118\n" +
                        "701\t-948263339\t1326447242\t592859671\t592859671\n" +
                        "706\t-847531048\t-1191262516\t-2041844972\tNaN\n" +
                        "-714\t-1575378703\t806715481\t1545253512\t350\n" +
                        "116\t1573662097\t-409854405\t339631474\tNaN\n" +
                        "67\t1904508147\t-1532328444\t-1458132197\tNaN\n" +
                        "207\t-1849627000\t-1432278050\t426455968\tNaN\n" +
                        "-55\t-1792928964\t-1844391305\t-1520872171\tNaN\n" +
                        "-104\t-1153445279\t1404198\t-1715058769\tNaN\n" +
                        "-127\t1631244228\t-1975183723\t-1252906348\tNaN\n" +
                        "790\t-761275053\t-2119387831\t-212807500\tNaN\n" +
                        "881\t1110979454\t1253890363\t-113506296\tNaN\n" +
                        "-535\t-938514914\t-547127752\t-1271909747\tNaN\n" +
                        "-973\t-342047842\t-2132716300\t2006313928\tNaN\n" +
                        "-463\t-27395319\t264240638\t2085282008\tNaN\n" +
                        "-667\t2137969456\t1890602616\t-1272693194\tNaN\n" +
                        "578\t1036510002\t-2002373666\t44173540\tNaN\n" +
                        "940\t410717394\t-2144581835\t1978144263\tNaN\n" +
                        "-54\t-1162267908\t2031014705\t-530317703\tNaN\n" +
                        "-393\t-296610933\t936627841\t326010667\tNaN\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when -920 then a\n" +
                        "        when 701 then c\n" +
                        "        when -714 then 350\n" +
                        "    end k\n" +
                        "from tanc",
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
        assertFailure("select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when -920 then a\n" +
                        "        when 701 then c\n" +
                        "        when -714 then 350\n" +
                        "        when 701 then c\n" +
                        "    end k\n" +
                        "from tanc",
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
                "x\ta\tb\tc\tk\n" +
                        "-920\t315515118\t1548800833\t-727724771\t315515118\n" +
                        "701\t-948263339\t1326447242\t592859671\t592859671\n" +
                        "706\t-847531048\t-1191262516\t-2041844972\t-1191262516\n" +
                        "-714\t-1575378703\t806715481\t1545253512\t350\n" +
                        "116\t1573662097\t-409854405\t339631474\t-409854405\n" +
                        "67\t1904508147\t-1532328444\t-1458132197\t-1532328444\n" +
                        "207\t-1849627000\t-1432278050\t426455968\t-1432278050\n" +
                        "-55\t-1792928964\t-1844391305\t-1520872171\t-1844391305\n" +
                        "-104\t-1153445279\t1404198\t-1715058769\t1404198\n" +
                        "-127\t1631244228\t-1975183723\t-1252906348\t-1975183723\n" +
                        "790\t-761275053\t-2119387831\t-212807500\t-2119387831\n" +
                        "881\t1110979454\t1253890363\t-113506296\t1253890363\n" +
                        "-535\t-938514914\t-547127752\t-1271909747\t-547127752\n" +
                        "-973\t-342047842\t-2132716300\t2006313928\t-2132716300\n" +
                        "-463\t-27395319\t264240638\t2085282008\t264240638\n" +
                        "-667\t2137969456\t1890602616\t-1272693194\t1890602616\n" +
                        "578\t1036510002\t-2002373666\t44173540\t-2002373666\n" +
                        "940\t410717394\t-2144581835\t1978144263\t-2144581835\n" +
                        "-54\t-1162267908\t2031014705\t-530317703\t2031014705\n" +
                        "-393\t-296610933\t936627841\t326010667\t936627841\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when -920 then a\n" +
                        "        when 701 then c\n" +
                        "        when -714 then 350\n" +
                        "        else b\n" +
                        "    end k\n" +
                        "from tanc",
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
                "x\ta\tb\tc\tk\n" +
                        "-920\t00000000 ee 41 1d 15 55 8a 17 fa d8 cc 14 ce f1 59 88 c4\n" +
                        "00000010 91 3b 72 db f3 04 1b c7 88 de a0 79 3c 77 15 68\t00000000 61 26 af 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1\n" +
                        "00000010 1e 38 8d 1b 9e f4 c8 39 09 fe d8 9d 30 78 36 6a\t00000000 32 de e4 7c d2 35 07 42 fc 31 79 5f 8b 81 2b 93\n" +
                        "00000010 4d 1a 8e 78 b5 b9 11 53 d0 fb 64 bb 1a d4 f0 2d\t00000000 ee 41 1d 15 55 8a 17 fa d8 cc 14 ce f1 59 88 c4\n" +
                        "00000010 91 3b 72 db f3 04 1b c7 88 de a0 79 3c 77 15 68\n" +
                        "-352\t00000000 e2 4b b1 3e e3 f1 f1 1e ca 9c 1d 06 ac 37 c8 cd\n" +
                        "00000010 82 89 2b 4d 5f f6 46 90 c3 b3 59 8e e5 61 2f 64\t00000000 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47 dc d2 85 7f\n" +
                        "00000010 a5 b8 7b 4a 9d 46 7c 8d dd 93 e6 d0 b3 2b 07 98\t00000000 cc 76 48 a3 bb 64 d2 ad 49 1c f2 3c ed 39 ac a8\n" +
                        "00000010 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a e7 0c\t00000000 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47 dc d2 85 7f\n" +
                        "00000010 a5 b8 7b 4a 9d 46 7c 8d dd 93 e6 d0 b3 2b 07 98\n" +
                        "-743\t00000000 14 58 63 b7 c2 9f 29 8e 29 5e 69 c6 eb ea c3 c9\n" +
                        "00000010 73 93 46 fe c2 d3 68 79 8b 43 1d 57 34 04 23 8d\t00000000 d8 57 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb 67\n" +
                        "00000010 44 a7 6a 71 34 e0 b0 e9 98 f7 67 62 28 60 b0 ec\t00000000 0b 92 58 7d 24 bc 2e 60 6a 1c 0b 20 a2 86 89 37\n" +
                        "00000010 11 2c 14 0c 2d 20 84 52 d9 6f 04 ab 27 47 8f 23\t00000000 d8 57 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb 67\n" +
                        "00000010 44 a7 6a 71 34 e0 b0 e9 98 f7 67 62 28 60 b0 ec\n" +
                        "-601\t00000000 ae 7c 9f 77 04 e9 0c ea 4e ea 8b f5 0f 2d b3 14\n" +
                        "00000010 33 80 c9 eb a3 67 7a 1a 79 e4 35 e4 3a dc 5c 65\t00000000 ff 27 67 77 12 54 52 d0 29 26 c5 aa da 18 ce 5f\n" +
                        "00000010 b2 8b 5c 54 90 25 c2 20 ff 70 3a c7 8a b3 14 cd\t00000000 47 0b 0c 39 12 f7 05 10 f4 6d f1 e3 ee 58 35 61\n" +
                        "00000010 52 8b 0b 93 e5 57 a5 db a1 76 1c 1c 26 fb 2e 42\t00000000 ff 27 67 77 12 54 52 d0 29 26 c5 aa da 18 ce 5f\n" +
                        "00000010 b2 8b 5c 54 90 25 c2 20 ff 70 3a c7 8a b3 14 cd\n" +
                        "-398\t00000000 f5 6e 8f 80 e3 54 b8 07 b1 32 57 ff 9a ef 88 cb\n" +
                        "00000010 4b a1 cf cf 41 7d a6 d1 3e b4 48 d4 41 9d fb 49\t00000000 40 44 49 96 cf 2b b3 71 a7 d5 af 11 96 37 08 dd\n" +
                        "00000010 98 ef 54 88 2a a2 ad e7 d4 62 e1 4e d6 b2 57 5b\t00000000 e3 71 3d 20 e2 37 f2 64 43 84 55 a0 dd 44 11 e2\n" +
                        "00000010 a3 24 4e 44 a8 0d fe 27 ec 53 13 5d b2 15 e7 b8\t00000000 40 44 49 96 cf 2b b3 71 a7 d5 af 11 96 37 08 dd\n" +
                        "00000010 98 ef 54 88 2a a2 ad e7 d4 62 e1 4e d6 b2 57 5b\n" +
                        "437\t00000000 67 9c 94 b9 8e 28 b6 a9 17 ec 0e 01 c4 eb 9f 13\n" +
                        "00000010 8f bb 2a 4b af 8f 89 df 35 8f da fe 33 98 80 85\t00000000 20 53 3b 51 9d 5d 28 ac 02 2e fe 05 3b 94 5f ec\n" +
                        "00000010 d3 dc f8 43 b2 e3 75 62 60 af 6d 8c d8 ac c8 46\t00000000 3b 47 3c e1 72 3b 9d ef c4 4a c9 cf fb 9d 63 ca\n" +
                        "00000010 94 00 6b dd 18 fe 71 76 bc 45 24 cd 13 00 7c fb\t00000000 20 53 3b 51 9d 5d 28 ac 02 2e fe 05 3b 94 5f ec\n" +
                        "00000010 d3 dc f8 43 b2 e3 75 62 60 af 6d 8c d8 ac c8 46\n" +
                        "-231\t00000000 19 ca f2 bf 84 5a 6f 38 35 15 29 83 1f c3 2f ed\n" +
                        "00000010 b0 ba 08 e0 2c ee 41 de b6 81 df b7 6c 4b fb 2d\t00000000 16 f3 89 a3 83 64 de d6 fd c4 5b c4 e9 19 47 8d\n" +
                        "00000010 ad 11 bc fe b9 52 dd 4d f3 f9 76 f6 85 ab a3 ab\t00000000 ee 6d 54 75 10 b3 4c 0e 8f f1 0c c5 60 b7 d1 5a\n" +
                        "00000010 0c e9 db 51 13 4d 59 20 c9 37 a1 00 f8 42 23 37\t00000000 16 f3 89 a3 83 64 de d6 fd c4 5b c4 e9 19 47 8d\n" +
                        "00000010 ad 11 bc fe b9 52 dd 4d f3 f9 76 f6 85 ab a3 ab\n" +
                        "19\t00000000 a1 8c 47 64 59 1a d4 ab be 30 fa 8d ac 3d 98 a0\n" +
                        "00000010 ad 9a 5d df dc 72 d7 97 cb f6 2c 23 45 a3 76 60\t00000000 15 c1 8c d9 11 69 94 3f 7d ef 3b b8 be f8 a1 46\n" +
                        "00000010 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8 ab 3f a1\t00000000 f5 4b ea 01 c9 63 b4 fc 92 60 1f df 41 ec 2c 38\n" +
                        "00000010 88 88 e7 59 40 10 20 81 c6 3d bc b5 05 2b 73 51\t00000000 15 c1 8c d9 11 69 94 3f 7d ef 3b b8 be f8 a1 46\n" +
                        "00000010 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8 ab 3f a1\n" +
                        "215\t00000000 c3 7e c0 1d 6c a9 65 81 ad 79 87 fc 92 83 fc 88\n" +
                        "00000010 f3 32 27 70 c8 01 b0 dc c9 3a 5b 7e 0e 98 0a 8a\t00000000 0b 1e c4 fd a2 9e b3 77 f8 f6 78 09 1c 5d 88 f5\n" +
                        "00000010 52 fd 36 02 50 d9 a0 b5 90 6c 9c 23 22 89 99 ad\t00000000 f7 fe 9a 9e 1b fd a9 d7 0e 39 5a 28 ed 97 99 d8\n" +
                        "00000010 77 33 3f b2 67 da 98 47 47 bf 4f ea 5f 48 ed f6\t00000000 0b 1e c4 fd a2 9e b3 77 f8 f6 78 09 1c 5d 88 f5\n" +
                        "00000010 52 fd 36 02 50 d9 a0 b5 90 6c 9c 23 22 89 99 ad\n" +
                        "819\t00000000 28 a2 3c d0 65 5e b7 95 2e 4a af c6 d0 19 6a de\n" +
                        "00000010 46 04 d3 81 e7 a2 16 22 35 3b 1c 9c 1d 5c c1 5d\t00000000 2d 44 ea 00 81 c4 19 a1 ec 74 f8 10 fc 6e 23 3d\n" +
                        "00000010 e0 2d 04 86 e7 ca 29 98 07 69 ca 5b d6 cf 09 69\t00000000 01 b1 55 38 ad b2 4a 4e 7d 85 f9 39 25 42 67 78\n" +
                        "00000010 47 b3 80 69 b9 14 d6 fc ee 03 22 81 b8 06 c4 06\t00000000 2d 44 ea 00 81 c4 19 a1 ec 74 f8 10 fc 6e 23 3d\n" +
                        "00000010 e0 2d 04 86 e7 ca 29 98 07 69 ca 5b d6 cf 09 69\n" +
                        "15\t00000000 38 71 1f e1 e4 91 7d e9 5d 4b 6a cd 4e f9 17 9e\n" +
                        "00000010 cf 6a 34 2c 37 a3 6f 2a 12 61 3a 9a ad 98 2e 75\t00000000 52 ad 62 87 88 45 b9 9d 20 13 51 c0 e0 b7 a4 24\n" +
                        "00000010 40 4d 50 b1 8c 4d 66 e8 32 6a 9b cd bb 2e 74 cd\t00000000 44 54 13 3f ff b6 7e cd 04 27 66 94 89 db 3c 1a\n" +
                        "00000010 23 f3 88 83 73 1c 04 63 f9 ac 3d 61 6b 04 33 2b\t00000000 52 ad 62 87 88 45 b9 9d 20 13 51 c0 e0 b7 a4 24\n" +
                        "00000010 40 4d 50 b1 8c 4d 66 e8 32 6a 9b cd bb 2e 74 cd\n" +
                        "-307\t00000000 25 07 db 62 44 33 6e 00 8e 93 bd 27 42 f8 25 2a\n" +
                        "00000010 42 71 a3 7a 58 e5 78 b8 1c d6 fc 7a ac 4c 11 9e\t00000000 60 de 1d 43 8c 7e f3 04 4a 73 f0 31 3e 55 3e 3b\n" +
                        "00000010 6f 93 3f ab ab ac 21 61 99 be 2d f5 30 78 6d 5a\t00000000 3b 2b 30 3c 8b 88 7b 6e a6 6b 91 c7 5f 78 05 e5\n" +
                        "00000010 c0 4e cc d6 e3 7b 34 cd 15 35 bb a4 a3 c8 66 0c\t00000000 60 de 1d 43 8c 7e f3 04 4a 73 f0 31 3e 55 3e 3b\n" +
                        "00000010 6f 93 3f ab ab ac 21 61 99 be 2d f5 30 78 6d 5a\n" +
                        "-272\t00000000 71 ea 20 7e 43 97 27 1f 5c d9 ee 04 5b 9c 17 f2\n" +
                        "00000010 8c bf 95 30 57 1d 91 72 30 04 b7 02 cb 03 23 61\t00000000 b4 bf 04 03 5b 11 44 83 06 63 2b 58 3b 4b b7 e2\n" +
                        "00000010 7f ab 6e 23 03 dd c7 d6 65 29 89 d3 f8 ca 36 84\t00000000 41 fd 48 c5 c5 d7 77 a7 2f 9f 56 79 35 d8 a2 8f\n" +
                        "00000010 f9 64 ed e3 2c 97 0b f5 ef 3b be 85 7c 11 f7 34\t00000000 b4 bf 04 03 5b 11 44 83 06 63 2b 58 3b 4b b7 e2\n" +
                        "00000010 7f ab 6e 23 03 dd c7 d6 65 29 89 d3 f8 ca 36 84\n" +
                        "-559\t00000000 6c 3e 51 d7 eb b1 07 71 32 1f af 40 4e 8c 47 84\n" +
                        "00000010 e9 c0 55 12 44 dc 4b c0 d9 1c 71 cf 5a 8f 21 06\t00000000 b2 3f 0e 41 93 89 27 ca 10 2f 60 ce 59 1c 79 dd\n" +
                        "00000010 02 5e 87 d7 fe ac 8a a3 83 d5 7d e1 4f 56 6b 65\t00000000 a4 53 38 e9 cd c1 a7 ee 86 75 ad a5 2d 49 48 68\n" +
                        "00000010 36 f0 35 dc 45 21 95 01 ef 2d 99 66 3d db c1 cc\t00000000 b2 3f 0e 41 93 89 27 ca 10 2f 60 ce 59 1c 79 dd\n" +
                        "00000010 02 5e 87 d7 fe ac 8a a3 83 d5 7d e1 4f 56 6b 65\n" +
                        "560\t00000000 82 3d ec f3 66 5e 70 38 5e bc e0 8f 10 c3 50 ce\n" +
                        "00000010 4a 20 0f 7f 97 2b 04 b0 97 a4 0f ec 69 cd 73 bb\t00000000 9b c5 95 db 61 91 ce 60 fe 01 a0 ba a5 d1 63 ca\n" +
                        "00000010 32 e5 0d 68 52 c6 94 c3 18 c9 7c 70 9f dc 01 48\t00000000 15 3e 0c 7f 3f 8f e4 b5 ab 34 21 29 23 e8 17 ca\n" +
                        "00000010 f4 c0 8e e1 15 9c aa 69 48 c3 a0 d6 14 8b 7f 03\t00000000 9b c5 95 db 61 91 ce 60 fe 01 a0 ba a5 d1 63 ca\n" +
                        "00000010 32 e5 0d 68 52 c6 94 c3 18 c9 7c 70 9f dc 01 48\n" +
                        "687\t00000000 1a 54 1d 18 c6 1a 8a eb 14 37 55 fd 05 0e 55 76\n" +
                        "00000010 37 bb c3 ec 4b 97 27 df cd 7a 14 07 92 01 f5 6a\t00000000 a1 31 cd cb c2 a2 b4 8e 99 a8 2b 8d 35 c5 85 9a\n" +
                        "00000010 00 4a a1 06 7e 3f 4e 27 42 f2 f8 5e 29 d3 b9 67\t00000000 75 95 fa 1f 92 24 b1 b8 67 65 08 b7 f8 41 00 33\n" +
                        "00000010 ac 30 77 91 b2 de 58 45 d0 1b 58 be 33 92 cd 5c\t00000000 a1 31 cd cb c2 a2 b4 8e 99 a8 2b 8d 35 c5 85 9a\n" +
                        "00000010 00 4a a1 06 7e 3f 4e 27 42 f2 f8 5e 29 d3 b9 67\n" +
                        "629\t00000000 84 36 25 63 2b 63 61 43 1c 47 7d b6 46 ba bb 98\n" +
                        "00000010 ca 08 be a4 96 01 07 cf 1f e7 da eb 6d 5e 37 e4\t00000000 68 2a 96 06 46 b6 aa 36 92 43 c7 e9 ce 9c 54 02\n" +
                        "00000010 9f c2 37 98 60 bb 38 d1 36 18 32 90 33 b5 de f9\t00000000 49 10 e7 7c 3f d6 88 3a 93 ef 24 a5 e2 bc 86 f9\n" +
                        "00000010 92 a3 f1 92 08 f1 96 7f a0 cf 00 74 7c 32 16 38\t00000000 68 2a 96 06 46 b6 aa 36 92 43 c7 e9 ce 9c 54 02\n" +
                        "00000010 9f c2 37 98 60 bb 38 d1 36 18 32 90 33 b5 de f9\n" +
                        "-592\t00000000 7d f4 03 ed c9 2a 4e 91 c5 e4 39 b2 dd 0d a7 bb\n" +
                        "00000010 d5 71 72 ba 9c ac 89 76 dd e7 1f eb 30 58 15 38\t00000000 83 18 dd 1a 16 b2 d8 83 f5 95 7c 95 fd 52 bb 50\n" +
                        "00000010 c9 02 4d 40 ef 1f b8 17 f7 41 ff c1 a7 5c c3 31\t00000000 17 dd 8d c1 cf 5c 24 32 1e 3d ba 5f e3 91 7b ae\n" +
                        "00000010 d1 be 82 5f 87 9e 79 1c c4 fa b8 a3 f7 ba 94 d7\t00000000 83 18 dd 1a 16 b2 d8 83 f5 95 7c 95 fd 52 bb 50\n" +
                        "00000010 c9 02 4d 40 ef 1f b8 17 f7 41 ff c1 a7 5c c3 31\n" +
                        "-228\t00000000 1c dd fc d2 8e 79 ec 02 b2 31 9c 69 be 74 9a ad\n" +
                        "00000010 cc cf b8 e4 d1 7a 4f fb 16 fa 19 a2 df 43 81 a2\t00000000 93 38 ef 38 75 01 cb 8b 64 50 48 10 64 65 32 e1\n" +
                        "00000010 a2 d4 70 b2 53 92 83 24 53 60 4d 04 c2 f0 7a 07\t00000000 d4 a3 d1 5f 0d fe 63 10 0d 8f 53 7d a0 9d a0 50\n" +
                        "00000010 db b2 18 66 ca 85 56 e2 44 db b8 e9 93 fc d9 cb\t00000000 93 38 ef 38 75 01 cb 8b 64 50 48 10 64 65 32 e1\n" +
                        "00000010 a2 d4 70 b2 53 92 83 24 53 60 4d 04 c2 f0 7a 07\n" +
                        "625\t00000000 9c 48 24 83 dc 35 1b b9 0f 97 f5 77 7e a3 2d ce\n" +
                        "00000010 fe eb cd 47 06 53 61 97 40 33 a8 22 95 14 45 fc\t00000000 5c 27 c7 97 9b 8b f8 04 6f d6 af 3f 2f 84 d5 12\n" +
                        "00000010 fb 71 99 34 03 82 08 fb e7 94 3a 32 5d 8a 66 0b\t00000000 e4 85 f1 13 06 f2 27 0f 0c ae 8c 49 a1 ce bf 46\n" +
                        "00000010 36 0d 5b 7f 48 92 ff 37 63 be 5f b7 70 a0 07 8f\t00000000 5c 27 c7 97 9b 8b f8 04 6f d6 af 3f 2f 84 d5 12\n" +
                        "00000010 fb 71 99 34 03 82 08 fb e7 94 3a 32 5d 8a 66 0b\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when -920 then a\n" +
                        "        when 701 then c\n" +
                        "        else b\n" +
                        "    end k\n" +
                        "from tanc",
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
                "x\ta\tb\tc\tk\n" +
                        "-920\t4729996258992366\t1548800833\t-727724771\t4729996258992366\n" +
                        "701\t8920866532787660373\t1326447242\t592859671\t592859671\n" +
                        "706\t-1675638984090602536\t-1191262516\t-2041844972\tNaN\n" +
                        "-714\t-7489826605295361807\t806715481\t1545253512\t350\n" +
                        "116\t8173439391403617681\t-409854405\t339631474\tNaN\n" +
                        "67\t-8968886490993754893\t-1532328444\t-1458132197\tNaN\n" +
                        "207\t-8284534269888369016\t-1432278050\t426455968\tNaN\n" +
                        "-55\t5539350449504785212\t-1844391305\t-1520872171\tNaN\n" +
                        "-104\t-4100339045953973663\t1404198\t-1715058769\tNaN\n" +
                        "-127\t2811900023577169860\t-1975183723\t-1252906348\tNaN\n" +
                        "790\t7700030475747712339\t-2119387831\t-212807500\tNaN\n" +
                        "881\t9194293009132827518\t1253890363\t-113506296\tNaN\n" +
                        "-535\t7199909180655756830\t-547127752\t-1271909747\tNaN\n" +
                        "-973\t6404066507400987550\t-2132716300\t2006313928\tNaN\n" +
                        "-463\t8573481508564499209\t264240638\t2085282008\tNaN\n" +
                        "-667\t-8480005421611953360\t1890602616\t-1272693194\tNaN\n" +
                        "578\t8325936937764905778\t-2002373666\t44173540\tNaN\n" +
                        "940\t-7885528361265853230\t-2144581835\t1978144263\tNaN\n" +
                        "-54\t3152466304308949756\t2031014705\t-530317703\tNaN\n" +
                        "-393\t6179044593759294347\t936627841\t326010667\tNaN\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when -920 then a\n" +
                        "        when 701 then c\n" +
                        "        when -714 then 350\n" +
                        "    end k\n" +
                        "from tanc",
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
        assertFailure("select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when cast('0x00' as long256) then a\n" +
                        "        when cast('0x00' as long256) then c\n" +
                        "        when cast('0x00' as long256) then 350\n" +
                        "        else b\n" +
                        "    end k\n" +
                        "from tanc",
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
        assertFailure("select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when -920 then a\n" +
                        "        when 701 then c\n" +
                        "        when -714 then 350\n" +
                        "        when 701 then c\n" +
                        "    end k\n" +
                        "from tanc",
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
                "x\ta\tb\tc\tk\n" +
                        "856\t315515118\t1548800833\t-727724771\t1548800833\n" +
                        "29\t-948263339\t1326447242\t592859671\t1326447242\n" +
                        "-6\t-847531048\t-1191262516\t-2041844972\t-1191262516\n" +
                        "934\t-1575378703\t806715481\t1545253512\t806715481\n" +
                        "-60\t1573662097\t-409854405\t339631474\t-409854405\n" +
                        "859\t1904508147\t-1532328444\t-1458132197\t-1532328444\n" +
                        "-57\t-1849627000\t-1432278050\t426455968\t-1432278050\n" +
                        "-727\t-1792928964\t-1844391305\t-1520872171\t-1844391305\n" +
                        "-16\t-1153445279\t1404198\t-1715058769\t-1715058769\n" +
                        "353\t1631244228\t-1975183723\t-1252906348\t-1975183723\n" +
                        "86\t-761275053\t-2119387831\t-212807500\t-2119387831\n" +
                        "57\t1110979454\t1253890363\t-113506296\t350\n" +
                        "-951\t-938514914\t-547127752\t-1271909747\t-547127752\n" +
                        "-157\t-342047842\t-2132716300\t2006313928\t-2132716300\n" +
                        "-839\t-27395319\t264240638\t2085282008\t-27395319\n" +
                        "717\t2137969456\t1890602616\t-1272693194\t1890602616\n" +
                        "10\t1036510002\t-2002373666\t44173540\t-2002373666\n" +
                        "-364\t410717394\t-2144581835\t1978144263\t-2144581835\n" +
                        "106\t-1162267908\t2031014705\t-530317703\t2031014705\n" +
                        "767\t-296610933\t936627841\t326010667\t936627841\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when -839 then a\n" +
                        "        when -16 then c\n" +
                        "        when 57 then 350\n" +
                        "        else b\n" +
                        "    end k\n" +
                        "from tanc",
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
        assertFailure("select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when -920 then a\n" +
                        "        when 701 then c\n" +
                        "        when c then 350\n" +
                        "    end k\n" +
                        "from tanc",
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
                "a\tk\n" +
                        "-27056\t0\n" +
                        "-13027\t0\n" +
                        "-1398\t0\n" +
                        "-19496\t0\n" +
                        "-4914\t0\n" +
                        "-19832\t0\n" +
                        "7739\t7739\n" +
                        "31987\t0\n" +
                        "-1593\t0\n" +
                        "13216\t0\n" +
                        "-11657\t0\n" +
                        "-11679\t0\n" +
                        "18457\t0\n" +
                        "10900\t21558\n" +
                        "-19127\t0\n" +
                        "13182\t0\n" +
                        "27809\t0\n" +
                        "12941\t0\n" +
                        "21748\t0\n" +
                        "-1271\t0\n",
                "select \n" +
                        "    a,\n" +
                        "    case a\n" +
                        "        when cast(7739 as short) then a\n" +
                        "        when cast(10900 as short) then b\n" +
                        "    end k \n" +
                        "from tanc",
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
                "x\ta\tb\tc\tk\n" +
                        "-27056\t315515118\t1548800833\t-727724771\t1548800833\n" +
                        "-21227\t-948263339\t1326447242\t592859671\t1326447242\n" +
                        "30202\t-847531048\t-1191262516\t-2041844972\t-1191262516\n" +
                        "-4914\t-1575378703\t806715481\t1545253512\t806715481\n" +
                        "-31548\t1573662097\t-409854405\t339631474\t-409854405\n" +
                        "-24357\t1904508147\t-1532328444\t-1458132197\t-1532328444\n" +
                        "-1593\t-1849627000\t-1432278050\t426455968\t-1432278050\n" +
                        "26745\t-1792928964\t-1844391305\t-1520872171\t-1792928964\n" +
                        "-30872\t-1153445279\t1404198\t-1715058769\t1404198\n" +
                        "18457\t1631244228\t-1975183723\t-1252906348\t-1975183723\n" +
                        "21558\t-761275053\t-2119387831\t-212807500\t-2119387831\n" +
                        "8793\t1110979454\t1253890363\t-113506296\t1253890363\n" +
                        "27809\t-938514914\t-547127752\t-1271909747\t-547127752\n" +
                        "4635\t-342047842\t-2132716300\t2006313928\t2006313928\n" +
                        "24121\t-27395319\t264240638\t2085282008\t264240638\n" +
                        "-1379\t2137969456\t1890602616\t-1272693194\t1890602616\n" +
                        "-22934\t1036510002\t-2002373666\t44173540\t-2002373666\n" +
                        "1404\t410717394\t-2144581835\t1978144263\t350\n" +
                        "-10942\t-1162267908\t2031014705\t-530317703\t2031014705\n" +
                        "22367\t-296610933\t936627841\t326010667\t936627841\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when cast(26745 as short) then a\n" +
                        "        when cast(4635 as short) then c\n" +
                        "        when cast(1404 as short) then 350\n" +
                        "        else b\n" +
                        "    end k\n" +
                        "from tanc",
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
    public void testStrCharOrElse() throws Exception {
        assertQuery(
                "x\ta\tb\tc\tk\n" +
                        "A\t315515118\t1548800833\t-727724771\tthis is A\n" +
                        "B\t-948263339\t1326447242\t592859671\tthis is B\n" +
                        "C\t-847531048\t-1191262516\t-2041844972\tthis is something else\n" +
                        "C\t-1575378703\t806715481\t1545253512\tthis is something else\n" +
                        "A\t1573662097\t-409854405\t339631474\tthis is A\n" +
                        "D\t1904508147\t-1532328444\t-1458132197\tthis is D\n" +
                        "D\t-1849627000\t-1432278050\t426455968\tthis is D\n" +
                        "D\t-1792928964\t-1844391305\t-1520872171\tthis is D\n" +
                        "A\t-1153445279\t1404198\t-1715058769\tthis is A\n" +
                        "D\t1631244228\t-1975183723\t-1252906348\tthis is D\n" +
                        "C\t-761275053\t-2119387831\t-212807500\tthis is something else\n" +
                        "B\t1110979454\t1253890363\t-113506296\tthis is B\n" +
                        "D\t-938514914\t-547127752\t-1271909747\tthis is D\n" +
                        "B\t-342047842\t-2132716300\t2006313928\tthis is B\n" +
                        "D\t-27395319\t264240638\t2085282008\tthis is D\n" +
                        "D\t2137969456\t1890602616\t-1272693194\tthis is D\n" +
                        "C\t1036510002\t-2002373666\t44173540\tthis is something else\n" +
                        "A\t410717394\t-2144581835\t1978144263\tthis is A\n" +
                        "C\t-1162267908\t2031014705\t-530317703\tthis is something else\n" +
                        "B\t-296610933\t936627841\t326010667\tthis is B\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when 'A' then 'this is A'\n" +
                        "        when 'B' then 'this is B'\n" +
                        "        when 'D' then 'this is D'\n" +
                        "        else 'this is something else'\n" +
                        "    end k\n" +
                        "from tanc",
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
                "x\ta\tb\tc\tk\n" +
                        "JWCPSWHYR\t-2041844972\t-1436881714\t-1575378703\t-1436881714\n" +
                        "RXG\t339631474\t1530831067\t1904508147\t1530831067\n" +
                        "IBBTGPGW\t-1101822104\t-1153445279\t1404198\t-1153445279\n" +
                        "EYYQEHBHFO\t-113506296\t-422941535\t-938514914\t-422941535\n" +
                        "YSBEOU\t264240638\t2085282008\t-483853667\t264240638\n" +
                        "UED\t-2002373666\t44173540\t458818940\t44173540\n" +
                        "OFJGET\t-296610933\t936627841\t326010667\t936627841\n" +
                        "RYRFBV\t-1787109293\t-66297136\t-1515787781\t-66297136\n" +
                        "OZZVDZ\t-235358133\t-1299391311\t-1212175298\t-1299391311\n" +
                        "CXZO\t1196016669\t-307026682\t-1566901076\t-1566901076\n" +
                        "KGH\t-1582495445\t-1424048819\t532665695\t-1424048819\n" +
                        "OTSEDYYCT\t-1794809330\t-1609750740\t-731466113\t350\n" +
                        "XWCK\t-880943673\t-2075675260\t1254404167\t-2075675260\n" +
                        "DSWUGSHOL\t1864113037\t-1966408995\t183633043\t-1966408995\n" +
                        "BZX\t2124174232\t-2043803188\t544695670\t-2043803188\n" +
                        "JSMSSU\t-2111250190\t462277692\t614536941\t462277692\n" +
                        "KVVSJOJ\t1238491107\t-1056463214\t-636975106\t-1056463214\n" +
                        "PIH\t1362833895\t576104460\t-805434743\t576104460\n" +
                        "LJU\t454820511\t-246923735\t-514934130\t-246923735\n" +
                        "MLLEO\t387510473\t1431425139\t-948252781\t1431425139\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when 'YSBEOU' then a\n" +
                        "        when 'CXZO' then c\n" +
                        "        when 'OTSEDYYCT' then 350\n" +
                        "        else b\n" +
                        "    end k\n" +
                        "from tanc",
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
                "x\ta\tb\tc\tk\n" +
                        "JWCPSWHYR\tEHNRX\tSXUXI\tTGPGW\tSXUXI\n" +
                        "YUDEYYQEHB\tOWLPDXYSB\tUOJSHRUEDR\tULOFJGE\tUOJSHRUEDR\n" +
                        "RSZSRYRF\tTMHGOOZZVD\tMYICCXZO\tCWEKG\tMYICCXZO\n" +
                        "UVSDOTSE\tYCTGQO\tXWCK\tSUWDSWU\tXWCK\n" +
                        "HOLNV\tQBZXIOVIK\tMSSUQSR\tKVVSJOJ\tMSSUQSR\n" +
                        "HZEPIHVLT\tLJU\tGLHMLLEOYP\tIPZIMNZZR\tGLHMLLEOYP\n" +
                        "MBEZGHW\tKFL\tJOXPKR\tIHYH\tJOXPKR\n" +
                        "QMYS\tPGLUOHN\tZSQLDGLOG\tOUSZMZV\tZSQLDGLOG\n" +
                        "BNDCQCEHNO\tELLKK\tWNWIFFLR\tOMNXKUIZUL\tWNWIFFLR\n" +
                        "YVFZF\tZLUOG\tFVWSWSR\tONFCLTJCKF\tFVWSWSR\n" +
                        "NTO\tXUKLG\tSLUQDY\tHNIMYFF\tXUKLG\n" +
                        "NPH\tPBNH\tWWC\tGTNLEGPUHH\tWWC\n" +
                        "GGLN\tZLCBDMIG\tVKHTLQ\tLQVF\tVKHTLQ\n" +
                        "PRGSXBHYS\tYMIZJS\tNPIWZNFK\tVMCGFN\tVMCGFN\n" +
                        "RMDGGIJ\tVRVNG\tEQODRZEI\tOQKYH\tEQODRZEI\n" +
                        "UWQOEE\tEBQQEMXDK\tJCTIZK\tLUHZQSN\tJCTIZK\n" +
                        "MKJSMKIX\tVTUPDHH\tIWHPZRHH\tZJYYFLSVI\tIWHPZRHH\n" +
                        "WWLEVM\tCJBEV\tHLIHYBT\tNCLNXFS\tHLIHYBT\n" +
                        "PNXH\tTZODWKOCPF\tPVKNC\tLNLRH\tPVKNC\n" +
                        "XYPO\tDBZWNI\tEHR\tPBMB\tWORKS!\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when 'NTO' then a\n" +
                        "        when 'PRGSXBHYS' then c\n" +
                        "        when 'XYPO' then 'WORKS!'\n" +
                        "        else b\n" +
                        "    end k\n" +
                        "from tanc",
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
        assertFailure("select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when 'NTO' then a\n" +
                        "        when 'PRGSXBHYS' then c\n" +
                        "        when 'NTO' then 'WORKS!'\n" +
                        "        else b\n" +
                        "    end k\n" +
                        "from tanc",
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
        assertQuery(
                "x\ta\tb\tc\tk\n" +
                        "a1\tT\tJ\tW\tJ\n" +
                        "b2\tP\tS\tW\tP\n" +
                        "b2\tY\tR\tX\tY\n" +
                        "\tE\tH\tN\tZ\n" +
                        "b2\tX\tG\tZ\tX\n" +
                        "c3\tX\tU\tX\tU\n" +
                        "c3\tB\tB\tT\tB\n" +
                        "a1\tP\tG\tW\tG\n" +
                        "\tF\tY\tU\tZ\n" +
                        "c3\tE\tY\tY\tY\n" +
                        "a1\tE\tH\tB\tH\n" +
                        "b2\tF\tO\tW\tF\n" +
                        "a1\tP\tD\tX\tD\n" +
                        "d4\tS\tB\tE\tE\n" +
                        "d4\tU\tO\tJ\tJ\n" +
                        "c3\tH\tR\tU\tR\n" +
                        "d4\tD\tR\tQ\tQ\n" +
                        "a1\tU\tL\tO\tL\n" +
                        "\tJ\tG\tE\tZ\n" +
                        "d4\tJ\tR\tS\tS\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when 'b2' then a\n" +
                        "        when 'd4' then c\n" +
                        "        when null then 'Z'\n" +
                        "        else b\n" +
                        "    end k\n" +
                        "from tanc",
                "create table tanc as (" +
                        "select rnd_symbol('a1', 'b2', 'c3', 'd4', null) x," +
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
    public void testSymbolOrElse() throws Exception {
        assertQuery(
                "x\ta\tb\tc\tk\n" +
                        "a1\tT\tJ\tW\tZ\n" +
                        "b2\tP\tS\tW\tP\n" +
                        "b2\tY\tR\tX\tY\n" +
                        "\tE\tH\tN\tH\n" +
                        "b2\tX\tG\tZ\tX\n" +
                        "c3\tX\tU\tX\tU\n" +
                        "c3\tB\tB\tT\tB\n" +
                        "a1\tP\tG\tW\tZ\n" +
                        "\tF\tY\tU\tY\n" +
                        "c3\tE\tY\tY\tY\n" +
                        "a1\tE\tH\tB\tZ\n" +
                        "b2\tF\tO\tW\tF\n" +
                        "a1\tP\tD\tX\tZ\n" +
                        "d4\tS\tB\tE\tE\n" +
                        "d4\tU\tO\tJ\tJ\n" +
                        "c3\tH\tR\tU\tR\n" +
                        "d4\tD\tR\tQ\tQ\n" +
                        "a1\tU\tL\tO\tZ\n" +
                        "\tJ\tG\tE\tG\n" +
                        "d4\tJ\tR\tS\tS\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when 'b2' then a\n" +
                        "        when 'd4' then c\n" +
                        "        when 'a1' then 'Z'\n" +
                        "        else b\n" +
                        "    end k\n" +
                        "from tanc",
                "create table tanc as (" +
                        "select rnd_symbol('a1', 'b2', 'c3', 'd4', null) x," +
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
    public void testTimestampOrElse() throws Exception {
        assertQuery(
                "x\ta\tb\tc\tk\n" +
                        "1970-01-01T00:00:00.000000Z\tV\tT\tJ\tT\n" +
                        "1970-01-01T00:00:00.100000Z\tW\tC\tP\tC\n" +
                        "1970-01-01T00:00:00.200000Z\tS\tW\tH\tW\n" +
                        "1970-01-01T00:00:00.300000Z\tY\tR\tX\tR\n" +
                        "1970-01-01T00:00:00.400000Z\tP\tE\tH\tE\n" +
                        "1970-01-01T00:00:00.500000Z\tN\tR\tX\tN\n" +
                        "1970-01-01T00:00:00.600000Z\tG\tZ\tS\tZ\n" +
                        "1970-01-01T00:00:00.700000Z\tX\tU\tX\tU\n" +
                        "1970-01-01T00:00:00.800000Z\tI\tB\tB\tB\n" +
                        "1970-01-01T00:00:00.900000Z\tT\tG\tP\tG\n" +
                        "1970-01-01T00:00:01.000000Z\tG\tW\tF\tW\n" +
                        "1970-01-01T00:00:01.100000Z\tF\tY\tU\tY\n" +
                        "1970-01-01T00:00:01.200000Z\tD\tE\tY\tE\n" +
                        "1970-01-01T00:00:01.300000Z\tY\tQ\tE\tQ\n" +
                        "1970-01-01T00:00:01.400000Z\tH\tB\tH\tB\n" +
                        "1970-01-01T00:00:01.500000Z\tF\tO\tW\tO\n" +
                        "1970-01-01T00:00:01.600000Z\tL\tP\tD\tP\n" +
                        "1970-01-01T00:00:01.700000Z\tX\tY\tS\tY\n" +
                        "1970-01-01T00:00:01.800000Z\tB\tE\tO\tE\n" +
                        "1970-01-01T00:00:01.900000Z\tU\tO\tJ\tZ\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when cast('1970-01-01T00:00:00.500Z' as date) then a\n" +
                        "        when cast('1970-01-01T00:48:12.010Z' as date) then c\n" +
                        "        when cast('1970-01-01T00:00:01.900Z' as date) then 'Z'\n" +
                        "        else b\n" +
                        "    end k\n" +
                        "from tanc",
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
