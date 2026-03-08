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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class RightStrFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstLarge() throws Exception {
        assertQuery(
                "k\tright\n" +
                        "JWCPSWHYRXPEHNRX\tJWCPSWHYRXPEHNRX\n" +
                        "SXUXIBBTGPGWFFYU\tSXUXIBBTGPGWFFYU\n" +
                        "YYQEHBHFOWLPDXYSBEO\tYYQEHBHFOWLPDXYSBEO\n" +
                        "JSHRUEDRQQUL\tJSHRUEDRQQUL\n" +
                        "\t\n" +
                        "GETJRSZSRYRFBVTMHGOO\tGETJRSZSRYRFBVTMHGOO\n" +
                        "VDZJMYICCXZOUIC\tVDZJMYICCXZOUIC\n" +
                        "KGHVUVSDOTSEDYYCTGQO\tKGHVUVSDOTSEDYYCTGQO\n" +
                        "\t\n" +
                        "WCKYLSUWDSWUGSH\tWCKYLSUWDSWUGSH\n",
                "select k, right(k,50) from x", // 50 > than max string len
                "create table x as (select rnd_str(10,20,1) k from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testConstNeg() throws Exception {
        assertQuery(
                "k\tright\n" +
                        "JWCPSWHYRXPEHNRX\tWCPSWHYRXPEHNRX\n" +
                        "SXUXIBBTGPGWFFYU\tXUXIBBTGPGWFFYU\n" +
                        "YYQEHBHFOWLPDXYSBEO\tYQEHBHFOWLPDXYSBEO\n" +
                        "JSHRUEDRQQUL\tSHRUEDRQQUL\n" +
                        "\t\n" +
                        "GETJRSZSRYRFBVTMHGOO\tETJRSZSRYRFBVTMHGOO\n" +
                        "VDZJMYICCXZOUIC\tDZJMYICCXZOUIC\n" +
                        "KGHVUVSDOTSEDYYCTGQO\tGHVUVSDOTSEDYYCTGQO\n" +
                        "\t\n" +
                        "WCKYLSUWDSWUGSH\tCKYLSUWDSWUGSH\n",
                "select k, right(k,-1) from x",
                "create table x as (select rnd_str(10,20,1) k from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testConstNegLarge() throws Exception {
        assertQuery(
                "k\tright\n" +
                        "JWCPSWHYRXPEHNRX\t\n" +
                        "SXUXIBBTGPGWFFYU\t\n" +
                        "YYQEHBHFOWLPDXYSBEO\t\n" +
                        "JSHRUEDRQQUL\t\n" +
                        "\t\n" +
                        "GETJRSZSRYRFBVTMHGOO\t\n" +
                        "VDZJMYICCXZOUIC\t\n" +
                        "KGHVUVSDOTSEDYYCTGQO\t\n" +
                        "\t\n" +
                        "WCKYLSUWDSWUGSH\t\n",
                "select k, right(k,-100) from x",
                "create table x as (select rnd_str(10,20,1) k from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testConstNull() throws Exception {
        assertQuery(
                "k\tright\n" +
                        "JWCPSWHYRXPEHNRX\t\n" +
                        "SXUXIBBTGPGWFFYU\t\n" +
                        "YYQEHBHFOWLPDXYSBEO\t\n" +
                        "JSHRUEDRQQUL\t\n" +
                        "\t\n" +
                        "GETJRSZSRYRFBVTMHGOO\t\n" +
                        "VDZJMYICCXZOUIC\t\n" +
                        "KGHVUVSDOTSEDYYCTGQO\t\n" +
                        "\t\n" +
                        "WCKYLSUWDSWUGSH\t\n",
                "select k, right(k,null) from x",
                "create table x as (select rnd_str(10,20,1) k from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testSimple() throws Exception {
        assertQuery(
                "k\tright\n" +
                        "JWCPSWHYRXPEHNRX\tRX\n" +
                        "SXUXIBBTGPGWFFYU\tYU\n" +
                        "YYQEHBHFOWLPDXYSBEO\tEO\n" +
                        "JSHRUEDRQQUL\tUL\n" +
                        "\t\n" +
                        "GETJRSZSRYRFBVTMHGOO\tOO\n" +
                        "VDZJMYICCXZOUIC\tIC\n" +
                        "KGHVUVSDOTSEDYYCTGQO\tQO\n" +
                        "\t\n" +
                        "WCKYLSUWDSWUGSH\tSH\n",
                "select k, right(k,2) from x",
                "create table x as (select rnd_str(10,20,1) k from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testVar() throws Exception {
        assertQuery(
                "k\tn\tright\n" +
                        "JWCPSWHYRXPEHNRX\t5\tEHNRX\n" +
                        "\tnull\t\n" +
                        "IBBTGPGWFFYUDEY\t15\tIBBTGPGWFFYUDEY\n" +
                        "BHFOWLPDXY\t-1\tHFOWLPDXY\n" +
                        "UOJSHRUEDRQQULO\t9\tUEDRQQULO\n" +
                        "\t6\t\n" +
                        "SZSRYRFBVTMHGOOZ\t8\tVTMHGOOZ\n" +
                        "JMYICCXZOUICWEKGH\t16\tMYICCXZOUICWEKGH\n" +
                        "\t11\t\n" +
                        "SEDYYCTGQOLY\t10\tDYYCTGQOLY\n",
                "select k, n, right(k,n) from x",
                "create table x as (select rnd_str(10,20,1) k, rnd_int(-1, 20, 1) n from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testWhenCountIsZeroThenReturnsEmptyStringOrNull() throws Exception {
        assertQuery(
                "k\tright\n" +
                        "BT\t\n" +
                        "\t\n" +
                        "PGW\t\n" +
                        "PE\t\n" +
                        "TJ\t\n",
                "select k, right(k,0) from x",
                "create table x as (select rnd_str(10,2,3,3) k from long_sequence(5))",
                null,
                true,
                true
        );
    }
}
