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

public class LeftStrFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstLarge() throws Exception {
        assertQuery(
                "k\tleft\n" +
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
                "select k, left(k,50) from x", // 50 > than max string len
                "create table x as (select rnd_str(10,20,1) k from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testConstNeg() throws Exception {
        assertQuery(
                "k\tleft\n" +
                        "JWCPSWHYRXPEHNRX\tJWCPSWHYRXPEHNR\n" +
                        "SXUXIBBTGPGWFFYU\tSXUXIBBTGPGWFFY\n" +
                        "YYQEHBHFOWLPDXYSBEO\tYYQEHBHFOWLPDXYSBE\n" +
                        "JSHRUEDRQQUL\tJSHRUEDRQQU\n" +
                        "\t\n" +
                        "GETJRSZSRYRFBVTMHGOO\tGETJRSZSRYRFBVTMHGO\n" +
                        "VDZJMYICCXZOUIC\tVDZJMYICCXZOUI\n" +
                        "KGHVUVSDOTSEDYYCTGQO\tKGHVUVSDOTSEDYYCTGQ\n" +
                        "\t\n" +
                        "WCKYLSUWDSWUGSH\tWCKYLSUWDSWUGS\n",
                "select k, left(k,-1) from x",
                "create table x as (select rnd_str(10,20,1) k from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testConstNegLarge() throws Exception {
        assertQuery(
                "k\tleft\n" +
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
                "select k, left(k,-40) from x",
                "create table x as (select rnd_str(10,20,1) k from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testConstNull() throws Exception {
        assertQuery(
                "k\tleft\n" +
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
                "select k, left(k,null) from x",
                "create table x as (select rnd_str(10,20,1) k from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testSimple() throws Exception {
        assertQuery(
                "k\tleft\n" +
                        "JWCPSWHYRXPEHNRX\tJW\n" +
                        "SXUXIBBTGPGWFFYU\tSX\n" +
                        "YYQEHBHFOWLPDXYSBEO\tYY\n" +
                        "JSHRUEDRQQUL\tJS\n" +
                        "\t\n" +
                        "GETJRSZSRYRFBVTMHGOO\tGE\n" +
                        "VDZJMYICCXZOUIC\tVD\n" +
                        "KGHVUVSDOTSEDYYCTGQO\tKG\n" +
                        "\t\n" +
                        "WCKYLSUWDSWUGSH\tWC\n",
                "select k, left(k,2) from x",
                "create table x as (select rnd_str(10,20,1) k from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testVar() throws Exception {
        assertQuery(
                "k\tn\tleft\n" +
                        "JWCPSWHYRXPEHNRX\t5\tJWCPS\n" +
                        "\tnull\t\n" +
                        "IBBTGPGWFFYUDEY\t15\tIBBTGPGWFFYUDEY\n" +
                        "BHFOWLPDXY\t-1\tBHFOWLPDX\n" +
                        "UOJSHRUEDRQQULO\t9\tUOJSHRUED\n" +
                        "\t6\t\n" +
                        "SZSRYRFBVTMHGOOZ\t8\tSZSRYRFB\n" +
                        "JMYICCXZOUICWEKGH\t16\tJMYICCXZOUICWEKG\n" +
                        "\t11\t\n" +
                        "SEDYYCTGQOLY\t10\tSEDYYCTGQO\n",
                "select k, n, left(k,n) from x",
                "create table x as (select rnd_str(10,20,1) k, rnd_int(-1, 20, 1) n from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testWhenCountIsZeroThenReturnsEmptyStringOrNull() throws Exception {
        assertQuery(
                "k\tleft\n" +
                        "JWC\t\n" +
                        "WHYRX\t\n" +
                        "HNRX\t\n" +
                        "SXUX\t\n" +
                        "\t\n",
                "select k, left(k,0) from x",
                "create table x as (select rnd_str(3,5,1) k from long_sequence(5))",
                null,
                true,
                true
        );
    }
}
