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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class LeftStrFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstLarge() throws Exception {
        assertQuery("select k, left(k,50) from x") // 50 > than max string len
                .ddl("create table x as (select rnd_str(10,20,1) k from long_sequence(10))")
                .expectSize()
                .returns("""
                        k\tleft
                        JWCPSWHYRXPEHNRX\tJWCPSWHYRXPEHNRX
                        SXUXIBBTGPGWFFYU\tSXUXIBBTGPGWFFYU
                        YYQEHBHFOWLPDXYSBEO\tYYQEHBHFOWLPDXYSBEO
                        JSHRUEDRQQUL\tJSHRUEDRQQUL
                        \t
                        GETJRSZSRYRFBVTMHGOO\tGETJRSZSRYRFBVTMHGOO
                        VDZJMYICCXZOUIC\tVDZJMYICCXZOUIC
                        KGHVUVSDOTSEDYYCTGQO\tKGHVUVSDOTSEDYYCTGQO
                        \t
                        WCKYLSUWDSWUGSH\tWCKYLSUWDSWUGSH
                        """);
    }

    @Test
    public void testConstNeg() throws Exception {
        assertQuery("select k, left(k,-1) from x")
                .ddl("create table x as (select rnd_str(10,20,1) k from long_sequence(10))")
                .expectSize()
                .returns("""
                        k\tleft
                        JWCPSWHYRXPEHNRX\tJWCPSWHYRXPEHNR
                        SXUXIBBTGPGWFFYU\tSXUXIBBTGPGWFFY
                        YYQEHBHFOWLPDXYSBEO\tYYQEHBHFOWLPDXYSBE
                        JSHRUEDRQQUL\tJSHRUEDRQQU
                        \t
                        GETJRSZSRYRFBVTMHGOO\tGETJRSZSRYRFBVTMHGO
                        VDZJMYICCXZOUIC\tVDZJMYICCXZOUI
                        KGHVUVSDOTSEDYYCTGQO\tKGHVUVSDOTSEDYYCTGQ
                        \t
                        WCKYLSUWDSWUGSH\tWCKYLSUWDSWUGS
                        """);
    }

    @Test
    public void testConstNegLarge() throws Exception {
        assertQuery("select k, left(k,-40) from x")
                .ddl("create table x as (select rnd_str(10,20,1) k from long_sequence(10))")
                .expectSize()
                .returns("""
                        k\tleft
                        JWCPSWHYRXPEHNRX\t
                        SXUXIBBTGPGWFFYU\t
                        YYQEHBHFOWLPDXYSBEO\t
                        JSHRUEDRQQUL\t
                        \t
                        GETJRSZSRYRFBVTMHGOO\t
                        VDZJMYICCXZOUIC\t
                        KGHVUVSDOTSEDYYCTGQO\t
                        \t
                        WCKYLSUWDSWUGSH\t
                        """);
    }

    @Test
    public void testConstNull() throws Exception {
        assertQuery("select k, left(k,null) from x")
                .ddl("create table x as (select rnd_str(10,20,1) k from long_sequence(10))")
                .expectSize()
                .returns("""
                        k\tleft
                        JWCPSWHYRXPEHNRX\t
                        SXUXIBBTGPGWFFYU\t
                        YYQEHBHFOWLPDXYSBEO\t
                        JSHRUEDRQQUL\t
                        \t
                        GETJRSZSRYRFBVTMHGOO\t
                        VDZJMYICCXZOUIC\t
                        KGHVUVSDOTSEDYYCTGQO\t
                        \t
                        WCKYLSUWDSWUGSH\t
                        """);
    }

    @Test
    public void testSimple() throws Exception {
        assertQuery("select k, left(k,2) from x")
                .ddl("create table x as (select rnd_str(10,20,1) k from long_sequence(10))")
                .expectSize()
                .returns("""
                        k\tleft
                        JWCPSWHYRXPEHNRX\tJW
                        SXUXIBBTGPGWFFYU\tSX
                        YYQEHBHFOWLPDXYSBEO\tYY
                        JSHRUEDRQQUL\tJS
                        \t
                        GETJRSZSRYRFBVTMHGOO\tGE
                        VDZJMYICCXZOUIC\tVD
                        KGHVUVSDOTSEDYYCTGQO\tKG
                        \t
                        WCKYLSUWDSWUGSH\tWC
                        """);
    }

    @Test
    public void testVar() throws Exception {
        assertQuery("select k, n, left(k,n) from x")
                .ddl("create table x as (select rnd_str(10,20,1) k, rnd_int(-1, 20, 1) n from long_sequence(10))")
                .expectSize()
                .returns("""
                        k\tn\tleft
                        JWCPSWHYRXPEHNRX\t5\tJWCPS
                        \tnull\t
                        IBBTGPGWFFYUDEY\t15\tIBBTGPGWFFYUDEY
                        BHFOWLPDXY\t-1\tBHFOWLPDX
                        UOJSHRUEDRQQULO\t9\tUOJSHRUED
                        \t6\t
                        SZSRYRFBVTMHGOOZ\t8\tSZSRYRFB
                        JMYICCXZOUICWEKGH\t16\tJMYICCXZOUICWEKG
                        \t11\t
                        SEDYYCTGQOLY\t10\tSEDYYCTGQO
                        """);
    }

    @Test
    public void testWhenCountIsZeroThenReturnsEmptyStringOrNull() throws Exception {
        assertQuery("select k, left(k,0) from x")
                .ddl("create table x as (select rnd_str(3,5,1) k from long_sequence(5))")
                .expectSize()
                .returns("""
                        k\tleft
                        JWC\t
                        WHYRX\t
                        HNRX\t
                        SXUX\t
                        \t
                        """);
    }
}
