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

public class RightStrFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstLarge() throws Exception {
        assertQuery("select k, right(k,50) from x") // 50 > than max string len
                .ddl("create table x as (select rnd_str(10,20,1) k from long_sequence(10))")
                .expectSize()
                .returns("""
                        k\tright
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
        assertQuery("select k, right(k,-1) from x")
                .ddl("create table x as (select rnd_str(10,20,1) k from long_sequence(10))")
                .expectSize()
                .returns("""
                        k\tright
                        JWCPSWHYRXPEHNRX\tWCPSWHYRXPEHNRX
                        SXUXIBBTGPGWFFYU\tXUXIBBTGPGWFFYU
                        YYQEHBHFOWLPDXYSBEO\tYQEHBHFOWLPDXYSBEO
                        JSHRUEDRQQUL\tSHRUEDRQQUL
                        \t
                        GETJRSZSRYRFBVTMHGOO\tETJRSZSRYRFBVTMHGOO
                        VDZJMYICCXZOUIC\tDZJMYICCXZOUIC
                        KGHVUVSDOTSEDYYCTGQO\tGHVUVSDOTSEDYYCTGQO
                        \t
                        WCKYLSUWDSWUGSH\tCKYLSUWDSWUGSH
                        """);
    }

    @Test
    public void testConstNegLarge() throws Exception {
        assertQuery("select k, right(k,-100) from x")
                .ddl("create table x as (select rnd_str(10,20,1) k from long_sequence(10))")
                .expectSize()
                .returns("""
                        k\tright
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
        assertQuery("select k, right(k,null) from x")
                .ddl("create table x as (select rnd_str(10,20,1) k from long_sequence(10))")
                .expectSize()
                .returns("""
                        k\tright
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
        assertQuery("select k, right(k,2) from x")
                .ddl("create table x as (select rnd_str(10,20,1) k from long_sequence(10))")
                .expectSize()
                .returns("""
                        k\tright
                        JWCPSWHYRXPEHNRX\tRX
                        SXUXIBBTGPGWFFYU\tYU
                        YYQEHBHFOWLPDXYSBEO\tEO
                        JSHRUEDRQQUL\tUL
                        \t
                        GETJRSZSRYRFBVTMHGOO\tOO
                        VDZJMYICCXZOUIC\tIC
                        KGHVUVSDOTSEDYYCTGQO\tQO
                        \t
                        WCKYLSUWDSWUGSH\tSH
                        """);
    }

    @Test
    public void testVar() throws Exception {
        assertQuery("select k, n, right(k,n) from x")
                .ddl("create table x as (select rnd_str(10,20,1) k, rnd_int(-1, 20, 1) n from long_sequence(10))")
                .expectSize()
                .returns("""
                        k\tn\tright
                        JWCPSWHYRXPEHNRX\t5\tEHNRX
                        \tnull\t
                        IBBTGPGWFFYUDEY\t15\tIBBTGPGWFFYUDEY
                        BHFOWLPDXY\t-1\tHFOWLPDXY
                        UOJSHRUEDRQQULO\t9\tUEDRQQULO
                        \t6\t
                        SZSRYRFBVTMHGOOZ\t8\tVTMHGOOZ
                        JMYICCXZOUICWEKGH\t16\tMYICCXZOUICWEKGH
                        \t11\t
                        SEDYYCTGQOLY\t10\tDYYCTGQOLY
                        """);
    }

    @Test
    public void testWhenCountIsZeroThenReturnsEmptyStringOrNull() throws Exception {
        assertQuery("select k, right(k,0) from x")
                .ddl("create table x as (select rnd_str(10,2,3,3) k from long_sequence(5))")
                .expectSize()
                .returns("""
                        k\tright
                        BT\t
                        \t
                        PGW\t
                        PE\t
                        TJ\t
                        """);
    }
}
