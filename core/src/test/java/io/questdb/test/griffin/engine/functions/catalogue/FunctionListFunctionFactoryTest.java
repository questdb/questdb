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

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.test.AbstractGriffinTest;
import org.junit.Test;

public class FunctionListFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testFunctions() throws Exception {
        assertSql(
                "functions() ORDER BY 1",
                "name\tsignature\tsignature_translated\truntime_constant\ttype\n" +
                        "VARCHAR\tVARCHAR(I)\tVARCHAR(var int)\tfalse\tSTANDARD\n" +
                        "abs\tabs(I)\tabs(var int)\tfalse\tSTANDARD\n" +
                        "abs\tabs(E)\tabs(var short)\tfalse\tSTANDARD\n" +
                        "abs\tabs(L)\tabs(var long)\tfalse\tSTANDARD\n" +
                        "abs\tabs(D)\tabs(var double)\tfalse\tSTANDARD\n" +
                        "acos\tacos(D)\tacos(var double)\tfalse\tSTANDARD\n" +
                        "all_tables\tall_tables()\tall_tables()\ttrue\tSTANDARD\n" +
                        "and\tand(TT)\tand(var boolean, var boolean)\tfalse\tSTANDARD\n" +
                        "asin\tasin(D)\tasin(var double)\tfalse\tSTANDARD\n" +
                        "atan\tatan(D)\tatan(var double)\tfalse\tSTANDARD\n" +
                        "atan2\tatan2(DD)\tatan2(var double, var double)\tfalse\tSTANDARD\n" +
                        "avg\tavg(D)\tavg(var double)\tfalse\tGROUP_BY\n" +
                        "avg\tavg(T)\tavg(var boolean)\tfalse\tGROUP_BY\n" +
                        "base64\tbase64(Ui)\tbase64(var binary, const int)\tfalse\tSTANDARD\n" +
                        "between\tbetween(NNN)\tbetween(var timestamp, var timestamp, var timestamp)\tfalse\tSTANDARD\n" +
                        "build\tbuild()\tbuild()\ttrue\tSTANDARD\n" +
                        "case\tcase(V)\tcase(var var_arg)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Dt)\tcast(var double, const boolean)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Di)\tcast(var double, const int)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Dd)\tcast(var double, const double)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Df)\tcast(var double, const float)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Dh)\tcast(var double, const long256)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Dl)\tcast(var double, const long)\tfalse\tSTANDARD\n" +
                        "cast\tcast(De)\tcast(var double, const short)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Db)\tcast(var double, const byte)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ds)\tcast(var double, const string)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Dk)\tcast(var double, const symbol)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Da)\tcast(var double, const char)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Dm)\tcast(var double, const date)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Dn)\tcast(var double, const timestamp)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ft)\tcast(var float, const boolean)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Fi)\tcast(var float, const int)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Fd)\tcast(var float, const double)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ff)\tcast(var float, const float)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Fh)\tcast(var float, const long256)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Fl)\tcast(var float, const long)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Fe)\tcast(var float, const short)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Fb)\tcast(var float, const byte)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Fs)\tcast(var float, const string)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Fk)\tcast(var float, const symbol)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Fa)\tcast(var float, const char)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Fm)\tcast(var float, const date)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Fn)\tcast(var float, const timestamp)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ee)\tcast(var short, const short)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Eb)\tcast(var short, const byte)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ea)\tcast(var short, const char)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ei)\tcast(var short, const int)\tfalse\tSTANDARD\n" +
                        "cast\tcast(El)\tcast(var short, const long)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ef)\tcast(var short, const float)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ed)\tcast(var short, const double)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Es)\tcast(var short, const string)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Em)\tcast(var short, const date)\tfalse\tSTANDARD\n" +
                        "cast\tcast(En)\tcast(var short, const timestamp)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ek)\tcast(var short, const symbol)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Eh)\tcast(var short, const long256)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Et)\tcast(var short, const boolean)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ie)\tcast(var int, const short)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ib)\tcast(var int, const byte)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ia)\tcast(var int, const char)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ii)\tcast(var int, const int)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Il)\tcast(var int, const long)\tfalse\tSTANDARD\n" +
                        "cast\tcast(If)\tcast(var int, const float)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Id)\tcast(var int, const double)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Is)\tcast(var int, const string)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Im)\tcast(var int, const date)\tfalse\tSTANDARD\n" +
                        "cast\tcast(In)\tcast(var int, const timestamp)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ik)\tcast(var int, const symbol)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ih)\tcast(var int, const long256)\tfalse\tSTANDARD\n" +
                        "cast\tcast(It)\tcast(var int, const boolean)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Le)\tcast(var long, const short)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Lb)\tcast(var long, const byte)\tfalse\tSTANDARD\n" +
                        "cast\tcast(La)\tcast(var long, const char)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Li)\tcast(var long, const int)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ll)\tcast(var long, const long)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Lf)\tcast(var long, const float)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ld)\tcast(var long, const double)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ls)\tcast(var long, const string)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Lm)\tcast(var long, const date)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ln)\tcast(var long, const timestamp)\tfalse\tSTANDARD\n" +
                        "cast\tcast(oV)\tcast(const null, var var_arg)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Lk)\tcast(var long, const symbol)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Lh)\tcast(var long, const long256)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Lt)\tcast(var long, const boolean)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Lg)\tcast(var long, const geohash)\tfalse\tSTANDARD\n" +
                        "cast\tcast(He)\tcast(var long256, const short)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Hb)\tcast(var long256, const byte)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ha)\tcast(var long256, const char)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Hi)\tcast(var long256, const int)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Hl)\tcast(var long256, const long)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Hf)\tcast(var long256, const float)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Hd)\tcast(var long256, const double)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Hs)\tcast(var long256, const string)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Hm)\tcast(var long256, const date)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Hn)\tcast(var long256, const timestamp)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Hk)\tcast(var long256, const symbol)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Hl)\tcast(var long256, const long)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ht)\tcast(var long256, const boolean)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Me)\tcast(var date, const short)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Mb)\tcast(var date, const byte)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ma)\tcast(var date, const char)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Mi)\tcast(var date, const int)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ml)\tcast(var date, const long)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Mf)\tcast(var date, const float)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Md)\tcast(var date, const double)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ms)\tcast(var date, const string)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Mm)\tcast(var date, const date)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Mn)\tcast(var date, const timestamp)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Mk)\tcast(var date, const symbol)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Mh)\tcast(var date, const long256)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Mt)\tcast(var date, const boolean)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ne)\tcast(var timestamp, const short)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Nb)\tcast(var timestamp, const byte)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Na)\tcast(var timestamp, const char)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ni)\tcast(var timestamp, const int)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Nl)\tcast(var timestamp, const long)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Nf)\tcast(var timestamp, const float)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Nd)\tcast(var timestamp, const double)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ns)\tcast(var timestamp, const string)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Nm)\tcast(var timestamp, const date)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Nn)\tcast(var timestamp, const timestamp)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Nk)\tcast(var timestamp, const symbol)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Nh)\tcast(var timestamp, const long256)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Nt)\tcast(var timestamp, const boolean)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Be)\tcast(var byte, const short)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Bb)\tcast(var byte, const byte)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ba)\tcast(var byte, const char)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Bi)\tcast(var byte, const int)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Bl)\tcast(var byte, const long)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Bf)\tcast(var byte, const float)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Bd)\tcast(var byte, const double)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Bs)\tcast(var byte, const string)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Bm)\tcast(var byte, const date)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Bn)\tcast(var byte, const timestamp)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Bk)\tcast(var byte, const symbol)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Bh)\tcast(var byte, const long256)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Bt)\tcast(var byte, const boolean)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Te)\tcast(var boolean, const short)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Tb)\tcast(var boolean, const byte)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ta)\tcast(var boolean, const char)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ti)\tcast(var boolean, const int)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Tl)\tcast(var boolean, const long)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Tf)\tcast(var boolean, const float)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Td)\tcast(var boolean, const double)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ts)\tcast(var boolean, const string)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Tm)\tcast(var boolean, const date)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Tn)\tcast(var boolean, const timestamp)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Tk)\tcast(var boolean, const symbol)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Th)\tcast(var boolean, const long256)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Tt)\tcast(var boolean, const boolean)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ae)\tcast(var char, const short)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ab)\tcast(var char, const byte)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Aa)\tcast(var char, const char)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ai)\tcast(var char, const int)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Al)\tcast(var char, const long)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Af)\tcast(var char, const float)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ad)\tcast(var char, const double)\tfalse\tSTANDARD\n" +
                        "cast\tcast(As)\tcast(var char, const string)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Am)\tcast(var char, const date)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ak)\tcast(var char, const symbol)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ah)\tcast(var char, const long256)\tfalse\tSTANDARD\n" +
                        "cast\tcast(An)\tcast(var char, const timestamp)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Si)\tcast(var string, const int)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Sd)\tcast(var string, const double)\tfalse\tSTANDARD\n" +
                        "cast\tcast(At)\tcast(var char, const boolean)\tfalse\tSTANDARD\n" +
                        "cast\tcast(St)\tcast(var string, const boolean)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Sf)\tcast(var string, const float)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Sh)\tcast(var string, const long256)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Sl)\tcast(var string, const long)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Sp)\tcast(var string, const reg_class)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Sq)\tcast(var string, const reg_procedure)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Sw)\tcast(var string, const array_string)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Se)\tcast(var string, const short)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Sb)\tcast(var string, const byte)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ss)\tcast(var string, const string)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Sk)\tcast(var string, const symbol)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Sa)\tcast(var string, const char)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Sm)\tcast(var string, const date)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Sn)\tcast(var string, const timestamp)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Su)\tcast(var string, const binary)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Sg)\tcast(var string, const geohash)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Gg)\tcast(var geohash, const geohash)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Sz)\tcast(var string, const uuid)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Zs)\tcast(var uuid, const string)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ki)\tcast(var symbol, const int)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Kd)\tcast(var symbol, const double)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Kf)\tcast(var symbol, const float)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Kh)\tcast(var symbol, const long256)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Kl)\tcast(var symbol, const long)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ke)\tcast(var symbol, const short)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Kb)\tcast(var symbol, const byte)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ks)\tcast(var symbol, const string)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Kk)\tcast(var symbol, const symbol)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Ka)\tcast(var symbol, const char)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Km)\tcast(var symbol, const date)\tfalse\tSTANDARD\n" +
                        "cast\tcast(Kn)\tcast(var symbol, const timestamp)\tfalse\tSTANDARD\n" +
                        "ceil\tceil(D)\tceil(var double)\tfalse\tSTANDARD\n" +
                        "ceil\tceil(F)\tceil(var float)\tfalse\tSTANDARD\n" +
                        "ceiling\tceiling(D)\tceiling(var double)\tfalse\tSTANDARD\n" +
                        "ceiling\tceiling(F)\tceiling(var float)\tfalse\tSTANDARD\n" +
                        "coalesce\tcoalesce(V)\tcoalesce(var var_arg)\tfalse\tSTANDARD\n" +
                        "concat\tconcat(V)\tconcat(var var_arg)\tfalse\tSTANDARD\n" +
                        "cos\tcos(D)\tcos(var double)\tfalse\tSTANDARD\n" +
                        "cot\tcot(D)\tcot(var double)\tfalse\tSTANDARD\n" +
                        "count\tcount()\tcount()\tfalse\tGROUP_BY\n" +
                        "count\tcount(D)\tcount(var double)\tfalse\tGROUP_BY\n" +
                        "count\tcount(F)\tcount(var float)\tfalse\tGROUP_BY\n" +
                        "count\tcount(G)\tcount(var geohash)\tfalse\tGROUP_BY\n" +
                        "count\tcount(I)\tcount(var int)\tfalse\tGROUP_BY\n" +
                        "count\tcount(L)\tcount(var long)\tfalse\tGROUP_BY\n" +
                        "count\tcount(H)\tcount(var long256)\tfalse\tGROUP_BY\n" +
                        "count\tcount(S)\tcount(var string)\tfalse\tGROUP_BY\n" +
                        "count\tcount(K)\tcount(var symbol)\tfalse\tGROUP_BY\n" +
                        "count_distinct\tcount_distinct(S)\tcount_distinct(var string)\tfalse\tGROUP_BY\n" +
                        "count_distinct\tcount_distinct(K)\tcount_distinct(var symbol)\tfalse\tGROUP_BY\n" +
                        "count_distinct\tcount_distinct(H)\tcount_distinct(var long256)\tfalse\tGROUP_BY\n" +
                        "count_distinct\tcount_distinct(L)\tcount_distinct(var long)\tfalse\tGROUP_BY\n" +
                        "count_distinct\tcount_distinct(I)\tcount_distinct(var int)\tfalse\tGROUP_BY\n" +
                        "count_distinct\tcount_distinct(Z)\tcount_distinct(var uuid)\tfalse\tGROUP_BY\n" +
                        "current_database\tcurrent_database()\tcurrent_database()\ttrue\tSTANDARD\n" +
                        "current_schema\tcurrent_schema()\tcurrent_schema()\ttrue\tSTANDARD\n" +
                        "current_schemas\tcurrent_schemas(T)\tcurrent_schemas(var boolean)\tfalse\tSTANDARD\n" +
                        "current_user\tcurrent_user()\tcurrent_user()\ttrue\tSTANDARD\n" +
                        "date_trunc\tdate_trunc(sN)\tdate_trunc(const string, var timestamp)\tfalse\tSTANDARD\n" +
                        "dateadd\tdateadd(AIN)\tdateadd(var char, var int, var timestamp)\tfalse\tSTANDARD\n" +
                        "datediff\tdatediff(ANN)\tdatediff(var char, var timestamp, var timestamp)\tfalse\tSTANDARD\n" +
                        "day\tday(N)\tday(var timestamp)\tfalse\tSTANDARD\n" +
                        "day_of_week\tday_of_week(N)\tday_of_week(var timestamp)\tfalse\tSTANDARD\n" +
                        "day_of_week_sunday_first\tday_of_week_sunday_first(N)\tday_of_week_sunday_first(var timestamp)\tfalse\tSTANDARD\n" +
                        "days_in_month\tdays_in_month(N)\tdays_in_month(var timestamp)\tfalse\tSTANDARD\n" +
                        "degrees\tdegrees(D)\tdegrees(var double)\tfalse\tSTANDARD\n" +
                        "dump_memory_usage\tdump_memory_usage()\tdump_memory_usage()\tfalse\tSTANDARD\n" +
                        "dump_thread_stacks\tdump_thread_stacks()\tdump_thread_stacks()\tfalse\tSTANDARD\n" +
                        "extract\textract(sN)\textract(const string, var timestamp)\tfalse\tSTANDARD\n" +
                        "first\tfirst(D)\tfirst(var double)\tfalse\tGROUP_BY\n" +
                        "first\tfirst(F)\tfirst(var float)\tfalse\tGROUP_BY\n" +
                        "first\tfirst(I)\tfirst(var int)\tfalse\tGROUP_BY\n" +
                        "first\tfirst(A)\tfirst(var char)\tfalse\tGROUP_BY\n" +
                        "first\tfirst(E)\tfirst(var short)\tfalse\tGROUP_BY\n" +
                        "first\tfirst(B)\tfirst(var byte)\tfalse\tGROUP_BY\n" +
                        "first\tfirst(N)\tfirst(var timestamp)\tfalse\tGROUP_BY\n" +
                        "first\tfirst(L)\tfirst(var long)\tfalse\tGROUP_BY\n" +
                        "first\tfirst(M)\tfirst(var date)\tfalse\tGROUP_BY\n" +
                        "first\tfirst(G)\tfirst(var geohash)\tfalse\tGROUP_BY\n" +
                        "first\tfirst(Z)\tfirst(var uuid)\tfalse\tGROUP_BY\n" +
                        "first\tfirst(K)\tfirst(var symbol)\tfalse\tGROUP_BY\n" +
                        "floor\tfloor(D)\tfloor(var double)\tfalse\tSTANDARD\n" +
                        "floor\tfloor(F)\tfloor(var float)\tfalse\tSTANDARD\n" +
                        "flush_query_cache\tflush_query_cache()\tflush_query_cache()\tfalse\tSTANDARD\n" +
                        "format_type\tformat_type(II)\tformat_type(var int, var int)\tfalse\tSTANDARD\n" +
                        "functions\tfunctions()\tfunctions()\ttrue\tSTANDARD\n" +
                        "haversine_dist_deg\thaversine_dist_deg(DDN)\thaversine_dist_deg(var double, var double, var timestamp)\tfalse\tGROUP_BY\n" +
                        "hour\thour(N)\thour(var timestamp)\tfalse\tSTANDARD\n" +
                        "ilike\tilike(SS)\tilike(var string, var string)\tfalse\tSTANDARD\n" +
                        "in\tin(KC)\tin(var symbol, var cursor)\tfalse\tSTANDARD\n" +
                        "in\tin(Sv)\tin(var string, const var_arg)\tfalse\tSTANDARD\n" +
                        "in\tin(Av)\tin(var char, const var_arg)\tfalse\tSTANDARD\n" +
                        "in\tin(Kv)\tin(var symbol, const var_arg)\tfalse\tSTANDARD\n" +
                        "in\tin(NS)\tin(var timestamp, var string)\tfalse\tSTANDARD\n" +
                        "in\tin(NV)\tin(var timestamp, var var_arg)\tfalse\tSTANDARD\n" +
                        "in\tin(Zv)\tin(var uuid, const var_arg)\tfalse\tSTANDARD\n" +
                        "information_schema._pg_expandarray\tinformation_schema._pg_expandarray(V)\tinformation_schema._pg_expandarray(var var_arg)\ttrue\tCURSOR\n" +
                        "isOrdered\tisOrdered(L)\tisOrdered(var long)\tfalse\tGROUP_BY\n" +
                        "is_leap_year\tis_leap_year(N)\tis_leap_year(var timestamp)\tfalse\tSTANDARD\n" +
                        "keywords\tkeywords()\tkeywords()\ttrue\tSTANDARD\n" +
                        "ksum\tksum(D)\tksum(var double)\tfalse\tGROUP_BY\n" +
                        "last\tlast(D)\tlast(var double)\tfalse\tGROUP_BY\n" +
                        "last\tlast(F)\tlast(var float)\tfalse\tGROUP_BY\n" +
                        "last\tlast(I)\tlast(var int)\tfalse\tGROUP_BY\n" +
                        "last\tlast(A)\tlast(var char)\tfalse\tGROUP_BY\n" +
                        "last\tlast(E)\tlast(var short)\tfalse\tGROUP_BY\n" +
                        "last\tlast(B)\tlast(var byte)\tfalse\tGROUP_BY\n" +
                        "last\tlast(K)\tlast(var symbol)\tfalse\tGROUP_BY\n" +
                        "last\tlast(N)\tlast(var timestamp)\tfalse\tGROUP_BY\n" +
                        "last\tlast(M)\tlast(var date)\tfalse\tGROUP_BY\n" +
                        "last\tlast(L)\tlast(var long)\tfalse\tGROUP_BY\n" +
                        "last\tlast(G)\tlast(var geohash)\tfalse\tGROUP_BY\n" +
                        "last\tlast(Z)\tlast(var uuid)\tfalse\tGROUP_BY\n" +
                        "left\tleft(SI)\tleft(var string, var int)\tfalse\tSTANDARD\n" +
                        "length\tlength(S)\tlength(var string)\tfalse\tSTANDARD\n" +
                        "length\tlength(K)\tlength(var symbol)\tfalse\tSTANDARD\n" +
                        "length\tlength(U)\tlength(var binary)\tfalse\tSTANDARD\n" +
                        "like\tlike(SS)\tlike(var string, var string)\tfalse\tSTANDARD\n" +
                        "list\tlist(V)\tlist(var var_arg)\tfalse\tSTANDARD\n" +
                        "ln\tln(D)\tln(var double)\tfalse\tSTANDARD\n" +
                        "log\tlog(D)\tlog(var double)\tfalse\tSTANDARD\n" +
                        "long_sequence\tlong_sequence(v)\tlong_sequence(const var_arg)\tfalse\tSTANDARD\n" +
                        "lower\tlower(S)\tlower(var string)\tfalse\tSTANDARD\n" +
                        "lpad\tlpad(SI)\tlpad(var string, var int)\tfalse\tSTANDARD\n" +
                        "lpad\tlpad(SIS)\tlpad(var string, var int, var string)\tfalse\tSTANDARD\n" +
                        "ltrim\tltrim(S)\tltrim(var string)\tfalse\tSTANDARD\n" +
                        "make_geohash\tmake_geohash(DDi)\tmake_geohash(var double, var double, const int)\tfalse\tSTANDARD\n" +
                        "max\tmax(D)\tmax(var double)\tfalse\tGROUP_BY\n" +
                        "max\tmax(T)\tmax(var boolean)\tfalse\tGROUP_BY\n" +
                        "max\tmax(I)\tmax(var int)\tfalse\tGROUP_BY\n" +
                        "max\tmax(L)\tmax(var long)\tfalse\tGROUP_BY\n" +
                        "max\tmax(A)\tmax(var char)\tfalse\tGROUP_BY\n" +
                        "max\tmax(N)\tmax(var timestamp)\tfalse\tGROUP_BY\n" +
                        "max\tmax(M)\tmax(var date)\tfalse\tGROUP_BY\n" +
                        "max\tmax(F)\tmax(var float)\tfalse\tGROUP_BY\n" +
                        "max\tmax(S)\tmax(var string)\tfalse\tGROUP_BY\n" +
                        "memory_metrics\tmemory_metrics()\tmemory_metrics()\tfalse\tSTANDARD\n" +
                        "micros\tmicros(N)\tmicros(var timestamp)\tfalse\tSTANDARD\n" +
                        "millis\tmillis(N)\tmillis(var timestamp)\tfalse\tSTANDARD\n" +
                        "min\tmin(D)\tmin(var double)\tfalse\tGROUP_BY\n" +
                        "min\tmin(T)\tmin(var boolean)\tfalse\tGROUP_BY\n" +
                        "min\tmin(F)\tmin(var float)\tfalse\tGROUP_BY\n" +
                        "min\tmin(L)\tmin(var long)\tfalse\tGROUP_BY\n" +
                        "min\tmin(I)\tmin(var int)\tfalse\tGROUP_BY\n" +
                        "min\tmin(A)\tmin(var char)\tfalse\tGROUP_BY\n" +
                        "min\tmin(N)\tmin(var timestamp)\tfalse\tGROUP_BY\n" +
                        "min\tmin(M)\tmin(var date)\tfalse\tGROUP_BY\n" +
                        "min\tmin(S)\tmin(var string)\tfalse\tGROUP_BY\n" +
                        "minute\tminute(N)\tminute(var timestamp)\tfalse\tSTANDARD\n" +
                        "month\tmonth(N)\tmonth(var timestamp)\tfalse\tSTANDARD\n" +
                        "not\tnot(T)\tnot(var boolean)\tfalse\tSTANDARD\n" +
                        "now\tnow()\tnow()\ttrue\tSTANDARD\n" +
                        "npe\tnpe()\tnpe()\tfalse\tSTANDARD\n" +
                        "nsum\tnsum(D)\tnsum(var double)\tfalse\tGROUP_BY\n" +
                        "nullif\tnullif(AA)\tnullif(var char, var char)\tfalse\tSTANDARD\n" +
                        "nullif\tnullif(II)\tnullif(var int, var int)\tfalse\tSTANDARD\n" +
                        "nullif\tnullif(LL)\tnullif(var long, var long)\tfalse\tSTANDARD\n" +
                        "nullif\tnullif(SS)\tnullif(var string, var string)\tfalse\tSTANDARD\n" +
                        "or\tor(TT)\tor(var boolean, var boolean)\tfalse\tSTANDARD\n" +
                        "pg_advisory_unlock_all\tpg_advisory_unlock_all()\tpg_advisory_unlock_all()\ttrue\tSTANDARD\n" +
                        "pg_attrdef\tpg_attrdef()\tpg_attrdef()\ttrue\tSTANDARD\n" +
                        "pg_attribute\tpg_attribute()\tpg_attribute()\ttrue\tSTANDARD\n" +
                        "pg_catalog.age\tpg_catalog.age(L)\tpg_catalog.age(var long)\tfalse\tSTANDARD\n" +
                        "pg_catalog.current_database\tpg_catalog.current_database()\tpg_catalog.current_database()\ttrue\tSTANDARD\n" +
                        "pg_catalog.current_schema\tpg_catalog.current_schema()\tpg_catalog.current_schema()\ttrue\tSTANDARD\n" +
                        "pg_catalog.current_schemas\tpg_catalog.current_schemas(T)\tpg_catalog.current_schemas(var boolean)\tfalse\tSTANDARD\n" +
                        "pg_catalog.pg_attrdef\tpg_catalog.pg_attrdef()\tpg_catalog.pg_attrdef()\ttrue\tSTANDARD\n" +
                        "pg_catalog.pg_attribute\tpg_catalog.pg_attribute()\tpg_catalog.pg_attribute()\ttrue\tSTANDARD\n" +
                        "pg_catalog.pg_class\tpg_catalog.pg_class()\tpg_catalog.pg_class()\ttrue\tCURSOR\n" +
                        "pg_catalog.pg_database\tpg_catalog.pg_database()\tpg_catalog.pg_database()\ttrue\tCURSOR\n" +
                        "pg_catalog.pg_description\tpg_catalog.pg_description()\tpg_catalog.pg_description()\ttrue\tCURSOR\n" +
                        "pg_catalog.pg_get_expr\tpg_catalog.pg_get_expr(SIT)\tpg_catalog.pg_get_expr(var string, var int, var boolean)\ttrue\tSTANDARD\n" +
                        "pg_catalog.pg_get_expr\tpg_catalog.pg_get_expr(SI)\tpg_catalog.pg_get_expr(var string, var int)\ttrue\tSTANDARD\n" +
                        "pg_catalog.pg_get_keywords\tpg_catalog.pg_get_keywords()\tpg_catalog.pg_get_keywords()\ttrue\tSTANDARD\n" +
                        "pg_catalog.pg_get_partkeydef\tpg_catalog.pg_get_partkeydef(I)\tpg_catalog.pg_get_partkeydef(var int)\ttrue\tSTANDARD\n" +
                        "pg_catalog.pg_get_userbyid\tpg_catalog.pg_get_userbyid(I)\tpg_catalog.pg_get_userbyid(var int)\ttrue\tSTANDARD\n" +
                        "pg_catalog.pg_index\tpg_catalog.pg_index()\tpg_catalog.pg_index()\ttrue\tCURSOR\n" +
                        "pg_catalog.pg_inherits\tpg_catalog.pg_inherits()\tpg_catalog.pg_inherits()\ttrue\tCURSOR\n" +
                        "pg_catalog.pg_is_in_recovery\tpg_catalog.pg_is_in_recovery()\tpg_catalog.pg_is_in_recovery()\tfalse\tSTANDARD\n" +
                        "pg_catalog.pg_locks\tpg_catalog.pg_locks()\tpg_catalog.pg_locks()\ttrue\tCURSOR\n" +
                        "pg_catalog.pg_namespace\tpg_catalog.pg_namespace()\tpg_catalog.pg_namespace()\ttrue\tSTANDARD\n" +
                        "pg_catalog.pg_roles\tpg_catalog.pg_roles()\tpg_catalog.pg_roles()\ttrue\tCURSOR\n" +
                        "pg_catalog.pg_shdescription\tpg_catalog.pg_shdescription()\tpg_catalog.pg_shdescription()\ttrue\tCURSOR\n" +
                        "pg_catalog.pg_table_is_visible\tpg_catalog.pg_table_is_visible(I)\tpg_catalog.pg_table_is_visible(var int)\ttrue\tSTANDARD\n" +
                        "pg_catalog.pg_type\tpg_catalog.pg_type()\tpg_catalog.pg_type()\tfalse\tCURSOR\n" +
                        "pg_catalog.txid_current\tpg_catalog.txid_current()\tpg_catalog.txid_current()\tfalse\tSTANDARD\n" +
                        "pg_catalog.version\tpg_catalog.version()\tpg_catalog.version()\ttrue\tSTANDARD\n" +
                        "pg_class\tpg_class()\tpg_class()\ttrue\tCURSOR\n" +
                        "pg_database\tpg_database()\tpg_database()\ttrue\tCURSOR\n" +
                        "pg_description\tpg_description()\tpg_description()\ttrue\tCURSOR\n" +
                        "pg_get_expr\tpg_get_expr(SIT)\tpg_get_expr(var string, var int, var boolean)\ttrue\tSTANDARD\n" +
                        "pg_get_expr\tpg_get_expr(SI)\tpg_get_expr(var string, var int)\ttrue\tSTANDARD\n" +
                        "pg_get_keywords\tpg_get_keywords()\tpg_get_keywords()\ttrue\tSTANDARD\n" +
                        "pg_get_partkeydef\tpg_get_partkeydef(I)\tpg_get_partkeydef(var int)\ttrue\tSTANDARD\n" +
                        "pg_index\tpg_index()\tpg_index()\ttrue\tCURSOR\n" +
                        "pg_inherits\tpg_inherits()\tpg_inherits()\ttrue\tCURSOR\n" +
                        "pg_is_in_recovery\tpg_is_in_recovery()\tpg_is_in_recovery()\tfalse\tSTANDARD\n" +
                        "pg_locks\tpg_locks()\tpg_locks()\ttrue\tCURSOR\n" +
                        "pg_namespace\tpg_namespace()\tpg_namespace()\ttrue\tSTANDARD\n" +
                        "pg_postmaster_start_time\tpg_postmaster_start_time()\tpg_postmaster_start_time()\tfalse\tSTANDARD\n" +
                        "pg_proc\tpg_proc()\tpg_proc()\ttrue\tCURSOR\n" +
                        "pg_range\tpg_range()\tpg_range()\ttrue\tCURSOR\n" +
                        "pg_roles\tpg_roles()\tpg_roles()\ttrue\tCURSOR\n" +
                        "pg_type\tpg_type()\tpg_type()\tfalse\tCURSOR\n" +
                        "pi\tpi()\tpi()\tfalse\tSTANDARD\n" +
                        "position\tposition(SS)\tposition(var string, var string)\tfalse\tSTANDARD\n" +
                        "power\tpower(DD)\tpower(var double, var double)\tfalse\tSTANDARD\n" +
                        "radians\tradians(D)\tradians(var double)\tfalse\tSTANDARD\n" +
                        "rank\trank()\trank()\tfalse\tWINDOW\n" +
                        "reader_pool\treader_pool()\treader_pool()\tfalse\tSTANDARD\n" +
                        "regexp_replace\tregexp_replace(SSS)\tregexp_replace(var string, var string, var string)\tfalse\tSTANDARD\n" +
                        "replace\treplace(SSS)\treplace(var string, var string, var string)\tfalse\tSTANDARD\n" +
                        "right\tright(SI)\tright(var string, var int)\tfalse\tSTANDARD\n" +
                        "rnd_bin\trnd_bin(lli)\trnd_bin(const long, const long, const int)\tfalse\tSTANDARD\n" +
                        "rnd_bin\trnd_bin()\trnd_bin()\tfalse\tSTANDARD\n" +
                        "rnd_boolean\trnd_boolean()\trnd_boolean()\tfalse\tSTANDARD\n" +
                        "rnd_byte\trnd_byte(ii)\trnd_byte(const int, const int)\tfalse\tSTANDARD\n" +
                        "rnd_byte\trnd_byte()\trnd_byte()\tfalse\tSTANDARD\n" +
                        "rnd_char\trnd_char()\trnd_char()\tfalse\tSTANDARD\n" +
                        "rnd_date\trnd_date(mmi)\trnd_date(const date, const date, const int)\tfalse\tSTANDARD\n" +
                        "rnd_date\trnd_date()\trnd_date()\tfalse\tSTANDARD\n" +
                        "rnd_double\trnd_double(i)\trnd_double(const int)\tfalse\tSTANDARD\n" +
                        "rnd_double\trnd_double()\trnd_double()\tfalse\tSTANDARD\n" +
                        "rnd_float\trnd_float(i)\trnd_float(const int)\tfalse\tSTANDARD\n" +
                        "rnd_float\trnd_float()\trnd_float()\tfalse\tSTANDARD\n" +
                        "rnd_geohash\trnd_geohash(i)\trnd_geohash(const int)\tfalse\tSTANDARD\n" +
                        "rnd_int\trnd_int()\trnd_int()\tfalse\tSTANDARD\n" +
                        "rnd_int\trnd_int(iii)\trnd_int(const int, const int, const int)\tfalse\tSTANDARD\n" +
                        "rnd_log\trnd_log(ld)\trnd_log(const long, const double)\tfalse\tSTANDARD\n" +
                        "rnd_long\trnd_long(lli)\trnd_long(const long, const long, const int)\tfalse\tSTANDARD\n" +
                        "rnd_long\trnd_long()\trnd_long()\tfalse\tSTANDARD\n" +
                        "rnd_long256\trnd_long256()\trnd_long256()\tfalse\tSTANDARD\n" +
                        "rnd_long256\trnd_long256(i)\trnd_long256(const int)\tfalse\tSTANDARD\n" +
                        "rnd_short\trnd_short(ii)\trnd_short(const int, const int)\tfalse\tSTANDARD\n" +
                        "rnd_short\trnd_short()\trnd_short()\tfalse\tSTANDARD\n" +
                        "rnd_str\trnd_str(iii)\trnd_str(const int, const int, const int)\tfalse\tSTANDARD\n" +
                        "rnd_str\trnd_str(iiii)\trnd_str(const int, const int, const int, const int)\tfalse\tSTANDARD\n" +
                        "rnd_str\trnd_str(V)\trnd_str(var var_arg)\tfalse\tSTANDARD\n" +
                        "rnd_symbol\trnd_symbol(iiii)\trnd_symbol(const int, const int, const int, const int)\tfalse\tSTANDARD\n" +
                        "rnd_symbol\trnd_symbol(V)\trnd_symbol(var var_arg)\tfalse\tSTANDARD\n" +
                        "rnd_timestamp\trnd_timestamp(nni)\trnd_timestamp(const timestamp, const timestamp, const int)\tfalse\tSTANDARD\n" +
                        "rnd_uuid4\trnd_uuid4()\trnd_uuid4()\tfalse\tSTANDARD\n" +
                        "round\tround(D)\tround(var double)\tfalse\tSTANDARD\n" +
                        "round\tround(DI)\tround(var double, var int)\tfalse\tSTANDARD\n" +
                        "round_down\tround_down(DI)\tround_down(var double, var int)\tfalse\tSTANDARD\n" +
                        "round_half_even\tround_half_even(DI)\tround_half_even(var double, var int)\tfalse\tSTANDARD\n" +
                        "round_up\tround_up(DI)\tround_up(var double, var int)\tfalse\tSTANDARD\n" +
                        "row_number\trow_number()\trow_number()\tfalse\tWINDOW\n" +
                        "rpad\trpad(SI)\trpad(var string, var int)\tfalse\tSTANDARD\n" +
                        "rpad\trpad(SIS)\trpad(var string, var int, var string)\tfalse\tSTANDARD\n" +
                        "rtrim\trtrim(S)\trtrim(var string)\tfalse\tSTANDARD\n" +
                        "second\tsecond(N)\tsecond(var timestamp)\tfalse\tSTANDARD\n" +
                        "session_user\tsession_user()\tsession_user()\ttrue\tSTANDARD\n" +
                        "simulate_crash\tsimulate_crash(a)\tsimulate_crash(const char)\tfalse\tSTANDARD\n" +
                        "sin\tsin(D)\tsin(var double)\tfalse\tSTANDARD\n" +
                        "size_pretty\tsize_pretty(L)\tsize_pretty(var long)\tfalse\tSTANDARD\n" +
                        "split_part\tsplit_part(SSI)\tsplit_part(var string, var string, var int)\tfalse\tSTANDARD\n" +
                        "split_part\tsplit_part(SAI)\tsplit_part(var string, var char, var int)\tfalse\tSTANDARD\n" +
                        "sqrt\tsqrt(D)\tsqrt(var double)\tfalse\tSTANDARD\n" +
                        "starts_with\tstarts_with(SS)\tstarts_with(var string, var string)\tfalse\tSTANDARD\n" +
                        "stddev_samp\tstddev_samp(D)\tstddev_samp(var double)\tfalse\tGROUP_BY\n" +
                        "string_agg\tstring_agg(Sa)\tstring_agg(var string, const char)\tfalse\tGROUP_BY\n" +
                        "strpos\tstrpos(SS)\tstrpos(var string, var string)\tfalse\tSTANDARD\n" +
                        "strpos\tstrpos(SA)\tstrpos(var string, var char)\tfalse\tSTANDARD\n" +
                        "substring\tsubstring(SII)\tsubstring(var string, var int, var int)\tfalse\tSTANDARD\n" +
                        "sum\tsum(D)\tsum(var double)\tfalse\tGROUP_BY\n" +
                        "sum\tsum(F)\tsum(var float)\tfalse\tGROUP_BY\n" +
                        "sum\tsum(I)\tsum(var int)\tfalse\tGROUP_BY\n" +
                        "sum\tsum(L)\tsum(var long)\tfalse\tGROUP_BY\n" +
                        "sum\tsum(H)\tsum(var long256)\tfalse\tGROUP_BY\n" +
                        "sum_t\tsum_t(D)\tsum_t(var double)\tfalse\tGROUP_BY\n" +
                        "sum_t\tsum_t(S)\tsum_t(var string)\tfalse\tGROUP_BY\n" +
                        "sumx\tsumx(DS)\tsumx(var double, var string)\tfalse\tGROUP_BY\n" +
                        "switch\tswitch(V)\tswitch(var var_arg)\tfalse\tSTANDARD\n" +
                        "sysdate\tsysdate()\tsysdate()\tfalse\tSTANDARD\n" +
                        "systimestamp\tsystimestamp()\tsystimestamp()\tfalse\tSTANDARD\n" +
                        "table_columns\ttable_columns(s)\ttable_columns(const string)\tfalse\tSTANDARD\n" +
                        "table_partitions\ttable_partitions(s)\ttable_partitions(const string)\tfalse\tCURSOR\n" +
                        "table_writer_metrics\ttable_writer_metrics()\ttable_writer_metrics()\tfalse\tSTANDARD\n" +
                        "tables\ttables()\ttables()\ttrue\tSTANDARD\n" +
                        "tan\ttan(D)\ttan(var double)\tfalse\tSTANDARD\n" +
                        "timestamp_ceil\ttimestamp_ceil(sN)\ttimestamp_ceil(const string, var timestamp)\tfalse\tSTANDARD\n" +
                        "timestamp_floor\ttimestamp_floor(sN)\ttimestamp_floor(const string, var timestamp)\tfalse\tSTANDARD\n" +
                        "timestamp_sequence\ttimestamp_sequence(NL)\ttimestamp_sequence(var timestamp, var long)\tfalse\tSTANDARD\n" +
                        "timestamp_shuffle\ttimestamp_shuffle(nn)\ttimestamp_shuffle(const timestamp, const timestamp)\tfalse\tSTANDARD\n" +
                        "to_char\tto_char(U)\tto_char(var binary)\tfalse\tSTANDARD\n" +
                        "to_date\tto_date(Ss)\tto_date(var string, const string)\tfalse\tSTANDARD\n" +
                        "to_long128\tto_long128(LL)\tto_long128(var long, var long)\tfalse\tSTANDARD\n" +
                        "to_lowercase\tto_lowercase(S)\tto_lowercase(var string)\tfalse\tSTANDARD\n" +
                        "to_pg_date\tto_pg_date(S)\tto_pg_date(var string)\tfalse\tSTANDARD\n" +
                        "to_str\tto_str(Ms)\tto_str(var date, const string)\tfalse\tSTANDARD\n" +
                        "to_str\tto_str(Ns)\tto_str(var timestamp, const string)\tfalse\tSTANDARD\n" +
                        "to_timestamp\tto_timestamp(Ss)\tto_timestamp(var string, const string)\tfalse\tSTANDARD\n" +
                        "to_timestamp\tto_timestamp(S)\tto_timestamp(var string)\tfalse\tSTANDARD\n" +
                        "to_timezone\tto_timezone(NS)\tto_timezone(var timestamp, var string)\tfalse\tSTANDARD\n" +
                        "to_uppercase\tto_uppercase(S)\tto_uppercase(var string)\tfalse\tSTANDARD\n" +
                        "to_utc\tto_utc(NS)\tto_utc(var timestamp, var string)\tfalse\tSTANDARD\n" +
                        "touch\ttouch(C)\ttouch(var cursor)\tfalse\tSTANDARD\n" +
                        "trim\ttrim(S)\ttrim(var string)\tfalse\tSTANDARD\n" +
                        "txid_current\ttxid_current()\ttxid_current()\tfalse\tSTANDARD\n" +
                        "typeOf\ttypeOf(V)\ttypeOf(var var_arg)\tfalse\tSTANDARD\n" +
                        "upper\tupper(S)\tupper(var string)\tfalse\tSTANDARD\n" +
                        "version\tversion()\tversion()\ttrue\tSTANDARD\n" +
                        "wal_tables\twal_tables()\twal_tables()\ttrue\tSTANDARD\n" +
                        "week_of_year\tweek_of_year(N)\tweek_of_year(var timestamp)\tfalse\tSTANDARD\n" +
                        "year\tyear(N)\tyear(var timestamp)\tfalse\tSTANDARD\n"
        );
    }

    @Test
    public void testFunctionsWithFilter() throws Exception {
        assertSql(
                "SELECT name, count(name) as overrides, type FROM functions() GROUP BY name, type ORDER BY name",
                "name\toverrides\ttype\n" +
                        "VARCHAR\t1\tSTANDARD\n" +
                        "abs\t4\tSTANDARD\n" +
                        "acos\t1\tSTANDARD\n" +
                        "all_tables\t1\tSTANDARD\n" +
                        "and\t1\tSTANDARD\n" +
                        "asin\t1\tSTANDARD\n" +
                        "atan\t1\tSTANDARD\n" +
                        "atan2\t1\tSTANDARD\n" +
                        "avg\t2\tGROUP_BY\n" +
                        "base64\t1\tSTANDARD\n" +
                        "between\t1\tSTANDARD\n" +
                        "build\t1\tSTANDARD\n" +
                        "case\t1\tSTANDARD\n" +
                        "cast\t178\tSTANDARD\n" +
                        "ceil\t2\tSTANDARD\n" +
                        "ceiling\t2\tSTANDARD\n" +
                        "coalesce\t1\tSTANDARD\n" +
                        "concat\t1\tSTANDARD\n" +
                        "cos\t1\tSTANDARD\n" +
                        "cot\t1\tSTANDARD\n" +
                        "count\t9\tGROUP_BY\n" +
                        "count_distinct\t6\tGROUP_BY\n" +
                        "current_database\t1\tSTANDARD\n" +
                        "current_schema\t1\tSTANDARD\n" +
                        "current_schemas\t1\tSTANDARD\n" +
                        "current_user\t1\tSTANDARD\n" +
                        "date_trunc\t1\tSTANDARD\n" +
                        "dateadd\t1\tSTANDARD\n" +
                        "datediff\t1\tSTANDARD\n" +
                        "day\t1\tSTANDARD\n" +
                        "day_of_week\t1\tSTANDARD\n" +
                        "day_of_week_sunday_first\t1\tSTANDARD\n" +
                        "days_in_month\t1\tSTANDARD\n" +
                        "degrees\t1\tSTANDARD\n" +
                        "dump_memory_usage\t1\tSTANDARD\n" +
                        "dump_thread_stacks\t1\tSTANDARD\n" +
                        "extract\t1\tSTANDARD\n" +
                        "first\t12\tGROUP_BY\n" +
                        "floor\t2\tSTANDARD\n" +
                        "flush_query_cache\t1\tSTANDARD\n" +
                        "format_type\t1\tSTANDARD\n" +
                        "functions\t1\tSTANDARD\n" +
                        "haversine_dist_deg\t1\tGROUP_BY\n" +
                        "hour\t1\tSTANDARD\n" +
                        "ilike\t1\tSTANDARD\n" +
                        "in\t7\tSTANDARD\n" +
                        "information_schema._pg_expandarray\t1\tCURSOR\n" +
                        "isOrdered\t1\tGROUP_BY\n" +
                        "is_leap_year\t1\tSTANDARD\n" +
                        "keywords\t1\tSTANDARD\n" +
                        "ksum\t1\tGROUP_BY\n" +
                        "last\t12\tGROUP_BY\n" +
                        "left\t1\tSTANDARD\n" +
                        "length\t3\tSTANDARD\n" +
                        "like\t1\tSTANDARD\n" +
                        "list\t1\tSTANDARD\n" +
                        "ln\t1\tSTANDARD\n" +
                        "log\t1\tSTANDARD\n" +
                        "long_sequence\t1\tSTANDARD\n" +
                        "lower\t1\tSTANDARD\n" +
                        "lpad\t2\tSTANDARD\n" +
                        "ltrim\t1\tSTANDARD\n" +
                        "make_geohash\t1\tSTANDARD\n" +
                        "max\t9\tGROUP_BY\n" +
                        "memory_metrics\t1\tSTANDARD\n" +
                        "micros\t1\tSTANDARD\n" +
                        "millis\t1\tSTANDARD\n" +
                        "min\t9\tGROUP_BY\n" +
                        "minute\t1\tSTANDARD\n" +
                        "month\t1\tSTANDARD\n" +
                        "not\t1\tSTANDARD\n" +
                        "now\t1\tSTANDARD\n" +
                        "npe\t1\tSTANDARD\n" +
                        "nsum\t1\tGROUP_BY\n" +
                        "nullif\t4\tSTANDARD\n" +
                        "or\t1\tSTANDARD\n" +
                        "pg_advisory_unlock_all\t1\tSTANDARD\n" +
                        "pg_attrdef\t1\tSTANDARD\n" +
                        "pg_attribute\t1\tSTANDARD\n" +
                        "pg_catalog.age\t1\tSTANDARD\n" +
                        "pg_catalog.current_database\t1\tSTANDARD\n" +
                        "pg_catalog.current_schema\t1\tSTANDARD\n" +
                        "pg_catalog.current_schemas\t1\tSTANDARD\n" +
                        "pg_catalog.pg_attrdef\t1\tSTANDARD\n" +
                        "pg_catalog.pg_attribute\t1\tSTANDARD\n" +
                        "pg_catalog.pg_class\t1\tCURSOR\n" +
                        "pg_catalog.pg_database\t1\tCURSOR\n" +
                        "pg_catalog.pg_description\t1\tCURSOR\n" +
                        "pg_catalog.pg_get_expr\t2\tSTANDARD\n" +
                        "pg_catalog.pg_get_keywords\t1\tSTANDARD\n" +
                        "pg_catalog.pg_get_partkeydef\t1\tSTANDARD\n" +
                        "pg_catalog.pg_get_userbyid\t1\tSTANDARD\n" +
                        "pg_catalog.pg_index\t1\tCURSOR\n" +
                        "pg_catalog.pg_inherits\t1\tCURSOR\n" +
                        "pg_catalog.pg_is_in_recovery\t1\tSTANDARD\n" +
                        "pg_catalog.pg_locks\t1\tCURSOR\n" +
                        "pg_catalog.pg_namespace\t1\tSTANDARD\n" +
                        "pg_catalog.pg_roles\t1\tCURSOR\n" +
                        "pg_catalog.pg_shdescription\t1\tCURSOR\n" +
                        "pg_catalog.pg_table_is_visible\t1\tSTANDARD\n" +
                        "pg_catalog.pg_type\t1\tCURSOR\n" +
                        "pg_catalog.txid_current\t1\tSTANDARD\n" +
                        "pg_catalog.version\t1\tSTANDARD\n" +
                        "pg_class\t1\tCURSOR\n" +
                        "pg_database\t1\tCURSOR\n" +
                        "pg_description\t1\tCURSOR\n" +
                        "pg_get_expr\t2\tSTANDARD\n" +
                        "pg_get_keywords\t1\tSTANDARD\n" +
                        "pg_get_partkeydef\t1\tSTANDARD\n" +
                        "pg_index\t1\tCURSOR\n" +
                        "pg_inherits\t1\tCURSOR\n" +
                        "pg_is_in_recovery\t1\tSTANDARD\n" +
                        "pg_locks\t1\tCURSOR\n" +
                        "pg_namespace\t1\tSTANDARD\n" +
                        "pg_postmaster_start_time\t1\tSTANDARD\n" +
                        "pg_proc\t1\tCURSOR\n" +
                        "pg_range\t1\tCURSOR\n" +
                        "pg_roles\t1\tCURSOR\n" +
                        "pg_type\t1\tCURSOR\n" +
                        "pi\t1\tSTANDARD\n" +
                        "position\t1\tSTANDARD\n" +
                        "power\t1\tSTANDARD\n" +
                        "radians\t1\tSTANDARD\n" +
                        "rank\t1\tWINDOW\n" +
                        "reader_pool\t1\tSTANDARD\n" +
                        "regexp_replace\t1\tSTANDARD\n" +
                        "replace\t1\tSTANDARD\n" +
                        "right\t1\tSTANDARD\n" +
                        "rnd_bin\t2\tSTANDARD\n" +
                        "rnd_boolean\t1\tSTANDARD\n" +
                        "rnd_byte\t2\tSTANDARD\n" +
                        "rnd_char\t1\tSTANDARD\n" +
                        "rnd_date\t2\tSTANDARD\n" +
                        "rnd_double\t2\tSTANDARD\n" +
                        "rnd_float\t2\tSTANDARD\n" +
                        "rnd_geohash\t1\tSTANDARD\n" +
                        "rnd_int\t2\tSTANDARD\n" +
                        "rnd_log\t1\tSTANDARD\n" +
                        "rnd_long\t2\tSTANDARD\n" +
                        "rnd_long256\t2\tSTANDARD\n" +
                        "rnd_short\t2\tSTANDARD\n" +
                        "rnd_str\t3\tSTANDARD\n" +
                        "rnd_symbol\t2\tSTANDARD\n" +
                        "rnd_timestamp\t1\tSTANDARD\n" +
                        "rnd_uuid4\t1\tSTANDARD\n" +
                        "round\t2\tSTANDARD\n" +
                        "round_down\t1\tSTANDARD\n" +
                        "round_half_even\t1\tSTANDARD\n" +
                        "round_up\t1\tSTANDARD\n" +
                        "row_number\t1\tWINDOW\n" +
                        "rpad\t2\tSTANDARD\n" +
                        "rtrim\t1\tSTANDARD\n" +
                        "second\t1\tSTANDARD\n" +
                        "session_user\t1\tSTANDARD\n" +
                        "simulate_crash\t1\tSTANDARD\n" +
                        "sin\t1\tSTANDARD\n" +
                        "size_pretty\t1\tSTANDARD\n" +
                        "split_part\t2\tSTANDARD\n" +
                        "sqrt\t1\tSTANDARD\n" +
                        "starts_with\t1\tSTANDARD\n" +
                        "stddev_samp\t1\tGROUP_BY\n" +
                        "string_agg\t1\tGROUP_BY\n" +
                        "strpos\t2\tSTANDARD\n" +
                        "substring\t1\tSTANDARD\n" +
                        "sum\t5\tGROUP_BY\n" +
                        "sum_t\t2\tGROUP_BY\n" +
                        "sumx\t1\tGROUP_BY\n" +
                        "switch\t1\tSTANDARD\n" +
                        "sysdate\t1\tSTANDARD\n" +
                        "systimestamp\t1\tSTANDARD\n" +
                        "table_columns\t1\tSTANDARD\n" +
                        "table_partitions\t1\tCURSOR\n" +
                        "table_writer_metrics\t1\tSTANDARD\n" +
                        "tables\t1\tSTANDARD\n" +
                        "tan\t1\tSTANDARD\n" +
                        "timestamp_ceil\t1\tSTANDARD\n" +
                        "timestamp_floor\t1\tSTANDARD\n" +
                        "timestamp_sequence\t1\tSTANDARD\n" +
                        "timestamp_shuffle\t1\tSTANDARD\n" +
                        "to_char\t1\tSTANDARD\n" +
                        "to_date\t1\tSTANDARD\n" +
                        "to_long128\t1\tSTANDARD\n" +
                        "to_lowercase\t1\tSTANDARD\n" +
                        "to_pg_date\t1\tSTANDARD\n" +
                        "to_str\t2\tSTANDARD\n" +
                        "to_timestamp\t2\tSTANDARD\n" +
                        "to_timezone\t1\tSTANDARD\n" +
                        "to_uppercase\t1\tSTANDARD\n" +
                        "to_utc\t1\tSTANDARD\n" +
                        "touch\t1\tSTANDARD\n" +
                        "trim\t1\tSTANDARD\n" +
                        "txid_current\t1\tSTANDARD\n" +
                        "typeOf\t1\tSTANDARD\n" +
                        "upper\t1\tSTANDARD\n" +
                        "version\t1\tSTANDARD\n" +
                        "wal_tables\t1\tSTANDARD\n" +
                        "week_of_year\t1\tSTANDARD\n" +
                        "year\t1\tSTANDARD\n"
        );
    }
}
