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
                "name\tsignature\tsignature_translated\ttype\n" +
                        "VARCHAR\tVARCHAR(I)\tVARCHAR(var int)\tSTANDARD\n" +
                        "abs\tabs(I)\tabs(var int)\tSTANDARD\n" +
                        "abs\tabs(E)\tabs(var short)\tSTANDARD\n" +
                        "abs\tabs(L)\tabs(var long)\tSTANDARD\n" +
                        "abs\tabs(D)\tabs(var double)\tSTANDARD\n" +
                        "acos\tacos(D)\tacos(var double)\tSTANDARD\n" +
                        "all_tables\tall_tables()\tall_tables()\tSTANDARD\n" +
                        "and\tand(TT)\tand(var boolean, var boolean)\tSTANDARD\n" +
                        "asin\tasin(D)\tasin(var double)\tSTANDARD\n" +
                        "atan\tatan(D)\tatan(var double)\tSTANDARD\n" +
                        "atan2\tatan2(DD)\tatan2(var double, var double)\tSTANDARD\n" +
                        "avg\tavg(D)\tavg(var double)\tGROUP_BY\n" +
                        "avg\tavg(T)\tavg(var boolean)\tGROUP_BY\n" +
                        "base64\tbase64(Ui)\tbase64(var binary, const int)\tSTANDARD\n" +
                        "between\tbetween(NNN)\tbetween(var timestamp, var timestamp, var timestamp)\tSTANDARD\n" +
                        "build\tbuild()\tbuild()\tSTANDARD\n" +
                        "case\tcase(V)\tcase(var var_arg)\tSTANDARD\n" +
                        "cast\tcast(Dt)\tcast(var double, const boolean)\tSTANDARD\n" +
                        "cast\tcast(Di)\tcast(var double, const int)\tSTANDARD\n" +
                        "cast\tcast(Dd)\tcast(var double, const double)\tSTANDARD\n" +
                        "cast\tcast(Df)\tcast(var double, const float)\tSTANDARD\n" +
                        "cast\tcast(Dh)\tcast(var double, const long256)\tSTANDARD\n" +
                        "cast\tcast(Dl)\tcast(var double, const long)\tSTANDARD\n" +
                        "cast\tcast(De)\tcast(var double, const short)\tSTANDARD\n" +
                        "cast\tcast(Db)\tcast(var double, const byte)\tSTANDARD\n" +
                        "cast\tcast(Ds)\tcast(var double, const string)\tSTANDARD\n" +
                        "cast\tcast(Dk)\tcast(var double, const symbol)\tSTANDARD\n" +
                        "cast\tcast(Da)\tcast(var double, const char)\tSTANDARD\n" +
                        "cast\tcast(Dm)\tcast(var double, const date)\tSTANDARD\n" +
                        "cast\tcast(Dn)\tcast(var double, const timestamp)\tSTANDARD\n" +
                        "cast\tcast(Ft)\tcast(var float, const boolean)\tSTANDARD\n" +
                        "cast\tcast(Fi)\tcast(var float, const int)\tSTANDARD\n" +
                        "cast\tcast(Fd)\tcast(var float, const double)\tSTANDARD\n" +
                        "cast\tcast(Ff)\tcast(var float, const float)\tSTANDARD\n" +
                        "cast\tcast(Fh)\tcast(var float, const long256)\tSTANDARD\n" +
                        "cast\tcast(Fl)\tcast(var float, const long)\tSTANDARD\n" +
                        "cast\tcast(Fe)\tcast(var float, const short)\tSTANDARD\n" +
                        "cast\tcast(Fb)\tcast(var float, const byte)\tSTANDARD\n" +
                        "cast\tcast(Fs)\tcast(var float, const string)\tSTANDARD\n" +
                        "cast\tcast(Fk)\tcast(var float, const symbol)\tSTANDARD\n" +
                        "cast\tcast(Fa)\tcast(var float, const char)\tSTANDARD\n" +
                        "cast\tcast(Fm)\tcast(var float, const date)\tSTANDARD\n" +
                        "cast\tcast(Fn)\tcast(var float, const timestamp)\tSTANDARD\n" +
                        "cast\tcast(Ee)\tcast(var short, const short)\tSTANDARD\n" +
                        "cast\tcast(Eb)\tcast(var short, const byte)\tSTANDARD\n" +
                        "cast\tcast(Ea)\tcast(var short, const char)\tSTANDARD\n" +
                        "cast\tcast(Ei)\tcast(var short, const int)\tSTANDARD\n" +
                        "cast\tcast(El)\tcast(var short, const long)\tSTANDARD\n" +
                        "cast\tcast(Ef)\tcast(var short, const float)\tSTANDARD\n" +
                        "cast\tcast(Ed)\tcast(var short, const double)\tSTANDARD\n" +
                        "cast\tcast(Es)\tcast(var short, const string)\tSTANDARD\n" +
                        "cast\tcast(Em)\tcast(var short, const date)\tSTANDARD\n" +
                        "cast\tcast(En)\tcast(var short, const timestamp)\tSTANDARD\n" +
                        "cast\tcast(Ek)\tcast(var short, const symbol)\tSTANDARD\n" +
                        "cast\tcast(Eh)\tcast(var short, const long256)\tSTANDARD\n" +
                        "cast\tcast(Et)\tcast(var short, const boolean)\tSTANDARD\n" +
                        "cast\tcast(Ie)\tcast(var int, const short)\tSTANDARD\n" +
                        "cast\tcast(Ib)\tcast(var int, const byte)\tSTANDARD\n" +
                        "cast\tcast(Ia)\tcast(var int, const char)\tSTANDARD\n" +
                        "cast\tcast(Ii)\tcast(var int, const int)\tSTANDARD\n" +
                        "cast\tcast(Il)\tcast(var int, const long)\tSTANDARD\n" +
                        "cast\tcast(If)\tcast(var int, const float)\tSTANDARD\n" +
                        "cast\tcast(Id)\tcast(var int, const double)\tSTANDARD\n" +
                        "cast\tcast(Is)\tcast(var int, const string)\tSTANDARD\n" +
                        "cast\tcast(Im)\tcast(var int, const date)\tSTANDARD\n" +
                        "cast\tcast(In)\tcast(var int, const timestamp)\tSTANDARD\n" +
                        "cast\tcast(Ik)\tcast(var int, const symbol)\tSTANDARD\n" +
                        "cast\tcast(Ih)\tcast(var int, const long256)\tSTANDARD\n" +
                        "cast\tcast(It)\tcast(var int, const boolean)\tSTANDARD\n" +
                        "cast\tcast(Le)\tcast(var long, const short)\tSTANDARD\n" +
                        "cast\tcast(Lb)\tcast(var long, const byte)\tSTANDARD\n" +
                        "cast\tcast(La)\tcast(var long, const char)\tSTANDARD\n" +
                        "cast\tcast(Li)\tcast(var long, const int)\tSTANDARD\n" +
                        "cast\tcast(Ll)\tcast(var long, const long)\tSTANDARD\n" +
                        "cast\tcast(Lf)\tcast(var long, const float)\tSTANDARD\n" +
                        "cast\tcast(Ld)\tcast(var long, const double)\tSTANDARD\n" +
                        "cast\tcast(Ls)\tcast(var long, const string)\tSTANDARD\n" +
                        "cast\tcast(Lm)\tcast(var long, const date)\tSTANDARD\n" +
                        "cast\tcast(Ln)\tcast(var long, const timestamp)\tSTANDARD\n" +
                        "cast\tcast(oV)\tcast(const null, var var_arg)\tSTANDARD\n" +
                        "cast\tcast(Lk)\tcast(var long, const symbol)\tSTANDARD\n" +
                        "cast\tcast(Lh)\tcast(var long, const long256)\tSTANDARD\n" +
                        "cast\tcast(Lt)\tcast(var long, const boolean)\tSTANDARD\n" +
                        "cast\tcast(Lg)\tcast(var long, const geohash)\tSTANDARD\n" +
                        "cast\tcast(He)\tcast(var long256, const short)\tSTANDARD\n" +
                        "cast\tcast(Hb)\tcast(var long256, const byte)\tSTANDARD\n" +
                        "cast\tcast(Ha)\tcast(var long256, const char)\tSTANDARD\n" +
                        "cast\tcast(Hi)\tcast(var long256, const int)\tSTANDARD\n" +
                        "cast\tcast(Hl)\tcast(var long256, const long)\tSTANDARD\n" +
                        "cast\tcast(Hf)\tcast(var long256, const float)\tSTANDARD\n" +
                        "cast\tcast(Hd)\tcast(var long256, const double)\tSTANDARD\n" +
                        "cast\tcast(Hs)\tcast(var long256, const string)\tSTANDARD\n" +
                        "cast\tcast(Hm)\tcast(var long256, const date)\tSTANDARD\n" +
                        "cast\tcast(Hn)\tcast(var long256, const timestamp)\tSTANDARD\n" +
                        "cast\tcast(Hk)\tcast(var long256, const symbol)\tSTANDARD\n" +
                        "cast\tcast(Hl)\tcast(var long256, const long)\tSTANDARD\n" +
                        "cast\tcast(Ht)\tcast(var long256, const boolean)\tSTANDARD\n" +
                        "cast\tcast(Me)\tcast(var date, const short)\tSTANDARD\n" +
                        "cast\tcast(Mb)\tcast(var date, const byte)\tSTANDARD\n" +
                        "cast\tcast(Ma)\tcast(var date, const char)\tSTANDARD\n" +
                        "cast\tcast(Mi)\tcast(var date, const int)\tSTANDARD\n" +
                        "cast\tcast(Ml)\tcast(var date, const long)\tSTANDARD\n" +
                        "cast\tcast(Mf)\tcast(var date, const float)\tSTANDARD\n" +
                        "cast\tcast(Md)\tcast(var date, const double)\tSTANDARD\n" +
                        "cast\tcast(Ms)\tcast(var date, const string)\tSTANDARD\n" +
                        "cast\tcast(Mm)\tcast(var date, const date)\tSTANDARD\n" +
                        "cast\tcast(Mn)\tcast(var date, const timestamp)\tSTANDARD\n" +
                        "cast\tcast(Mk)\tcast(var date, const symbol)\tSTANDARD\n" +
                        "cast\tcast(Mh)\tcast(var date, const long256)\tSTANDARD\n" +
                        "cast\tcast(Mt)\tcast(var date, const boolean)\tSTANDARD\n" +
                        "cast\tcast(Ne)\tcast(var timestamp, const short)\tSTANDARD\n" +
                        "cast\tcast(Nb)\tcast(var timestamp, const byte)\tSTANDARD\n" +
                        "cast\tcast(Na)\tcast(var timestamp, const char)\tSTANDARD\n" +
                        "cast\tcast(Ni)\tcast(var timestamp, const int)\tSTANDARD\n" +
                        "cast\tcast(Nl)\tcast(var timestamp, const long)\tSTANDARD\n" +
                        "cast\tcast(Nf)\tcast(var timestamp, const float)\tSTANDARD\n" +
                        "cast\tcast(Nd)\tcast(var timestamp, const double)\tSTANDARD\n" +
                        "cast\tcast(Ns)\tcast(var timestamp, const string)\tSTANDARD\n" +
                        "cast\tcast(Nm)\tcast(var timestamp, const date)\tSTANDARD\n" +
                        "cast\tcast(Nn)\tcast(var timestamp, const timestamp)\tSTANDARD\n" +
                        "cast\tcast(Nk)\tcast(var timestamp, const symbol)\tSTANDARD\n" +
                        "cast\tcast(Nh)\tcast(var timestamp, const long256)\tSTANDARD\n" +
                        "cast\tcast(Nt)\tcast(var timestamp, const boolean)\tSTANDARD\n" +
                        "cast\tcast(Be)\tcast(var byte, const short)\tSTANDARD\n" +
                        "cast\tcast(Bb)\tcast(var byte, const byte)\tSTANDARD\n" +
                        "cast\tcast(Ba)\tcast(var byte, const char)\tSTANDARD\n" +
                        "cast\tcast(Bi)\tcast(var byte, const int)\tSTANDARD\n" +
                        "cast\tcast(Bl)\tcast(var byte, const long)\tSTANDARD\n" +
                        "cast\tcast(Bf)\tcast(var byte, const float)\tSTANDARD\n" +
                        "cast\tcast(Bd)\tcast(var byte, const double)\tSTANDARD\n" +
                        "cast\tcast(Bs)\tcast(var byte, const string)\tSTANDARD\n" +
                        "cast\tcast(Bm)\tcast(var byte, const date)\tSTANDARD\n" +
                        "cast\tcast(Bn)\tcast(var byte, const timestamp)\tSTANDARD\n" +
                        "cast\tcast(Bk)\tcast(var byte, const symbol)\tSTANDARD\n" +
                        "cast\tcast(Bh)\tcast(var byte, const long256)\tSTANDARD\n" +
                        "cast\tcast(Bt)\tcast(var byte, const boolean)\tSTANDARD\n" +
                        "cast\tcast(Te)\tcast(var boolean, const short)\tSTANDARD\n" +
                        "cast\tcast(Tb)\tcast(var boolean, const byte)\tSTANDARD\n" +
                        "cast\tcast(Ta)\tcast(var boolean, const char)\tSTANDARD\n" +
                        "cast\tcast(Ti)\tcast(var boolean, const int)\tSTANDARD\n" +
                        "cast\tcast(Tl)\tcast(var boolean, const long)\tSTANDARD\n" +
                        "cast\tcast(Tf)\tcast(var boolean, const float)\tSTANDARD\n" +
                        "cast\tcast(Td)\tcast(var boolean, const double)\tSTANDARD\n" +
                        "cast\tcast(Ts)\tcast(var boolean, const string)\tSTANDARD\n" +
                        "cast\tcast(Tm)\tcast(var boolean, const date)\tSTANDARD\n" +
                        "cast\tcast(Tn)\tcast(var boolean, const timestamp)\tSTANDARD\n" +
                        "cast\tcast(Tk)\tcast(var boolean, const symbol)\tSTANDARD\n" +
                        "cast\tcast(Th)\tcast(var boolean, const long256)\tSTANDARD\n" +
                        "cast\tcast(Tt)\tcast(var boolean, const boolean)\tSTANDARD\n" +
                        "cast\tcast(Ae)\tcast(var char, const short)\tSTANDARD\n" +
                        "cast\tcast(Ab)\tcast(var char, const byte)\tSTANDARD\n" +
                        "cast\tcast(Aa)\tcast(var char, const char)\tSTANDARD\n" +
                        "cast\tcast(Ai)\tcast(var char, const int)\tSTANDARD\n" +
                        "cast\tcast(Al)\tcast(var char, const long)\tSTANDARD\n" +
                        "cast\tcast(Af)\tcast(var char, const float)\tSTANDARD\n" +
                        "cast\tcast(Ad)\tcast(var char, const double)\tSTANDARD\n" +
                        "cast\tcast(As)\tcast(var char, const string)\tSTANDARD\n" +
                        "cast\tcast(Am)\tcast(var char, const date)\tSTANDARD\n" +
                        "cast\tcast(Ak)\tcast(var char, const symbol)\tSTANDARD\n" +
                        "cast\tcast(Ah)\tcast(var char, const long256)\tSTANDARD\n" +
                        "cast\tcast(An)\tcast(var char, const timestamp)\tSTANDARD\n" +
                        "cast\tcast(Si)\tcast(var string, const int)\tSTANDARD\n" +
                        "cast\tcast(Sd)\tcast(var string, const double)\tSTANDARD\n" +
                        "cast\tcast(At)\tcast(var char, const boolean)\tSTANDARD\n" +
                        "cast\tcast(St)\tcast(var string, const boolean)\tSTANDARD\n" +
                        "cast\tcast(Sf)\tcast(var string, const float)\tSTANDARD\n" +
                        "cast\tcast(Sh)\tcast(var string, const long256)\tSTANDARD\n" +
                        "cast\tcast(Sl)\tcast(var string, const long)\tSTANDARD\n" +
                        "cast\tcast(Sp)\tcast(var string, const reg_class)\tSTANDARD\n" +
                        "cast\tcast(Sq)\tcast(var string, const reg_procedure)\tSTANDARD\n" +
                        "cast\tcast(Sw)\tcast(var string, const array_string)\tSTANDARD\n" +
                        "cast\tcast(Se)\tcast(var string, const short)\tSTANDARD\n" +
                        "cast\tcast(Sb)\tcast(var string, const byte)\tSTANDARD\n" +
                        "cast\tcast(Ss)\tcast(var string, const string)\tSTANDARD\n" +
                        "cast\tcast(Sk)\tcast(var string, const symbol)\tSTANDARD\n" +
                        "cast\tcast(Sa)\tcast(var string, const char)\tSTANDARD\n" +
                        "cast\tcast(Sm)\tcast(var string, const date)\tSTANDARD\n" +
                        "cast\tcast(Sn)\tcast(var string, const timestamp)\tSTANDARD\n" +
                        "cast\tcast(Su)\tcast(var string, const binary)\tSTANDARD\n" +
                        "cast\tcast(Sg)\tcast(var string, const geohash)\tSTANDARD\n" +
                        "cast\tcast(Gg)\tcast(var geohash, const geohash)\tSTANDARD\n" +
                        "cast\tcast(Sz)\tcast(var string, const uuid)\tSTANDARD\n" +
                        "cast\tcast(Zs)\tcast(var uuid, const string)\tSTANDARD\n" +
                        "cast\tcast(Ki)\tcast(var symbol, const int)\tSTANDARD\n" +
                        "cast\tcast(Kd)\tcast(var symbol, const double)\tSTANDARD\n" +
                        "cast\tcast(Kf)\tcast(var symbol, const float)\tSTANDARD\n" +
                        "cast\tcast(Kh)\tcast(var symbol, const long256)\tSTANDARD\n" +
                        "cast\tcast(Kl)\tcast(var symbol, const long)\tSTANDARD\n" +
                        "cast\tcast(Ke)\tcast(var symbol, const short)\tSTANDARD\n" +
                        "cast\tcast(Kb)\tcast(var symbol, const byte)\tSTANDARD\n" +
                        "cast\tcast(Ks)\tcast(var symbol, const string)\tSTANDARD\n" +
                        "cast\tcast(Kk)\tcast(var symbol, const symbol)\tSTANDARD\n" +
                        "cast\tcast(Ka)\tcast(var symbol, const char)\tSTANDARD\n" +
                        "cast\tcast(Km)\tcast(var symbol, const date)\tSTANDARD\n" +
                        "cast\tcast(Kn)\tcast(var symbol, const timestamp)\tSTANDARD\n" +
                        "ceil\tceil(D)\tceil(var double)\tSTANDARD\n" +
                        "ceil\tceil(F)\tceil(var float)\tSTANDARD\n" +
                        "ceiling\tceiling(D)\tceiling(var double)\tSTANDARD\n" +
                        "ceiling\tceiling(F)\tceiling(var float)\tSTANDARD\n" +
                        "coalesce\tcoalesce(V)\tcoalesce(var var_arg)\tSTANDARD\n" +
                        "concat\tconcat(V)\tconcat(var var_arg)\tSTANDARD\n" +
                        "cos\tcos(D)\tcos(var double)\tSTANDARD\n" +
                        "cot\tcot(D)\tcot(var double)\tSTANDARD\n" +
                        "count\tcount()\tcount()\tGROUP_BY\n" +
                        "count\tcount(D)\tcount(var double)\tGROUP_BY\n" +
                        "count\tcount(F)\tcount(var float)\tGROUP_BY\n" +
                        "count\tcount(G)\tcount(var geohash)\tGROUP_BY\n" +
                        "count\tcount(I)\tcount(var int)\tGROUP_BY\n" +
                        "count\tcount(L)\tcount(var long)\tGROUP_BY\n" +
                        "count\tcount(H)\tcount(var long256)\tGROUP_BY\n" +
                        "count\tcount(S)\tcount(var string)\tGROUP_BY\n" +
                        "count\tcount(K)\tcount(var symbol)\tGROUP_BY\n" +
                        "count_distinct\tcount_distinct(S)\tcount_distinct(var string)\tGROUP_BY\n" +
                        "count_distinct\tcount_distinct(K)\tcount_distinct(var symbol)\tGROUP_BY\n" +
                        "count_distinct\tcount_distinct(H)\tcount_distinct(var long256)\tGROUP_BY\n" +
                        "count_distinct\tcount_distinct(L)\tcount_distinct(var long)\tGROUP_BY\n" +
                        "count_distinct\tcount_distinct(I)\tcount_distinct(var int)\tGROUP_BY\n" +
                        "count_distinct\tcount_distinct(Z)\tcount_distinct(var uuid)\tGROUP_BY\n" +
                        "current_database\tcurrent_database()\tcurrent_database()\tSTANDARD\n" +
                        "current_schema\tcurrent_schema()\tcurrent_schema()\tSTANDARD\n" +
                        "current_schemas\tcurrent_schemas(T)\tcurrent_schemas(var boolean)\tSTANDARD\n" +
                        "current_user\tcurrent_user()\tcurrent_user()\tSTANDARD\n" +
                        "date_trunc\tdate_trunc(sN)\tdate_trunc(const string, var timestamp)\tSTANDARD\n" +
                        "dateadd\tdateadd(AIN)\tdateadd(var char, var int, var timestamp)\tSTANDARD\n" +
                        "datediff\tdatediff(ANN)\tdatediff(var char, var timestamp, var timestamp)\tSTANDARD\n" +
                        "day\tday(N)\tday(var timestamp)\tSTANDARD\n" +
                        "day_of_week\tday_of_week(N)\tday_of_week(var timestamp)\tSTANDARD\n" +
                        "day_of_week_sunday_first\tday_of_week_sunday_first(N)\tday_of_week_sunday_first(var timestamp)\tSTANDARD\n" +
                        "days_in_month\tdays_in_month(N)\tdays_in_month(var timestamp)\tSTANDARD\n" +
                        "degrees\tdegrees(D)\tdegrees(var double)\tSTANDARD\n" +
                        "dump_memory_usage\tdump_memory_usage()\tdump_memory_usage()\tSTANDARD\n" +
                        "dump_thread_stacks\tdump_thread_stacks()\tdump_thread_stacks()\tSTANDARD\n" +
                        "extract\textract(sN)\textract(const string, var timestamp)\tSTANDARD\n" +
                        "first\tfirst(D)\tfirst(var double)\tGROUP_BY\n" +
                        "first\tfirst(F)\tfirst(var float)\tGROUP_BY\n" +
                        "first\tfirst(I)\tfirst(var int)\tGROUP_BY\n" +
                        "first\tfirst(A)\tfirst(var char)\tGROUP_BY\n" +
                        "first\tfirst(E)\tfirst(var short)\tGROUP_BY\n" +
                        "first\tfirst(B)\tfirst(var byte)\tGROUP_BY\n" +
                        "first\tfirst(N)\tfirst(var timestamp)\tGROUP_BY\n" +
                        "first\tfirst(L)\tfirst(var long)\tGROUP_BY\n" +
                        "first\tfirst(M)\tfirst(var date)\tGROUP_BY\n" +
                        "first\tfirst(G)\tfirst(var geohash)\tGROUP_BY\n" +
                        "first\tfirst(Z)\tfirst(var uuid)\tGROUP_BY\n" +
                        "first\tfirst(K)\tfirst(var symbol)\tGROUP_BY\n" +
                        "floor\tfloor(D)\tfloor(var double)\tSTANDARD\n" +
                        "floor\tfloor(F)\tfloor(var float)\tSTANDARD\n" +
                        "flush_query_cache\tflush_query_cache()\tflush_query_cache()\tSTANDARD\n" +
                        "format_type\tformat_type(II)\tformat_type(var int, var int)\tSTANDARD\n" +
                        "functions\tfunctions()\tfunctions()\tSTANDARD\n" +
                        "haversine_dist_deg\thaversine_dist_deg(DDN)\thaversine_dist_deg(var double, var double, var timestamp)\tGROUP_BY\n" +
                        "hour\thour(N)\thour(var timestamp)\tSTANDARD\n" +
                        "ilike\tilike(SS)\tilike(var string, var string)\tSTANDARD\n" +
                        "in\tin(KC)\tin(var symbol, var cursor)\tSTANDARD\n" +
                        "in\tin(Sv)\tin(var string, const var_arg)\tSTANDARD\n" +
                        "in\tin(Av)\tin(var char, const var_arg)\tSTANDARD\n" +
                        "in\tin(Kv)\tin(var symbol, const var_arg)\tSTANDARD\n" +
                        "in\tin(NS)\tin(var timestamp, var string)\tSTANDARD\n" +
                        "in\tin(NV)\tin(var timestamp, var var_arg)\tSTANDARD\n" +
                        "in\tin(Zv)\tin(var uuid, const var_arg)\tSTANDARD\n" +
                        "information_schema._pg_expandarray\tinformation_schema._pg_expandarray(V)\tinformation_schema._pg_expandarray(var var_arg)\tCURSOR\n" +
                        "isOrdered\tisOrdered(L)\tisOrdered(var long)\tGROUP_BY\n" +
                        "is_leap_year\tis_leap_year(N)\tis_leap_year(var timestamp)\tSTANDARD\n" +
                        "keywords\tkeywords()\tkeywords()\tSTANDARD\n" +
                        "ksum\tksum(D)\tksum(var double)\tGROUP_BY\n" +
                        "last\tlast(D)\tlast(var double)\tGROUP_BY\n" +
                        "last\tlast(F)\tlast(var float)\tGROUP_BY\n" +
                        "last\tlast(I)\tlast(var int)\tGROUP_BY\n" +
                        "last\tlast(A)\tlast(var char)\tGROUP_BY\n" +
                        "last\tlast(E)\tlast(var short)\tGROUP_BY\n" +
                        "last\tlast(B)\tlast(var byte)\tGROUP_BY\n" +
                        "last\tlast(K)\tlast(var symbol)\tGROUP_BY\n" +
                        "last\tlast(N)\tlast(var timestamp)\tGROUP_BY\n" +
                        "last\tlast(M)\tlast(var date)\tGROUP_BY\n" +
                        "last\tlast(L)\tlast(var long)\tGROUP_BY\n" +
                        "last\tlast(G)\tlast(var geohash)\tGROUP_BY\n" +
                        "last\tlast(Z)\tlast(var uuid)\tGROUP_BY\n" +
                        "left\tleft(SI)\tleft(var string, var int)\tSTANDARD\n" +
                        "length\tlength(S)\tlength(var string)\tSTANDARD\n" +
                        "length\tlength(K)\tlength(var symbol)\tSTANDARD\n" +
                        "length\tlength(U)\tlength(var binary)\tSTANDARD\n" +
                        "like\tlike(SS)\tlike(var string, var string)\tSTANDARD\n" +
                        "list\tlist(V)\tlist(var var_arg)\tSTANDARD\n" +
                        "ln\tln(D)\tln(var double)\tSTANDARD\n" +
                        "log\tlog(D)\tlog(var double)\tSTANDARD\n" +
                        "long_sequence\tlong_sequence(v)\tlong_sequence(const var_arg)\tSTANDARD\n" +
                        "lower\tlower(S)\tlower(var string)\tSTANDARD\n" +
                        "lpad\tlpad(SI)\tlpad(var string, var int)\tSTANDARD\n" +
                        "lpad\tlpad(SIS)\tlpad(var string, var int, var string)\tSTANDARD\n" +
                        "ltrim\tltrim(S)\tltrim(var string)\tSTANDARD\n" +
                        "make_geohash\tmake_geohash(DDi)\tmake_geohash(var double, var double, const int)\tSTANDARD\n" +
                        "max\tmax(D)\tmax(var double)\tGROUP_BY\n" +
                        "max\tmax(T)\tmax(var boolean)\tGROUP_BY\n" +
                        "max\tmax(I)\tmax(var int)\tGROUP_BY\n" +
                        "max\tmax(L)\tmax(var long)\tGROUP_BY\n" +
                        "max\tmax(A)\tmax(var char)\tGROUP_BY\n" +
                        "max\tmax(N)\tmax(var timestamp)\tGROUP_BY\n" +
                        "max\tmax(M)\tmax(var date)\tGROUP_BY\n" +
                        "max\tmax(F)\tmax(var float)\tGROUP_BY\n" +
                        "max\tmax(S)\tmax(var string)\tGROUP_BY\n" +
                        "memory_metrics\tmemory_metrics()\tmemory_metrics()\tSTANDARD\n" +
                        "micros\tmicros(N)\tmicros(var timestamp)\tSTANDARD\n" +
                        "millis\tmillis(N)\tmillis(var timestamp)\tSTANDARD\n" +
                        "min\tmin(D)\tmin(var double)\tGROUP_BY\n" +
                        "min\tmin(T)\tmin(var boolean)\tGROUP_BY\n" +
                        "min\tmin(F)\tmin(var float)\tGROUP_BY\n" +
                        "min\tmin(L)\tmin(var long)\tGROUP_BY\n" +
                        "min\tmin(I)\tmin(var int)\tGROUP_BY\n" +
                        "min\tmin(A)\tmin(var char)\tGROUP_BY\n" +
                        "min\tmin(N)\tmin(var timestamp)\tGROUP_BY\n" +
                        "min\tmin(M)\tmin(var date)\tGROUP_BY\n" +
                        "min\tmin(S)\tmin(var string)\tGROUP_BY\n" +
                        "minute\tminute(N)\tminute(var timestamp)\tSTANDARD\n" +
                        "month\tmonth(N)\tmonth(var timestamp)\tSTANDARD\n" +
                        "not\tnot(T)\tnot(var boolean)\tSTANDARD\n" +
                        "now\tnow()\tnow()\tSTANDARD\n" +
                        "npe\tnpe()\tnpe()\tSTANDARD\n" +
                        "nsum\tnsum(D)\tnsum(var double)\tGROUP_BY\n" +
                        "nullif\tnullif(AA)\tnullif(var char, var char)\tSTANDARD\n" +
                        "nullif\tnullif(II)\tnullif(var int, var int)\tSTANDARD\n" +
                        "nullif\tnullif(LL)\tnullif(var long, var long)\tSTANDARD\n" +
                        "nullif\tnullif(SS)\tnullif(var string, var string)\tSTANDARD\n" +
                        "or\tor(TT)\tor(var boolean, var boolean)\tSTANDARD\n" +
                        "pg_advisory_unlock_all\tpg_advisory_unlock_all()\tpg_advisory_unlock_all()\tSTANDARD\n" +
                        "pg_attrdef\tpg_attrdef()\tpg_attrdef()\tSTANDARD\n" +
                        "pg_attribute\tpg_attribute()\tpg_attribute()\tSTANDARD\n" +
                        "pg_catalog.age\tpg_catalog.age(L)\tpg_catalog.age(var long)\tSTANDARD\n" +
                        "pg_catalog.current_database\tpg_catalog.current_database()\tpg_catalog.current_database()\tSTANDARD\n" +
                        "pg_catalog.current_schema\tpg_catalog.current_schema()\tpg_catalog.current_schema()\tSTANDARD\n" +
                        "pg_catalog.current_schemas\tpg_catalog.current_schemas(T)\tpg_catalog.current_schemas(var boolean)\tSTANDARD\n" +
                        "pg_catalog.pg_attrdef\tpg_catalog.pg_attrdef()\tpg_catalog.pg_attrdef()\tSTANDARD\n" +
                        "pg_catalog.pg_attribute\tpg_catalog.pg_attribute()\tpg_catalog.pg_attribute()\tSTANDARD\n" +
                        "pg_catalog.pg_class\tpg_catalog.pg_class()\tpg_catalog.pg_class()\tCURSOR\n" +
                        "pg_catalog.pg_database\tpg_catalog.pg_database()\tpg_catalog.pg_database()\tCURSOR\n" +
                        "pg_catalog.pg_description\tpg_catalog.pg_description()\tpg_catalog.pg_description()\tCURSOR\n" +
                        "pg_catalog.pg_get_expr\tpg_catalog.pg_get_expr(SIT)\tpg_catalog.pg_get_expr(var string, var int, var boolean)\tSTANDARD\n" +
                        "pg_catalog.pg_get_expr\tpg_catalog.pg_get_expr(SI)\tpg_catalog.pg_get_expr(var string, var int)\tSTANDARD\n" +
                        "pg_catalog.pg_get_keywords\tpg_catalog.pg_get_keywords()\tpg_catalog.pg_get_keywords()\tSTANDARD\n" +
                        "pg_catalog.pg_get_partkeydef\tpg_catalog.pg_get_partkeydef(I)\tpg_catalog.pg_get_partkeydef(var int)\tSTANDARD\n" +
                        "pg_catalog.pg_get_userbyid\tpg_catalog.pg_get_userbyid(I)\tpg_catalog.pg_get_userbyid(var int)\tSTANDARD\n" +
                        "pg_catalog.pg_index\tpg_catalog.pg_index()\tpg_catalog.pg_index()\tCURSOR\n" +
                        "pg_catalog.pg_inherits\tpg_catalog.pg_inherits()\tpg_catalog.pg_inherits()\tCURSOR\n" +
                        "pg_catalog.pg_is_in_recovery\tpg_catalog.pg_is_in_recovery()\tpg_catalog.pg_is_in_recovery()\tSTANDARD\n" +
                        "pg_catalog.pg_locks\tpg_catalog.pg_locks()\tpg_catalog.pg_locks()\tCURSOR\n" +
                        "pg_catalog.pg_namespace\tpg_catalog.pg_namespace()\tpg_catalog.pg_namespace()\tSTANDARD\n" +
                        "pg_catalog.pg_roles\tpg_catalog.pg_roles()\tpg_catalog.pg_roles()\tCURSOR\n" +
                        "pg_catalog.pg_shdescription\tpg_catalog.pg_shdescription()\tpg_catalog.pg_shdescription()\tCURSOR\n" +
                        "pg_catalog.pg_table_is_visible\tpg_catalog.pg_table_is_visible(I)\tpg_catalog.pg_table_is_visible(var int)\tSTANDARD\n" +
                        "pg_catalog.pg_type\tpg_catalog.pg_type()\tpg_catalog.pg_type()\tCURSOR\n" +
                        "pg_catalog.txid_current\tpg_catalog.txid_current()\tpg_catalog.txid_current()\tSTANDARD\n" +
                        "pg_catalog.version\tpg_catalog.version()\tpg_catalog.version()\tSTANDARD\n" +
                        "pg_class\tpg_class()\tpg_class()\tCURSOR\n" +
                        "pg_database\tpg_database()\tpg_database()\tCURSOR\n" +
                        "pg_description\tpg_description()\tpg_description()\tCURSOR\n" +
                        "pg_get_expr\tpg_get_expr(SIT)\tpg_get_expr(var string, var int, var boolean)\tSTANDARD\n" +
                        "pg_get_expr\tpg_get_expr(SI)\tpg_get_expr(var string, var int)\tSTANDARD\n" +
                        "pg_get_keywords\tpg_get_keywords()\tpg_get_keywords()\tSTANDARD\n" +
                        "pg_get_partkeydef\tpg_get_partkeydef(I)\tpg_get_partkeydef(var int)\tSTANDARD\n" +
                        "pg_index\tpg_index()\tpg_index()\tCURSOR\n" +
                        "pg_inherits\tpg_inherits()\tpg_inherits()\tCURSOR\n" +
                        "pg_is_in_recovery\tpg_is_in_recovery()\tpg_is_in_recovery()\tSTANDARD\n" +
                        "pg_locks\tpg_locks()\tpg_locks()\tCURSOR\n" +
                        "pg_namespace\tpg_namespace()\tpg_namespace()\tSTANDARD\n" +
                        "pg_postmaster_start_time\tpg_postmaster_start_time()\tpg_postmaster_start_time()\tSTANDARD\n" +
                        "pg_proc\tpg_proc()\tpg_proc()\tCURSOR\n" +
                        "pg_range\tpg_range()\tpg_range()\tCURSOR\n" +
                        "pg_roles\tpg_roles()\tpg_roles()\tCURSOR\n" +
                        "pg_type\tpg_type()\tpg_type()\tCURSOR\n" +
                        "pi\tpi()\tpi()\tSTANDARD\n" +
                        "position\tposition(SS)\tposition(var string, var string)\tSTANDARD\n" +
                        "power\tpower(DD)\tpower(var double, var double)\tSTANDARD\n" +
                        "radians\tradians(D)\tradians(var double)\tSTANDARD\n" +
                        "rank\trank()\trank()\tWINDOW\n" +
                        "reader_pool\treader_pool()\treader_pool()\tSTANDARD\n" +
                        "regexp_replace\tregexp_replace(SSS)\tregexp_replace(var string, var string, var string)\tSTANDARD\n" +
                        "replace\treplace(SSS)\treplace(var string, var string, var string)\tSTANDARD\n" +
                        "right\tright(SI)\tright(var string, var int)\tSTANDARD\n" +
                        "rnd_bin\trnd_bin(lli)\trnd_bin(const long, const long, const int)\tSTANDARD\n" +
                        "rnd_bin\trnd_bin()\trnd_bin()\tSTANDARD\n" +
                        "rnd_boolean\trnd_boolean()\trnd_boolean()\tSTANDARD\n" +
                        "rnd_byte\trnd_byte(ii)\trnd_byte(const int, const int)\tSTANDARD\n" +
                        "rnd_byte\trnd_byte()\trnd_byte()\tSTANDARD\n" +
                        "rnd_char\trnd_char()\trnd_char()\tSTANDARD\n" +
                        "rnd_date\trnd_date(mmi)\trnd_date(const date, const date, const int)\tSTANDARD\n" +
                        "rnd_date\trnd_date()\trnd_date()\tSTANDARD\n" +
                        "rnd_double\trnd_double(i)\trnd_double(const int)\tSTANDARD\n" +
                        "rnd_double\trnd_double()\trnd_double()\tSTANDARD\n" +
                        "rnd_float\trnd_float(i)\trnd_float(const int)\tSTANDARD\n" +
                        "rnd_float\trnd_float()\trnd_float()\tSTANDARD\n" +
                        "rnd_geohash\trnd_geohash(i)\trnd_geohash(const int)\tSTANDARD\n" +
                        "rnd_int\trnd_int()\trnd_int()\tSTANDARD\n" +
                        "rnd_int\trnd_int(iii)\trnd_int(const int, const int, const int)\tSTANDARD\n" +
                        "rnd_log\trnd_log(ld)\trnd_log(const long, const double)\tSTANDARD\n" +
                        "rnd_long\trnd_long(lli)\trnd_long(const long, const long, const int)\tSTANDARD\n" +
                        "rnd_long\trnd_long()\trnd_long()\tSTANDARD\n" +
                        "rnd_long256\trnd_long256()\trnd_long256()\tSTANDARD\n" +
                        "rnd_long256\trnd_long256(i)\trnd_long256(const int)\tSTANDARD\n" +
                        "rnd_short\trnd_short(ii)\trnd_short(const int, const int)\tSTANDARD\n" +
                        "rnd_short\trnd_short()\trnd_short()\tSTANDARD\n" +
                        "rnd_str\trnd_str(iii)\trnd_str(const int, const int, const int)\tSTANDARD\n" +
                        "rnd_str\trnd_str(iiii)\trnd_str(const int, const int, const int, const int)\tSTANDARD\n" +
                        "rnd_str\trnd_str(V)\trnd_str(var var_arg)\tSTANDARD\n" +
                        "rnd_symbol\trnd_symbol(iiii)\trnd_symbol(const int, const int, const int, const int)\tSTANDARD\n" +
                        "rnd_symbol\trnd_symbol(V)\trnd_symbol(var var_arg)\tSTANDARD\n" +
                        "rnd_timestamp\trnd_timestamp(nni)\trnd_timestamp(const timestamp, const timestamp, const int)\tSTANDARD\n" +
                        "rnd_uuid4\trnd_uuid4()\trnd_uuid4()\tSTANDARD\n" +
                        "round\tround(D)\tround(var double)\tSTANDARD\n" +
                        "round\tround(DI)\tround(var double, var int)\tSTANDARD\n" +
                        "round_down\tround_down(DI)\tround_down(var double, var int)\tSTANDARD\n" +
                        "round_half_even\tround_half_even(DI)\tround_half_even(var double, var int)\tSTANDARD\n" +
                        "round_up\tround_up(DI)\tround_up(var double, var int)\tSTANDARD\n" +
                        "row_number\trow_number()\trow_number()\tWINDOW\n" +
                        "rpad\trpad(SI)\trpad(var string, var int)\tSTANDARD\n" +
                        "rpad\trpad(SIS)\trpad(var string, var int, var string)\tSTANDARD\n" +
                        "rtrim\trtrim(S)\trtrim(var string)\tSTANDARD\n" +
                        "second\tsecond(N)\tsecond(var timestamp)\tSTANDARD\n" +
                        "session_user\tsession_user()\tsession_user()\tSTANDARD\n" +
                        "simulate_crash\tsimulate_crash(a)\tsimulate_crash(const char)\tSTANDARD\n" +
                        "sin\tsin(D)\tsin(var double)\tSTANDARD\n" +
                        "size_pretty\tsize_pretty(L)\tsize_pretty(var long)\tSTANDARD\n" +
                        "split_part\tsplit_part(SSI)\tsplit_part(var string, var string, var int)\tSTANDARD\n" +
                        "split_part\tsplit_part(SAI)\tsplit_part(var string, var char, var int)\tSTANDARD\n" +
                        "sqrt\tsqrt(D)\tsqrt(var double)\tSTANDARD\n" +
                        "starts_with\tstarts_with(SS)\tstarts_with(var string, var string)\tSTANDARD\n" +
                        "stddev_samp\tstddev_samp(D)\tstddev_samp(var double)\tGROUP_BY\n" +
                        "string_agg\tstring_agg(Sa)\tstring_agg(var string, const char)\tGROUP_BY\n" +
                        "strpos\tstrpos(SS)\tstrpos(var string, var string)\tSTANDARD\n" +
                        "strpos\tstrpos(SA)\tstrpos(var string, var char)\tSTANDARD\n" +
                        "substring\tsubstring(SII)\tsubstring(var string, var int, var int)\tSTANDARD\n" +
                        "sum\tsum(D)\tsum(var double)\tGROUP_BY\n" +
                        "sum\tsum(F)\tsum(var float)\tGROUP_BY\n" +
                        "sum\tsum(I)\tsum(var int)\tGROUP_BY\n" +
                        "sum\tsum(L)\tsum(var long)\tGROUP_BY\n" +
                        "sum\tsum(H)\tsum(var long256)\tGROUP_BY\n" +
                        "sum_t\tsum_t(D)\tsum_t(var double)\tGROUP_BY\n" +
                        "sum_t\tsum_t(S)\tsum_t(var string)\tGROUP_BY\n" +
                        "sumx\tsumx(DS)\tsumx(var double, var string)\tGROUP_BY\n" +
                        "switch\tswitch(V)\tswitch(var var_arg)\tSTANDARD\n" +
                        "sysdate\tsysdate()\tsysdate()\tSTANDARD\n" +
                        "systimestamp\tsystimestamp()\tsystimestamp()\tSTANDARD\n" +
                        "table_columns\ttable_columns(s)\ttable_columns(const string)\tSTANDARD\n" +
                        "table_partitions\ttable_partitions(s)\ttable_partitions(const string)\tCURSOR\n" +
                        "table_writer_metrics\ttable_writer_metrics()\ttable_writer_metrics()\tSTANDARD\n" +
                        "tables\ttables()\ttables()\tSTANDARD\n" +
                        "tan\ttan(D)\ttan(var double)\tSTANDARD\n" +
                        "timestamp_ceil\ttimestamp_ceil(sN)\ttimestamp_ceil(const string, var timestamp)\tSTANDARD\n" +
                        "timestamp_floor\ttimestamp_floor(sN)\ttimestamp_floor(const string, var timestamp)\tSTANDARD\n" +
                        "timestamp_sequence\ttimestamp_sequence(NL)\ttimestamp_sequence(var timestamp, var long)\tSTANDARD\n" +
                        "timestamp_shuffle\ttimestamp_shuffle(nn)\ttimestamp_shuffle(const timestamp, const timestamp)\tSTANDARD\n" +
                        "to_char\tto_char(U)\tto_char(var binary)\tSTANDARD\n" +
                        "to_date\tto_date(Ss)\tto_date(var string, const string)\tSTANDARD\n" +
                        "to_long128\tto_long128(LL)\tto_long128(var long, var long)\tSTANDARD\n" +
                        "to_lowercase\tto_lowercase(S)\tto_lowercase(var string)\tSTANDARD\n" +
                        "to_pg_date\tto_pg_date(S)\tto_pg_date(var string)\tSTANDARD\n" +
                        "to_str\tto_str(Ms)\tto_str(var date, const string)\tSTANDARD\n" +
                        "to_str\tto_str(Ns)\tto_str(var timestamp, const string)\tSTANDARD\n" +
                        "to_timestamp\tto_timestamp(Ss)\tto_timestamp(var string, const string)\tSTANDARD\n" +
                        "to_timestamp\tto_timestamp(S)\tto_timestamp(var string)\tSTANDARD\n" +
                        "to_timezone\tto_timezone(NS)\tto_timezone(var timestamp, var string)\tSTANDARD\n" +
                        "to_uppercase\tto_uppercase(S)\tto_uppercase(var string)\tSTANDARD\n" +
                        "to_utc\tto_utc(NS)\tto_utc(var timestamp, var string)\tSTANDARD\n" +
                        "touch\ttouch(C)\ttouch(var cursor)\tSTANDARD\n" +
                        "trim\ttrim(S)\ttrim(var string)\tSTANDARD\n" +
                        "txid_current\ttxid_current()\ttxid_current()\tSTANDARD\n" +
                        "typeOf\ttypeOf(V)\ttypeOf(var var_arg)\tSTANDARD\n" +
                        "upper\tupper(S)\tupper(var string)\tSTANDARD\n" +
                        "version\tversion()\tversion()\tSTANDARD\n" +
                        "wal_tables\twal_tables()\twal_tables()\tSTANDARD\n" +
                        "week_of_year\tweek_of_year(N)\tweek_of_year(var timestamp)\tSTANDARD\n" +
                        "year\tyear(N)\tyear(var timestamp)\tSTANDARD\n"
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
