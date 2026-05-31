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

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class VarcharFunctionsTest extends AbstractCairoTest {

    @Test
    public void testLength() throws Exception {
        assertQuery("select count() from x where length(v_str) <> length(v_varchar)")
                .ddl("""
                        create table x as (\
                        select rnd_varchar(\
                        '㮾劈渁ꑿ朰怲㔂퉮꾾桠云澆ⵥ⥤貚뛺퉧᧖晔姬䶡㭫ᆳҗ㋶\u1ADC㛚ᶎ\u0BFF㈲ꡭဦ㽋㫃䗬\u1ACC캱䬚焌㊈晎ꉰ瑟⿄䓔⒑ꬂ뗖麻䩕녏핥⊆',
                        '臑죐涕驠녃馀蚯㆙ࡀ詈韮⋊Ზ晻켏問᳡蔹\u20C8᧲㋳歭퐴顯赤쐙葦鬶凕㯅迧㾤恲넦┊兊',
                        '苻읺ꃻ籬璻㯹쐙儱쥺宎荎䴵晚㱎綦๒䮚要嫶ꤡ籘Ȏ犦\u0E7A➛\uAAFB嬃谉鎷죸ᣛਡ溶࣭㷠禐꣔',
                        'ೖ֩黡莟ꀱ푕㰆Ⳅꔻ᳅䈋\u0019㟛즈ǆ㏆帮ꪏᤌ四䞾凙㤶᧚煍 \u09B1건᧮傋否쾙걗Ե䔽鸃愘ᖈ㡑氤띕ㄶ\u0BE3榞╅骥Ӑ䮄溉콽㣡됥કⵗ呦',
                        '쫪쾜앻쾾麠☫븐ᮖ䀿ӝ롗湝',
                        '잖㇕ꕲ뉨펷釾閦餌飓竃̟湰ϻ\u0CDF됗摡添Į༇匋鍘ả뎃礦鈨돓쾶▆٫礤㣪뼕핶喘㑺ꤸ뉙샪豧䰅䳮䭘쮆錭й㤩⎪颜貝鐷',
                        'Ẉள뗙틾訃棯ꚭʸ\u0CD0籱愱阆뉮꒰뎼',
                        '룩隺쳃봬찂ϝ싵\u245B諑箥몞陼庛刐䪪团㒯ꁅЭ뫛눤➲댦ᯤᲃ⭮䝚ꡌ窀텴瘪次꿨ډ叭㜳⪞纎䷲㺽쾍⋍\u0084埆淶㿜䣃₪췪⸂渂떄\u0A56㖉䩳甓䨰慡璾鄵墀ꃢ봶撉䮗᥍䰈',
                        '님鏰\uAB0F캫듅歨⁽瘘ꑨ漬뛡혝둟樢굸',
                        'ꕅ덻∫\u09CF⼺蝑㝌酵䰈✟쫃믴靉ᐩ龺燤匂핷妺拊퐂툺计鉃맏奖넆唩ῷŸ祖䢻뵚麼ꎃ睾얧苖㟗⭇ᶹఱ勠찀ቬ㑅阻푪Ⱕ褁ⴵ抯⯪麷',
                        '轇㯋鱝덡簽麽ᴠ媐ᝮ効홻㩽㧓捣荹◁胂萗负琀ୀ\u0B29氏ⲗ渹\u0A44⧕谞㉎迱霌묫銊돴厎뵚궡ῒ딠濖ඃ緎㸦㷴ꎖ왆ሯធ\u19AE샇㻉뢖苛',
                        '斄ꕴ牐㽅駬쒀ꫵ䫝୍賬♨翲艆䪲捲茨稡냨ख혢⸺⚻쬆툜৷ᦞ觯ꖪꆪ狢ꓨ돷䜳䓃䷡◌饇䃛ኄ멩潛\u0A00箶瞯ᯭㅦ೮徑䴔\u05CC硎뾏궄᧱밋؟멆懅풨㙛㛔橧좣냜媽',
                        '싼슐듯쬼㊛⥇᪭椙梱㱇툚戋戧\u0A52䶧ฯ宂픁\u1316鱈眃⑲炢递邞ᚇべ禗㮆䅏䯽낼禼샜轞묏⺹캔\u2068⬶䗬騂Ȏ䔅杵셶嶑栄ᗫ㹺퇧浣莓单⮉챴䳻⣭椞㎰㵇➣ꮾ昈콱贔',
                        '頁䛂퇹憹鏂웾Ὡ㓜购픖⺑뢈귈뽐셵글뼫諀蔞⸢䎤ἐ펒ⱎ❩峛퍒穜㊊儔鞴笏㋙篣把撖祱鲆䕬ῥ걨欞ힶ癫⚨㌿졽ᇌ䩳៓쯀삜▵ː괝',
                        'ウ穿举괧⃗㆗閔℄榱걓꒫쪲\uA87F๕咞똍ኆᚍ靱歈꿘⹌박ꈻ㦸埇婅⯱鴩넮힠墆ḅ幛豵傾cㄵࣃ껑㉱腼纤椒ꊆ磘⾮⎰棡櫀뚷ଡᚠ킥㾽鳬⭾ڴ䗠㰯ꚼ䘵虸忇띔䱉℣쌋ꛉ㯼䖀ꋜꇬᆺ⨛胢\u1AF1㼫',
                        '쪍萫\u0098暌荏尿艴ꯑꛫ컎䇝ǌ敆֭豨∨\u05CB喀뺌즂꺒웴辐ኒ렺᭼鹼ꮌ拠',
                        '箲䂍ꁐ耵붃⢜穣菜뀍៘藟띻㴬ḁᝄ係愵',
                        '載巒ꈬ轿⁝䯰ȃ⼂\u0D80뿺穂偓泵唺꣫㯂幒⇄쳎떂\u09B5賭≂礣綦ꀦᖮ飰ᑺ仱⩃䰒ⷘ쨷翻佹',
                        '窑읽Ը兼너潭砸簠ꎹ颴ퟹ鶡矤謍嵛쇌띄哻ཛ닚흼劁㩰읏亄哨坩輓睘묊ᩔꍉԛᕋ㣞顤힕',
                        '웈된轭云㘊홤ヸꐔ苩⩩㲍광稷섷↮ꊨ肆锖⹀㥝菔뇃렔录ꛃ뗰瞉⟙✣ᅷ◀的뜦莱ݶ㔲緲쏮\u009FḜ妒㫯姧ꀥѯ轗䜬쒺呉続ꚋ멵㴴嗳㈕뺋嬇䉆',
                        '⼝ࣞаꔭ叒扠혦裖컌目瘋㜄篊쇳㪒╸㑧횙빍㑢㫪骞鵌놊붒戯ꓰ兢ꊘ䎇✵嚸ř璜꽯놁彽芳䎾慐㣅⾛ⁱ팗劁虮ẙമ쟑礽鞧⾖⳩ࣹ\u0B7D훅▮痬Ⴕ铂鲱뼰ᣇ',
                        '랊劫㐌㯼굞ᑾ¾۟♉룝㱝뎰鸏主Щ',
                        'ಝ㞃ꑻ؎刀膸駞蕳ⳍ짣ᦕ꾮⎿휋뽁駄\uAAFCᇤꭦǈꙆ쒨ஂ귓ඞ渇ᦱ㋴짿᳨皴欝沽埏팯᪓㾊瀘♂婑殶귽읊뺈졎앟᱉ဤᢂ佭ી폕귉豭⋙낁鱶尨晦䞌㗵\u200F좏譗朠삏귿䋰漾睺㕚栅骂猈朅蚬砰',
                        '㉠ꇊᎏ栍헁예匋ଵ䭗ꁲ␢ೠ鱶ꐭ핋⥩쒒탹쁰諲ೱ깏冔ȴ㭪珵䄃\u17FDힱ萪',
                        '崨귅ꯟ枖傖뚼沄㽕쇯\uAAFB❰䬊틒귖䡩綞좐趵ミ婱꼮ኄ䟹ݸ௨挑冱⿕䀍湹ూ㢻趪ꭳ屃㕴笠헝㴱埡꞊羇티봃ꫢãꂩ鿟獘嬃楍븽⒉굌櫇梋뒜籑過띟馇ꪇ뙓钩튆鍇婉ꇦꞳ險튗맽魷൴큒먗쉾碶烙'\
                        ) v_varchar, '' v_str\
                         from long_sequence(200))""")
                .mutateWith("update x set v_str = v_varchar")
                .noRandomAccess()
                .expectSize()
                .returns("count\n200\n", "count\n0\n");
    }

    @Test
    public void testLtrim() throws Exception {
        assertQuery("select k, ltrim(k) from x")
                .ddl("create table x as (select rnd_varchar('  abc', 'abc  ', '   ') k from long_sequence(5))")
                .expectSize()
                .returns("""
                        k\tltrim
                          abc\tabc
                          abc\tabc
                        abc  \tabc \s
                           \t
                           \t
                        """);
    }

    @Test
    public void testPosition() throws Exception {
        assertQuery("select k, position(k, 'a') from x")
                .ddl("create table x as (select rnd_varchar('xa', 'xx', 'aax', '') k from long_sequence(5))")
                .expectSize()
                .returns("""
                        k\tposition
                        xa\t2
                        aax\t1
                        xx\t0
                        \t0
                        xx\t0
                        """);
    }

    @Test
    public void testReplace() throws Exception {
        assertQuery("select k, replace(k, 'a', 'b') from x")
                .ddl("create table x as (select rnd_varchar('xa', 'xx', 'aax', '') k from long_sequence(5))")
                .expectSize()
                .returns("""
                        k\treplace
                        xa\txb
                        aax\tbbx
                        xx\txx
                        \t
                        xx\txx
                        """);
    }

    @Test
    public void testReplaceConstant() throws Exception {
        assertQuery("select replace('tom'::varchar, 't'::varchar, 's'::varchar) from x")
                .ddl("create table x as (select x from long_sequence(1))")
                .expectSize()
                .returns("""
                        replace
                        som
                        """);
    }

    @Test
    public void testReplaceConstantFirstArgNull() throws Exception {
        assertQuery("select replace(NULL, 't'::varchar, 's'::varchar) from x")
                .ddl("create table x as (select x from long_sequence(1))")
                .expectSize()
                .returns("""
                        replace
                        null
                        """);
    }

    @Test
    public void testReplaceConstantSecondArgEmpty() throws Exception {
        assertQuery("select replace('tom'::varchar, ''::varchar, 's'::varchar) from x")
                .ddl("create table x as (select x from long_sequence(1))")
                .expectSize()
                .returns("""
                        replace
                        tom
                        """);
    }

    @Test
    public void testReplaceConstantSecondArgNull() throws Exception {
        assertQuery("select replace('tom'::varchar, NULL, 's'::varchar) from x")
                .ddl("create table x as (select x from long_sequence(1))")
                .expectSize()
                .returns("""
                        replace
                        
                        """);
    }

    @Test
    public void testReplaceConstantThirdArgEmpty() throws Exception {
        assertQuery("select replace('tom'::varchar, 't'::varchar, ''::varchar) from x")
                .ddl("create table x as (select x from long_sequence(1))")
                .expectSize()
                .returns("""
                        replace
                        om
                        """);
    }

    @Test
    public void testReplaceConstantThirdArgNull() throws Exception {
        assertQuery("select replace('tom'::varchar, 't'::varchar, NULL) from x")
                .ddl("create table x as (select x from long_sequence(1))")
                .expectSize()
                .returns("""
                        replace
                        
                        """);
    }

    @Test
    public void testRtrim() throws Exception {
        assertQuery("select k, rtrim(k) from x")
                .ddl("create table x as (select rnd_varchar('  abc', 'abc  ', '   ') k from long_sequence(5))")
                .expectSize()
                .returns("""
                        k\trtrim
                          abc\t  abc
                          abc\t  abc
                        abc  \tabc
                           \t
                           \t
                        """);
    }

    @Test
    public void testStartsWithLongPrefix() throws Exception {
        assertQuery("select k, starts_with(k, 'abcdefghijk') from x")
                .ddl("create table x as (select rnd_varchar(" +
                        "'xabcdefghijk', 'abcdefghijx', 'abcdefghij', 'abcdefghijkx', 'ab', 'xx'" +
                        ") k from long_sequence(20))")
                .expectSize()
                .returns("""
                        k\tstarts_with
                        xabcdefghijk\tfalse
                        xabcdefghijk\tfalse
                        abcdefghijx\tfalse
                        xx\tfalse
                        xx\tfalse
                        xx\tfalse
                        abcdefghij\tfalse
                        abcdefghijx\tfalse
                        xabcdefghijk\tfalse
                        ab\tfalse
                        ab\tfalse
                        abcdefghij\tfalse
                        ab\tfalse
                        abcdefghijx\tfalse
                        abcdefghijx\tfalse
                        xabcdefghijk\tfalse
                        xabcdefghijk\tfalse
                        abcdefghijx\tfalse
                        abcdefghijkx\ttrue
                        ab\tfalse
                        """);
    }

    @Test
    public void testStartsWithMidsizePrefix() throws Exception {
        assertQuery("select k, starts_with(k, 'abcdefgh') from x")
                .ddl("create table x as (select rnd_varchar(" +
                        "'xabcdefgh', 'abcdefgx', 'abcdefg', 'abcdefghx', 'abcdefghxxxx', 'ab', 'xx'" +
                        ") k from long_sequence(20))")
                .expectSize()
                .returns("""
                        k\tstarts_with
                        xabcdefgh\tfalse
                        abcdefg\tfalse
                        xx\tfalse
                        abcdefghxxxx\ttrue
                        abcdefghx\ttrue
                        abcdefg\tfalse
                        abcdefgx\tfalse
                        ab\tfalse
                        abcdefghx\ttrue
                        xabcdefgh\tfalse
                        abcdefghx\ttrue
                        xx\tfalse
                        abcdefg\tfalse
                        abcdefghx\ttrue
                        ab\tfalse
                        ab\tfalse
                        ab\tfalse
                        xabcdefgh\tfalse
                        abcdefg\tfalse
                        xabcdefgh\tfalse
                        """);
    }

    @Test
    public void testStartsWithShortPrefix() throws Exception {
        assertQuery("select k, starts_with(k, 'abcde') from x")
                .ddl("create table x as (select rnd_varchar(" +
                        "'xabcde', 'abcdx', 'abcde', 'abcdex', 'abcdexxxx', 'ab', 'xx'" +
                        ") k from long_sequence(20))")
                .expectSize()
                .returns("""
                        k\tstarts_with
                        xabcde\tfalse
                        abcde\ttrue
                        xx\tfalse
                        abcdexxxx\ttrue
                        abcdex\ttrue
                        abcde\ttrue
                        abcdx\tfalse
                        ab\tfalse
                        abcdex\ttrue
                        xabcde\tfalse
                        abcdex\ttrue
                        xx\tfalse
                        abcde\ttrue
                        abcdex\ttrue
                        ab\tfalse
                        ab\tfalse
                        ab\tfalse
                        xabcde\tfalse
                        abcde\ttrue
                        xabcde\tfalse
                        """);
    }

    @Test
    public void testStrpos() throws Exception {
        assertQuery("select k, strpos(k, 'a') from x")
                .ddl("create table x as (select rnd_varchar('xa', 'xx', 'aax', '') k from long_sequence(5))")
                .expectSize()
                .returns("""
                        k\tstrpos
                        xa\t2
                        aax\t1
                        xx\t0
                        \t0
                        xx\t0
                        """);
    }

    @Test
    public void testToDate() throws Exception {
        assertQuery("select c from x where to_date(c, 'yyyy-MM-dd') = to_date('1999-07-05', 'yyyy-MM-dd')")
                .ddl("create table x as (select cast('1999-07-05' as varchar) c from long_sequence(1))")
                .returns("""
                        c
                        1999-07-05
                        """);
    }

    @Test
    public void testToDateUkr() throws Exception {
        setProperty(PropertyKey.CAIRO_DATE_LOCALE, "uk");
        assertQuery("select c from x where to_date(c, 'd MMM y') = '1999-07-05'")
                .ddl("create table x as (select cast('5 лип. 1999' as varchar) c from long_sequence(1))")
                .returns("""
                        c
                        5 лип. 1999
                        """);
    }

    @Test
    public void testToDateUs() throws Exception {
        setProperty(PropertyKey.CAIRO_DATE_LOCALE, "en-US");
        assertQuery("select c from x where to_date(c, 'd MMM y') = to_date('1999-07-05', 'yyyy-MM-dd')")
                .ddl("create table x as (select cast('5 Jul 1999' as varchar) c from long_sequence(1))")
                .returns("""
                        c
                        5 Jul 1999
                        """);
    }

    @Test
    public void testToPgDate() throws Exception {
        assertQuery("select c from x where to_pg_date(c) = to_date('1999-07-05', 'yyyy-MM-dd')")
                .ddl("create table x as (select cast('1999-07-05' as varchar) c from long_sequence(1))")
                .returns("""
                        c
                        1999-07-05
                        """);
    }

    @Test
    public void testToPgDateUkr() throws Exception {
        setProperty(PropertyKey.PG_DATE_LOCALE, "uk");
        assertQuery("select c from x where to_pg_date(c) = '1999-07-05'")
                .ddl("create table x as (select cast('1999-07-05' as varchar) c from long_sequence(1))")
                .returns("""
                        c
                        1999-07-05
                        """);
    }

    @Test
    public void testTrim() throws Exception {
        assertQuery("select k, trim(k) from x")
                .ddl("create table x as (select rnd_varchar('  abc', 'abc  ', '  abc  ', '   ') k from long_sequence(5))")
                .expectSize()
                .returns("""
                        k\ttrim
                          abc\tabc
                          abc  \tabc
                        abc  \tabc
                           \t
                        abc  \tabc
                        """);
    }
}
