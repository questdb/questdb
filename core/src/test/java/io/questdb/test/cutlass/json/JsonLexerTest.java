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

package io.questdb.test.cutlass.json;

import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.json.JsonParser;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.IntStack;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.ServerMainVectorGroupByTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JsonLexerTest {

    private static final JsonLexer LEXER = new JsonLexer(4, 1024);
    private static final JsonAssemblingParser listener = new JsonAssemblingParser();

    @AfterClass
    public static void tearDown() {
        LEXER.close();
    }

    @Before
    public void setUp() {
        LEXER.clear();
        listener.clear();
    }

    @Test
    public void testArrayObjArray() throws Exception {
        assertThat("[{\"A\":[\"122\",\"133\"],\"x\":\"y\"},\"134\",\"abc\"]", """
                [
                {"A":[122, 133], "x": "y"}, 134  , "abc"
                ]""");
    }

    @Test
    public void testBreakOnValue() throws Exception {
        String in = "{\"x\": \"abcdefhijklmn\"}";
        int len = in.length();
        long address = TestUtils.toMemory(in);
        try {
            LEXER.parse(address, address + len - 7, listener);
            LEXER.parse(address + len - 7, address + len, listener);
            LEXER.parseLast();
            TestUtils.assertEquals("{\"x\":\"abcdefhijklmn\"}", listener.value());
        } finally {
            Unsafe.free(address, len, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testCacheDisabled() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String json = "{\"a\":1, \"b\": \"123456789012345678901234567890\"}";
            int len = json.length();
            long address = TestUtils.toMemory(json);

            // JsonLexer cache is disabled
            try (JsonLexer lexer = new JsonLexer(0, 0)) {
                // passing the entire message in, no need for cache
                lexer.parse(address, address + len, listener);
                lexer.parseLast();
                lexer.clear();

                // cutting the message at string boundary, no need for cache
                int subLen = "{\"a\":1, \"b\":".length();
                lexer.parse(address, address + subLen, listener);
                lexer.parse(address + subLen, address + len, listener);
                lexer.parseLast();
                lexer.clear();

                // cutting the message in the middle of a string,
                // would need cache, but it is disabled so an error expected
                try {
                    subLen = "{\"a\":1, \"b\": \"1234".length();
                    lexer.parse(address, address + subLen, listener);
                    lexer.parse(address + subLen, address + len, listener);
                    lexer.parseLast();
                    Assert.fail();
                } catch (JsonException e) {
                    TestUtils.assertEquals("JSON lexer cache is disabled", e.getFlyweightMessage());
                    Assert.assertEquals(18, e.getPosition());
                }
            } finally {
                Unsafe.free(address, json.length(), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDanglingArrayEnd() {
        assertError("Dangling ]", 8, "[1,2,3]]");
    }

    @Test
    public void testDanglingComma() {
        assertError("Attribute name expected", 12, "{\"x\": \"abc\",}");
    }

    @Test
    public void testDanglingObjectEnd() {
        assertError("Dangling }", 8, "[1,2,3]}");
    }

    @Test
    public void testEmptyArray() throws Exception {
        assertThat("[]", "[]");
    }

    @Test
    public void testEmptyObject() throws Exception {
        assertThat("{}", "{}");
    }

    @Test
    public void testExponent() throws Exception {
        assertThat("[\"-1.34E4\",\"3\"]", "[-1.34E4,3]");
    }

    @Test
    public void testIncorrectArrayStart() {
        assertError("[ is not expected here", 3, "[1[]]");
    }

    @Test
    public void testInvalidObjectNesting() {
        assertError("{ is not expected here", 11, "{\"a\":\"x\", {}}");
    }

    @Test
    public void testInvalidUtf8Value() {
        byte[] bytesA = "{\"x\":\"违法违,控网站漏洞风\", \"y\":\"站漏洞风".getBytes(Files.UTF_8);
        byte[] bytesB = {-116, -76, -55, 55, -34, 0, -11, 15, 13};
        byte[] bytesC = "\"}".getBytes(Files.UTF_8);

        byte[] bytes = new byte[bytesA.length + bytesB.length + bytesC.length];
        System.arraycopy(bytesA, 0, bytes, 0, bytesA.length);
        System.arraycopy(bytesB, 0, bytes, bytesA.length, bytesB.length);
        System.arraycopy(bytesC, 0, bytes, bytesA.length + bytesB.length, bytesC.length);


        int len = bytes.length;
        long address = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < len; i++) {
            Unsafe.putByte(address + i, bytes[i]);
        }
        try {
            for (int i = 0; i < len; i++) {
                try {
                    listener.clear();
                    LEXER.clear();
                    LEXER.parse(address, address + i, listener);
                    LEXER.parse(address + i, address + len, listener);
                    LEXER.parseLast();
                    Assert.fail();
                } catch (JsonException e) {
                    TestUtils.assertEquals("Unsupported encoding", e.getFlyweightMessage());
                    Assert.assertEquals(43, e.getPosition());
                }
            }
        } finally {
            Unsafe.free(address, len, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testJsonSlicingAndPositions() throws Exception {
        assertThat(
                "<1>[<2>{<4>\"name\":<11>\"null\"<18>,\"type\":<25>\"true\"<32>,\"formatPattern\":<47>\"12E-2\"<55>,\"locale\":<65>\"en-GB\"<71>}<72>]",
                "[{\"name\": null, \"type\": true, \"formatPattern\":12E-2, \"locale\": \"en-GB\"}]",
                true);
    }

    @Test
    public void testMisplacedArrayEnd() {
        assertError("] is not expected here. You have non-terminated object", 18, "{\"a\":1, \"b\": 15.2]}");
    }

    @Test
    public void testMisplacedColon() {
        assertError("Misplaced ':'", 9, "{\"a\":\"x\":}");
    }

    @Test
    public void testMisplacedQuote() {
        assertError("Unexpected quote '\"'", 9, "{\"a\":\"1\"\", \"b\": 15.2}");
    }

    @Test
    public void testMisplacesObjectEnd() {
        assertError("} is not expected here. You have non-terminated array", 7, "[1,2,3}");
    }

    @Test
    public void testMissingArrayValue() {
        assertError("Unexpected comma", 2, "[,]");
    }

    @Test
    public void testMissingAttributeValue() {
        assertError("Attribute value expected", 6, "{\"x\": }");
    }

    @Test
    public void testNestedObjNestedArray() throws Exception {
        assertThat("{\"x\":{\"y\":[[\"1\",\"2\",\"3\"],[\"5\",\"2\",\"3\"],[\"0\",\"1\"]],\"a\":\"b\"}}", "{\"x\": { \"y\": [[1,2,3], [5,2,3], [0,1]], \"a\":\"b\"}}");
    }

    @Test
    public void testNestedObjects() throws Exception {
        assertThat("{\"abc\":{\"x\":\"123\"},\"val\":\"000\"}", "{\"abc\": {\"x\":\"123\"}, \"val\": \"000\"}");
    }

    @Test
    public void testParseLargeFile() throws Exception {
        String path = Files.getResourcePath(getClass().getResource("/json/test.json"));
        try (Path p = new Path()) {
            if (Os.isWindows() && path.startsWith("/")) {
                p.of(path.substring(1));
            } else {
                p.of(path);
            }
            long l = Files.length(p.$());
            long fd = TestFilesFacadeImpl.INSTANCE.openRO(p.$());
            JsonParser listener = new NoOpParser();
            try {
                long buf = Unsafe.malloc(l, MemoryTag.NATIVE_DEFAULT);
                long bufA = Unsafe.malloc(l, MemoryTag.NATIVE_DEFAULT);
                long bufB = Unsafe.malloc(l, MemoryTag.NATIVE_DEFAULT);
                try {
                    Assert.assertEquals(l, Files.read(fd, buf, (int) l, 0));

                    for (int i = 0; i < l; i++) {
                        try {
                            LEXER.clear();
                            Unsafe.copyMemory(buf, bufA, i);
                            Unsafe.copyMemory(buf + i, bufB, l - i);
                            LEXER.parse(bufA, bufA + i, listener);
                            LEXER.parse(bufB, bufB + l - i, listener);
                            LEXER.parseLast();
                        } catch (JsonException e) {
                            System.out.println(i);
                            throw e;
                        }
                    }
                } finally {
                    Unsafe.free(buf, l, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(bufA, l, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(bufB, l, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                TestFilesFacadeImpl.INSTANCE.close(fd);
            }
        }
    }

    @Test
    public void testQuoteEscape() throws Exception {
        assertThat("{\"x\":\"a\\\"bc\"}", "{\"x\": \"a\\\"bc\"}");
    }

    @Test
    public void testSimpleJson() throws Exception {
        assertThat("{\"abc\":\"123\"}", "{\"abc\": \"123\"}");
    }

    @Test
    public void testStringTooLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String json = "{\"a\":1, \"b\": \"123456789012345678901234567890\"]}";
            int len = json.length() - 6;
            long address = TestUtils.toMemory(json);
            try (JsonLexer lexer = new JsonLexer(4, 4)) {
                try {
                    lexer.parse(address, address + len, listener);
                    lexer.parseLast();
                    Assert.fail();
                } catch (JsonException e) {
                    TestUtils.assertEquals("String is too long", e.getFlyweightMessage());
                    Assert.assertEquals(41, e.getPosition());
                }
            } finally {
                Unsafe.free(address, json.length(), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testUnclosedQuote() {
        assertError("Unexpected symbol", 11, "{\"a\":\"1, \"b\": 15.2}");
    }

    @Test
    public void testUnquotedNumbers() throws Exception {
        assertThat("[{\"A\":\"122\"},\"134\",\"abc\"]", """
                [
                {"A":122}, 134  , "abc"
                ]""");
    }

    @Test
    public void testUnterminatedArray() {
        assertError("Unterminated array", 37, "{\"x\": { \"y\": [[1,2,3], [5,2,3], [0,1]");
    }

    @Test
    public void testUnterminatedObject() {
        assertError("Unterminated object", 38, "{\"x\": { \"y\": [[1,2,3], [5,2,3], [0,1]]");
    }

    @Test
    public void testUnterminatedString() {
        assertError("Unterminated string", 46, "{\"x\": { \"y\": [[1,2,3], [5,2,3], [0,1]], \"a\":\"b");
    }

    @Test
    public void testUtf8() throws Exception {
        assertThat(
                "{\"id\":\"japanese_cheat_sheet\",\"name\":\"Basic Japanese\",\"description\":\"A guide to basic Japanese\",\"metadata\":,{\"sourceName\":\"Tofugu\",\"sourceUrl\":\"https://www.tofugu.com/japanese/important-japanese-words/\"},\"template_type\":\"language\",\"section_order\":,[\"Numbers\",\"Polite Phrases\",\"Greetings\",\"Questions\",\"Locations\",\"Verbs\"],\"sections\":,{\"Numbers\":[{\"trn\":\"rei/zero\",\"val\":\"零\",\"key\":\"0\"},{\"trn\":\"ichi\",\"val\":\"一\",\"key\":\"1\"},{\"trn\":\"ni\",\"val\":\"二\",\"key\":\"2\"},{\"trn\":\"san\",\"val\":\"三\",\"key\":\"3\"},{\"trn\":\"shi/yon\",\"val\":\"四\",\"key\":\"4\"},{\"trn\":\"go\",\"val\":\"五\",\"key\":\"5\"},{\"trn\":\"roku\",\"val\":\"六\",\"key\":\"6\"},{\"trn\":\"nana/shichi\",\"val\":\"七\",\"key\":\"7\"},{\"trn\":\"hachi\",\"val\":\"八\",\"key\":\"8\"},{\"trn\":\"kyū\",\"val\":\"九\",\"key\":\"9\"},{\"trn\":\"jū\",\"val\":\"十\",\"key\":\"10\"},{\"trn\":\"nijū\",\"val\":\"二十\",\"key\":\"20\"},{\"trn\":\"sanjū\",\"val\":\"三十\",\"key\":\"30\"},{\"trn\":\"yonjū\",\"val\":\"四十\",\"key\":\"40\"},{\"trn\":\"gojū\",\"val\":\"五十\",\"key\":\"50\"},{\"trn\":\"hyaku\",\"val\":\"百\",\"key\":\"100\"},{\"trn\":\"gohyaku\",\"val\":\"五百\",\"key\":\"500\"},{\"trn\":\"sen\",\"val\":\"千\",\"key\":\"1000\"},{\"trn\":\"sanzen\",\"val\":\"三千\",\"key\":\"3000\"},{\"trn\":\"gosen\",\"val\":\"五千\",\"key\":\"5000\"},{\"trn\":\"ichiman\",\"val\":\"一万/萬\",\"key\":\"10000\"}],\"Polite Phrases\":,[{\"trn\":\"arigatou gozaimasu\",\"val\":\"ありがとうございます\",\"key\":\"Thank you\"},{\"trn\":\"gomen-nasai\",\"val\":\"ごめんなさい\",\"key\":\"I'm sorry\"},{\"trn\":\"sumimasen\",\"val\":\"すみません\",\"key\":\"Excuse me/I'm sorry\"},{\"trn\":\"domo arigatou gozaimasu\",\"val\":\"どうもありがとうございます\",\"key\":\"Thank you very much\"},{\"trn\":\"itadakimasu\",\"val\":\"いただきます\",\"key\":\"Thanks for the food\"}],\"Greetings\":,[{\"trn\":\"Matthew desu\",\"val\":\"マシューです\",\"key\":\"I'm Matthew\"},{\"trn\":\"hajimemashite\",\"val\":\"はじめまして\",\"key\":\"How do you do\"},{\"trn\":\"anata no namae wa nandesuka\",\"val\":\"あなたの名前は何ですか\",\"key\":\"What's your name\"},{\"trn\":\"yoroshiku onegai shimasu\",\"val\":\"よろしくお願いします\",\"key\":\"I'm pleased to meet you\"},{\"trn\":\"ohayou gozaimasu\",\"val\":\"おはようございます\",\"key\":\"Good morning\"},{\"trn\":\"kon-nichi-wa\",\"val\":\"こんにちは\",\"key\":\"Hello/Good Afternoon\"},{\"trn\":\"kon-ban-wa\",\"val\":\"こんばんは\",\"key\":\"Good Evening\"},{\"trn\":\"oyasuminasai\",\"val\":\"おやすみなさい\",\"key\":\"Good Night\"},{\"trn\":\"sayōnara\",\"val\":\"さようなら\",\"key\":\"GoodBye\"},{\"trn\":\"mata aimashō\",\"val\":\"また会いましょう\",\"key\":\"See you again\"},{\"trn\":\"dewamata\",\"val\":\"ではまた\",\"key\":\"See you later\"},{\"trn\":\"mata ashita\",\"val\":\"また明日\",\"key\":\"See you Tomorrow\"}],\"Questions\":,[{\"trn\":\"dare desu ka\",\"val\":\"誰ですか\",\"key\":\"Who is it\"},{\"trn\":\"dō desu ka\",\"val\":\"どうですか\",\"key\":\"How is it\"},{\"trn\":\"doko desu ka\",\"val\":\"どこですか\",\"key\":\"Where is it\"},{\"trn\":\"doushita\",\"val\":\"どうした\",\"key\":\"What happened\"},{\"trn\":\"dōshite\",\"val\":\"どうして\",\"key\":\"Why\"},{\"trn\":\"dore\",\"val\":\"どれ\",\"key\":\"Which one\"},{\"trn\":\"ikura desu ka\",\"val\":\"いくらですか\",\"key\":\"How much is this?\"},{\"trn\":\"itsu\",\"val\":\"いつ\",\"key\":\"When\"},{\"trn\":\"nan desu ka\",\"val\":\"何ですか\",\"key\":\"What is it\"},{\"trn\":\"ima nanji desu ka\",\"val\":\"今何時ですか\",\"key\":\"What time is it\"}],\"Locations\":,[{\"trn\":\"hoteru\",\"val\":\"ホテル\",\"key\":\"Hotel\"},{\"trn\":\"kuukou\",\"val\":\"空港\",\"key\":\"Airport\"},{\"trn\":\"eki\",\"val\":\"駅\",\"key\":\"Station\"},{\"trn\":\"nihon/nippon\",\"val\":\"日本\",\"key\":\"Japan\"},{\"trn\":\"daigaku\",\"val\":\"大学\",\"key\":\"university\"},{\"trn\":\"takushi\",\"val\":\"タクシー\",\"key\":\"Taxi\"}],\"Verbs\":,[{\"trn\":\"iku\",\"val\":\"行く\",\"key\":\"To Go\"},{\"trn\":\"kaeru\",\"val\":\"帰る\",\"key\":\"To Return\"},{\"trn\":\"taberu\",\"val\":\"食べる\",\"key\":\"Eat\"},{\"trn\":\"yaru/suru\",\"val\":\"やる・する\",\"key\":\"To Do\"},{\"trn\":\"miru\",\"val\":\"見る\",\"key\":\"To See\"},{\"trn\":\"kau\",\"val\":\"買う\",\"key\":\"To Buy\"},{\"trn\":\"matsu\",\"val\":\"待つ\",\"key\":\"To Wait\"},{\"trn\":\"tomaru\",\"val\":\"止まる\",\"key\":\"To Stop\"},{\"trn\":\"oshieru\",\"val\":\"教える\",\"key\":\"Teach\"},{\"trn\":\"hanasu\",\"val\":\"話す\",\"key\":\"To Speak\"}]}}",
                """
                        {
                           "id":"japanese_cheat_sheet",
                           "name":"Basic Japanese",
                           "description":"A guide to basic Japanese",
                           "metadata":{
                              "sourceName":"Tofugu",
                              "sourceUrl":"https://www.tofugu.com/japanese/important-japanese-words/"
                           },
                           "template_type":"language",
                           "section_order":[
                              "Numbers",
                              "Polite Phrases",
                              "Greetings",
                              "Questions",
                              "Locations",
                              "Verbs"
                           ],
                           "sections":{
                              "Numbers":[
                                 {
                                    "trn":"rei/zero",
                                    "val":"零",
                                    "key":"0"
                                 },
                                 {
                                    "trn":"ichi",
                                    "val":"一",
                                    "key":"1"
                                 },
                                 {
                                    "trn":"ni",
                                    "val":"二",
                                    "key":"2"
                                 },
                                 {
                                    "trn":"san",
                                    "val":"三",
                                    "key":"3"
                                 },
                                 {
                                    "trn":"shi/yon",
                                    "val":"四",
                                    "key":"4"
                                 },
                                 {
                                    "trn":"go",
                                    "val":"五",
                                    "key":"5"
                                 },
                                 {
                                    "trn":"roku",
                                    "val":"六",
                                    "key":"6"
                                 },
                                 {
                                    "trn":"nana/shichi",
                                    "val":"七",
                                    "key":"7"
                                 },
                                 {
                                    "trn":"hachi",
                                    "val":"八",
                                    "key":"8"
                                 },
                                 {
                                    "trn":"kyū",
                                    "val":"九",
                                    "key":"9"
                                 },
                                 {
                                    "trn":"jū",
                                    "val":"十",
                                    "key":"10"
                                 },
                                 {
                                    "trn":"nijū",
                                    "val":"二十",
                                    "key":"20"
                                 },
                                 {
                                    "trn":"sanjū",
                                    "val":"三十",
                                    "key":"30"
                                 },
                                 {
                                    "trn":"yonjū",
                                    "val":"四十",
                                    "key":"40"
                                 },
                                 {
                                    "trn":"gojū",
                                    "val":"五十",
                                    "key":"50"
                                 },
                                 {
                                    "trn":"hyaku",
                                    "val":"百",
                                    "key":"100"
                                 },
                                 {
                                    "trn":"gohyaku",
                                    "val":"五百",
                                    "key":"500"
                                 },
                                 {
                                    "trn":"sen",
                                    "val":"千",
                                    "key":"1000"
                                 },
                                 {
                                    "trn":"sanzen",
                                    "val":"三千",
                                    "key":"3000"
                                 },
                                 {
                                    "trn":"gosen",
                                    "val":"五千",
                                    "key":"5000"
                                 },
                                 {
                                    "trn":"ichiman",
                                    "val":"一万/萬",
                                    "key":"10000"
                                 }
                              ],
                              "Polite Phrases":[
                                 {
                                    "trn":"arigatou gozaimasu",
                                    "val":"ありがとうございます",
                                    "key":"Thank you"
                                 },
                                 {
                                    "trn":"gomen-nasai",
                                    "val":"ごめんなさい",
                                    "key":"I'm sorry"
                                 },
                                 {
                                    "trn":"sumimasen",
                                    "val":"すみません",
                                    "key":"Excuse me/I'm sorry"
                                 },
                                 {
                                    "trn":"domo arigatou gozaimasu",
                                    "val":"どうもありがとうございます",
                                    "key":"Thank you very much"
                                 },
                                 {
                                    "trn":"itadakimasu",
                                    "val":"いただきます",
                                    "key":"Thanks for the food"
                                 }
                              ],
                              "Greetings":[
                                 {
                                    "trn":"Matthew desu",
                                    "val":"マシューです",
                                    "key":"I'm Matthew"
                                 },
                                 {
                                    "trn":"hajimemashite",
                                    "val":"はじめまして",
                                    "key":"How do you do"
                                 },
                                 {
                                    "trn":"anata no namae wa nandesuka",
                                    "val":"あなたの名前は何ですか",
                                    "key":"What's your name"
                                 },
                                 {
                                    "trn":"yoroshiku onegai shimasu",
                                    "val":"よろしくお願いします",
                                    "key":"I'm pleased to meet you"
                                 },
                                 {
                                    "trn":"ohayou gozaimasu",
                                    "val":"おはようございます",
                                    "key":"Good morning"
                                 },
                                 {
                                    "trn":"kon-nichi-wa",
                                    "val":"こんにちは",
                                    "key":"Hello/Good Afternoon"
                                 },
                                 {
                                    "trn":"kon-ban-wa",
                                    "val":"こんばんは",
                                    "key":"Good Evening"
                                 },
                                 {
                                    "trn":"oyasuminasai",
                                    "val":"おやすみなさい",
                                    "key":"Good Night"
                                 },
                                 {
                                    "trn":"sayōnara",
                                    "val":"さようなら",
                                    "key":"GoodBye"
                                 },
                                 {
                                    "trn":"mata aimashō",
                                    "val":"また会いましょう",
                                    "key":"See you again"
                                 },
                                 {
                                    "trn":"dewamata",
                                    "val":"ではまた",
                                    "key":"See you later"
                                 },
                                 {
                                    "trn":"mata ashita",
                                    "val":"また明日",
                                    "key":"See you Tomorrow"
                                 }
                              ],
                              "Questions":[
                                 {
                                    "trn":"dare desu ka",
                                    "val":"誰ですか",
                                    "key":"Who is it"
                                 },
                                 {
                                    "trn":"dō desu ka",
                                    "val":"どうですか",
                                    "key":"How is it"
                                 },
                                 {
                                    "trn":"doko desu ka",
                                    "val":"どこですか",
                                    "key":"Where is it"
                                 },
                                 {
                                    "trn":"doushita",
                                    "val":"どうした",
                                    "key":"What happened"
                                 },
                                 {
                                    "trn":"dōshite",
                                    "val":"どうして",
                                    "key":"Why"
                                 },
                                 {
                                    "trn":"dore",
                                    "val":"どれ",
                                    "key":"Which one"
                                 },
                                 {
                                    "trn":"ikura desu ka",
                                    "val":"いくらですか",
                                    "key":"How much is this?"
                                 },
                                 {
                                    "trn":"itsu",
                                    "val":"いつ",
                                    "key":"When"
                                 },
                                 {
                                    "trn":"nan desu ka",
                                    "val":"何ですか",
                                    "key":"What is it"
                                 },
                                 {
                                    "trn":"ima nanji desu ka",
                                    "val":"今何時ですか",
                                    "key":"What time is it"
                                 }
                              ],
                              "Locations":[
                                 {
                                    "trn":"hoteru",
                                    "val":"ホテル",
                                    "key":"Hotel"
                                 },
                                 {
                                    "trn":"kuukou",
                                    "val":"空港",
                                    "key":"Airport"
                                 },
                                 {
                                    "trn":"eki",
                                    "val":"駅",
                                    "key":"Station"
                                 },
                                 {
                                    "trn":"nihon/nippon",
                                    "val":"日本",
                                    "key":"Japan"
                                 },
                                 {
                                    "trn":"daigaku",
                                    "val":"大学",
                                    "key":"university"
                                 },
                                 {
                                    "trn":"takushi",
                                    "val":"タクシー",
                                    "key":"Taxi"
                                 }
                              ],
                              "Verbs":[
                                 {
                                    "trn":"iku",
                                    "val":"行く",
                                    "key":"To Go"
                                 },
                                 {
                                    "trn":"kaeru",
                                    "val":"帰る",
                                    "key":"To Return"
                                 },
                                 {
                                    "trn":"taberu",
                                    "val":"食べる",
                                    "key":"Eat"
                                 },
                                 {
                                    "trn":"yaru/suru",
                                    "val":"やる・する",
                                    "key":"To Do"
                                 },
                                 {
                                    "trn":"miru",
                                    "val":"見る",
                                    "key":"To See"
                                 },
                                 {
                                    "trn":"kau",
                                    "val":"買う",
                                    "key":"To Buy"
                                 },
                                 {
                                    "trn":"matsu",
                                    "val":"待つ",
                                    "key":"To Wait"
                                 },
                                 {
                                    "trn":"tomaru",
                                    "val":"止まる",
                                    "key":"To Stop"
                                 },
                                 {
                                    "trn":"oshieru",
                                    "val":"教える",
                                    "key":"Teach"
                                 },
                                 {
                                    "trn":"hanasu",
                                    "val":"話す",
                                    "key":"To Speak"
                                 }
                              ]
                           }
                        }"""
        );
    }

    @Test
    public void testWrongQuote() {
        assertError("Unexpected symbol", 10, "{\"x\": \"a\"bc\",}");
    }

    private void assertError(String expected, int expectedPosition, String input) {
        int len = input.length();
        long address = TestUtils.toMemory(input);
        try (JsonLexer lexer = new JsonLexer(4, 4)) {
            for (int i = 0; i < len; i++) {
                try {
                    listener.clear();
                    lexer.clear();
                    lexer.parse(address, address + i, listener);
                    lexer.parse(address + i, address + len, listener);
                    lexer.parseLast();
                    Assert.fail();
                } catch (JsonException e) {
                    TestUtils.assertEquals(expected, e.getFlyweightMessage());
                    Assert.assertEquals(expectedPosition, e.getPosition());
                }
            }
        } finally {
            Unsafe.free(address, len, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertThat(String expected, String input) throws Exception {
        assertThat(expected, input, false);
    }

    private void assertThat(String expected, String input, boolean recordPositions) throws Exception {
        byte[] bytes = input.getBytes(Files.UTF_8);
        int len = bytes.length;
        long address = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < len; i++) {
            Unsafe.putByte(address + i, bytes[i]);
        }
        try {
            listener.recordPositions = recordPositions;

            for (int i = 0; i < len; i++) {
                listener.clear();
                LEXER.clear();
                LEXER.parse(address, address + i, listener);
                LEXER.parse(address + i, address + len, listener);
                LEXER.parseLast();
                TestUtils.assertEquals(expected, listener.value());
            }
        } finally {
            Unsafe.free(address, len, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static class JsonAssemblingParser implements JsonParser, Mutable {
        private final StringBuffer buffer = new StringBuffer();
        private final IntStack itemCountStack = new IntStack();
        private int itemCount = 0;
        private boolean recordPositions = false;

        @Override
        public void clear() {
            buffer.setLength(0);
            itemCount = 0;
            itemCountStack.clear();
        }

        @Override
        public void onEvent(int code, CharSequence tag, int position) {
            if (recordPositions) {
                buffer.append('<').append(position).append('>');
            }
            switch (code) {
                case JsonLexer.EVT_OBJ_START:
                    if (itemCount++ > 0) {
                        buffer.append(',');
                    }
                    buffer.append('{');
                    itemCountStack.push(itemCount);
                    itemCount = 0;
                    break;
                case JsonLexer.EVT_OBJ_END:
                    buffer.append('}');
                    itemCount = itemCountStack.pop();
                    break;
                case JsonLexer.EVT_ARRAY_START:
                    if (itemCount++ > 0) {
                        buffer.append(',');
                    }
                    buffer.append('[');
                    itemCountStack.push(itemCount);
                    itemCount = 0;
                    break;
                case JsonLexer.EVT_ARRAY_END:
                    itemCount = itemCountStack.pop();
                    buffer.append(']');
                    break;
                case JsonLexer.EVT_NAME:
                    if (itemCount > 0) {
                        buffer.append(',');
                    }
                    buffer.append('"');
                    buffer.append(tag);
                    buffer.append('"');
                    buffer.append(':');
                    break;
                case JsonLexer.EVT_VALUE:
                    buffer.append('"');
                    buffer.append(tag);
                    buffer.append('"');
                    itemCount++;
                    break;
                case JsonLexer.EVT_ARRAY_VALUE:
                    if (itemCount++ > 0) {
                        buffer.append(',');
                    }
                    buffer.append('"');
                    buffer.append(tag);
                    buffer.append('"');
                    break;
                default:
                    break;
            }
        }

        public CharSequence value() {
            return buffer;
        }
    }

    private static final class NoOpParser implements JsonParser {
        @Override
        public void onEvent(int code, CharSequence tag, int position) {
        }
    }

    static {
        // needed to prevent a false positive in memory leak detection
        LogFactory.getLog(ServerMainVectorGroupByTest.class);
    }
}
