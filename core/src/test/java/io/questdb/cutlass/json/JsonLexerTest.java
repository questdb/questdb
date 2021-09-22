/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cutlass.json;

import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

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
    public void tesStringTooLong() throws Exception {
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
    public void testArrayObjArray() throws Exception {
        assertThat("[{\"A\":[\"122\",\"133\"],\"x\":\"y\"},\"134\",\"abc\"]", "[\n" +
                "{\"A\":[122, 133], \"x\": \"y\"}, 134  , \"abc\"\n" +
                "]");
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
        byte[] bytesA = "{\"x\":\"违法违,控网站漏洞风\", \"y\":\"站漏洞风".getBytes(StandardCharsets.UTF_8);
        byte[] bytesB = {-116, -76, -55, 55, -34, 0, -11, 15, 13};
        byte[] bytesC = "\"}".getBytes(StandardCharsets.UTF_8);

        byte[] bytes = new byte[bytesA.length + bytesB.length + bytesC.length];
        System.arraycopy(bytesA, 0, bytes, 0, bytesA.length);
        System.arraycopy(bytesB, 0, bytes, bytesA.length, bytesB.length);
        System.arraycopy(bytesC, 0, bytes, bytesA.length + bytesB.length, bytesC.length);


        int len = bytes.length;
        long address = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(address + i, bytes[i]);
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
        String path = JsonLexerTest.class.getResource("/json/test.json").getPath();

        try (Path p = new Path()) {
            if (Os.type == Os.WINDOWS && path.startsWith("/")) {
                p.of(path.substring(1));
            } else {
                p.of(path);
            }
            long l = Files.length(p.$());
            long fd = Files.openRO(p);
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
                            Unsafe.getUnsafe().copyMemory(buf, bufA, i);
                            Unsafe.getUnsafe().copyMemory(buf + i, bufB, l - i);
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
                Files.close(fd);
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
    public void testUnclosedQuote() {
        assertError("Unexpected symbol", 11, "{\"a\":\"1, \"b\": 15.2}");
    }

    @Test
    public void testUnquotedNumbers() throws Exception {
        assertThat("[{\"A\":\"122\"},\"134\",\"abc\"]", "[\n" +
                "{\"A\":122}, 134  , \"abc\"\n" +
                "]");
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
                "{\n" +
                        "   \"id\":\"japanese_cheat_sheet\",\n" +
                        "   \"name\":\"Basic Japanese\",\n" +
                        "   \"description\":\"A guide to basic Japanese\",\n" +
                        "   \"metadata\":{\n" +
                        "      \"sourceName\":\"Tofugu\",\n" +
                        "      \"sourceUrl\":\"https://www.tofugu.com/japanese/important-japanese-words/\"\n" +
                        "   },\n" +
                        "   \"template_type\":\"language\",\n" +
                        "   \"section_order\":[\n" +
                        "      \"Numbers\",\n" +
                        "      \"Polite Phrases\",\n" +
                        "      \"Greetings\",\n" +
                        "      \"Questions\",\n" +
                        "      \"Locations\",\n" +
                        "      \"Verbs\"\n" +
                        "   ],\n" +
                        "   \"sections\":{\n" +
                        "      \"Numbers\":[\n" +
                        "         {\n" +
                        "            \"trn\":\"rei/zero\",\n" +
                        "            \"val\":\"零\",\n" +
                        "            \"key\":\"0\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"ichi\",\n" +
                        "            \"val\":\"一\",\n" +
                        "            \"key\":\"1\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"ni\",\n" +
                        "            \"val\":\"二\",\n" +
                        "            \"key\":\"2\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"san\",\n" +
                        "            \"val\":\"三\",\n" +
                        "            \"key\":\"3\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"shi/yon\",\n" +
                        "            \"val\":\"四\",\n" +
                        "            \"key\":\"4\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"go\",\n" +
                        "            \"val\":\"五\",\n" +
                        "            \"key\":\"5\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"roku\",\n" +
                        "            \"val\":\"六\",\n" +
                        "            \"key\":\"6\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"nana/shichi\",\n" +
                        "            \"val\":\"七\",\n" +
                        "            \"key\":\"7\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"hachi\",\n" +
                        "            \"val\":\"八\",\n" +
                        "            \"key\":\"8\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"kyū\",\n" +
                        "            \"val\":\"九\",\n" +
                        "            \"key\":\"9\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"jū\",\n" +
                        "            \"val\":\"十\",\n" +
                        "            \"key\":\"10\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"nijū\",\n" +
                        "            \"val\":\"二十\",\n" +
                        "            \"key\":\"20\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"sanjū\",\n" +
                        "            \"val\":\"三十\",\n" +
                        "            \"key\":\"30\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"yonjū\",\n" +
                        "            \"val\":\"四十\",\n" +
                        "            \"key\":\"40\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"gojū\",\n" +
                        "            \"val\":\"五十\",\n" +
                        "            \"key\":\"50\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"hyaku\",\n" +
                        "            \"val\":\"百\",\n" +
                        "            \"key\":\"100\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"gohyaku\",\n" +
                        "            \"val\":\"五百\",\n" +
                        "            \"key\":\"500\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"sen\",\n" +
                        "            \"val\":\"千\",\n" +
                        "            \"key\":\"1000\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"sanzen\",\n" +
                        "            \"val\":\"三千\",\n" +
                        "            \"key\":\"3000\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"gosen\",\n" +
                        "            \"val\":\"五千\",\n" +
                        "            \"key\":\"5000\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"ichiman\",\n" +
                        "            \"val\":\"一万/萬\",\n" +
                        "            \"key\":\"10000\"\n" +
                        "         }\n" +
                        "      ],\n" +
                        "      \"Polite Phrases\":[\n" +
                        "         {\n" +
                        "            \"trn\":\"arigatou gozaimasu\",\n" +
                        "            \"val\":\"ありがとうございます\",\n" +
                        "            \"key\":\"Thank you\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"gomen-nasai\",\n" +
                        "            \"val\":\"ごめんなさい\",\n" +
                        "            \"key\":\"I'm sorry\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"sumimasen\",\n" +
                        "            \"val\":\"すみません\",\n" +
                        "            \"key\":\"Excuse me/I'm sorry\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"domo arigatou gozaimasu\",\n" +
                        "            \"val\":\"どうもありがとうございます\",\n" +
                        "            \"key\":\"Thank you very much\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"itadakimasu\",\n" +
                        "            \"val\":\"いただきます\",\n" +
                        "            \"key\":\"Thanks for the food\"\n" +
                        "         }\n" +
                        "      ],\n" +
                        "      \"Greetings\":[\n" +
                        "         {\n" +
                        "            \"trn\":\"Matthew desu\",\n" +
                        "            \"val\":\"マシューです\",\n" +
                        "            \"key\":\"I'm Matthew\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"hajimemashite\",\n" +
                        "            \"val\":\"はじめまして\",\n" +
                        "            \"key\":\"How do you do\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"anata no namae wa nandesuka\",\n" +
                        "            \"val\":\"あなたの名前は何ですか\",\n" +
                        "            \"key\":\"What's your name\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"yoroshiku onegai shimasu\",\n" +
                        "            \"val\":\"よろしくお願いします\",\n" +
                        "            \"key\":\"I'm pleased to meet you\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"ohayou gozaimasu\",\n" +
                        "            \"val\":\"おはようございます\",\n" +
                        "            \"key\":\"Good morning\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"kon-nichi-wa\",\n" +
                        "            \"val\":\"こんにちは\",\n" +
                        "            \"key\":\"Hello/Good Afternoon\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"kon-ban-wa\",\n" +
                        "            \"val\":\"こんばんは\",\n" +
                        "            \"key\":\"Good Evening\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"oyasuminasai\",\n" +
                        "            \"val\":\"おやすみなさい\",\n" +
                        "            \"key\":\"Good Night\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"sayōnara\",\n" +
                        "            \"val\":\"さようなら\",\n" +
                        "            \"key\":\"GoodBye\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"mata aimashō\",\n" +
                        "            \"val\":\"また会いましょう\",\n" +
                        "            \"key\":\"See you again\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"dewamata\",\n" +
                        "            \"val\":\"ではまた\",\n" +
                        "            \"key\":\"See you later\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"mata ashita\",\n" +
                        "            \"val\":\"また明日\",\n" +
                        "            \"key\":\"See you Tomorrow\"\n" +
                        "         }\n" +
                        "      ],\n" +
                        "      \"Questions\":[\n" +
                        "         {\n" +
                        "            \"trn\":\"dare desu ka\",\n" +
                        "            \"val\":\"誰ですか\",\n" +
                        "            \"key\":\"Who is it\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"dō desu ka\",\n" +
                        "            \"val\":\"どうですか\",\n" +
                        "            \"key\":\"How is it\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"doko desu ka\",\n" +
                        "            \"val\":\"どこですか\",\n" +
                        "            \"key\":\"Where is it\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"doushita\",\n" +
                        "            \"val\":\"どうした\",\n" +
                        "            \"key\":\"What happened\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"dōshite\",\n" +
                        "            \"val\":\"どうして\",\n" +
                        "            \"key\":\"Why\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"dore\",\n" +
                        "            \"val\":\"どれ\",\n" +
                        "            \"key\":\"Which one\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"ikura desu ka\",\n" +
                        "            \"val\":\"いくらですか\",\n" +
                        "            \"key\":\"How much is this?\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"itsu\",\n" +
                        "            \"val\":\"いつ\",\n" +
                        "            \"key\":\"When\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"nan desu ka\",\n" +
                        "            \"val\":\"何ですか\",\n" +
                        "            \"key\":\"What is it\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"ima nanji desu ka\",\n" +
                        "            \"val\":\"今何時ですか\",\n" +
                        "            \"key\":\"What time is it\"\n" +
                        "         }\n" +
                        "      ],\n" +
                        "      \"Locations\":[\n" +
                        "         {\n" +
                        "            \"trn\":\"hoteru\",\n" +
                        "            \"val\":\"ホテル\",\n" +
                        "            \"key\":\"Hotel\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"kuukou\",\n" +
                        "            \"val\":\"空港\",\n" +
                        "            \"key\":\"Airport\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"eki\",\n" +
                        "            \"val\":\"駅\",\n" +
                        "            \"key\":\"Station\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"nihon/nippon\",\n" +
                        "            \"val\":\"日本\",\n" +
                        "            \"key\":\"Japan\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"daigaku\",\n" +
                        "            \"val\":\"大学\",\n" +
                        "            \"key\":\"university\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"takushi\",\n" +
                        "            \"val\":\"タクシー\",\n" +
                        "            \"key\":\"Taxi\"\n" +
                        "         }\n" +
                        "      ],\n" +
                        "      \"Verbs\":[\n" +
                        "         {\n" +
                        "            \"trn\":\"iku\",\n" +
                        "            \"val\":\"行く\",\n" +
                        "            \"key\":\"To Go\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"kaeru\",\n" +
                        "            \"val\":\"帰る\",\n" +
                        "            \"key\":\"To Return\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"taberu\",\n" +
                        "            \"val\":\"食べる\",\n" +
                        "            \"key\":\"Eat\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"yaru/suru\",\n" +
                        "            \"val\":\"やる・する\",\n" +
                        "            \"key\":\"To Do\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"miru\",\n" +
                        "            \"val\":\"見る\",\n" +
                        "            \"key\":\"To See\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"kau\",\n" +
                        "            \"val\":\"買う\",\n" +
                        "            \"key\":\"To Buy\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"matsu\",\n" +
                        "            \"val\":\"待つ\",\n" +
                        "            \"key\":\"To Wait\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"tomaru\",\n" +
                        "            \"val\":\"止まる\",\n" +
                        "            \"key\":\"To Stop\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"oshieru\",\n" +
                        "            \"val\":\"教える\",\n" +
                        "            \"key\":\"Teach\"\n" +
                        "         },\n" +
                        "         {\n" +
                        "            \"trn\":\"hanasu\",\n" +
                        "            \"val\":\"話す\",\n" +
                        "            \"key\":\"To Speak\"\n" +
                        "         }\n" +
                        "      ]\n" +
                        "   }\n" +
                        "}"
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
        byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
        int len = bytes.length;
        long address = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(address + i, bytes[i]);
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

    private static final class NoOpParser implements JsonParser {
        @Override
        public void onEvent(int code, CharSequence tag, int position) {
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

        public CharSequence value() {
            return buffer;
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
    }
}