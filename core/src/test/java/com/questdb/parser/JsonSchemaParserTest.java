package com.questdb.parser;

import com.questdb.BootstrapEnv;
import com.questdb.misc.Unsafe;
import com.questdb.parser.json.JsonException;
import com.questdb.parser.json.JsonLexer;
import com.questdb.std.ObjList;
import com.questdb.std.time.DateFormatFactory;
import com.questdb.std.time.DateLocaleFactory;
import com.questdb.std.time.TimeZoneRuleFactory;
import com.questdb.test.tools.TestUtils;
import org.junit.*;

public class JsonSchemaParserTest {
    private static final JsonLexer LEXER = new JsonLexer(1024);
    private static JsonSchemaParser jsonSchemaParser;
    private static String defaultLocaleId;

    @BeforeClass
    public static void setUpClass() throws Exception {
        BootstrapEnv env = new BootstrapEnv();
        env.dateFormatFactory = new DateFormatFactory();
        env.dateLocaleFactory = new DateLocaleFactory(new TimeZoneRuleFactory());
        defaultLocaleId = env.dateLocaleFactory.getDefaultDateLocale().getId();
        jsonSchemaParser = new JsonSchemaParser(env);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        LEXER.close();
    }

    @Before
    public void setUp() throws Exception {
        LEXER.clear();
        jsonSchemaParser.clear();
    }

    @Test
    public void testArrayProperty() throws Exception {
        String in = "[\n" +
                "{\"name\": \"x\", \"type\": \"DOUBLE\", \"pattern\":\"xyz\", \"locale\": []}\n" +
                "]";
        try {
            parseMetadata(in);
            Assert.fail();
        } catch (JsonException e) {
            Assert.assertEquals("Unexpected array", e.getMessage());
            Assert.assertEquals(62, e.getPosition());
        }
    }

    @Test
    public void testCorrectSchema() throws Exception {
        String in = "[\n" +
                "{\"name\": \"x\", \"type\": \"INT\", \"pattern\":\"xyz\", \"locale\": \"en-US\"},\n" +
                "{\"name\": \"y\", \"type\": \"DATE\", \"pattern\":\"xyz\"}\n" +
                "]";

        ObjList<ImportedColumnMetadata> metadata = parseMetadata(in);
        Assert.assertEquals(2, metadata.size());
        Assert.assertEquals("ImportedColumnMetadata{importedColumnType=INT, pattern=xyz, dateLocale=en-US, name=x}", metadata.get(0).toString());
        Assert.assertEquals("ImportedColumnMetadata{importedColumnType=DATE, pattern=xyz, dateLocale=" + defaultLocaleId + ", name=y}", metadata.get(1).toString());
    }

    @Test
    public void testEmptyList() throws Exception {
        Assert.assertEquals(0, parseMetadata("[]").size());
    }

    @Test
    public void testEmptyObject() throws Exception {
        try {
            parseMetadata("[{}]");
            Assert.fail();
        } catch (JsonException e) {
            Assert.assertEquals(3, e.getPosition());
        }
    }

    @Test
    public void testMissingName() throws Exception {
        String in = "[\n" +
                "{\"name\": \"x\", \"type\": \"INT\", \"pattern\":\"xyz\", \"locale\": \"en-GB\"},\n" +
                "{\"type\": \"DOUBLE\", \"pattern\":\"xyz\"}\n" +
                "]";

        try {
            parseMetadata(in);
            Assert.fail();
        } catch (JsonException e) {
            Assert.assertEquals("Missing 'name' property", e.getMessage());
            Assert.assertEquals(103, e.getPosition());
        }
    }

    @Test
    public void testMissingType() throws Exception {
        String in = "[\n" +
                "{\"name\": \"x\", \"pattern\":\"xyz\", \"locale\": \"en-GB\"},\n" +
                "{\"name\": \"y\", \"type\": \"DOUBLE\", \"pattern\":\"xyz\"}\n" +
                "]";
        try {
            parseMetadata(in);
            Assert.fail();
        } catch (JsonException e) {
            Assert.assertEquals("Missing 'type' property", e.getMessage());
            Assert.assertEquals(51, e.getPosition());
        }
    }

    @Test
    public void testNonArray() throws Exception {
        try {
            parseMetadata("{}");
            Assert.fail();
        } catch (JsonException e) {
            Assert.assertEquals("Unexpected object", e.getMessage());
            Assert.assertEquals(1, e.getPosition());
        }
    }

    @Test
    public void testNonObjectArrayMember() throws Exception {
        String in = "[2,\n" +
                "{\"name\": \"x\", \"type\": \"DOUBLE\", \"pattern\":\"xyz\"}\n" +
                "]";
        try {
            parseMetadata(in);
            Assert.fail();
        } catch (JsonException e) {
            Assert.assertEquals("Must be an object", e.getMessage());
            Assert.assertEquals(2, e.getPosition());
        }
    }

    @Test
    public void testWrongDateLocale() throws Exception {
        String in = "[\n" +
                "{\"name\": \"x\", \"type\": \"DOUBLE\", \"pattern\":\"xyz\", \"locale\": \"enk\"}\n" +
                "]";
        try {
            parseMetadata(in);
            Assert.fail();
        } catch (JsonException e) {
            Assert.assertEquals("Invalid date locale", e.getMessage());
            Assert.assertEquals(63, e.getPosition());
        }
    }

    @Test
    public void testWrongType() throws Exception {
        String in = "[\n" +
                "{\"name\": \"y\", \"type\": \"ABC\", \"pattern\":\"xyz\"}\n" +
                "]";
        try {
            parseMetadata(in);
            Assert.fail();
        } catch (JsonException e) {
            Assert.assertEquals("Invalid type", e.getMessage());
            Assert.assertEquals(26, e.getPosition());
        }
    }

    private ObjList<ImportedColumnMetadata> parseMetadata(CharSequence in) throws JsonException {
        long buf = TestUtils.toMemory(in);
        try {
            LEXER.parse(buf, in.length(), jsonSchemaParser);
            return jsonSchemaParser.getMetadata();
        } finally {
            Unsafe.free(buf, in.length());
        }
    }
}