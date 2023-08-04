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

package io.questdb.test.cutlass.text.types;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.text.DefaultTextConfiguration;
import io.questdb.cutlass.text.TextConfiguration;
import io.questdb.cutlass.text.types.InputFormatConfiguration;
import io.questdb.cutlass.text.types.TypeManager;
import io.questdb.std.Misc;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.microtime.TimestampFormatFactory;
import io.questdb.std.datetime.millitime.DateFormatFactory;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.DirectCharSink;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

import java.io.File;
import java.io.IOException;

public class TypeManagerTest extends AbstractTest {
    private static JsonLexer jsonLexer;
    private static DirectCharSink utf8Sink;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractTest.setUpStatic();
        utf8Sink = new DirectCharSink(64);
        jsonLexer = new JsonLexer(1024, 2048);
    }

    @AfterClass
    public static void tearDownStatic() throws Exception {
        Misc.free(utf8Sink);
        Misc.free(jsonLexer);
        AbstractTest.tearDownStatic();
    }

    @Before
    public void setUp2() {
        jsonLexer.clear();
    }

    @Test
    public void testDateFormatArray() {
        assertFailure("/textloader/types/date_format_array.json", 127, "format value expected (array)");
    }

    @Test
    public void testDateFormatNull() {
        assertFailure("/textloader/types/date_format_null.json", 37, "null format");
    }

    @Test
    public void testDateFormatObj() {
        assertFailure("/textloader/types/date_format_obj.json", 127, "format value expected (obj)");
    }

    @Test
    public void testDateLocaleArray() {
        assertFailure("/textloader/types/date_locale_array.json", 171, "locale value expected (array)");
    }

    @Test
    public void testDateLocaleObj() {
        assertFailure("/textloader/types/date_locale_obj.json", 171, "locale value expected (obj)");
    }

    @Test
    public void testDateMissingFormat() {
        assertFailure("/textloader/types/date_missing_format.json", 49, "date format is missing");
    }

    @Test
    public void testDateObj() {
        assertFailure("/textloader/types/date_obj.json", 13, "array expected (obj)");
    }

    @Test
    public void testDateUnknownLocale() {
        assertFailure("/textloader/types/date_unknown_locale.json", 65, "invalid [locale=zyx]");
    }

    @Test
    public void testDateUnknownTag() {
        assertFailure("/textloader/types/date_unknown_tag.json", 55, "unknown [tag=whatsup]");
    }

    @Test
    public void testDateValue() {
        assertFailure("/textloader/types/date_value.json", 14, "array expected (value)");
    }

    @Test
    public void testDefaultFileName() throws JsonException, IOException {
        File configFile = new File(root, "text_loader.json");
        TestUtils.writeStringToFile(configFile, "{\n}\n");
        TypeManager typeManager = createTypeManager("/text_loader.json");
        Assert.assertEquals("[CHAR,INT,LONG,DOUBLE,BOOLEAN,LONG256,UUID,IPv4]", typeManager.getAllAdapters().toString());
    }

    @Test
    public void testEmpty() throws JsonException {
        TypeManager typeManager = createTypeManager("/textloader/types/empty.json");
        Assert.assertEquals("[CHAR,INT,LONG,DOUBLE,BOOLEAN,LONG256,UUID,IPv4]", typeManager.getAllAdapters().toString());
    }

    @Test
    public void testIllegalMethodParameterBinary() {
        testIllegalParameterForGetTypeAdapter(ColumnType.BINARY);
    }

    @Test
    public void testIllegalMethodParameterDate() {
        testIllegalParameterForGetTypeAdapter(ColumnType.DATE);
    }

    @Test
    public void testIllegalMethodParameterTimestamp() {
        testIllegalParameterForGetTypeAdapter(ColumnType.TIMESTAMP);
    }

    @Test
    public void testIllegalMethodParameterGeoInt() {
        testIllegalParameterForGetTypeAdapter(ColumnType.GEOINT);
    }

    @Test
    public void testNonDefaultFileName() throws JsonException, IOException {
        File configFile = new File(root, "my_awesome_text_loader.json");
        TestUtils.writeStringToFile(configFile, "{\n}\n");
        TypeManager typeManager = createTypeManager("/my_awesome_text_loader.json");
        Assert.assertEquals("[CHAR,INT,LONG,DOUBLE,BOOLEAN,LONG256,UUID,IPv4]", typeManager.getAllAdapters().toString());
    }

    @Test
    public void testResourceNotFound() {
        assertFailure("/textloader/types/not_found.json", 0, "could not find input format config");
    }

    @Test
    public void testTimestampFormatArray() {
        assertFailure("/textloader/types/timestamp_format_array.json", 243, "format value expected (array)");
    }

    @Test
    public void testTimestampFormatNull() {
        assertFailure("/textloader/types/timestamp_format_null.json", 229, "null format");
    }

    @Test
    public void testTimestampFormatObj() {
        assertFailure("/textloader/types/timestamp_format_obj.json", 243, "format value expected (obj)");
    }

    @Test
    public void testTimestampLocaleArray() {
        assertFailure("/textloader/types/timestamp_locale_array.json", 287, "locale value expected (array)");
    }

    @Test
    public void testTimestampLocaleObj() {
        assertFailure("/textloader/types/timestamp_locale_obj.json", 366, "locale value expected (obj)");
    }

    @Test
    public void testTimestampMissingFormat() {
        assertFailure("/textloader/types/timestamp_missing_format.json", 241, "timestamp format is missing");
    }

    @Test
    public void testTimestampObj() {
        assertFailure("/textloader/types/timestamp_obj.json", 244, "array expected (obj)");
    }

    @Test
    public void testTimestampUnknownLocale() {
        assertFailure("/textloader/types/timestamp_unknown_locale.json", 313, "invalid [locale=zyx]");
    }

    @Test
    public void testTimestampUnknownTag() {
        assertFailure("/textloader/types/timestamp_unknown_tag.json", 303, "unknown [tag=ehlo]");
    }

    @Test
    public void testTimestampValue() {
        assertFailure("/textloader/types/timestamp_value.json", 245, "array expected (value)");
    }

    @Test
    public void testUnknownTopLevelProp() {
        assertFailure("/textloader/types/unknown_top_level_prop.json", 309, "'date' and/or 'timestamp' expected");
    }

    private void assertFailure(String resourceName, int position, CharSequence text) {
        try {
            createTypeManager(resourceName);
            Assert.fail("has to fail");
        } catch (JsonException e) {
            Assert.assertEquals(position, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), text);
        }
    }

    private TypeManager createTypeManager(String fileResource) throws JsonException {
        InputFormatConfiguration inputFormatConfiguration = new InputFormatConfiguration(
                new DateFormatFactory(),
                DateLocaleFactory.INSTANCE,
                new TimestampFormatFactory(),
                DateFormatUtils.enLocale
        );

        inputFormatConfiguration.parseConfiguration(getClass(), jsonLexer, root, fileResource);
        return new TypeManager(new DefaultTextConfiguration(getClass(), root, fileResource), utf8Sink);
    }

    private void testIllegalParameterForGetTypeAdapter(int columnType) {
        TextConfiguration textConfiguration = new DefaultTextConfiguration();
        TypeManager typeManager = new TypeManager(textConfiguration, utf8Sink);
        try {
            typeManager.getTypeAdapter(columnType);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "no adapter for type");
        }
    }
}