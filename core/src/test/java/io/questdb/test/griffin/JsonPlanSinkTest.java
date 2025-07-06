/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.JsonPlanSink;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

public class JsonPlanSinkTest {

    @Test
    public void testSink() {
        String expected = "[\n" +
                "  {\n" +
                "    \"Plan\": {\n" +
                "        \"Node Type\": \"test\",\n" +
                "        \"geohash\": \"\"00000000000000011000101010010010\"\",\n" +
                "        \"long256\": \"0x04000000000000000300000000000000020000000000000001\",\n" +
                "        \"plan\": \"null\",\n" +
                "        \"uuid\": \"00000000-0000-0002-0000-000000000001\",\n" +
                "        \"ipv4\": \"0.0.0.1\",\n" +
                "        \"uuid_null\": \"null\"\n" +
                "    }\n" +
                "  }\n" +
                "]";

        JsonPlanSink sink = new JsonPlanSink();
        sink.of(new TestFactory(), null);
        Assert.assertEquals(expected, sink.getSink().toString());
    }

    @Test
    public void testSinkWithNumericalValues() {
        String expected = "[\n" +
                "  {\n" +
                "    \"Plan\": {\n" +
                "        \"Node Type\": \"test\",\n" +
                "        \"testString\": \"data\",\n" +
                "        \"double\":  0.0,\n" +
                "        \"float\":  1.0,\n" +
                "        \"int\":  2,\n" +
                "        \"long\":  100\n" +
                "    }\n" +
                "  }\n" +
                "]";

        JsonPlanSink sink = new JsonPlanSink();
        sink.of(new NumericalTestFactory(), null);
        Assert.assertEquals(expected, sink.getSink().toString());
    }

    static class NumericalTestFactory implements RecordCursorFactory {

        @Override
        public RecordMetadata getMetadata() {
            return null;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type("test");
            sink.attr("testString");
            sink.val("data");
            sink.attr("double");
            sink.val(0.0d);
            sink.attr("float");
            sink.val(1.0f);
            sink.attr("int");
            sink.val(2);
            sink.attr("long");
            sink.val(100L);
        }
    }

    static class TestFactory implements RecordCursorFactory {

        @Override
        public RecordMetadata getMetadata() {
            return null;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type("test");
            sink.attr("geohash");
            sink.val(101010L, 32);
            sink.attr("long256");
            sink.valLong256(1L, 2L, 3L, 4L);
            sink.attr("plan");
            sink.val((Plannable) null);
            sink.attr("uuid");
            sink.valUuid(1L, 2L);
            sink.attr("ipv4");
            sink.valIPv4(1);
            sink.attr("uuid_null");
            sink.valUuid(Numbers.LONG_NULL, Numbers.LONG_NULL);
        }
    }
}
