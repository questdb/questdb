/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.line.interop;

import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;

public final class Column {
    private final DataType type;
    private final String name;
    private final Object value;

    private Column(DataType type, String name, Object value) {
        this.type = type;
        this.name = name;
        this.value = value;
    }

    public DataType getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public String valueAsString() {
        if (type != DataType.STRING) {
            throw new IllegalStateException("type is " + type);
        }
        return (String) value;
    }

    public long valueAsLong() {
        if (type != DataType.LONG) {
            throw new IllegalStateException("type is " + type);
        }
        return (long) value;
    }

    public boolean valueAsBoolean() {
        if (type != DataType.BOOLEAN) {
            throw new IllegalStateException("type is " + type);
        }
        return (boolean) value;
    }

    public double valueAsDouble() {
        if (type != DataType.DOUBLE) {
            throw new IllegalStateException("type is " + type);
        }
        return (double) value;
    }

    public static Column fromJson(JsonObject jsonObject) {
        String name = jsonObject.get("name").asString();
        DataType dataType = DataType.valueOf(jsonObject.get("type").asString());
        JsonValue jsonValue = jsonObject.get("value");
        Object value;
        switch (dataType) {
            case STRING:
                value = jsonValue.asString();
                break;
            case LONG:
                value = jsonValue.asLong();
                break;
            case BOOLEAN:
                value = jsonValue.asBoolean();
                break;
            case DOUBLE:
                value = jsonValue.asDouble();
                break;
            default:
                throw new IllegalStateException("missing implementation " + dataType);
        }
        return new Column(dataType, name, value);
    }
}
