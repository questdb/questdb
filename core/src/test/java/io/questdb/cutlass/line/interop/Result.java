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

public class Result {
    private final ResultStatus status;
    private final String line;

    public ResultStatus getStatus() {
        return status;
    }

    public String getLine() {
        return line;
    }

    private Result(ResultStatus status, String line) {
        this.status = status;
        this.line = line;
    }

    public static Result fromJson(JsonObject jsonObject) {
        ResultStatus status = ResultStatus.valueOf(jsonObject.get("status").asString());
        String line = null;
        if (status == ResultStatus.SUCCESS) {
            line = jsonObject.get("line").asString();
        }
        return new Result(status, line);
    }
}
