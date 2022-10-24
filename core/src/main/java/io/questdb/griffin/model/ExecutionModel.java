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

package io.questdb.griffin.model;

public interface ExecutionModel {
    int QUERY = 1;
    int CREATE_TABLE = 2;
    int RENAME_TABLE = 3;
    int INSERT = 4;
    int COPY = 5;
    int UPDATE = 6;
    int EXPLAIN = 7;
    int MAX = EXPLAIN + 1;

    int getModelType();

    default QueryModel getQueryModel() {
        return null;
    }

    default String getTypeName() {
        return Inner.typeNameMap[getModelType()];
    }

    default CharSequence getTargetTableName() {
        return null;
    }

    class Inner {
        private static final String[] typeNameMap = new String[ExecutionModel.MAX];

        static {
            typeNameMap[ExecutionModel.QUERY] = "QUERY";
            typeNameMap[ExecutionModel.CREATE_TABLE] = "CREATE_TABLE";
            typeNameMap[ExecutionModel.RENAME_TABLE] = "RENAME_TABLE";
            typeNameMap[ExecutionModel.INSERT] = "INSERT";
            typeNameMap[ExecutionModel.COPY] = "COPY";
            typeNameMap[ExecutionModel.UPDATE] = "UPDATE";
            typeNameMap[ExecutionModel.EXPLAIN] = "EXPLAIN";
        }
    }
}



