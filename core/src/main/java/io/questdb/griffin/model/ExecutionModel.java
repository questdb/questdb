/*******************************************************************************
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

package io.questdb.griffin.model;

import org.jetbrains.annotations.Nullable;

public interface ExecutionModel {
    int QUERY = 1;                          // 1
    int CREATE_TABLE = QUERY + 1;           // 2
    int RENAME_TABLE = CREATE_TABLE + 1;    // 3
    int INSERT = RENAME_TABLE + 1;          // 4
    int COPY = INSERT + 1;                  // 5
    int UPDATE = COPY + 1;                  // 6
    int EXPLAIN = UPDATE + 1;               // 7
    int CREATE_MAT_VIEW = EXPLAIN + 1;      // 8
    int CREATE_VIEW = CREATE_MAT_VIEW + 1;  // 9
    int COMPILE_VIEW = CREATE_VIEW + 1;     // 10
    int MAX = COMPILE_VIEW + 1;

    int getModelType();

    default QueryModel getQueryModel() {
        return null;
    }

    default @Nullable CharSequence getSelectText() {
        return null;
    }

    default CharSequence getTableName() {
        return null;
    }

    default ExpressionNode getTableNameExpr() {
        return null;
    }

    default String getTypeName() {
        return Inner.typeNameMap[getModelType()];
    }

    class Inner {
        private static final String[] typeNameMap = new String[ExecutionModel.MAX];

        static {
            typeNameMap[ExecutionModel.QUERY] = "Query";
            typeNameMap[ExecutionModel.CREATE_TABLE] = "Create";
            typeNameMap[ExecutionModel.RENAME_TABLE] = "Rename";
            typeNameMap[ExecutionModel.INSERT] = "Insert into";
            typeNameMap[ExecutionModel.COPY] = "Copy";
            typeNameMap[ExecutionModel.UPDATE] = "Update";
            typeNameMap[ExecutionModel.EXPLAIN] = "Explain";
            typeNameMap[ExecutionModel.CREATE_MAT_VIEW] = "Create materialized";
            typeNameMap[ExecutionModel.CREATE_VIEW] = "Create view";
            typeNameMap[ExecutionModel.COMPILE_VIEW] = "Compile view";
        }
    }
}



