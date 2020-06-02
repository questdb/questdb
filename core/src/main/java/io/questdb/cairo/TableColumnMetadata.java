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

package io.questdb.cairo;

public class TableColumnMetadata {
    private final String name;
    private final int type;
    private final boolean symbolTableStatic;
    private int indexValueBlockCapacity;
    private boolean indexed;

    public TableColumnMetadata(String name, int type) {
        this(name, type, false, 0, false);
        // Do not allow using this constructor for symbol types.
        // Use version where you specify symbol table parameters
        assert type != ColumnType.SYMBOL;
    }

    public TableColumnMetadata(
            String name,
            int type,
            boolean indexFlag,
            int indexValueBlockCapacity,
            boolean symbolTableStatic
    ) {
        this.name = name;
        this.type = type;
        this.indexed = indexFlag;
        this.indexValueBlockCapacity = indexValueBlockCapacity;
        this.symbolTableStatic = symbolTableStatic;
    }

    public int getIndexValueBlockCapacity() {
        return indexValueBlockCapacity;
    }

    public void setIndexValueBlockCapacity(int indexValueBlockCapacity) {
        this.indexValueBlockCapacity = indexValueBlockCapacity;
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }

    public boolean isIndexed() {
        return indexed;
    }

    public void setIndexed(boolean value) {
        indexed = value;
    }

    public boolean isSymbolTableStatic() {
        return symbolTableStatic;
    }
}
