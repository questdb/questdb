/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.factory.configuration;

import com.nfsdb.ex.NoSuchColumnException;

public abstract class AbstractRecordMetadata implements RecordMetadata {
    private String alias;

    @Override
    public String getAlias() {
        return alias;
    }

    @Override
    public void setAlias(String alias) {
        this.alias = alias;
    }

    @Override
    public final RecordColumnMetadata getColumn(CharSequence name) {
        return getColumnQuick(getColumnIndex(name));
    }

    @Override
    public final int getColumnIndex(CharSequence columnName) {
        int index = getColumnIndexQuiet(columnName);
        if (index == -1) {
            throw new NoSuchColumnException(columnName);

        }
        return index;
    }

    @Override
    public String getColumnName(int index) {
        return getColumnQuick(index).getName();
    }
}
