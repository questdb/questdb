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

package com.nfsdb.io;

import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.std.ObjectFactory;

public class ImportedColumnMetadata extends ColumnMetadata {
    public static final ObjectFactory<ImportedColumnMetadata> FACTORY = new ObjectFactory<ImportedColumnMetadata>() {
        @Override
        public ImportedColumnMetadata newInstance() {
            return new ImportedColumnMetadata();
        }
    };

    public int columnIndex;
    public ImportedColumnType importedType;

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + columnIndex;
        return 31 * result + importedType.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ImportedColumnMetadata that = (ImportedColumnMetadata) o;
        return columnIndex == that.columnIndex && importedType == that.importedType;

    }
}

