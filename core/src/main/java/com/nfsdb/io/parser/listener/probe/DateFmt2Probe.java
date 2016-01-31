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

package com.nfsdb.io.parser.listener.probe;

import com.nfsdb.ex.NumericException;
import com.nfsdb.io.ImportedColumnMetadata;
import com.nfsdb.io.ImportedColumnType;
import com.nfsdb.misc.Dates;
import com.nfsdb.store.ColumnType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class DateFmt2Probe implements TypeProbe {
    @Override
    public void getMetadata(ImportedColumnMetadata m) {
        m.type = ColumnType.DATE;
        m.importedType = ImportedColumnType.DATE_2;
        m.size = 8;
    }

    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_RETURN_FALSE")
    @Override
    public boolean probe(CharSequence seq) {
        try {
            Dates.parseDateTimeFmt2(seq);
            return true;
        } catch (NumericException e) {
            return false;
        }
    }
}
