/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

package com.nfsdb.ex;

import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.log.Log;
import com.nfsdb.log.LogFactory;
import com.nfsdb.log.LogRecord;

public class JournalMetadataException extends JournalException {
    private static final Log LOG = LogFactory.getLog(JournalMetadataException.class);
    private static final int FIRST_COL_PAD = 15;
    private static final int DEFAULT_COL_PAD = 40;

    public JournalMetadataException(JournalMetadata mo, JournalMetadata mn) {
        super("Checksum mismatch. Check log for details");
        LogRecord b = LOG.error();
        b.$("Metadata mismatch for journal:\n");
        b.$("Location: ").$(mo.getLocation()).$('\n');
        sep(b);
        b(b);
        pad(b, FIRST_COL_PAD, "column#");
        pad(b, "OLD");
        pad(b, "NEW");
        e(b);
        sep(b);

        b(b);
        boolean diff = mo.getPartitionType() != mn.getPartitionType();
        pad(b, FIRST_COL_PAD, (diff ? "*" : "") + "Partition by");
        pad(b, mo.getPartitionType().name());
        pad(b, mn.getPartitionType().name());
        e(b);
        sep(b);

        int i = 0;
        while (true) {
            ColumnMetadata cmo = i < mo.getColumnCount() ? mo.getColumn(i) : null;
            ColumnMetadata cmn = i < mn.getColumnCount() ? mn.getColumn(i) : null;

            if (cmo == null && cmn == null) {
                break;
            }

            diff = cmo == null || cmn == null;
            diff = diff || !cmo.name.equals(cmn.name);
            diff = diff || cmo.size != cmn.size;
            diff = diff || cmo.type != cmn.type;
            diff = diff || cmo.distinctCountHint != cmn.distinctCountHint;
            diff = diff || (cmo.sameAs == null && cmn.sameAs != null) || (cmo.sameAs != null && !cmo.sameAs.equals(cmn.sameAs));

            b(b);

            pad(b, FIRST_COL_PAD, (diff ? "*" : "") + i);

            if (cmo != null) {
                col(b, cmo);
            } else {
                skip(b);
            }

            if (cmn != null) {
                col(b, cmn);
            } else {
                skip(b);
            }
            i++;
            e(b);
        }
        sep(b);
        b.$();
    }

    private void b(LogRecord r) {
        r.$('|');
    }

    private void col(LogRecord r, ColumnMetadata m) {
        pad(r, (m.distinctCountHint > 0 ? m.distinctCountHint + " ~ " : "") + (m.indexed ? '#' : "") + m.name + (m.sameAs != null ? " -> " + m.sameAs : "") + ' ' + m.type.name() + '(' + m.size + ')');
    }

    private void e(LogRecord r) {
        r.$('\n');
    }

    private void pad(LogRecord b, int w, String value) {
        int pad = value == null ? w : w - value.length();
        for (int i = 0; i < pad; i++) {
            b.$(' ');
        }
        if (value != null) {
            b.$(value);
        }

        b.$("  |");
    }

    private void pad(LogRecord r, String value) {
        pad(r, DEFAULT_COL_PAD, value);
    }

    private void sep(LogRecord b) {
        b.$("+-------------------------------------------------------------------------------------------------------+\n");
    }

    private void skip(LogRecord r) {
        pad(r, "");
    }
}
