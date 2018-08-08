/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.ex;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.log.LogRecord;
import com.questdb.std.ex.JournalException;
import com.questdb.store.ColumnType;
import com.questdb.store.PartitionBy;
import com.questdb.store.factory.configuration.ColumnMetadata;
import com.questdb.store.factory.configuration.JournalMetadata;

public class JournalMetadataException extends JournalException {
    private static final Log LOG = LogFactory.getLog(JournalMetadataException.class);
    private static final int FIRST_COL_PAD = 15;
    private static final int DEFAULT_COL_PAD = 40;

    public JournalMetadataException(JournalMetadata mo, JournalMetadata mn) {
        super("Checksum mismatch. Check log for details");
        LogRecord b = LOG.error();
        b.$("Metadata mismatch for journal:\n");
        b.$("Name: ").$(mo.getName()).$('\n');
        sep(b);
        b(b);
        pad(b, FIRST_COL_PAD, "column#");
        pad(b, "OLD");
        pad(b, "NEW");
        e(b);
        sep(b);

        b(b);
        boolean diff = mo.getPartitionBy() != mn.getPartitionBy();
        pad(b, FIRST_COL_PAD, (diff ? "*" : "") + "Partition by");
        pad(b, PartitionBy.toString(mo.getPartitionBy()));
        pad(b, PartitionBy.toString(mn.getPartitionBy()));
        e(b);
        sep(b);

        int i = 0;
        while (true) {
            ColumnMetadata cmo = i < mo.getColumnCount() ? mo.getColumnQuick(i) : null;
            ColumnMetadata cmn = i < mn.getColumnCount() ? mn.getColumnQuick(i) : null;

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
        pad(r, (m.distinctCountHint > 0 ? m.distinctCountHint + " ~ " : "") + (m.indexed ? '#' : "") + m.name + (m.sameAs != null ? " -> " + m.sameAs : "") + ' ' + ColumnType.nameOf(m.type) + '(' + m.size + ')');
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
