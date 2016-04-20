/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 ******************************************************************************/

package com.questdb.ex;

import com.questdb.factory.configuration.ColumnMetadata;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.log.LogRecord;

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
