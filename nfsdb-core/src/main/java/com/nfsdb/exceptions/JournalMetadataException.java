/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
 */

package com.nfsdb.exceptions;

import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.logging.Logger;

public class JournalMetadataException extends JournalException {
    private static final Logger LOGGER = Logger.getLogger(JournalMetadataException.class);
    private static final int FIRST_COL_PAD = 15;
    private static final int DEFAULT_COL_PAD = 40;

    private final StringBuilder b = new StringBuilder();


    public JournalMetadataException(JournalMetadata mo, JournalMetadata mn) {
        super("Checksum mismatch. Check log for details");
        b.append("Metadata mismatch for journal:\n");
        b.append("Location: ").append(mo.getLocation()).append("\n");
        sep();
        b();
        pad(FIRST_COL_PAD, "column#");
        pad("OLD");
        pad("NEW");
        e();
        sep();

        b();
        boolean diff = mo.getPartitionType() != mn.getPartitionType();
        pad(FIRST_COL_PAD, (diff ? "*" : "") + "Partition by");
        pad(mo.getPartitionType().name());
        pad(mn.getPartitionType().name());
        e();
        sep();

        int i = 0;
        while (true) {
            ColumnMetadata cmo = i < mo.getColumnCount() ? mo.getColumnMetadata(i) : null;
            ColumnMetadata cmn = i < mn.getColumnCount() ? mn.getColumnMetadata(i) : null;

            if (cmo == null && cmn == null) {
                break;
            }

            diff = cmo == null || cmn == null;
            diff = diff || !cmo.name.equals(cmn.name);
            diff = diff || cmo.size != cmn.size;
            diff = diff || cmo.type != cmn.type;
            diff = diff || cmo.distinctCountHint != cmn.distinctCountHint;
            diff = diff || (cmo.sameAs == null && cmn.sameAs != null) || (cmo.sameAs != null && !cmo.sameAs.equals(cmn.sameAs));

            b();

            pad(FIRST_COL_PAD, (diff ? "*" : "") + i);

            if (cmo != null) {
                col(cmo);
            } else {
                skip();
            }

            if (cmn != null) {
                col(cmn);
            } else {
                skip();
            }
            i++;
            e();
        }
        sep();
        LOGGER.error(b.toString());
    }

    private void pad(String value) {
        pad(DEFAULT_COL_PAD, value);
    }

    private void pad(int w, String value) {
        int pad = value == null ? w : w - value.length();
        for (int i = 0; i < pad; i++) {
            b.append(' ');
        }
        if (value != null) {
            b.append(value);
        }

        b.append("  |");
    }

    private void sep() {
        b.append("+-------------------------------------------------------------------------------------------------------+\n");
    }

    private void b() {
        b.append("|");
    }

    private void e() {
        b.append("\n");
    }

    private void col(ColumnMetadata m) {
        pad((m.distinctCountHint > 0 ? m.distinctCountHint + " ~ " : "") + (m.indexed ? "#" : "") + m.name + (m.sameAs != null ? " -> " + m.sameAs : "") + " " + m.type.name() + "(" + m.size + ")");
    }

    private void skip() {
        pad("");
    }
}
