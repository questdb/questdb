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

package io.questdb.test.griffin.fuzz;

import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

/**
 * Index descriptor attached to a SYMBOL {@link FuzzColumn}. Covers both of
 * QuestDB's index families: the classic BITMAP index ({@code INDEX}) and the
 * POSTING index ({@code INDEX TYPE POSTING}, optionally DELTA- or EF-encoded).
 * Only POSTING indexes may carry covering columns via {@code INCLUDE (...)};
 * the list is empty for BITMAP indexes and for plain POSTING indexes without a
 * covering layer.
 * <p>
 * The designated timestamp is auto-appended to every posting index's covering
 * set by the engine, so the generator never lists it in {@link #coveringColumns}.
 */
public final class FuzzIndex {
    private final ObjList<String> coveringColumns;
    private final Kind kind;

    public FuzzIndex(Kind kind, ObjList<String> coveringColumns) {
        this.kind = kind;
        this.coveringColumns = coveringColumns;
    }

    /**
     * Appends the column-level DDL fragment that follows the column type in
     * CREATE TABLE, e.g. {@code " INDEX"} or
     * {@code " INDEX TYPE POSTING DELTA INCLUDE (c0, c3)"} (leading space
     * included).
     */
    public void appendDdl(StringSink sink) {
        switch (kind) {
            case BITMAP -> sink.put(" INDEX");
            case POSTING -> sink.put(" INDEX TYPE POSTING");
            case POSTING_DELTA -> sink.put(" INDEX TYPE POSTING DELTA");
            case POSTING_EF -> sink.put(" INDEX TYPE POSTING EF");
        }
        if (coveringColumns != null && coveringColumns.size() > 0) {
            sink.put(" INCLUDE (");
            for (int i = 0, n = coveringColumns.size(); i < n; i++) {
                if (i > 0) {
                    sink.put(", ");
                }
                sink.put(coveringColumns.getQuick(i));
            }
            sink.put(')');
        }
    }

    /**
     * Same fragment as {@link #appendDdl} but materialized as a string, for
     * the schema log line so a failure can be reproduced from the log alone.
     */
    public String describe() {
        StringSink sink = new StringSink();
        appendDdl(sink);
        return sink.toString();
    }

    public ObjList<String> getCoveringColumns() {
        return coveringColumns;
    }

    public Kind getKind() {
        return kind;
    }

    public boolean isPosting() {
        return kind != Kind.BITMAP;
    }

    public enum Kind {
        BITMAP,        // INDEX
        POSTING,       // INDEX TYPE POSTING
        POSTING_DELTA, // INDEX TYPE POSTING DELTA
        POSTING_EF     // INDEX TYPE POSTING EF
    }
}
