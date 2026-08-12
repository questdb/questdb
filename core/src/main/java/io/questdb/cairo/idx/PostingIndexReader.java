/*+*****************************************************************************
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

package io.questdb.cairo.idx;

/**
 * The posting-index primitives the covering query path reaches through a
 * reader, separated from {@link AbstractPostingIndexReader}'s chain and
 * generation machinery so a Parquet-backed reader can serve them without
 * inheriting it.
 * <p>
 * Deliberately narrow: it declares exactly the methods
 * {@code CoveringIndexRecordCursorFactory} calls through the concrete type.
 * Adding to it couples a new caller to every implementation, so a new method
 * belongs here only when more than one implementation can answer it.
 */
public interface PostingIndexReader extends IndexReader {

    /**
     * Exact count of postings for {@code key} within
     * {@code [minValue, maxValueClamped]}, or {@code -1} when the reader
     * cannot answer from metadata alone and the caller must walk a cursor.
     */
    long countMatchesClamped(int key, long minValue, long nullMaxValue, long maxValueClamped);

    /**
     * Highest row id the reader's current entry covers.
     */
    long getEntryMaxValue();

    /**
     * Warms any per-key cache the reader keeps.
     */
    void populateCacheForKey(int key);

    /**
     * Absolute row id of the {@code k}-th posting of {@code key} within
     * {@code [minValue, maxValueClamped]}, or {@code -1} when the reader
     * cannot resolve it from metadata alone.
     */
    long selectKthMatch(int key, long minValue, long nullMaxValue, long maxValueClamped, long k);
}
