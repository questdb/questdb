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

package io.questdb.cairo;

import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;

/**
 * Deletes the sealed generations of one posting index that its live head superseded - the files a build in a
 * compaction staging directory started from. On a published partition that is the purge job's, once no reader can
 * still pin them; in a staging directory nobody reads yet, so they go straight away instead of being published into
 * the partition. Not thread-safe.
 */
final class PostingSupersededFileRemover implements PostingIndexUtils.SealedFileVisitor {
    private final FilesFacade ff;
    private CharSequence columnName;
    private long columnNameTxn;
    private Path dir;
    private int dirLen;
    private long liveSealTxn;

    PostingSupersededFileRemover(FilesFacade ff) {
        this.ff = ff;
    }

    @Override
    public void onCoverDataFile(int includeIdx, long postingColumnNameTxn, long coveredColumnNameTxn, long sealTxn) {
        if (postingColumnNameTxn == columnNameTxn && sealTxn != liveSealTxn) {
            ff.removeQuiet(PostingIndexUtils.coverDataFileName(
                    dir.trimTo(dirLen), columnName, includeIdx, postingColumnNameTxn, coveredColumnNameTxn, sealTxn));
            dir.trimTo(dirLen);
        }
    }

    @Override
    public void onValueFile(long postingColumnNameTxn, long sealTxn) {
        if (postingColumnNameTxn == columnNameTxn && sealTxn != liveSealTxn) {
            ff.removeQuiet(PostingIndexUtils.valueFileName(dir.trimTo(dirLen), columnName, postingColumnNameTxn, sealTxn));
            dir.trimTo(dirLen);
        }
    }

    void remove(Path dir, int dirLen, CharSequence columnName, long columnNameTxn) {
        try {
            liveSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(ff, PostingIndexUtils.keyFileName(dir.trimTo(dirLen), columnName, columnNameTxn));
            if (liveSealTxn < 0) {
                // No readable head: keep everything rather than guess which generation is live.
                return;
            }
            this.dir = dir;
            this.dirLen = dirLen;
            this.columnName = columnName;
            this.columnNameTxn = columnNameTxn;
            PostingIndexUtils.scanSealedFiles(ff, dir, dirLen, columnName, this);
        } finally {
            this.dir = null;
            this.columnName = null;
            dir.trimTo(dirLen);
        }
    }
}
