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
 * Deletes the sealed generations of one posting index that its live head superseded - the files a build started from.
 * On an index readers can open that is the purge job's, once no reader can still pin them. A build no reader can see
 * yet removes them straight away instead of publishing purges nobody needs: a compaction staging directory, an index
 * ADD INDEX has not committed, and one REINDEX removed before it rebuilt it. Not thread-safe.
 */
final class PostingSupersededFileRemover implements PostingIndexUtils.SealedFileVisitor {
    private CharSequence columnName;
    private long columnNameTxn;
    private Path dir;
    private int dirLen;
    private FilesFacade ff;
    private long liveSealTxn;

    // scanSealedFiles() reports the files of the instance columnNameTxn only
    @Override
    public void onCoverDataFile(int includeIdx, long coveredColumnNameTxn, long sealTxn) {
        if (sealTxn != liveSealTxn) {
            ff.removeQuiet(PostingIndexUtils.coverDataFileName(
                    dir.trimTo(dirLen), columnName, includeIdx, columnNameTxn, coveredColumnNameTxn, sealTxn));
            dir.trimTo(dirLen);
        }
    }

    @Override
    public void onValueFile(long sealTxn) {
        if (sealTxn != liveSealTxn) {
            ff.removeQuiet(PostingIndexUtils.valueFileName(dir.trimTo(dirLen), columnName, columnNameTxn, sealTxn));
            dir.trimTo(dirLen);
        }
    }

    void remove(FilesFacade ff, Path dir, int dirLen, CharSequence columnName, long columnNameTxn) {
        try {
            liveSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(ff, PostingIndexUtils.keyFileName(dir.trimTo(dirLen), columnName, columnNameTxn));
            if (liveSealTxn < 0) {
                // No readable head: keep everything rather than guess which generation is live.
                return;
            }
            this.ff = ff;
            this.dir = dir;
            this.dirLen = dirLen;
            this.columnName = columnName;
            this.columnNameTxn = columnNameTxn;
            PostingIndexUtils.scanSealedFiles(ff, dir, dirLen, columnName, columnNameTxn, this);
        } finally {
            this.ff = null;
            this.dir = null;
            this.columnName = null;
            dir.trimTo(dirLen);
        }
    }
}
