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

package io.questdb.recovery;

import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;

import java.io.PrintStream;

/**
 * Owns WAL navigation state within the recovery session. The user navigates
 * a three-level WAL hierarchy: <em>wal root → wal dir → segment</em>.
 *
 * <p>Created when the user {@code cd wal} from table level. Cleared when
 * the user navigates back above WAL root.
 */
public class WalNavigationContext {
    private WalEventState cachedWalEventState;
    private WalScanState cachedWalScanState;
    private int currentSegmentId = -1;
    private int currentWalId = -1;

    public WalNavigationContext(WalScanState walScanState) {
        this.cachedWalScanState = walScanState;
    }

    public String buildPromptSuffix() {
        StringBuilder sb = new StringBuilder("/wal");
        if (currentWalId >= 0) {
            sb.append("/wal").append(currentWalId);
            if (currentSegmentId >= 0) {
                sb.append("/").append(currentSegmentId);
            }
        }
        return sb.toString();
    }

    public boolean cd(String target, PrintStream err) {
        if (currentWalId < 0) {
            return cdIntoWalDir(target, err);
        } else if (currentSegmentId < 0) {
            return cdIntoSegment(target, err);
        } else {
            err.println("already at leaf level (segment); cd .. to go up");
            return false;
        }
    }

    public void cdUp() {
        if (currentSegmentId >= 0) {
            currentSegmentId = -1;
            cachedWalEventState = null;
        } else if (currentWalId >= 0) {
            currentWalId = -1;
        }
        // else: at wal root — caller (NavigationContext) handles going above
    }

    public WalDirEntry findWalDirEntry(int walId) {
        if (cachedWalScanState == null) {
            return null;
        }
        ObjList<WalDirEntry> entries = cachedWalScanState.getEntries();
        for (int i = 0, n = entries.size(); i < n; i++) {
            WalDirEntry entry = entries.getQuick(i);
            if (entry.walId() == walId) {
                return entry;
            }
        }
        return null;
    }

    public WalEventState getCachedWalEventState() {
        return cachedWalEventState;
    }

    public WalScanState getCachedWalScanState() {
        return cachedWalScanState;
    }

    public int getCurrentSegmentId() {
        return currentSegmentId;
    }

    public int getCurrentWalId() {
        return currentWalId;
    }

    public WalLevel getLevel() {
        if (currentWalId < 0) {
            return WalLevel.WAL_ROOT;
        } else if (currentSegmentId < 0) {
            return WalLevel.WAL_DIR;
        } else {
            return WalLevel.WAL_SEGMENT;
        }
    }

    public boolean isAtWalDir() {
        return currentWalId >= 0 && currentSegmentId < 0;
    }

    public boolean isAtWalRoot() {
        return currentWalId < 0;
    }

    public boolean isAtWalSegment() {
        return currentWalId >= 0 && currentSegmentId >= 0;
    }

    public void setCachedWalEventState(WalEventState state) {
        this.cachedWalEventState = state;
    }

    public void setCachedWalScanState(WalScanState state) {
        this.cachedWalScanState = state;
    }

    private boolean cdIntoSegment(String target, PrintStream err) {
        int segmentId;
        try {
            segmentId = Numbers.parseInt(target);
        } catch (NumericException e) {
            err.println("segment must be a number: " + target);
            return false;
        }

        WalDirEntry walEntry = findWalDirEntry(currentWalId);
        if (walEntry == null) {
            err.println("wal" + currentWalId + " not found");
            return false;
        }

        ObjList<WalSegmentEntry> segments = walEntry.segments();
        for (int i = 0, n = segments.size(); i < n; i++) {
            if (segments.getQuick(i).segmentId() == segmentId) {
                currentSegmentId = segmentId;
                cachedWalEventState = null;
                return true;
            }
        }
        err.println("segment not found: " + target + " in wal" + currentWalId);
        return false;
    }

    private boolean cdIntoWalDir(String target, PrintStream err) {
        if (cachedWalScanState == null || cachedWalScanState.getEntries().size() == 0) {
            err.println("no WAL directories found");
            return false;
        }

        ObjList<WalDirEntry> entries = cachedWalScanState.getEntries();

        WalDirEntry entry;
        if (target.startsWith("wal")) {
            // "wal3" -> lookup by walId
            int walId;
            try {
                walId = Numbers.parseInt(target.substring(3));
            } catch (NumericException e) {
                err.println("invalid WAL directory: " + target);
                return false;
            }
            entry = findWalDirEntry(walId);
            if (entry == null) {
                err.println("WAL not found: " + target);
                return false;
            }
        } else {
            // bare number -> lookup by listing index
            int idx;
            try {
                idx = Numbers.parseInt(target);
            } catch (NumericException e) {
                err.println("invalid WAL directory: " + target);
                return false;
            }
            if (idx < 0 || idx >= entries.size()) {
                err.println("WAL index out of range: " + target);
                return false;
            }
            entry = entries.getQuick(idx);
        }

        if (entry.status() == WalScanStatus.MISSING) {
            err.println("WAL directory is missing from disk: wal" + entry.walId());
            return false;
        }

        currentWalId = entry.walId();
        return true;
    }
}
