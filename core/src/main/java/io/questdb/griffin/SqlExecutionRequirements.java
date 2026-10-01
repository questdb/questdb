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

package io.questdb.griffin;

import io.questdb.std.Chars;
import io.questdb.std.Mutable;

public final class SqlExecutionRequirements implements Mutable {
    public static final int NONE = 0;
    public static final int REQUIRES_ENTERPRISE_SECURITY_CONTEXT = 1 << 1;
    public static final int REQUIRES_LIVE_WAL_PROGRESS = 1;
    private String enterpriseSecurityContextFunctionName;
    private int enterpriseSecurityContextPosition = -1;
    private int flags;
    private int liveWalProgressPosition = -1;

    public void add(int requirements, int position, CharSequence functionName) {
        flags |= requirements;
        if ((requirements & REQUIRES_ENTERPRISE_SECURITY_CONTEXT) != 0 && enterpriseSecurityContextPosition < 0) {
            enterpriseSecurityContextFunctionName = Chars.toString(functionName);
            enterpriseSecurityContextPosition = position;
        }
        if ((requirements & REQUIRES_LIVE_WAL_PROGRESS) != 0 && liveWalProgressPosition < 0) {
            liveWalProgressPosition = position;
        }
    }

    @Override
    public void clear() {
        enterpriseSecurityContextFunctionName = null;
        enterpriseSecurityContextPosition = -1;
        flags = NONE;
        liveWalProgressPosition = -1;
    }

    public CharSequence getFunctionName(int requirement) {
        if (requirement == REQUIRES_ENTERPRISE_SECURITY_CONTEXT
                && (flags & REQUIRES_ENTERPRISE_SECURITY_CONTEXT) != 0) {
            return enterpriseSecurityContextFunctionName;
        }
        return null;
    }

    public int getPosition(int requirement) {
        if (requirement == REQUIRES_ENTERPRISE_SECURITY_CONTEXT
                && (flags & REQUIRES_ENTERPRISE_SECURITY_CONTEXT) != 0) {
            return enterpriseSecurityContextPosition;
        }
        if (requirement == REQUIRES_LIVE_WAL_PROGRESS && (flags & REQUIRES_LIVE_WAL_PROGRESS) != 0) {
            return liveWalProgressPosition;
        }
        return -1;
    }
}
