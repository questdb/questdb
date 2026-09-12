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

package io.questdb.mp.continuation;

public enum FiberDispatchRoute {
    DIRECT(false, false),
    DIRECT_PENDING(true, false),
    DISPATCH_YIELD(true, false),
    POST_PROCESS_RESIGNAL(true, false),
    REQUEST_RUN(true, true),
    SHUTDOWN_CLEANUP(false, false);

    // Whether a granted request may prefer the Fiber's last mounting Worker when it wakes a peer.
    final boolean isLastMountPreferenceAllowed;
    // Whether a granted request may use the owning Worker's local queue.
    final boolean isLocalPublicationAllowed;

    FiberDispatchRoute(boolean isLocalPublicationAllowed, boolean isLastMountPreferenceAllowed) {
        this.isLastMountPreferenceAllowed = isLastMountPreferenceAllowed;
        this.isLocalPublicationAllowed = isLocalPublicationAllowed;
    }
}
