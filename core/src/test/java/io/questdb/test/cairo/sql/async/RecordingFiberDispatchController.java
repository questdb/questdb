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

package io.questdb.test.cairo.sql.async;

import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberDispatchContext;
import io.questdb.mp.continuation.FiberDispatchController;
import io.questdb.mp.continuation.FiberDispatchRequest;
import io.questdb.mp.continuation.FiberDispatchSession;
import io.questdb.mp.continuation.FiberDispatchTicket;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberWakeSink;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

final class RecordingFiberDispatchController implements FiberDispatchController {
    private Runnable cooperativePollAction;
    private int cooperativePollCount;
    private final List<FiberDispatchContext> mountedContexts = new ArrayList<>();
    private final List<Long> mountedOwnerIds = new ArrayList<>();
    private final List<FiberDispatchContext> polledContexts = new ArrayList<>();
    private final List<Long> polledOwnerIds = new ArrayList<>();
    private final Session session = new Session();
    private final Ticket ticket = new Ticket();
    private int unmountCount;

    FiberRuntime createRuntime(int maxLiveFiberCount) {
        return new FiberRuntime(
                maxLiveFiberCount,
                maxLiveFiberCount,
                64,
                0,
                this,
                FiberWakeSink.NO_OP
        );
    }

    int getCooperativePollCount() {
        return cooperativePollCount;
    }

    int getMountCount() {
        return mountedContexts.size();
    }

    @Nullable
    FiberDispatchContext getMountedContext(int index) {
        return mountedContexts.get(index);
    }

    long getMountedOwnerId(int index) {
        return mountedOwnerIds.get(index);
    }

    FiberDispatchContext getPolledContext(int index) {
        return polledContexts.get(index);
    }

    long getPolledOwnerId(int index) {
        return polledOwnerIds.get(index);
    }

    int getUnmountCount() {
        return unmountCount;
    }

    @Override
    public FiberDispatchSession openSession(FiberRuntime runtime) {
        return session;
    }

    void setCooperativePollAction(Runnable cooperativePollAction) {
        this.cooperativePollAction = cooperativePollAction;
    }

    private final class Session implements FiberDispatchSession {
        private boolean isQuiescing;

        @Override
        public void beginQuiesce() {
            isQuiescing = true;
        }

        @Override
        public boolean isQuiesced() {
            return isQuiescing;
        }

        @Override
        public void progressQuiesce() {
        }

        @Override
        public void requestDispatch(FiberDispatchRequest request) {
            if (!request.grant(request.getDispatchEpoch(), ticket)) {
                throw new IllegalStateException("test dispatch request could not be granted");
            }
        }

        @Override
        public FiberDispatchTicket tryDispatchDirect(FiberDispatchRequest request) {
            return null;
        }
    }

    private final class Ticket implements FiberDispatchTicket {
        @Override
        public void onCooperativePoll() {
            cooperativePollCount++;
            final FiberDispatchContext context = Fiber.getDispatchContext();
            polledContexts.add(context);
            polledOwnerIds.add(context != null ? context.getQueryRegistryOwnerId() : -1);
            if (cooperativePollAction != null) {
                cooperativePollAction.run();
            }
        }

        @Override
        public void onMount(FiberDispatchRequest request) {
            final FiberDispatchContext context = request.getDispatchContext();
            mountedContexts.add(context);
            mountedOwnerIds.add(context != null ? context.getQueryRegistryOwnerId() : -1);
        }

        @Override
        public void onUnmount(FiberDispatchRequest request, boolean wasMounted) {
            unmountCount++;
        }
    }
}
