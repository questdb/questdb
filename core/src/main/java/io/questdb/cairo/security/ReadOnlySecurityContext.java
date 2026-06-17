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

package io.questdb.cairo.security;

import io.questdb.cairo.SecurityContext;
import io.questdb.griffin.engine.functions.catalogue.Constants;

/**
 * The concrete read-only security context. The shared singletons ({@link #INSTANCE} /
 * {@link #SETTINGS_READ_ONLY}) are instances of this class, and {@code forPrincipal} derives further
 * instances of it, so a derived context reports the authenticated user while preserving the read-only
 * (and settings-read-only) behavior. It extends {@link AbstractReadOnlySecurityContext}, which declares
 * {@code newPrincipalContext} abstract so every subclass must supply its own and is never silently
 * downgraded. Subclasses that need to allow more than plain read-only (e.g. the mat view refresh
 * context) can extend this concrete class instead of implementing the abstract base from scratch.
 */
public class ReadOnlySecurityContext extends AbstractReadOnlySecurityContext {
    public static final ReadOnlySecurityContext INSTANCE = new ReadOnlySecurityContext(false, Constants.USER_NAME);
    public static final ReadOnlySecurityContext SETTINGS_READ_ONLY = new ReadOnlySecurityContext(true, Constants.USER_NAME);

    protected ReadOnlySecurityContext() {
    }

    protected ReadOnlySecurityContext(boolean settingsReadOnly, CharSequence principal) {
        super(settingsReadOnly, principal);
    }

    @Override
    protected SecurityContext newPrincipalContext(CharSequence principal) {
        return new ReadOnlySecurityContext(settingsReadOnly, principal);
    }
}
