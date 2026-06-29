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
 * The concrete allow-all security context. The shared singletons ({@link #INSTANCE} /
 * {@link #SETTINGS_READ_ONLY}) are instances of this class, and {@code forPrincipal} derives further
 * instances of it, so a derived context reports the authenticated user while preserving the allow-all
 * (and settings-read-only) behavior.
 * <p>
 * A subclass that overrides an {@code authorize*} or identity method MUST also override
 * {@link #newPrincipalContext} to return its own type. This class's {@code newPrincipalContext} returns a
 * plain {@code AllowAllSecurityContext}, so {@code forPrincipal} on a subclass that does not override it
 * would silently drop the override and downgrade the context to plain allow-all. No factory calls
 * {@code forPrincipal} on a subclass today, so this is a latent trap rather than a live bug.
 */
public class AllowAllSecurityContext extends AbstractAllowAllSecurityContext {
    public static final AllowAllSecurityContext INSTANCE = new AllowAllSecurityContext(false, Constants.USER_NAME);
    public static final AllowAllSecurityContext SETTINGS_READ_ONLY = new AllowAllSecurityContext(true, Constants.USER_NAME);

    protected AllowAllSecurityContext() {
    }

    protected AllowAllSecurityContext(boolean settingsReadOnly, CharSequence principal) {
        super(settingsReadOnly, principal);
    }

    @Override
    protected SecurityContext newPrincipalContext(CharSequence principal) {
        return new AllowAllSecurityContext(settingsReadOnly, principal);
    }
}
