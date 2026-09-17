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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.security.DenyAllSecurityContext;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.std.LongList;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import org.junit.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static org.junit.Assert.*;

public class


SecurityContextTest {
    private static final Object[] NO_PARAM_ARGS = {};
    private static final ObjList<CharSequence> columns = new ObjList<>();
    private static final LongList permissions = new LongList();
    private final static String tableName = "tab";
    private static final Object[] THREE_PARAM_ARGS = {permissions, tableName, columns};
    private static final TableToken userTableToken = new TableToken(tableName, tableName, null, 0, false, false, false);
    private static final Object[] ONE_PARAM_ARGS = {userTableToken};
    private static final Object[] TWO_PARAM_ARGS = {userTableToken, columns};

    @Test
    public void testAllowAllSecurityContext() throws InvocationTargetException, IllegalAccessException {
        final SecurityContext sc = AllowAllSecurityContext.INSTANCE;
        assertTrue(sc.isSystemAdmin());
        assertTrue(sc.isQueryCancellationAllowed());
        assertFalse(sc.isExternal());
        for (Method method : SecurityContext.class.getMethods()) {
            String name = method.getName();
            if (name.startsWith("authorize")) {
                Class<?>[] parameters = method.getParameterTypes();
                switch (parameters.length) {
                    case 0:
                        method.invoke(sc, NO_PARAM_ARGS);
                        break;
                    case 1:
                        if (name.equals("authorizeCopyCancel")) {
                            method.invoke(sc, sc);
                        } else if (name.equals("authorizeTableBackup")) {
                            method.invoke(sc, new ObjHashSet<CharSequence>());
                        } else if (name.equals("authorizeTableCreate")) {
                            method.invoke(sc, TableUtils.TABLE_KIND_REGULAR_TABLE);
                        } else if (name.equals("authorizeSelect") && parameters[0] == ViewDefinition.class) {
                            final ViewDefinition viewDefinition = new ViewDefinition();
                            viewDefinition.init(userTableToken, tableName, 0L);
                            method.invoke(sc, viewDefinition);
                        } else {
                            method.invoke(sc, ONE_PARAM_ARGS);
                        }
                        break;
                    case 2:
                        method.invoke(sc, TWO_PARAM_ARGS);
                        break;
                    case 3:
                        method.invoke(sc, THREE_PARAM_ARGS);
                        break;
                    default:
                        throw new IndexOutOfBoundsException();
                }
            }
        }
    }

    @Test
    public void testDenyAllSecurityContext() throws IllegalAccessException {
        final SecurityContext sc = DenyAllSecurityContext.INSTANCE;
        assertTrue(sc.isSystemAdmin());
        assertFalse(sc.isQueryCancellationAllowed());
        assertFalse(sc.isExternal());
        for (Method method : SecurityContext.class.getMethods()) {
            String name = method.getName();
            if (name.startsWith("authorize")) {
                Class<?>[] parameters = method.getParameterTypes();
                try {
                    switch (parameters.length) {
                        case 0:
                            method.invoke(sc, NO_PARAM_ARGS);
                            fail();
                            break;
                        case 1:
                            if (name.equals("authorizeCopyCancel")) {
                                method.invoke(sc, sc);
                            } else if (name.equals("authorizeTableCreate")) {
                                method.invoke(sc, TableUtils.TABLE_KIND_REGULAR_TABLE);
                            } else if (name.equals("authorizeSelect") && parameters[0] == ViewDefinition.class) {
                                final ViewDefinition viewDefinition = new ViewDefinition();
                                viewDefinition.init(userTableToken, tableName, 0L);
                                method.invoke(sc, viewDefinition);
                            } else {
                                method.invoke(sc, ONE_PARAM_ARGS);
                            }
                            fail();
                            break;
                        case 2:
                            method.invoke(sc, TWO_PARAM_ARGS);
                            fail();
                            break;
                        case 3:
                            method.invoke(sc, THREE_PARAM_ARGS);
                            fail();
                        default:
                            throw new IndexOutOfBoundsException();
                    }
                } catch (IllegalArgumentException iae) {
                    throw new RuntimeException("Call failed for " + method, iae);
                } catch (InvocationTargetException err) {
                    assertTrue(err.getTargetException().getMessage().contains("permission denied"));
                }
            }
        }
    }

    @Test
    public void testReadOnlySecurityContext() throws IllegalAccessException {
        final SecurityContext sc = ReadOnlySecurityContext.INSTANCE;
        assertTrue(sc.isSystemAdmin());
        assertFalse(sc.isQueryCancellationAllowed());
        assertFalse(sc.isExternal());
        for (Method method : SecurityContext.class.getMethods()) {
            String name = method.getName();
            if (name.startsWith("authorize")) {
                Class<?>[] parameters = method.getParameterTypes();
                try {
                    switch (parameters.length) {
                        case 0:
                            method.invoke(sc, NO_PARAM_ARGS);
                            if (name.startsWith("authorizeSystemAdmin") || name.equals("authorizeSqlEngineAdmin") || name.equals("authorizeSettings")
                                    || name.equals("authorizeHttp") || name.equals("authorizePGWire") || name.equals("authorizeLineTcp")) {
                                continue;
                            }
                            fail();
                            break;
                        case 1:
                            if (name.equals("authorizeCopyCancel")) {
                                method.invoke(sc, sc);
                            } else if (name.equals("authorizeTableBackup")) {
                                method.invoke(sc, new ObjHashSet<CharSequence>());
                            } else if (name.equals("authorizeTableCreate")) {
                                method.invoke(sc, TableUtils.TABLE_KIND_REGULAR_TABLE);
                            } else if (name.equals("authorizeSelect") && parameters[0] == ViewDefinition.class) {
                                final ViewDefinition viewDefinition = new ViewDefinition();
                                viewDefinition.init(userTableToken, tableName, 0L);
                                method.invoke(sc, viewDefinition);
                            } else {
                                method.invoke(sc, ONE_PARAM_ARGS);
                            }
                            if (name.startsWith("authorizeShow") || name.startsWith("authorizeSelect")) {
                                continue;
                            }
                            fail();
                            break;
                        case 2:
                            method.invoke(sc, TWO_PARAM_ARGS);
                            if (name.equals("authorizeSelect")) {
                                continue;
                            }
                            fail();
                            break;
                        case 3:
                            method.invoke(sc, THREE_PARAM_ARGS);
                            fail();
                            break;
                        default:
                            throw new IndexOutOfBoundsException();
                    }
                } catch (IllegalArgumentException iae) {
                    throw new RuntimeException("Call failed for " + method, iae);
                } catch (InvocationTargetException err) {
                    assertTrue(err.getTargetException().getMessage().contains("permission denied"));
                }
            }
        }
    }

    /**
     * {@link SecurityContext#authorizeReconcileTable} is a {@code default} method that simply
     * delegates to {@code authorizeSystemAdmin()} (C8). No concrete class in the tree inherits
     * this default unoverridden today: {@link AllowAllSecurityContext} and
     * {@link ReadOnlySecurityContext} above both override it explicitly (no-op / unconditional
     * deny respectively, exercised by {@link #testAllowAllSecurityContext} /
     * {@link #testReadOnlySecurityContext}), and every Enterprise {@code EntSecurityContext}
     * implementor is required to override it too (the Enterprise interface redeclares the method
     * as abstract). So the default method's own body has zero coverage anywhere else in the tree.
     * <p>
     * A concrete class cannot "un-override" a default method it inherited through a concrete
     * superclass (e.g. {@code extends AllowAllSecurityContext} would keep dispatching to
     * {@code AllowAllSecurityContext}'s own no-op override, and {@code SecurityContext.super...}
     * is rejected by javac as a "redundant interface" in that shape). A {@link Proxy} sidesteps
     * this: a dynamic proxy never auto-implements default methods, so routing
     * {@code authorizeReconcileTable} through {@link InvocationHandler#invokeDefault} executes
     * the interface's own default body directly, which in turn calls back through the same
     * handler for {@code authorizeSystemAdmin()} -- toggled here to prove the delegation is
     * genuinely load-bearing in both directions, not a no-op stub.
     */
    @Test
    public void testDefaultAuthorizeReconcileTableDelegatesToAuthorizeSystemAdmin() {
        final boolean[] systemAdminAllowed = {true};
        final InvocationHandler handler = (proxy, method, args) -> {
            switch (method.getName()) {
                case "authorizeSystemAdmin":
                    if (!systemAdminAllowed[0]) {
                        throw CairoException.authorization().put("permission denied").setCacheable(true);
                    }
                    return null;
                case "authorizeReconcileTable":
                    // Executes SecurityContext.java's default body verbatim.
                    return InvocationHandler.invokeDefault(proxy, method, args);
                default:
                    return method.invoke(AllowAllSecurityContext.INSTANCE, args);
            }
        };
        final SecurityContext sc = (SecurityContext) Proxy.newProxyInstance(
                SecurityContext.class.getClassLoader(), new Class<?>[]{SecurityContext.class}, handler);

        systemAdminAllowed[0] = false;
        try {
            sc.authorizeReconcileTable(userTableToken);
            fail("default authorizeReconcileTable must deny when authorizeSystemAdmin() denies");
        } catch (CairoException e) {
            assertTrue(e.getMessage().contains("permission denied"));
        }

        systemAdminAllowed[0] = true;
        sc.authorizeReconcileTable(userTableToken); // must not throw
    }
}
