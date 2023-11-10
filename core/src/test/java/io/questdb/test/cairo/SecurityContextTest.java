/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.security.DenyAllSecurityContext;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.std.LongList;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class SecurityContextTest {
    private static final Object[] NO_PARAM_ARGS = {};
    private static final ObjList<CharSequence> columns = new ObjList<>();
    private static final LongList permissions = new LongList();
    private final static String tableName = "tab";
    private static final Object[] THREE_PARAM_ARGS = {permissions, tableName, columns};
    private static final TableToken userTableToken = new TableToken(tableName, tableName, 0, false, false);
    private static final Object[] ONE_PARAM_ARGS = {userTableToken};
    private static final Object[] TWO_PARAM_ARGS = {userTableToken, columns};

    @Test
    public void testAllowAllSecurityContext() throws InvocationTargetException, IllegalAccessException {
        SecurityContext sc = AllowAllSecurityContext.INSTANCE;
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
        SecurityContext sc = DenyAllSecurityContext.INSTANCE;
        for (Method method : SecurityContext.class.getMethods()) {
            String name = method.getName();
            if (name.startsWith("authorize")) {
                Class<?>[] parameters = method.getParameterTypes();
                try {
                    switch (parameters.length) {
                        case 0:
                            method.invoke(sc, NO_PARAM_ARGS);
                            Assert.fail();
                            break;
                        case 1:
                            if (name.equals("authorizeCopyCancel")) {
                                method.invoke(sc, sc);
                            } else if (name.equals("authorizeTableBackup")) {
                                method.invoke(sc, new ObjHashSet<CharSequence>());
                            } else {
                                method.invoke(sc, ONE_PARAM_ARGS);
                            }
                            Assert.fail();
                            break;
                        case 2:
                            method.invoke(sc, TWO_PARAM_ARGS);
                            Assert.fail();
                            break;
                        case 3:
                            method.invoke(sc, THREE_PARAM_ARGS);
                            Assert.fail();
                        default:
                            throw new IndexOutOfBoundsException();
                    }
                } catch (IllegalArgumentException iae) {
                    throw new RuntimeException("Call failed for " + method, iae);
                } catch (InvocationTargetException err) {
                    Assert.assertTrue(err.getTargetException().getMessage().contains("permission denied"));
                }
            }
        }
    }

    @Test
    public void testReadOnlySecurityContext() throws IllegalAccessException {
        SecurityContext sc = ReadOnlySecurityContext.INSTANCE;
        for (Method method : SecurityContext.class.getMethods()) {
            String name = method.getName();
            if (name.startsWith("authorize")) {
                Class<?>[] parameters = method.getParameterTypes();
                try {
                    switch (parameters.length) {
                        case 0:
                            method.invoke(sc, NO_PARAM_ARGS);
                            if (name.startsWith("authorizeAdminAction")
                                    || name.equals("authorizeHttp") || name.equals("authorizePGWire") || name.equals("authorizeLineTcp")) {
                                continue;
                            }
                            Assert.fail();
                            break;
                        case 1:
                            if (name.equals("authorizeCopyCancel")) {
                                method.invoke(sc, sc);
                            } else if (name.equals("authorizeTableBackup")) {
                                method.invoke(sc, new ObjHashSet<CharSequence>());
                            } else {
                                method.invoke(sc, ONE_PARAM_ARGS);
                            }
                            if (name.startsWith("authorizeShow")
                                    || name.startsWith("authorizeSelect")) {
                                continue;
                            }
                            Assert.fail();
                            break;
                        case 2:
                            method.invoke(sc, TWO_PARAM_ARGS);
                            if (name.equals("authorizeSelect")) {
                                continue;
                            }
                            Assert.fail();
                            break;
                        case 3:
                            method.invoke(sc, THREE_PARAM_ARGS);
                            Assert.fail();
                            break;
                        default:
                            throw new IndexOutOfBoundsException();
                    }
                } catch (IllegalArgumentException iae) {
                    throw new RuntimeException("Call failed for " + method, iae);
                } catch (InvocationTargetException err) {
                    Assert.assertTrue(err.getTargetException().getMessage().contains("permission denied"));
                }
            }
        }
    }
}
