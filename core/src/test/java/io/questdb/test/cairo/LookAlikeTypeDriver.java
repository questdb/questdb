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

import io.questdb.cairo.CastTarget;
import io.questdb.cairo.FixedSizeTypeDriver;
import io.questdb.cairo.IntTypeDriver;
import io.questdb.cairo.NullPolicy;
import io.questdb.cairo.PhysicalDescriptor;
import io.questdb.cairo.TypeFacts;

/**
 * A type driver registered under no tag that answers INT's facts and code, except for its name,
 * its arithmetic tier and its NULL policy: the shape of a later type that reads through INT's
 * accessor, such as an unsigned or a never-null INT. Tests hand it to the static helpers a site
 * calls with the type driver it found, since no tag can resolve to it.
 */
public final class LookAlikeTypeDriver extends FixedSizeTypeDriver {
    private final String name;

    private LookAlikeTypeDriver(String name, PhysicalDescriptor.Arithmetic arithmetic, NullPolicy nullPolicy) {
        super(
                new TypeFacts(
                        IntTypeDriver.INSTANCE.getTag(),
                        IntTypeDriver.INSTANCE.getMovement(),
                        arithmetic,
                        IntTypeDriver.INSTANCE.getAccessor(),
                        nullPolicy,
                        IntTypeDriver.INSTANCE.getWireKind(),
                        IntTypeDriver.INSTANCE.getRelationKind(),
                        IntTypeDriver.INSTANCE.getRelationBits(),
                        IntTypeDriver.INSTANCE.getImplicitCasts(),
                        IntTypeDriver.INSTANCE.getPgOid(),
                        IntTypeDriver.INSTANCE.getSignatureChar(),
                        IntTypeDriver.INSTANCE.getPgArrayOid(),
                        IntTypeDriver.INSTANCE.getNullLong(0),
                        CastTarget.ALWAYS,
                        name
                ),
                IntTypeDriver.INSTANCE::defineBindVariable,
                IntTypeDriver.INSTANCE::getNullConstant,
                IntTypeDriver.INSTANCE::getTypeConstant,
                IntTypeDriver.INSTANCE::newColumnFunction,
                IntTypeDriver.INSTANCE::newNullAppender,
                IntTypeDriver.INSTANCE::setNull
        );
        this.name = name;
    }

    /**
     * A never-null INT: signed 32-bit, every bit pattern a value.
     */
    public static LookAlikeTypeDriver neverNullInt() {
        return new LookAlikeTypeDriver("NN_INT", PhysicalDescriptor.Arithmetic.I32, NullPolicy.NONE);
    }

    /**
     * A signed 32-bit INT whose NULL is INT's sentinel: INT's facts under another name.
     */
    public static LookAlikeTypeDriver sentinelInt() {
        return new LookAlikeTypeDriver("INT32", PhysicalDescriptor.Arithmetic.I32, NullPolicy.SENTINEL);
    }

    /**
     * An unsigned 32-bit INT whose NULL is INT's sentinel.
     */
    public static LookAlikeTypeDriver unsignedInt() {
        return new LookAlikeTypeDriver("UINT32", PhysicalDescriptor.Arithmetic.U32, NullPolicy.SENTINEL);
    }

    @Override
    public String getTypeName() {
        return name;
    }
}
