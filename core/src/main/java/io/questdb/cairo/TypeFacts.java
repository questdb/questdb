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

/**
 * The facts of a fixed-size type driver: each answer of {@link TypeDriver} that is a value
 * rather than code. {@link FixedSizeTypeDriver} answers from one record per type; the answers
 * that carry code are its other constructor arguments.
 * <p>
 * Every component is required, so a type driver that leaves a fact out does not compile. A fact
 * with a closed set of values is an enum. No two neighbouring components share a Java type, so
 * swapping two neighbours does not compile either; a swap of two distant components of one type
 * fails {@code TypeDriverTest}, which pins every answer of every type.
 * <p>
 * The record holds data only and references no query-engine class, so building one initialises
 * nothing outside this package.
 *
 * @param tag           the tag the type driver serves, {@link TypeDriver#getTag()}
 * @param movement      how storage moves a value, which fixes its width, {@link TypeDriver#getMovement()}
 * @param arithmetic    the arithmetic tier, {@link TypeDriver#getArithmetic()}
 * @param accessor      the accessor family, {@link TypeDriver#getAccessor()}
 * @param nullPolicy    how the type represents NULL, {@link TypeDriver#getNullPolicy()}
 * @param wireKind      how values travel on the result protocols, {@link TypeDriver#getWireKind()}
 * @param relationKind  the class of values the relation rules group the type by, {@link TypeDriver#getRelationKind()}
 * @param relationBits  the value width in bits the relation rules read, {@link TypeDriver#getRelationBits()}
 * @param implicitCasts the overload row, best match first, {@link TypeDriver#getImplicitCasts()}
 * @param pgOid         the PostgreSQL type OID, {@link TypeDriver#getPgOid()}
 * @param signatureChar the character that names the type in a function signature, {@link TypeDriver#getSignatureChar()}
 * @param pgArrayOid    the PostgreSQL OID of an array of the type, {@link TypeDriver#getPgArrayOid()}
 * @param nullWord      every long of the type's NULL, {@link TypeDriver#getNullLong(int)}
 * @param castTarget    whether CAST takes the type as its target, {@link TypeDriver#isCastTarget(boolean)}
 * @param name          the name of the bare tag, {@link TypeDriver#getName(int)}
 */
public record TypeFacts(
        ColumnTypeTag tag,
        PhysicalDescriptor.Movement movement,
        PhysicalDescriptor.Arithmetic arithmetic,
        PhysicalDescriptor.Accessor accessor,
        NullPolicy nullPolicy,
        WireKind wireKind,
        RelationKind relationKind,
        int relationBits,
        short[] implicitCasts,
        int pgOid,
        char signatureChar,
        int pgArrayOid,
        long nullWord,
        CastTarget castTarget,
        String name
) {
}
