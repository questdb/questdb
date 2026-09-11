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

package io.questdb.test.griffin.fuzz.types;

import io.questdb.std.Rnd;
import io.questdb.test.griffin.fuzz.expr.FuzzConstant;

public final class IPv4Type implements FuzzColumnType {
    public static final IPv4Type INSTANCE = new IPv4Type();

    // Addresses whose 32-bit pattern sits on a boundary the storage and filter
    // layers treat specially: the null sentinel, the signed min/max pair that
    // separates unsigned from signed ordering, and the all-ones address.
    private static final String[] CORNER_ADDRESSES = {
            "0.0.0.0",           // Numbers.IPv4_NULL
            "0.0.0.1",
            "127.255.255.255",   // Integer.MAX_VALUE
            "128.0.0.0",         // Integer.MIN_VALUE, also the INT null sentinel
            "128.0.0.1",
            "255.255.255.255"    // -1
    };

    private IPv4Type() {
    }

    @Override
    public FuzzConstant generateConstant(Rnd rnd) {
        if (rnd.nextInt(32) == 0) {
            return FuzzConstant.nonBindable("null");
        }
        final String address = rnd.nextInt(3) == 0
                ? CORNER_ADDRESSES[rnd.nextInt(CORNER_ADDRESSES.length)]
                : randomAddress(rnd);
        // Prefer the bare quoted form over 'x.x.x.x'::IPv4: the JIT filter
        // compiler reads a quoted literal straight into an i32 immediate,
        // while the explicit cast is a function node it declines, so the cast
        // form would send every IPv4 predicate down the Java filter and take
        // the JIT differential oracle off the IPv4 code path entirely. Spell
        // the cast out now and then so the fallback keeps getting exercised;
        // only that form is bindable, because :b0::IPv4 compares two IPv4
        // operands while the bare literal compares an IPv4 against a STRING,
        // and the differential runner requires the two forms to agree.
        if (rnd.nextInt(8) == 0) {
            return new FuzzConstant("'" + address + "'::IPv4", "IPv4", address);
        }
        return FuzzConstant.nonBindable("'" + address + "'");
    }

    @Override
    public String getDdl() {
        return "IPv4";
    }

    @Override
    public ColumnKind getKind() {
        return ColumnKind.IPV4;
    }

    @Override
    public String getRndCall() {
        // rnd_ipv4() draws a uniform 32-bit pattern, which covers both halves
        // of the signed range but never lands on the null sentinel and reaches
        // the sign boundary itself only once in four billion rows. Weave the
        // corner addresses in so ordering predicates see them.
        return "CASE WHEN rnd_boolean() THEN rnd_ipv4()"
                + " WHEN rnd_boolean() THEN '128.0.0.0'::IPv4"
                + " WHEN rnd_boolean() THEN null::IPv4"
                + " WHEN rnd_boolean() THEN '127.255.255.255'::IPv4"
                + " WHEN rnd_boolean() THEN '255.255.255.255'::IPv4"
                + " ELSE '0.0.0.1'::IPv4 END";
    }

    @Override
    public String randomLiteral(Rnd rnd) {
        return generateConstant(rnd).literal();
    }

    private static String randomAddress(Rnd rnd) {
        return String.format(
                java.util.Locale.ROOT,
                "%d.%d.%d.%d",
                rnd.nextInt(256),
                rnd.nextInt(256),
                rnd.nextInt(256),
                rnd.nextInt(256)
        );
    }
}
