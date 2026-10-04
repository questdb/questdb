/*******************************************************************************
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

package io.questdb.test.cairo.types;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeTag;
import io.questdb.cairo.PhysicalDescriptor;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Set;

/**
 * The column types the conformance kit runs, each with the name a {@code CREATE TABLE}
 * declares it by.
 * <p>
 * Existing types: every real {@link ColumnTypeTag} (pseudo tags excluded, as
 * {@code TypeDriverTest} does), then the encoded variants the golden tables use
 * ({@code TypeRelationGoldenTest}). A tag whose column type carries parameters (geohash bits,
 * decimal precision and scale, array element and dimensions) is represented by one encoding of
 * its own, distinct from the variants, so every tag and every variant is a case of its own. The
 * ARRAY tag's entry is the DOUBLE[] variant, listed once. Existing types have a recording in
 * every {@code TypeConformance*Test}.
 * <p>
 * Types registered later: a real tag the list below does not name. It joins the kit by its
 * registration lines alone; the kit reads its declarations, NULL policy and paths from the
 * resource {@link #LATER_TYPES_RESOURCE}, one line each,
 * {@code tag | DDL | NULL policy | paths [| arithmetic tier [| refused sites]]}, and checks it with
 * {@link TypeConformanceInvariants} instead of a recording. The arithmetic tier comes from the
 * type's definition ({@code TypeDriver.getArithmetic()}); a tier on the line, which stood in for
 * that answer before S14b, must agree with it. The refused sites, comma-separated, are the guarded
 * sites the type is refused at on purpose, labelled as the site map
 * ({@link TypeConformanceInvariants#SITES_FILE}) labels them: a path that reaches one must fail
 * with that site's refusal. A later tag without a resource line is still listed, so every kit
 * class fails on it with a message that names it.
 */
public final class TypeConformanceTypes {
    public static final ObjList<Entry> ALL = new ObjList<>();
    public static final String LATER_TYPES_RESOURCE = "/io/questdb/test/cairo/types/later-types.txt";
    // tags that resolve overloads or mark parser state; none of their values is stored or computed
    static final Set<ColumnTypeTag> PSEUDO_TAGS = EnumSet.of(
            ColumnTypeTag.UNDEFINED, ColumnTypeTag.CURSOR, ColumnTypeTag.VAR_ARG, ColumnTypeTag.RECORD,
            ColumnTypeTag.GEOHASH, ColumnTypeTag.DECIMAL, ColumnTypeTag.REGCLASS, ColumnTypeTag.REGPROCEDURE,
            ColumnTypeTag.ARRAY_STRING, ColumnTypeTag.PARAMETER, ColumnTypeTag.NULL, ColumnTypeTag.UNKNOWN
    );

    private TypeConformanceTypes() {
    }

    public static Entry byLabel(CharSequence label) {
        for (int i = 0, n = ALL.size(); i < n; i++) {
            final Entry entry = ALL.getQuick(i);
            if (entry.label.contentEquals(label)) {
                return entry;
            }
        }
        throw new IllegalArgumentException("no conformance kit type: " + label);
    }

    /**
     * Whether a tag is one of the existing types, which the kit holds recordings for; false for
     * a pseudo tag and for a type registered later.
     */
    public static boolean isExistingTag(ColumnTypeTag tag) {
        return EXISTING_TAGS.contains(tag);
    }

    /**
     * Parses one declaration line of {@link #LATER_TYPES_RESOURCE},
     * {@code tag | DDL | NULL policy | paths [| arithmetic tier [| refused sites]]}, into its six
     * fields, trimmed; a field the line leaves out is empty. Every refused site must be one of
     * {@code declarableSites}.
     */
    static String[] parseLaterTypeLine(String line, Set<String> declarableSites) {
        final String[] parts = line.split("\\|", -1);
        if (parts.length < 4 || parts.length > 6) {
            throw new IllegalStateException("bad line in " + LATER_TYPES_RESOURCE + ": " + line);
        }
        final String[] fields = new String[6];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = i < parts.length ? parts[i].trim() : "";
        }
        final ObjList<String> sites = splitSites(fields[5]);
        for (int i = 0, n = sites.size(); i < n; i++) {
            if (!declarableSites.contains(sites.getQuick(i))) {
                throw new IllegalStateException("bad line in " + LATER_TYPES_RESOURCE + ": " + sites.getQuick(i)
                        + " is not a guarded site of " + TypeConformanceInvariants.SITES_FILE + ": " + line);
            }
        }
        return fields;
    }

    /**
     * The guarded-site labels of a declaration's sixth field, comma-separated; none for an empty
     * field.
     */
    static ObjList<String> splitSites(String field) {
        final ObjList<String> sites = new ObjList<>();
        for (String site : field.split(",")) {
            if (!site.trim().isEmpty()) {
                sites.add(site.trim());
            }
        }
        return sites;
    }

    private static void addExisting(EnumMap<ColumnTypeTag, Entry> byTag, ColumnTypeTag tag, String label, int columnType, String ddl) {
        final Entry entry = new Entry(label, columnType, ddl, tag, null, null, null, new ObjList<>());
        if (tag != null) {
            byTag.put(tag, entry);
        }
        ALL.add(entry);
    }

    private static void addLaterTypes(EnumMap<ColumnTypeTag, Entry> byTag) {
        final ObjList<String[]> lines = readLaterTypes();
        for (ColumnTypeTag tag : ColumnTypeTag.values()) {
            if (PSEUDO_TAGS.contains(tag) || byTag.containsKey(tag)) {
                continue;
            }
            boolean isDeclared = false;
            for (int i = 0, n = lines.size(); i < n; i++) {
                final String[] line = lines.getQuick(i);
                if (line[0].equals(tag.name())) {
                    // tag | ddl | NULL policy | paths | arithmetic tier | refused sites
                    ALL.add(new Entry(line[1], tag.code(), line[1], tag, line[2], line[3], tierOf(tag, line[4]), splitSites(line[5])));
                    isDeclared = true;
                }
            }
            if (!isDeclared) {
                ALL.add(new Entry(tag.name(), tag.code(), tag.name(), tag, null, null, null, new ObjList<>()));
            }
        }
    }

    /**
     * The arithmetic tier the kit derives rows and order from, as the definition answers it:
     * null for WIDE and NONE, whose minimum, maximum and order the tier alone does not give. A
     * tier the resource line declares must be the definition's.
     */
    @Nullable
    private static String tierOf(ColumnTypeTag tag, @Nullable String declaredTier) {
        final PhysicalDescriptor.Arithmetic arithmetic = ColumnType.getTypeDriver(tag.code()).getArithmetic();
        if (declaredTier != null && !declaredTier.isEmpty() && !declaredTier.equals(arithmetic.name())) {
            throw new IllegalStateException("arithmetic tier of " + tag.name() + " in " + LATER_TYPES_RESOURCE + " is "
                    + declaredTier + ", its definition answers " + arithmetic.name());
        }
        return switch (arithmetic) {
            case I8, I16, I32, I64, U8, U16, U32, F32, F64 -> arithmetic.name();
            case WIDE, NONE -> null;
        };
    }

    private static ObjList<String[]> readLaterTypes() {
        final ObjList<String> declarations = new ObjList<>();
        try (InputStream in = TypeConformanceTypes.class.getResourceAsStream(LATER_TYPES_RESOURCE)) {
            if (in == null) {
                return new ObjList<>();
            }
            final BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#") || line.startsWith("mix|")) {
                    // mixing cases belong to TypeConformanceInvariants
                    continue;
                }
                declarations.add(line);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        // the site map is read only when a line declares a refused site
        boolean hasRefusedSites = false;
        for (int i = 0, n = declarations.size(); i < n; i++) {
            final String[] parts = declarations.getQuick(i).split("\\|", -1);
            hasRefusedSites |= parts.length == 6 && !parts[5].isBlank();
        }
        final Set<String> declarableSites = hasRefusedSites ? TypeConformanceInvariants.declarableSites() : Set.of();
        final ObjList<String[]> lines = new ObjList<>();
        for (int i = 0, n = declarations.size(); i < n; i++) {
            lines.add(parseLaterTypeLine(declarations.getQuick(i), declarableSites));
        }
        return lines;
    }

    /**
     * One kit type: an encoded column type and its DDL name. {@link #label} names the type in
     * recordings and failure messages.
     */
    public static final class Entry {
        public final int columnType;
        public final String ddl;
        public final String label;
        /**
         * For a type registered later: the paths it runs on, as the resource lists them; null for
         * an existing type.
         */
        @Nullable
        public final String laterPaths;
        /**
         * For a type registered later: its NULL policy (SENTINEL, NONE, BITMAP or NOT_NULL), as
         * the resource declares it; null for an existing type.
         */
        @Nullable
        public final String laterPolicy;
        /**
         * For a type registered later: the guarded sites it is refused at on purpose, as the
         * resource declares them; empty for an existing type and for a later type that declares
         * none.
         */
        public final ObjList<String> laterRefusedSites;
        /**
         * For a type registered later: its arithmetic tier (I8, I16, I32, I64, U8, U16, U32, F32,
         * F64) as its definition answers it; null for WIDE and NONE, and for an existing type.
         */
        @Nullable
        public final String laterTier;
        public final ColumnTypeTag tag;

        Entry(
                String label,
                int columnType,
                String ddl,
                ColumnTypeTag tag,
                @Nullable String laterPolicy,
                @Nullable String laterPaths,
                @Nullable String laterTier,
                ObjList<String> laterRefusedSites
        ) {
            this.label = label;
            this.columnType = columnType;
            this.ddl = ddl;
            this.tag = tag;
            this.laterPolicy = laterPolicy;
            this.laterPaths = laterPaths;
            this.laterTier = laterTier == null || laterTier.isEmpty() ? null : laterTier;
            this.laterRefusedSites = laterRefusedSites;
        }

        /**
         * True when the type declares a floating-point arithmetic tier: its values compare as
         * floats, where every NaN is the same value.
         */
        public boolean isFloat() {
            return laterTier != null && laterTier.startsWith("F");
        }

        /**
         * True for a type registered after the S12 recording: it has no recording and the kit
         * checks it by invariants.
         */
        public boolean isLater() {
            return laterPolicy != null || (tag != null && !EXISTING_TAGS.contains(tag));
        }

        @Override
        public String toString() {
            return label;
        }
    }

    // the real tags at the S12 recording
    private static final Set<ColumnTypeTag> EXISTING_TAGS;

    static {
        final EnumMap<ColumnTypeTag, Entry> byTag = new EnumMap<>(ColumnTypeTag.class);
        addExisting(byTag, ColumnTypeTag.BOOLEAN, "BOOLEAN", ColumnType.BOOLEAN, "BOOLEAN");
        addExisting(byTag, ColumnTypeTag.BYTE, "BYTE", ColumnType.BYTE, "BYTE");
        addExisting(byTag, ColumnTypeTag.SHORT, "SHORT", ColumnType.SHORT, "SHORT");
        addExisting(byTag, ColumnTypeTag.CHAR, "CHAR", ColumnType.CHAR, "CHAR");
        addExisting(byTag, ColumnTypeTag.INT, "INT", ColumnType.INT, "INT");
        addExisting(byTag, ColumnTypeTag.LONG, "LONG", ColumnType.LONG, "LONG");
        addExisting(byTag, ColumnTypeTag.DATE, "DATE", ColumnType.DATE, "DATE");
        addExisting(byTag, ColumnTypeTag.TIMESTAMP, "TIMESTAMP", ColumnType.TIMESTAMP, "TIMESTAMP");
        addExisting(byTag, ColumnTypeTag.FLOAT, "FLOAT", ColumnType.FLOAT, "FLOAT");
        addExisting(byTag, ColumnTypeTag.DOUBLE, "DOUBLE", ColumnType.DOUBLE, "DOUBLE");
        addExisting(byTag, ColumnTypeTag.STRING, "STRING", ColumnType.STRING, "STRING");
        addExisting(byTag, ColumnTypeTag.SYMBOL, "SYMBOL", ColumnType.SYMBOL, "SYMBOL");
        addExisting(byTag, ColumnTypeTag.LONG256, "LONG256", ColumnType.LONG256, "LONG256");
        addExisting(byTag, ColumnTypeTag.GEOBYTE, "GEOBYTE", ColumnType.getGeoHashTypeWithBits(7), "GEOHASH(7b)");
        addExisting(byTag, ColumnTypeTag.GEOSHORT, "GEOSHORT", ColumnType.getGeoHashTypeWithBits(15), "GEOHASH(3c)");
        addExisting(byTag, ColumnTypeTag.GEOINT, "GEOINT", ColumnType.getGeoHashTypeWithBits(30), "GEOHASH(6c)");
        addExisting(byTag, ColumnTypeTag.GEOLONG, "GEOLONG", ColumnType.getGeoHashTypeWithBits(40), "GEOHASH(8c)");
        addExisting(byTag, ColumnTypeTag.BINARY, "BINARY", ColumnType.BINARY, "BINARY");
        addExisting(byTag, ColumnTypeTag.UUID, "UUID", ColumnType.UUID, "UUID");
        addExisting(byTag, ColumnTypeTag.LONG128, "LONG128", ColumnType.LONG128, "LONG128");
        addExisting(byTag, ColumnTypeTag.IPv4, "IPv4", ColumnType.IPv4, "IPV4");
        addExisting(byTag, ColumnTypeTag.VARCHAR, "VARCHAR", ColumnType.VARCHAR, "VARCHAR");
        addExisting(byTag, ColumnTypeTag.ARRAY, "DOUBLE[]", ColumnType.encodeArrayType(ColumnType.DOUBLE, 1), "DOUBLE[]");
        addExisting(byTag, ColumnTypeTag.DECIMAL8, "DECIMAL8", ColumnType.getDecimalType(2, 1), "DECIMAL(2,1)");
        addExisting(byTag, ColumnTypeTag.DECIMAL16, "DECIMAL16", ColumnType.getDecimalType(4, 2), "DECIMAL(4,2)");
        addExisting(byTag, ColumnTypeTag.DECIMAL32, "DECIMAL32", ColumnType.getDecimalType(9, 0), "DECIMAL(9,0)");
        addExisting(byTag, ColumnTypeTag.DECIMAL64, "DECIMAL64", ColumnType.getDecimalType(16, 4), "DECIMAL(16,4)");
        addExisting(byTag, ColumnTypeTag.DECIMAL128, "DECIMAL128", ColumnType.getDecimalType(38, 10), "DECIMAL(38,10)");
        addExisting(byTag, ColumnTypeTag.DECIMAL256, "DECIMAL256", ColumnType.getDecimalType(76, 20), "DECIMAL(76,20)");
        addExisting(byTag, ColumnTypeTag.INTERVAL, "INTERVAL", ColumnType.INTERVAL, "INTERVAL");
        addExisting(byTag, ColumnTypeTag.VARCHAR_SLICE, "VARCHAR_SLICE", ColumnType.VARCHAR_SLICE, "VARCHAR_SLICE");
        // the encoded variants of TypeRelationGoldenTest; DOUBLE[] is the ARRAY tag's entry above
        addExisting(byTag, null, "TIMESTAMP_NS", ColumnType.TIMESTAMP_NANO, "TIMESTAMP_NS");
        addExisting(byTag, null, "GEOHASH(1c)", ColumnType.getGeoHashTypeWithBits(5), "GEOHASH(1c)");
        addExisting(byTag, null, "GEOHASH(8b)", ColumnType.getGeoHashTypeWithBits(8), "GEOHASH(8b)");
        addExisting(byTag, null, "GEOHASH(31b)", ColumnType.getGeoHashTypeWithBits(31), "GEOHASH(31b)");
        addExisting(byTag, null, "GEOHASH(12c)", ColumnType.getGeoHashTypeWithBits(60), "GEOHASH(12c)");
        addExisting(byTag, null, "DECIMAL(5,2)", ColumnType.getDecimalType(5, 2), "DECIMAL(5,2)");
        addExisting(byTag, null, "DECIMAL(18,3)", ColumnType.getDecimalType(18, 3), "DECIMAL(18,3)");
        addExisting(byTag, null, "DOUBLE[][]", ColumnType.encodeArrayType(ColumnType.DOUBLE, 2), "DOUBLE[][]");
        addExisting(byTag, null, "INTERVAL(us)", ColumnType.INTERVAL_TIMESTAMP_MICRO, "INTERVAL");
        addExisting(byTag, null, "INTERVAL(ns)", ColumnType.INTERVAL_TIMESTAMP_NANO, "INTERVAL");
        EXISTING_TAGS = EnumSet.copyOf(byTag.keySet());
        addLaterTypes(byTag);
    }
}
