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

package io.questdb.cutlass.text.types;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cutlass.text.TextConfiguration;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.str.DirectCharSink;
import org.jetbrains.annotations.TestOnly;

public class TypeManager implements Mutable {
    // includes all available probes, including non-default like geohash
    private final int allProbeCount;
    // holds all type adapters used to validate file against types/formats supplied by user or fetched from existing table metadata
    private final ObjList<TypeAdapter> allTypeAdapterList = new ObjList<>();
    private final ObjectPool<DateUtf8Adapter> dateAdapterPool;
    // includes default probes only to avoid type clashes during detection (e.g. short vs int)
    private final int defaultProbeCount;

    // holds type adapters used for type detection, doesn't include types that might clash (e.g. byte, short or geohash)
    private final ObjList<TypeAdapter> defaultTypeAdapterList = new ObjList<>();
    private final SymbolAdapter indexedSymbolAdapter;
    private final InputFormatConfiguration inputFormatConfiguration;
    private final SymbolAdapter notIndexedSymbolAdapter;
    private final StringAdapter stringAdapter;
    private final ObjectPool<TimestampAdapter> timestampAdapterPool;
    private final ObjectPool<TimestampUtf8Adapter> timestampUtf8AdapterPool;
    private final IntList typeAdapterIndexList = new IntList();
    /* maps all column type to type adapter indexes (probe indexes) in this type manager*/
    private final IntObjHashMap<IntList> typeAdapterIndexMap = new IntObjHashMap<>();
    private final IntObjHashMap<ObjList<TypeAdapter>> typeAdapterMap = new IntObjHashMap<>();

    public TypeManager(
            TextConfiguration configuration,
            DirectCharSink utf16Sink
    ) {
        this.dateAdapterPool = new ObjectPool<>(() -> new DateUtf8Adapter(utf16Sink), configuration.getDateAdapterPoolCapacity());
        this.timestampUtf8AdapterPool = new ObjectPool<>(() -> new TimestampUtf8Adapter(utf16Sink), configuration.getTimestampAdapterPoolCapacity());
        this.timestampAdapterPool = new ObjectPool<>(TimestampAdapter::new, configuration.getTimestampAdapterPoolCapacity());
        this.inputFormatConfiguration = configuration.getInputFormatConfiguration();
        this.stringAdapter = new StringAdapter(utf16Sink);
        this.indexedSymbolAdapter = new SymbolAdapter(utf16Sink, true);
        this.notIndexedSymbolAdapter = new SymbolAdapter(utf16Sink, false);
        addDefaultAdapters();

        final ObjList<DateFormat> dateFormats = inputFormatConfiguration.getDateFormats();
        final ObjList<DateLocale> dateLocales = inputFormatConfiguration.getDateLocales();
        final ObjList<String> datePatterns = inputFormatConfiguration.getDatePatterns();
        final IntList dateUtf8Flags = inputFormatConfiguration.getDateUtf8Flags();
        for (int i = 0, n = dateFormats.size(); i < n; i++) {
            if (dateUtf8Flags.getQuick(i) == 1) {
                defaultTypeAdapterList.add(
                        new DateUtf8Adapter(utf16Sink).of(
                                datePatterns.getQuick(i),
                                dateFormats.getQuick(i),
                                dateLocales.getQuick(i)
                        )
                );
            } else {
                defaultTypeAdapterList.add(
                        new DateAdapter().of(
                                datePatterns.getQuick(i),
                                dateFormats.getQuick(i),
                                dateLocales.getQuick(i)
                        )
                );
            }
        }

        final ObjList<DateFormat> timestampFormats = inputFormatConfiguration.getTimestampFormats();
        final ObjList<DateLocale> timestampLocales = inputFormatConfiguration.getTimestampLocales();
        final ObjList<String> timestampPatterns = inputFormatConfiguration.getTimestampPatterns();
        final IntList timestampUtf8Flags = inputFormatConfiguration.getTimestampUtf8Flags();
        for (int i = 0, n = timestampFormats.size(); i < n; i++) {
            if (timestampUtf8Flags.getQuick(i) == 1) {
                defaultTypeAdapterList.add(new TimestampUtf8Adapter(utf16Sink).of(
                        timestampPatterns.getQuick(i),
                        timestampFormats.getQuick(i),
                        timestampLocales.getQuick(i))
                );
            } else {
                defaultTypeAdapterList.add(new TimestampAdapter().of(
                        timestampPatterns.getQuick(i),
                        timestampFormats.getQuick(i),
                        timestampLocales.getQuick(i))
                );
            }
        }
        this.defaultProbeCount = defaultTypeAdapterList.size();
        allTypeAdapterList.addAll(defaultTypeAdapterList);
        addNonDefaultAdapters(utf16Sink);
        this.allProbeCount = allTypeAdapterList.size();

        // index adapters by type
        for (int i = 0; i < allProbeCount; i++) {
            TypeAdapter typeAdapter = allTypeAdapterList.getQuick(i);
            ObjList<TypeAdapter> mappedList;
            IntList probeIndexList;
            int index = typeAdapterMap.keyIndex(typeAdapter.getType());
            int probeIndex = typeAdapterIndexMap.keyIndex(typeAdapter.getType());
            if (index > -1) {
                mappedList = new ObjList<>();
                probeIndexList = new IntList();
                typeAdapterMap.putAt(index, typeAdapter.getType(), mappedList);
                typeAdapterIndexMap.putAt(probeIndex, typeAdapter.getType(), probeIndexList);
            } else {
                mappedList = typeAdapterMap.valueAt(index);
                probeIndexList = typeAdapterIndexMap.valueAt(index);
            }
            mappedList.add(typeAdapter);
            probeIndexList.add(i);
        }

        for (int i = 0; i < allProbeCount; i++) {
            typeAdapterIndexList.add(i);
        }
    }

    @Override
    public void clear() {
        dateAdapterPool.clear();
        timestampUtf8AdapterPool.clear();
        timestampAdapterPool.clear();
    }

    @TestOnly
    public ObjList<TypeAdapter> getAllAdapters() {
        return allTypeAdapterList;
    }

    public int getAllProbeCount() {
        return allProbeCount;
    }

    public int getDefaultProbeCount() {
        return defaultProbeCount;
    }

    public ObjList<TypeAdapter> getDefaultTypeAdapterList() {
        return defaultTypeAdapterList;
    }

    public InputFormatConfiguration getInputFormatConfiguration() {
        return inputFormatConfiguration;
    }

    public TypeAdapter getProbe(int index) {
        return allTypeAdapterList.getQuick(index);
    }

    public TypeAdapter getTypeAdapter(int columnType) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BYTE:
                return ByteAdapter.INSTANCE;
            case ColumnType.SHORT:
                return ShortAdapter.INSTANCE;
            case ColumnType.CHAR:
                return CharAdapter.INSTANCE;
            case ColumnType.INT:
                return IntAdapter.INSTANCE;
            case ColumnType.LONG:
                return LongAdapter.INSTANCE;
            case ColumnType.BOOLEAN:
                return BooleanAdapter.INSTANCE;
            case ColumnType.FLOAT:
                return FloatAdapter.INSTANCE;
            case ColumnType.DOUBLE:
                return DoubleAdapter.INSTANCE;
            case ColumnType.STRING:
                return stringAdapter;
            case ColumnType.SYMBOL:
                return nextSymbolAdapter(false);
            case ColumnType.LONG256:
                return Long256Adapter.INSTANCE;
            case ColumnType.UUID:
                return UuidAdapter.INSTANCE;
            case ColumnType.IPv4:
                return IPv4Adapter.INSTANCE;
            case ColumnType.GEOBYTE:
            case ColumnType.GEOSHORT:
            case ColumnType.GEOINT:
            case ColumnType.GEOLONG:
                GeoHashAdapter adapter = GeoHashAdapter.getInstance(columnType);
                if (adapter != null) {
                    return adapter;
                }
            default:
                throw CairoException.nonCritical().put("no adapter for type [id=").put(columnType).put(", name=").put(ColumnType.nameOf(columnType)).put(']');
        }
    }

    public IntList getTypeAdapterIndexList() {
        return typeAdapterIndexList;
    }

    public IntObjHashMap<IntList> getTypeAdapterIndexMap() {
        return typeAdapterIndexMap;
    }

    public IntObjHashMap<ObjList<TypeAdapter>> getTypeAdapterMap() {
        return typeAdapterMap;
    }

    public DateUtf8Adapter nextDateAdapter() {
        return dateAdapterPool.next();
    }

    public TypeAdapter nextSymbolAdapter(boolean indexed) {
        return indexed ? indexedSymbolAdapter : notIndexedSymbolAdapter;
    }

    public TypeAdapter nextTimestampAdapter(String pattern, boolean decodeUtf8, DateFormat format, DateLocale locale) {
        if (decodeUtf8) {
            TimestampUtf8Adapter adapter = timestampUtf8AdapterPool.next();
            adapter.of(pattern, format, locale);
            return adapter;
        }

        TimestampAdapter adapter = timestampAdapterPool.next();
        adapter.of(pattern, format, locale);
        return adapter;
    }

    private void addDefaultAdapters() {
        defaultTypeAdapterList.add(getTypeAdapter(ColumnType.CHAR));
        defaultTypeAdapterList.add(getTypeAdapter(ColumnType.INT));
        defaultTypeAdapterList.add(getTypeAdapter(ColumnType.LONG));
        defaultTypeAdapterList.add(getTypeAdapter(ColumnType.DOUBLE));
        defaultTypeAdapterList.add(getTypeAdapter(ColumnType.BOOLEAN));
        defaultTypeAdapterList.add(getTypeAdapter(ColumnType.LONG256));
        defaultTypeAdapterList.add(getTypeAdapter(ColumnType.UUID));
        defaultTypeAdapterList.add(getTypeAdapter(ColumnType.IPv4));
    }

    private void addNonDefaultAdapters(DirectCharSink utf16Sink) {
        allTypeAdapterList.add(getTypeAdapter(ColumnType.BYTE));
        allTypeAdapterList.add(getTypeAdapter(ColumnType.SHORT));
        allTypeAdapterList.add(getTypeAdapter(ColumnType.FLOAT));

        for (int b = 1; b <= ColumnType.GEOLONG_MAX_BITS; b++) {
            int type = ColumnType.getGeoHashTypeWithBits(b);
            GeoHashAdapter adapter = GeoHashAdapter.getInstance(type);
            allTypeAdapterList.add(adapter);
        }

        // add timestamp formats as date formats
        final ObjList<DateLocale> timestampLocales = inputFormatConfiguration.getTimestampLocales();
        final ObjList<String> timestampPatterns = inputFormatConfiguration.getTimestampPatterns();
        final IntList timestampUtf8Flags = inputFormatConfiguration.getTimestampUtf8Flags();
        for (int i = 0, n = timestampPatterns.size(); i < n; i++) {
            String pattern = timestampPatterns.getQuick(i);

            // skip patterns containing micros or nanos, millis are fine
            if (pattern.contains("U") || pattern.contains("N")) {
                continue;
            }

            DateFormat dateFormat = inputFormatConfiguration.getDateFormatFactory().get(pattern);

            if (timestampUtf8Flags.getQuick(i) == 1) {
                allTypeAdapterList.add(new DateUtf8Adapter(utf16Sink).of(
                        pattern,
                        dateFormat,
                        timestampLocales.getQuick(i))
                );
            } else {
                allTypeAdapterList.add(new DateAdapter().of(
                        pattern,
                        dateFormat,
                        timestampLocales.getQuick(i))
                );
            }
        }
    }
}
