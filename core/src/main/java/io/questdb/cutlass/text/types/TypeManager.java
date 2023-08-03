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
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.str.DirectCharSink;

public class TypeManager implements Mutable {
    private final ObjectPool<DateUtf8Adapter> dateAdapterPool;
    private final SymbolAdapter indexedSymbolAdapter;
    private final InputFormatConfiguration inputFormatConfiguration;
    private final SymbolAdapter notIndexedSymbolAdapter;
    private final int probeCount;
    private final ObjList<TypeAdapter> probes = new ObjList<>();
    private final StringAdapter stringAdapter;
    private final ObjectPool<TimestampAdapter> timestampAdapterPool;
    private final ObjectPool<TimestampUtf8Adapter> timestampUtf8AdapterPool;

    public TypeManager(
            TextConfiguration configuration,
            DirectCharSink utf8Sink
    ) {
        this.dateAdapterPool = new ObjectPool<>(() -> new DateUtf8Adapter(utf8Sink), configuration.getDateAdapterPoolCapacity());
        this.timestampUtf8AdapterPool = new ObjectPool<>(() -> new TimestampUtf8Adapter(utf8Sink), configuration.getTimestampAdapterPoolCapacity());
        this.timestampAdapterPool = new ObjectPool<>(TimestampAdapter::new, configuration.getTimestampAdapterPoolCapacity());
        this.inputFormatConfiguration = configuration.getInputFormatConfiguration();
        this.stringAdapter = new StringAdapter(utf8Sink);
        this.indexedSymbolAdapter = new SymbolAdapter(utf8Sink, true);
        this.notIndexedSymbolAdapter = new SymbolAdapter(utf8Sink, false);
        addDefaultProbes();

        final ObjList<DateFormat> dateFormats = inputFormatConfiguration.getDateFormats();
        final ObjList<DateLocale> dateLocales = inputFormatConfiguration.getDateLocales();
        final IntList dateUtf8Flags = inputFormatConfiguration.getDateUtf8Flags();
        for (int i = 0, n = dateFormats.size(); i < n; i++) {
            if (dateUtf8Flags.getQuick(i) == 1) {
                probes.add(new DateUtf8Adapter(utf8Sink).of(dateFormats.getQuick(i), dateLocales.getQuick(i)));
            } else {
                probes.add(new DateAdapter().of(dateFormats.getQuick(i), dateLocales.getQuick(i)));
            }
        }

        final ObjList<DateFormat> timestampFormats = inputFormatConfiguration.getTimestampFormats();
        final ObjList<DateLocale> timestampLocales = inputFormatConfiguration.getTimestampLocales();
        final IntList timestampUtf8Flags = inputFormatConfiguration.getTimestampUtf8Flags();
        for (int i = 0, n = timestampFormats.size(); i < n; i++) {
            if (timestampUtf8Flags.getQuick(i) == 1) {
                probes.add(new TimestampUtf8Adapter(utf8Sink).of(timestampFormats.getQuick(i), timestampLocales.getQuick(i)));
            } else {
                probes.add(new TimestampAdapter().of(timestampFormats.getQuick(i), timestampLocales.getQuick(i)));
            }
        }
        this.probeCount = probes.size();
    }

    @Override
    public void clear() {
        dateAdapterPool.clear();
        timestampUtf8AdapterPool.clear();
        timestampAdapterPool.clear();
    }

    public ObjList<TypeAdapter> getAllAdapters() {
        return probes;
    }

    public InputFormatConfiguration getInputFormatConfiguration() {
        return inputFormatConfiguration;
    }

    public TypeAdapter getProbe(int index) {
        return probes.getQuick(index);
    }

    public int getProbeCount() {
        return probeCount;
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

    public DateUtf8Adapter nextDateAdapter() {
        return dateAdapterPool.next();
    }

    public TypeAdapter nextSymbolAdapter(boolean indexed) {
        return indexed ? indexedSymbolAdapter : notIndexedSymbolAdapter;
    }

    public TypeAdapter nextTimestampAdapter(boolean decodeUtf8, DateFormat format, DateLocale locale) {
        if (decodeUtf8) {
            TimestampUtf8Adapter adapter = timestampUtf8AdapterPool.next();
            adapter.of(format, locale);
            return adapter;
        }

        TimestampAdapter adapter = timestampAdapterPool.next();
        adapter.of(format, locale);
        return adapter;
    }

    private void addDefaultProbes() {
        probes.add(getTypeAdapter(ColumnType.CHAR));
        probes.add(getTypeAdapter(ColumnType.INT));
        probes.add(getTypeAdapter(ColumnType.LONG));
        probes.add(getTypeAdapter(ColumnType.DOUBLE));
        probes.add(getTypeAdapter(ColumnType.BOOLEAN));
        probes.add(getTypeAdapter(ColumnType.LONG256));
        probes.add(getTypeAdapter(ColumnType.UUID));
        probes.add(getTypeAdapter(ColumnType.IPv4));
    }
}
