/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

/*globals $:false */
/*globals module:false */

function VisBuilder(mapQueries, mapSeries, mapAxis) {
    'use strict';

    this.mapQueries = mapQueries;
    this.mapSeries = mapSeries;
    this.mapAxis = mapAxis;
}

function isReady(map) {
    'use strict';

    for (let sk in map) {
        if (map.hasOwnProperty(sk)) {
            const series = map[sk];
            if (!series.ready) {
                return false;
            }
        }
    }
    return true;
}

function invalidate(map) {
    'use strict';

    for (let k in map) {
        if (map.hasOwnProperty(k)) {
            map[k].ready = false;
        }
    }
}

function parseColumns(columns) {
    'use strict';

    const colArray = columns.split(',');
    const columnMap = new Map();
    const n = colArray.length;
    for (let i = 0; i < n; i++) {
        columnMap.set(colArray[i].trim(), null);
    }
    return columnMap;
}

function parseColumnsToMap(map) {
    'use strict';

    for (let sk in map) {
        if (map.hasOwnProperty(sk)) {
            const e = map[sk];
            e.columnMap = parseColumns(e.columns);
        }
    }
}

function createColumnHash(columns) {
    'use strict';

    const columnHash = [];
    // create hash of query column names for fast lookup
    for (let i = 0; i < columns.length; i++) {
        const col = columns[i];
        columnHash[col.name] = {
            name: col.name,
            index: i
        };
    }
    return columnHash;
}

function reportQueryError(jqXHR) {
    'use strict';

    console.log(jqXHR);
}

function assignDataToSeriesColumns(mapSeries, queryNamePrefix, queryColHash, ds) {
    'use strict';

    const prefixLen = queryNamePrefix.length;
    // analyse series and map datasource and column index in that datasource to each column in series
    for (let sk in mapSeries) {
        if (mapSeries.hasOwnProperty(sk)) {
            const series = mapSeries[sk];

            if (series.ready) {
                continue;
            }

            console.log('analysing series');
            console.log(series);
            console.log(queryColHash);
            const columnMap = series.columnMap;

            series.ready = true;

            for (let key of columnMap.keys()) {
                const col = key.startsWith(queryNamePrefix) ? key.substr(prefixLen, key.length - prefixLen) : key;
                const colDef = queryColHash[col];
                if (colDef) {
                    if (!series.dataLength) {
                        series.dataLength = ds.length;
                    } else if (series.dataLength !== ds.length) {
                        series.error = 'Column data is of unequal length';
                    }

                    columnMap.set(key, {
                        dataset: ds,
                        columnIndex: colDef.index
                    });
                }

                if (columnMap.get(key) === null) {
                    series.ready = false;
                }
            }
        }
    }
}

function assignDataToAxis(mapAxis, queryNamePrefix, queryColHash, ds) {
    'use strict';

    const prefixLen = queryNamePrefix.length;
    // analyse series and map datasource and column index in that datasource to each column in series
    for (let ak in mapAxis) {
        if (mapAxis.hasOwnProperty(ak)) {
            const axis = mapAxis[ak];
            if (axis.ready) {
                continue;
            }

            if (axis.valueType === 'Category column') {
                const col = axis.column.startsWith(queryNamePrefix) ? axis.column.substr(prefixLen, axis.column.length - prefixLen) : axis.column;
                const colDef = queryColHash[col];
                if (colDef) {
                    axis.ready = true;
                    axis.dataset = ds;
                    axis.columnIndex = colDef.index;
                } else {
                    axis.ready = false;
                }
            } else {
                axis.ready = true;
            }
        }
    }
}

function generateLegend(mapSeries) {
    'use strict';

    const data = [];
    for (let sk in mapSeries) {
        if (mapSeries.hasOwnProperty(sk)) {
            const series = mapSeries[sk];
            data.push(series.name);
        }
    }

    return {
        data
    };
}

function generateOptionSeries(mapSeries, xMap, yMap) {
    'use strict';

    console.log('maps');
    console.log(xMap);
    console.log(yMap);

    const optionSeries = [];
    for (let sk in mapSeries) {
        if (mapSeries.hasOwnProperty(sk)) {
            const series = mapSeries[sk];
            const columnMap = series.columnMap;
            const columnCount = columnMap.size;
            const colArray = [];

            for (let val of columnMap.values()) {
                colArray.push(val);
            }

            const chartSeries = {
                name: series.name
            };

            chartSeries.stack = series.stack;
            if (series.axis) {
                console.log('have axis: ' + series.axis);
                const yIndex = yMap[series.axis];
                if (yIndex) {
                    chartSeries.yAxisIndex = yIndex;
                } else {
                    chartSeries.yAxisIndex = 0;
                }

                const xIndex = xMap[series.axis];
                if (xIndex) {
                    chartSeries.xAxisIndex = xIndex;
                } else {
                    chartSeries.xAxisIndex = 0;
                }
            } else {
                chartSeries.xAxisIndex = 0;
                chartSeries.yAxisIndex = 0;
            }

            switch (series.chartType) {
                case 'Line':
                    chartSeries.type = 'line';
                    chartSeries.itemStyle = {normal: {lineStyle: {type: 'solid'}}};
                    break;
                case 'Bar':
                    chartSeries.type = 'bar';
                    break;
                case 'Area':
                    chartSeries.type = 'line';
                    chartSeries.itemStyle = {normal: {areaStyle: {type: 'default'}}};
                    break;
                case 'Scatter':
                    chartSeries.type = 'scatter';
                    break;
                default:
                    chartSeries.type = 'line';
                    break;
            }

            const n = series.dataLength;
            const data = new Array(n);
            chartSeries.data = data;

            if (columnCount === 1) {
                const def = colArray[0];
                for (let i = 0; i < n; i++) {
                    data[i] = def.dataset[i][def.columnIndex];
                }
            } else if (columnCount > 1) {

                for (let i = 0; i < n; i++) {
                    const row = new Array(columnMap.length);
                    for (let j = 0; j < columnCount; j++) {
                        const col = colArray[j];
                        row[j] = col.dataset[i][col.columnIndex];
                    }
                    data[i] = row;
                }
            }

            optionSeries.push(chartSeries);
        }
    }

    return optionSeries;
}

function generateOptionAxis(mapAxis) {
    'use strict';

    const yAxis = [];
    const xAxis = [];
    const xMap = [];
    const yMap = [];

    let xIndex = 0;
    let yIndex = 0;
    for (let ak in mapAxis) {
        if (mapAxis.hasOwnProperty(ak)) {
            const axis = mapAxis[ak];
            const optionAxis = {
                name: axis.name,
                scale: axis.scale
            };

            switch (axis.valueType) {
                case 'Category column':
                    optionAxis.type = 'category';
                    const n = axis.dataset.length;
                    const data = new Array(n);
                    for (let i = 0; i < n; i++) {
                        data[i] = axis.dataset[i][axis.columnIndex];
                    }
                    optionAxis.data = data;
                    break;

                case 'Category values':
                    optionAxis.type = 'category';
                    optionAxis.data = JSON.parse(axis.values);
                    break;
                default:
                    optionAxis.type = 'value';
                    break;
            }

            if (axis.type === 'X-axis') {
                xMap[axis.name] = xIndex++;
                xAxis.push(optionAxis);
            } else {
                yMap[axis.name] = yIndex++;
                yAxis.push(optionAxis);
            }
        }
    }

    return {
        xAxis,
        yAxis,
        xMap,
        yMap
    };
}

function parseQueryData(response, status, jqXHR) {
    'use strict';

    const columns = response.columns;
    const queryName = jqXHR.query.name + '.';
    const done = jqXHR.doneCallback;
    const queryColHash = createColumnHash(columns);

    console.log('query responded: ' + queryName);

    assignDataToSeriesColumns(jqXHR.mapSeries, queryName, queryColHash, response.dataset);
    assignDataToAxis(jqXHR.mapAxis, queryName, queryColHash, response.dataset);

    // check if all series are ready

    const seriesReady = isReady(jqXHR.mapSeries);
    const axisReady = isReady(jqXHR.mapAxis);

    console.log('series ready: ' + seriesReady);
    console.log('axis ready: ' + axisReady);

    if (!seriesReady || !axisReady) {
        done('incomplete');
        return;
    }

    console.log('before');
    console.log(jqXHR.chartOptions);
    jqXHR.chartOptions.legend = generateLegend(jqXHR.mapSeries);
    const axis = generateOptionAxis(jqXHR.mapAxis);
    jqXHR.chartOptions.yAxis = axis.yAxis;
    jqXHR.chartOptions.xAxis = axis.xAxis;
    jqXHR.chartOptions.series = generateOptionSeries(jqXHR.mapSeries, axis.xMap, axis.yMap);
    console.log('opts');
    console.log(jqXHR.chartOptions);
    done('done', jqXHR.chartOptions);
}

VisBuilder.prototype.generateOptions = function (done) {
    'use strict';

    const options = {};

    parseColumnsToMap(this.mapSeries);
    invalidate(this.mapSeries);
    invalidate(this.mapAxis);

    for (let q in this.mapQueries) {
        if (this.mapQueries.hasOwnProperty(q)) {
            const query = this.mapQueries[q];

            const queryRequestParams = {
                query: query.textNormalized,
                count: false
            };

            const jqXHR = $.get('/exec', queryRequestParams);
            jqXHR.query = query;
            jqXHR.mapSeries = this.mapSeries;
            jqXHR.mapAxis = this.mapAxis;
            jqXHR.doneCallback = done;
            jqXHR.chartOptions = options;
            jqXHR.done(parseQueryData).fail(reportQueryError);
        }
    }
};

function mapChanged(map, timestamp) {
    'use strict';

    let changed = false;
    for (let k in map) {
        if (map.hasOwnProperty(k)) {
            const entry = map[k];
            if (entry.timestamp && entry.timestamp > timestamp) {
                changed = true;
                break;
            }
        }
    }
    console.log('mapChanged');
    console.log(map);
    console.log(changed);
    return changed;
}

VisBuilder.prototype.stateChanged = function (timestamp) {
    'use strict';

    return mapChanged(this.mapQueries, timestamp) || mapChanged(this.mapAxis, timestamp) || mapChanged(this.mapSeries, timestamp);
};

VisBuilder.prototype.serializeState = function () {
    'use strict';

    const queries = [];
    let timestamp = 0;

    for (let k in this.mapQueries) {
        if (this.mapQueries.hasOwnProperty(k)) {
            const query = this.mapQueries[k];
            queries.push({
                id: query.id,
                name: query.name,
                text: query.text,
                textNormalized: query.textNormalized
            });

            if (query.timestamp && query.timestamp > timestamp) {
                timestamp = query.timestamp;
            }
        }
    }

    const axis = [];

    for (let k in this.mapAxis) {
        if (this.mapAxis.hasOwnProperty(k)) {
            const ax = this.mapAxis[k];
            axis.push({
                id: ax.id,
                name: ax.name,
                type: ax.type,
                valueType: ax.valueType,
                column: ax.column,
                scale: ax.scale
            });

            if (ax.timestamp && ax.timestamp > timestamp) {
                timestamp = axis.timestamp;
            }
        }
    }

    const series = [];

    for (let k in this.mapSeries) {
        if (this.mapSeries.hasOwnProperty(k)) {
            const ser = this.mapSeries[k];
            series.push({
                id: ser.id,
                name: ser.name,
                chartType: ser.chartType,
                stack: ser.stack,
                columns: ser.columns,
                axis: ser.axis
            });

            if (ser.timestamp && ser.timestamp > timestamp) {
                timestamp = ser.timestamp;
            }
        }
    }

    return {
        queries,
        axis,
        series,
        timestamp
    };
};
