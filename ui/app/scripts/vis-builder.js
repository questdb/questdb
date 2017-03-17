/*globals $:false */
/*globals module:false */

function VisBuilder(mapQueries, mapSeries, mapAxis) {
    'use strict';

    this.mapQueries = mapQueries;
    this.mapSeries = mapSeries;
    this.mapAxis = mapAxis;
}

function isReady(mapSeries) {
    'use strict';

    for (let sk in mapSeries) {
        if (mapSeries.hasOwnProperty(sk)) {
            const series = mapSeries[sk];
            if (!series.ready) {
                return false;
            }
        }
    }
    return true;
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

function generateOptionSeries(mapSeries) {
    'use strict';

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

            switch (series.chartType) {
                case 'Line':
                    chartSeries.type = 'line';
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

    for (let ak in mapAxis) {
        if (mapAxis.hasOwnProperty(ak)) {
            const axis = mapAxis[ak];
            const optionAxis = {
                name: axis.name
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
                xAxis.push(optionAxis);
            } else {
                yAxis.push(optionAxis);
            }
        }
    }

    return {
        xAxis,
        yAxis
    };
}

function parseQueryData(response, status, jqXHR) {
    'use strict';

    const columns = response.columns;
    const queryName = jqXHR.query.name + '.';
    const done = jqXHR.doneCallback;
    const queryColHash = createColumnHash(columns);

    assignDataToSeriesColumns(jqXHR.mapSeries, queryName, queryColHash, response.dataset);
    assignDataToAxis(jqXHR.mapAxis, queryName, queryColHash, response.dataset);

    // check if all series are ready

    if (!isReady(jqXHR.mapSeries) || !isReady(jqXHR.mapAxis)) {
        done('incomplete');
        return;
    }

    jqXHR.chartOptions.series = generateOptionSeries(jqXHR.mapSeries);
    console.log(jqXHR.mapAxis);
    const axis = generateOptionAxis(jqXHR.mapAxis);
    jqXHR.chartOptions.yAxis = axis.yAxis;
    jqXHR.chartOptions.xAxis = axis.xAxis;
    done('done', jqXHR.chartOptions);
}

VisBuilder.prototype.generateOptions = function (done) {
    'use strict';

    const options = {};

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
