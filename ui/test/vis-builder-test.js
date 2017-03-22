'use strict';

const $ = {};
const assert = chai.assert;

describe('VisBuilder', function () {
    describe('#Single series', function () {
        it('should produce one series', function (done) {
            const mapQueries = {
                'query1': {
                    id: '_li_query_1',
                    name: 'query1',
                    text: 'select yearID, salary from \'Salaries.csv\' where playerID = \'rodrial01\';↵',
                    textNormalized: 'select yearID, salary from \'Salaries.csv\' where playerID = \'rodrial01\''
                }
            };

            const mapSeries = {
                'li_series_1': {
                    columns: 'salary',
                    chartType: 'Line',
                    id: 'li_series_1',
                    name: 'series1',
                }
            };

            const response =
                {
                    'query': 'select yearID, salary from \'Salaries.csv\' where playerID = \'rodrial01\'',
                    'columns': [{'name': 'yearID', 'type': 'INT'}, {'name': 'salary', 'type': 'INT'}],
                    'dataset': [[1994, 442333], [1995, 442333], [1996, 442334], [1997, 1062500], [1998, 2162500], [1999, 3112500], [2000, 4362500], [2001, 22000000], [2002, 22000000], [2003, 22000000], [2004, 22000000], [2005, 26000000], [2006, 21680727], [2007, 22708525], [2008, 28000000], [2009, 33000000], [2010, 33000000], [2011, 32000000], [2012, 30000000], [2013, 29000000]],
                    'count': 20
                };

            $.get = function () {
                return {
                    done: function (callback) {
                        const jqXHR = this;
                        // setTimeout(1, function () {
                        // });

                        callback(response, '200 OK', jqXHR);
                        // console.log(callback);
                        return this;
                    },
                    fail: function () {
                    }
                }
            };

            const builder = new VisBuilder(mapQueries, mapSeries, null);
            builder.generateOptions(function (status, options) {
                switch (status) {
                    case 'done':
                        assert.isNotNull(options);
                        console.log(options);
                        assert.deepEqual(options, {
                            legend: {
                                data: ['series1']
                            },
                                series: [
                                    {
                                        name: 'series1',
                                        type: 'line',
                                        data: [
                                            442333, 442333, 442334, 1062500, 2162500, 3112500, 4362500, 22000000, 22000000, 22000000, 22000000, 26000000, 21680727, 22708525, 28000000, 33000000, 33000000, 32000000, 30000000, 29000000
                                        ]
                                    }
                                ],
                                yAxis: [],
                                xAxis: []
                            }
                        );
                        done();
                        break;
                    default:
                        done(status);
                        break;
                }
            });
        });
    });
    it('should serialize its state', function (done) {
        const mapQueries = {
            'query1': {
                id: '_li_query_1',
                name: 'query1',
                text: 'select yearID, salary from \'Salaries.csv\' where playerID = \'rodrial01\';↵',
                textNormalized: 'select yearID, salary from \'Salaries.csv\' where playerID = \'rodrial01\''
            }
        };

        const mapSeries = {
            'li_series_1': {
                columns: 'salary',
                chartType: 'Line',
                id: 'li_series_1',
                name: 'series1',
            }
        };

        const response =
            {
                'query': 'select yearID, salary from \'Salaries.csv\' where playerID = \'rodrial01\'',
                'columns': [{'name': 'yearID', 'type': 'INT'}, {'name': 'salary', 'type': 'INT'}],
                'dataset': [[1994, 442333], [1995, 442333], [1996, 442334], [1997, 1062500], [1998, 2162500], [1999, 3112500], [2000, 4362500], [2001, 22000000], [2002, 22000000], [2003, 22000000], [2004, 22000000], [2005, 26000000], [2006, 21680727], [2007, 22708525], [2008, 28000000], [2009, 33000000], [2010, 33000000], [2011, 32000000], [2012, 30000000], [2013, 29000000]],
                'count': 20
            };

        $.get = function () {
            return {
                done: function (callback) {
                    const jqXHR = this;
                    // setTimeout(1, function () {
                    // });

                    callback(response, '200 OK', jqXHR);
                    // console.log(callback);
                    return this;
                },
                fail: function () {
                }
            }
        };

        const builder = new VisBuilder(mapQueries, mapSeries, null);
        console.log('serialized:');
        console.log(builder.serializeState());
        done();
    });
});
