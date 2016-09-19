/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * The MIT License (MIT)
 *
 * Copyright (C) 2016 Appsicle
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR
 * ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
 * CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

/*globals jQuery:false */
/*globals vg:false */

(function ($) {
    'use strict';
    $.fn.chart = function (msgBus) {
        var bus = $(msgBus);
        // var container = $(this);

        function draw() {
            // line chart
            // var spec = {
            //     width: 900,
            //     height: 300,
            //     data: [
            //         {
            //             name: 'accidents',
            //             url: '/csv?query=%0A%0Aselect%20severity%2C%20timestamp%20ts%2C%20count()%20count%20from%20z2%20sample%20by%20Y',
            //             format: {type: 'csv', parse: {ts: 'date', count: 'number'}}
            //         }
            //     ],
            //     scales: [
            //         {
            //             name: 'a',
            //             type: 'time',
            //             range: 'width',
            //             domain: {data: 'accidents', field: 'ts'}
            //         },
            //         {
            //             name: 'b',
            //             type: 'linear',
            //             range: 'height',
            //             nice: true,
            //             domain: {data: 'accidents', field: 'count'}
            //         },
            //         {
            //             name: 'color',
            //             type: 'ordinal',
            //             domain: {data: 'accidents', field: 'severity'},
            //             range: 'category10',
            //             nice: true
            //         }
            //
            //     ],
            //     axes: [
            //         {type: 'x', scale: 'a'},
            //         {type: 'y', scale: 'b'}
            //     ],
            //     legends: [
            //         {
            //             fill: 'color',
            //             properties: {
            //                 title: {
            //                     fontSize: {value: 14}
            //                 },
            //                 labels: {
            //                     fontSize: {value: 12}
            //                 },
            //                 symbols: {
            //                     stroke: {value: 'transparent'}
            //                 },
            //                 legend: {
            //                     stroke: {value: '#ccc'},
            //                     strokeWidth: {value: 1.5}
            //                 }
            //             }
            //         }
            //     ],
            //     marks: [
            //         {
            //             type: 'group',
            //             from: {
            //                 data: 'accidents',
            //                 transform: [{type: 'facet', groupby: ['severity']}]
            //             },
            //             marks: [
            //                 {
            //                     type: 'line',
            //                     properties: {
            //                         enter: {
            //                             x: {scale: 'a', field: 'ts'},
            //                             y: {scale: 'b', field: 'count'},
            //                             stroke: {scale: 'color', field: 'severity'},
            //                             strokeWidth: {value: 4}
            //                         }
            //                     }
            //                 }
            //             ]
            //         }
            //     ]
            // };


            // stacked area chart

            // var spec = {
            //     width: 500,
            //     height: 200,
            //     // padding: {top: 10, left: 30, bottom: 30, right: 10},
            //     data: [
            //         {
            //             name: 'accidents',
            //             url: 'http://localhost:9000/csv?query=%0A%0Aselect%20severity%2C%20timestamp%20ts%2C%20count()%20count%20from%20z2%20where%20severity%20%3D%20\'Slight\'%20sample%20by%201M',
            //             format: {type: 'csv', parse: {ts: 'date', count: 'number'}}
            //         },
            //         {
            //             name: 'stats',
            //             source: 'accidents',
            //             transform: [
            //                 {
            //                     type: 'aggregate',
            //                     groupby: ['ts'],
            //                     summarize: [{field: 'count', ops: ['sum']}]
            //                 }
            //             ]
            //         }
            //     ],
            //     scales: [
            //         {
            //             name: 'x',
            //             type: 'time',
            //             range: 'width',
            //             points: true,
            //             domain: {data: 'accidents', field: 'ts'}
            //         },
            //         {
            //             name: 'y',
            //             type: 'linear',
            //             range: 'height',
            //             nice: true,
            //             domain: {data: 'stats', field: 'sum_count'}
            //         },
            //         {
            //             name: 'color',
            //             type: 'ordinal',
            //             range: 'category10',
            //             domain: {data: 'accidents', field: 'severity'},
            //             nice: true
            //         }
            //     ],
            //     axes: [
            //         {type: 'x', scale: 'x', grid: true},
            //         {type: 'y', scale: 'y', grid: true}
            //     ],
            //     marks: [
            //         {
            //             type: 'group',
            //             from: {
            //                 data: 'accidents',
            //                 transform: [
            //                     {type: 'stack', groupby: ['ts'], sortby: ['severity'], field: 'count'},
            //                     {type: 'facet', groupby: ['severity']}
            //                 ]
            //             },
            //             marks: [
            //                 {
            //                     type: 'area',
            //                     properties: {
            //                         enter: {
            //                             interpolate: {value: 'monotone'},
            //                             x: {scale: 'x', field: 'ts'},
            //                             y: {scale: 'y', field: 'layout_start'},
            //                             y2: {scale: 'y', field: 'layout_end'},
            //                             fill: {scale: 'color', field: 'severity'}
            //                         },
            //                         update: {
            //                             fillOpacity: {value: 1}
            //                         },
            //                         hover: {
            //                             fillOpacity: {value: 0.5}
            //                         }
            //                     }
            //                 }
            //             ]
            //         }
            //     ]
            // };

            // scatter plot

            var spec = {
                width: 500,
                height: 200,
                // padding: {top: 10, left: 30, bottom: 30, right: 10},
                data: [
                    {
                        name: 'accidents',
                        url: 'http://localhost:9000/csv?query=%0A%0Aselect%20severity%2C%20timestamp%20ts%2C%20count()%20count%20from%20z2%20%20sample%20by%201M',
                        format: {type: 'csv', parse: {ts: 'date', count: 'number'}}
                    }
                ],
                scales: [
                    {
                        name: 'x',
                        type: 'time',
                        range: 'width',
                        points: true,
                        domain: {data: 'accidents', field: 'ts'}
                    },
                    {
                        name: 'y',
                        type: 'linear',
                        range: 'height',
                        nice: true,
                        domain: {data: 'accidents', field: 'count'}
                    },
                    {
                        name: 'color',
                        type: 'ordinal',
                        range: 'category10',
                        domain: {data: 'accidents', field: 'severity'},
                        nice: true
                    }
                ],
                axes: [
                    {type: 'x', scale: 'x', grid: true},
                    {type: 'y', scale: 'y', grid: true}
                ],
                marks: [
                    {
                        type: 'group',
                        from: {
                            data: 'accidents',
                            transform: [
                                {type: 'stack', groupby: ['ts'], sortby: ['severity'], field: 'count'},
                                {type: 'facet', groupby: ['severity']}
                            ]
                        },
                        marks: [
                            {
                                type: 'symbol',
                                properties: {
                                    update: {
                                        x: {scale: 'x', field: 'ts'},
                                        y: {scale: 'y', field: 'count'},
                                        fill: {scale: 'color', field: 'severity'},
                                        size: {value: 10}
                                    }
                                }
                            }
                        ]
                    }
                ]
            };

            // stacked bar

            // var spec = {
            //     width: 500,
            //     height: 200,
            //     padding: {top: 10, left: 30, bottom: 30, right: 10},
            //     data: [
            //         {
            //             name: 'accidents',
            //             url: 'http://localhost:9000/csv?query=%0A%0Aselect%20severity%2C%20timestamp%20ts%2C%20count()%20count%20from%20z2%20%20sample%20by%201M',
            //             format: {type: 'csv', parse: {ts: 'date', count: 'number'}}
            //         },
            //         {
            //             name: 'stats',
            //             source: 'accidents',
            //             transform: [
            //                 {
            //                     type: 'aggregate',
            //                     groupby: ['ts'],
            //                     summarize: [{field: 'count', ops: ['sum']}]
            //                 }
            //             ]
            //         }
            //     ],
            //     scales: [
            //         {
            //             name: 'x',
            //             type: 'time',
            //             range: 'width',
            //             domain: {data: 'accidents', field: 'ts'}
            //         },
            //         {
            //             name: 'y',
            //             type: 'linear',
            //             range: 'height',
            //             nice: true,
            //             domain: {data: 'stats', field: 'sum_count'}
            //         },
            //         {
            //             name: 'color',
            //             type: 'ordinal',
            //             range: 'category10',
            //             domain: {data: 'accidents', field: 'severity'}
            //         }
            //     ],
            //     axes: [
            //         {type: 'x', scale: 'x', grid: true},
            //         {type: 'y', scale: 'y', grid: true}
            //     ],
            //     marks: [
            //         {
            //             type: 'rect',
            //             from: {
            //                 data: 'accidents',
            //                 transform: [
            //                     {type: 'stack', groupby: ['ts'], sortby: ['severity'], field: 'count'}
            //                 ]
            //             },
            //             properties: {
            //                 enter: {
            //                     x: {scale: 'x', field: 'ts'},
            //                     width: {value: 1},
            //                     y: {scale: 'y', field: 'layout_start'},
            //                     y2: {scale: 'y', field: 'layout_end'},
            //                     fill: {scale: 'color', field: 'severity'}
            //                 },
            //                 update: {
            //                     fillOpacity: {value: 1}
            //                 },
            //                 hover: {
            //                     fillOpacity: {value: 0.5}
            //                 }
            //             }
            //         }
            //     ]
            // };


            // pie chart
            // var spec = {
            //     width: 400,
            //     height: 400,
            //     data: [
            //         {
            //             name: 'table',
            //             url: 'http://localhost:9000/csv?query=%0A%0Aselect%20severity%2C%20count%20from%20(select%20severity%2C%20count()%20count%20from%20z2)',
            //             format: {type: 'csv', parse: {count: 'number'}},
            //             transform: [{type: 'pie', field: 'count'}]
            //         }
            //     ],
            //     scales: [
            //         {
            //             name: 'r',
            //             type: 'sqrt',
            //             domain: {data: 'table', field: 'count'},
            //             range: [20, 100]
            //         }
            //     ],
            //     marks: [
            //         {
            //             type: 'arc',
            //             from: {data: 'table'},
            //             properties: {
            //                 enter: {
            //                     x: {field: {group: 'width'}, mult: 0.5},
            //                     y: {field: {group: 'height'}, mult: 0.5},
            //                     startAngle: {field: 'layout_start'},
            //                     endAngle: {field: 'layout_end'},
            //                     innerRadius: {value: 20},
            //                     outerRadius: {scale: 'r', field: 'count'},
            //                     stroke: {value: '#fff'}
            //                 },
            //                 update: {
            //                     fill: {value: '#ccc'}
            //                 },
            //                 hover: {
            //                     fill: {value: 'pink'}
            //                 }
            //             }
            //         },
            //         {
            //             type: 'text',
            //             from: {data: 'table'},
            //             properties: {
            //                 enter: {
            //                     x: {field: {group: 'width'}, mult: 0.5},
            //                     y: {field: {group: 'height'}, mult: 0.5},
            //                     radius: {scale: 'r', field: 'count', offset: 8},
            //                     theta: {field: 'layout_mid'},
            //                     fill: {value: '#000'},
            //                     align: {value: 'center'},
            //                     baseline: {value: 'middle'},
            //                     text: {field: 'severity'}
            //                 }
            //             }
            //         }
            //     ]
            // };

            //
            vg.parse.spec(spec, function (error, chart) {
                console.log(error);
                chart({el: '#chart'}).update();
            });
        }

        bus.on('chart.draw', draw);
    };
}(jQuery));
