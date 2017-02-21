'use strict';

const assert = require('assert');
const jsdom = require("jsdom");
global.window = jsdom.jsdom().defaultView;
global.jQuery = global.$ = require('../bower_components/jquery/dist/jquery.js');
global.qdb = require('../app/scripts/globals.js');
require('../app/scripts/query.js');

describe('Array', function () {
    describe('#indexOf()', function () {
        it('should return -1 when the value is not present', function () {
            const bus = $({});
            const query = bus.query();
            bus.on('query.out.running', function (done) {
                console.log('query is running');
            });
            bus.trigger('query.in.exec', 'x');

            assert.equal(-1, [1, 2, 3].indexOf(4));
        });
    });
});
