
const cratedb = require('./db');
const csv = require('./csv');
const uuid = require('uuid/v4');
const expect = require('chai').expect;
const path = require('path');
const dataFile = path.resolve(path.dirname(require.main.filename), 'resources/log_entries.csv');

const tableName = "sensors";
const batchSize = 10;
const expectedRowCount = 10;        // wc -l log_entries.csv minus one for the header
const expectedUpdatedRowCount = 4;  // grep ",0$" log_entries.csv | wc -l


async function setUp() {
    await cratedb.execute(
        `CREATE TABLE ${tableName} (` +
        '        id symbol NOT NULL,' +
        '        temperature int,' +
        '        humidity int,' +
        '        client_ip ip NOT NULL,' +
        '        request string NOT NULL,' +
        '        status_code short NOT NULL,' +
        '        object_size long NOT NULL);'
    )
    return testTableName;
}


function checkData(testTableName) {
    cratedb.executeAsync(`SELECT * FROM ${testTableName};`, (data) => {
        if (!data) {
            throw new Error('expected data, got nothing back');
        }
        expect(data).to.have.lengthOf(expectedRowCount);
        let i = Math.floor(Math.random() * expectedRowCount);
        expect(data[i]).to.have.property('log_time');
        expect(data[i]).to.have.property('client_ip');
        expect(data[i]).to.have.property('request');
        expect(data[i]).to.have.property('status_code');
        expect(data[i]).to.have.property('object_size');
        cratedb.executeAsync(`update ${testTableName} set object_size = 40 where object_size=0 returning *;`, (data) => {
            if (!data) {
                throw new Error('expected data, got nothing back');
            }
            let updateCount = data.filter((row) => row['object_size'] == 40);
            expect(updateCount).to.have.lengthOf(expectedUpdatedRowCount);
            let zeroCount = data.filter((row) => row['object_size'] == 0);
            expect(zeroCount).to.have.lengthOf(0);
            cratedb.executeAsync(`DROP TABLE ${testTableName};`, (_) => {
                cratedb.endPool().then(() => {
                    process.exit(0)
                });
            });
        });
    });
}


(async () => csv.load(dataFile, ',', async (data) => {
    let testTableName = await setUp();
    let startLoadTs = Date.now();
    let inserts = cratedb.generateInserts(testTableName, data, batchSize);
    for (let i=0; i < inserts.length; i++) {
        await cratedb.execute(inserts[i]);
    }
    await cratedb.execute(`REFRESH TABLE ${testTableName};`);
    console.log(`${Date.now() - startLoadTs} ms to load the data.`);
    checkData(testTableName);
}))();
