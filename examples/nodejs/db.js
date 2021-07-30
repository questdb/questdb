
const Pool = require('pg').Pool;
const pgClientPool = new Pool({
    user: 'crate',
    password: '',
    host: process.argv.length === 4 ? process.argv[2] : 'localhost',
    port: process.argv.length === 4 ? process.argv[3] : 5432
});


async function endPool() {
    await pgClientPool.end();
}


function executeAsync(sql, callback) {
    pgClientPool.query(sql, (error, resultSet) => {
        if (error) {
            console.error(`The horror: ${error}`);
            throw error;
        }
        callback(collect(resultSet));
    });
}


async function execute(sql) {
    try {
        await pgClientPool.query(sql);
    } catch (error) {
        console.error(`The horror: ${error}`);
        throw error;
    }
}


function collect(resultSet) {
    let data = [];
    data.colNames = resultSet.fields.map(f => f.name);
    for (let i = 0; i < resultSet.rows.length; i++) {
        let row = resultSet.rows[i];
        let rowData = {};
        for (let j = 0; j < data.colNames.length; j++) {
            let colName = data.colNames[j];
            rowData[colName] = row[colName];
        }
        data.push(rowData);
    }
    return data;
}


const COMMA = ',';


function generateInserts(tableName, data, batchSize) {
    console.log(`Table ${tableName} ${data.length} rows (${Math.ceil(data.length / batchSize)} batches of ~${batchSize})`);
    let colNames = data.colNames.join(',');
    let inserts = [];
    let batchIdx = 0;
    let values = '';
    for (let rowIdx=0; rowIdx < data.length; rowIdx++, batchIdx++) {
        values += '(';
        let colValues = '';
        let row = data.getValues(rowIdx);
        for (let colIdx=0; colIdx < row.length; colIdx++) {
            let colVal = row[colIdx];
            if (isNaN(colVal)) {
                colValues += `'${colVal}'`;
            } else {
                colValues += `${colVal}`;
            }
            colValues += COMMA;
        }
        values += minusDelimiter(colValues);
        values += `)${COMMA}`;
        if (batchIdx >= batchSize) {
            batchIdx = 0;
            inserts.push(generateInsert(tableName, colNames, values));
            values = '';
        }
    }
    if (batchIdx > 0) {
        inserts.push(generateInsert(tableName, colNames, values));
    }
    return inserts;
}


function minusDelimiter(text) {
    return text.substring(0, text.length - COMMA.length);
}


function generateInsert(tableName, colNames, values) {
    return `INSERT INTO ${tableName}(${colNames}) VALUES${minusDelimiter(values)};`;
}


module.exports = {
    execute,
    executeAsync,
    generateInserts,
    endPool
};
