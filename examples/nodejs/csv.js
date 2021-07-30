
const csv = require('csv-parser')
const fs = require('fs')


function load(filePath, sep, callback) {
    let results = [];
    fs.createReadStream(filePath)
        .pipe(csv({separator: sep}))
        .on('data', (data) => results.push(data))
        .on('end', () => {
            if (results) {
                let colNames = getkeys(results[0]);
                results['colNames'] = colNames;
                results['getValues'] = (i) => {
                    let values = [];
                    let row = results[i];
                    for (let j=0; j < colNames.length; j++) {
                        let cn = colNames[j]
                        values.push(row[cn]);
                    }
                    return values;
                };
            }
            callback(results);
        });
}


function getkeys(obj) {
    let keys = [];
    for (let k in obj) {
        if (obj.hasOwnProperty(k)) {
            keys.push(k);
        }
    }
    return keys;
}


module.exports = {
    load
};
