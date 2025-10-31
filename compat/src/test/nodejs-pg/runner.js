const {Client, types} = require('pg');
const yaml = require('js-yaml');
const fs = require('fs');

// Force UTC timezone globally for the Node.js process
process.env.TZ = 'UTC';

class TestRunner {
    substituteVariables(text, variables) {
        if (!text) return null;
        return text.replace(/\${(\w+)}/g, (match, key) =>
            variables[key] !== undefined ? variables[key] : match
        );
    }

    adjustPlaceholderSyntax(query) {
        // Convert $[n] to $n for pg
        return query.replace(/\$\[(\d+)\]/g, '$$$1');
    }

    async executeQuery(client, query, parameters = []) {
        try {
            const result = await client.query(query, parameters);
            if (result.command === 'SELECT') {
                // console.log('Raw result:', result.rows);  // Debug log
                return result.rows.map(row => Object.values(row));
            } else {
                return [[result.rowCount]];
            }
        } catch (error) {
            return error.message;
        }
    }

    resolveParameters(typedParameters, variables) {
        const resolvedParameters = [];
        for (const param of typedParameters) {
            const type = param.type.toLowerCase();
            let value = param.value;

            if (typeof value === 'string') {
                value = this.substituteVariables(value, variables);
            }

            switch (type) {
                case 'int4':
                case 'int8':
                    resolvedParameters.push(parseInt(value));
                    break;
                case 'float4':
                case 'float8':
                    resolvedParameters.push(parseFloat(value));
                    break;
                case 'boolean':
                    value = String(value).toLowerCase().trim();
                    if (value === 'true') {
                        resolvedParameters.push(true);
                    } else if (value === 'false') {
                        resolvedParameters.push(false);
                    } else {
                        throw new Error(`Invalid boolean value: ${value}`);
                    }
                    break;
                case 'varchar':
                case 'char':
                    resolvedParameters.push(String(value));
                    break;
                case 'timestamp':
                    resolvedParameters.push(value);
                    break;
                case 'date':
                    resolvedParameters.push(value);
                    break;
                case 'array_float8':
                    // Handle floating point arrays like {-1, 2, 3, 4, 5.42}
                    if (typeof value === 'string') {
                        // Handle empty array case "{}"
                        if (value.trim() === '{}') {
                            resolvedParameters.push([]);
                            break;
                        }

                        // Strip curly braces and split by comma
                        const strippedValue = value.replace(/^\{|\}$/g, '');
                        // Convert each element to float, handling null values
                        const floatArray = strippedValue.split(',').map(item => {
                            const trimmedItem = item.trim();
                            if (!trimmedItem) return undefined;
                            if (trimmedItem.toLowerCase() === 'null') return null;
                            return parseFloat(trimmedItem);
                        }).filter(item => item !== undefined); // Filter out undefined values
                        resolvedParameters.push(floatArray);
                    }
                    // If already an array, ensure all elements are floats or null
                    else if (Array.isArray(value)) {
                        const floatArray = value.map(item => {
                            if (item === null || item === undefined) return null;
                            return parseFloat(item);
                        });
                        resolvedParameters.push(floatArray);
                    }
                    else {
                        throw new Error(`Invalid array_float8 value: ${value}`);
                    }
                    break;
                default:
                    resolvedParameters.push(value);
            }
        }
        return resolvedParameters;
    }

    formatValue(value) {
        if (value instanceof Date) {
            // Convert to ISO string and ensure exactly 6 decimal places
            const isoString = value.toISOString();
            // Remove the 'Z', split by the decimal point
            const [datePart, fractionPart] = isoString.slice(0, -1).split('.');
            // Pad or truncate to exactly 6 digits and add Z
            return `${datePart}.${(fractionPart + '000000').slice(0, 6)}Z`;
        }

        // Handle array values if they come back as JavaScript arrays
        if (Array.isArray(value)) {
            // Format as PostgreSQL array string with proper handling of null values
            return `{${value.map(v => {
                if (v === null || v === undefined) return 'null';
                if (typeof v === 'number') {
                    // For integers, append .0, for decimals keep their original precision
                    return Number.isInteger(v) ? `${v}.0` : v.toString();
                }
                return this.formatValue(v);
            }).join(',')}}`;
        }

        // Handle array strings to ensure consistent format
        // if (typeof value === 'string' && value.startsWith('{') && value.endsWith('}')) {
        //     // It's already a PostgreSQL array string, ensure numbers have .0 format
        //     return value.replace(/(-?\d+)(?!\.)/g, '$1.0').replace(/\s+/g, '');
        // }

        // For decimals, ensure that they are normalized (e.g., 1.0 becomes 1)
        if (typeof value === "string") {
            const num = Number(value);
            if (!isNaN(num)) {
                return num.toString();
            }
        }

        return value;
    }

    convertResult(result) {
        if (!Array.isArray(result)) return result;
        return result.map(row =>
            row.map(value => this.formatValue(value))
        );
    }

    assertResult(expect, actual) {
        const normalizedActual = this.convertResult(actual);

        if (expect.result !== undefined) {
            const expectedResult = expect.result;
            if (Array.isArray(expectedResult)) {
                if (typeof normalizedActual === 'string') {
                    throw new Error(`Expected result ${JSON.stringify(expectedResult)}, got status '${normalizedActual}'`);
                }
                const actualStr = JSON.stringify(normalizedActual);
                const expectedStr = JSON.stringify(expectedResult);
                if (actualStr !== expectedStr) {
                    throw new Error(`Expected result ${expectedStr}, got ${actualStr}`);
                }
            } else {
                if (String(normalizedActual) !== String(expectedResult)) {
                    throw new Error(`Expected result '${expectedResult}', got '${normalizedActual}'`);
                }
            }
        } else if (expect.result_contains) {
            if (typeof normalizedActual === 'string') {
                throw new Error(`Expected result containing ${JSON.stringify(expect.result_contains)}, got status '${normalizedActual}'`);
            }
            for (const expectedRow of expect.result_contains) {
                const found = normalizedActual.some(actualRow =>
                    JSON.stringify(actualRow) === JSON.stringify(expectedRow)
                );
                if (!found) {
                    throw new Error(`Expected row ${JSON.stringify(expectedRow)} not found in actual results.`);
                }
            }
        }
    }

    async executeLoop(loopDef, variables, client) {
        const loopVarName = loopDef.as;
        const loopVariables = {...variables};

        let iterable;
        if (loopDef.over !== undefined) {
            iterable = loopDef.over;
        } else if (loopDef.range !== undefined) {
            const {start, end} = loopDef.range;
            iterable = Array.from({length: end - start + 1}, (_, i) => start + i);
        } else {
            throw new Error("Loop must have 'over' or 'range' defined.");
        }

        for (const item of iterable) {
            loopVariables[loopVarName] = item;
            await this.executeSteps(loopDef.steps, loopVariables, client);
        }
    }

    async executeStep(step, variables, client) {
        const queryTemplate = step.query;
        const parameters = step.parameters || [];
        const expect = step.expect || {};

        const queryWithVars = this.substituteVariables(queryTemplate, variables);
        const query = this.adjustPlaceholderSyntax(queryWithVars);

        const resolvedParameters = this.resolveParameters(parameters, variables);
        const result = await this.executeQuery(client, query, resolvedParameters);

        if (Object.keys(expect).length > 0) {
            this.assertResult(expect, result);
        }
    }

    async executeSteps(steps, variables, client) {
        for (const step of steps) {
            if (step.loop) {
                await this.executeLoop(step.loop, variables, client);
            } else {
                await this.executeStep(step, variables, client);
            }
        }
    }

    async runTest(test, globalVariables, client) {
        const variables = {...globalVariables, ...(test.variables || {})};
        let testFailed = false;

        try {
            // Prepare phase
            const prepareSteps = test.prepare || [];
            await this.executeSteps(prepareSteps, variables, client);

            // Test steps
            const testSteps = test.steps || [];
            await this.executeSteps(testSteps, variables, client);

            console.log(`Test '${test.name}' passed.`);
        } catch (error) {
            console.log(`Test '${test.name}' failed: ${error.message}`);
            testFailed = true;
        } finally {
            // Teardown phase
            const teardownSteps = test.teardown || [];
            try {
                await this.executeSteps(teardownSteps, variables, client);
            } catch (error) {
                console.log(`Teardown for test '${test.name}' failed: ${error.message}`);
            }
        }

        if (testFailed) {
            process.exit(1);
        }
    }

    async main(yamlFile) {
        const data = yaml.load(fs.readFileSync(yamlFile, 'utf8'));
        const globalVariables = data.variables || {};
        const tests = data.tests || [];

        const port = process.env.PGPORT || 8812;

        for (const test of tests) {
            const iterations = test.iterations || 50;
            for (let i = 0; i < iterations; i++) {
                console.log(`Running test '${test.name}' iteration ${i + 1}...`);

                const client = new Client({
                    host: 'localhost',
                    port: port,
                    user: 'admin',
                    password: 'quest',
                    database: 'qdb',
                    // Force UTC timezone for PostgreSQL connections
                    timezone: 'UTC',
                    types: {
                        getTypeParser: function (oid, format) {
                            // Special handling for INT2 (oid=21), INT4 (oid=23) and INT8 (oid=20) types to ensure
                            // they're numbers since that's what our assertions expect.
                            // by default pg driver returns them as strings: https://node-postgres.com/features/types#strings-by-default
                            if (oid === 21 || oid === 23 || oid === 20) {
                                return value => parseInt(value, 10);
                            }

                            // Special handling for timestamps (oid=1114)
                            // since the default parser returns them as Date objects
                            // and Date in Javascript has millisecond precision while our tests assert microsecond precision
                            if (oid === 1114) {
                                return value => {
                                    // Split on the decimal point if it exists
                                    const [dateTimePart, fractionPart = '000000'] = value.split('.');
                                    // Pad or truncate to exactly 6 digits
                                    const normalizedFraction = (fractionPart + '000000').slice(0, 6);
                                    // Replace space with T and add normalized fraction and Z
                                    return `${dateTimePart.replace(' ', 'T')}.${normalizedFraction}Z`;
                                };
                            }

                            // otherwise use the default parser
                            return types.getTypeParser(oid, format);
                        }
                    }
                });

                try {
                    await client.connect();
                    await this.runTest(test, globalVariables, client);
                } finally {
                    await client.end();
                }
            }
        }
    }
}

// Command line execution
if (require.main === module) {
    if (process.argv.length !== 3) {
        console.log("Usage: node runner.js <test_file.yaml>");
        process.exit(1);
    }

    const yamlFile = process.argv[2];
    const runner = new TestRunner();
    runner.main(yamlFile).catch(error => {
        console.error('Error:', error);
        process.exit(1);
    });
}

module.exports = TestRunner;