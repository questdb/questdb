const postgres = require('postgres');
const yaml = require('js-yaml');
const fs = require('fs');

// Force UTC timezone globally for the Node.js process
process.env.TZ = 'UTC';

class TestRunner {
    constructor() {
        this.sql = null; // Will be initialized in main
    }

    substituteVariables(text, variables) {
        if (!text) return null;
        return text.replace(/\${(\w+)}/g, (match, key) =>
            variables[key] !== undefined ? variables[key] : match
        );
    }

    adjustPlaceholderSyntax(query) {
        // The postgres package uses $1, $2, etc. just like the pg package,
        // so we maintain this conversion from $[n] to $n
        return query.replace(/\$\[(\d+)\]/g, '$$$1');
    }

    async executeQuery(sql, query, parameters = []) {
        try {
            // postgres.js uses tagged template literals, but we can use .unsafe() for dynamic queries
            const result = await sql.unsafe(query, parameters);

            if (query.trim().toUpperCase().startsWith('SELECT')) {
                // Convert rows to array of values to match the original implementation
                return result.map(row => Object.values(row));
            } else {
                // For non-SELECT queries, return row count in the same format as the original
                return [[result.count || 0]];
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
                    } else {
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

    async executeLoop(loopDef, variables, sql) {
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
            await this.executeSteps(loopDef.steps, loopVariables, sql);
        }
    }

    async executeStep(step, variables, sql) {
        const queryTemplate = step.query;
        const parameters = step.parameters || [];
        const expect = step.expect || {};

        const queryWithVars = this.substituteVariables(queryTemplate, variables);
        const query = this.adjustPlaceholderSyntax(queryWithVars);

        const resolvedParameters = this.resolveParameters(parameters, variables);

        // Each step runs in its own transaction
        let result;
        await sql.begin(async (transaction) => {
            result = await this.executeQuery(transaction, query, resolvedParameters);
        });

        if (Object.keys(expect).length > 0) {
            this.assertResult(expect, result);
        }
    }

    async executeSteps(steps, variables, sql) {
        for (const step of steps) {
            if (step.loop) {
                await this.executeLoop(step.loop, variables, sql);
            } else {
                await this.executeStep(step, variables, sql);
            }
        }
    }

    async runTest(test, globalVariables, sql) {
        const variables = {...globalVariables, ...(test.variables || {})};
        let testFailed = false;

        try {
            // Prepare phase
            const prepareSteps = test.prepare || [];
            await this.executeSteps(prepareSteps, variables, sql);

            // Test steps
            const testSteps = test.steps || [];
            await this.executeSteps(testSteps, variables, sql);

            console.log(`Test '${test.name}' passed.`);
        } catch (error) {
            console.log(`Test '${test.name}' failed: ${error.message}`);
            testFailed = true;
        } finally {
            // Teardown phase - always execute teardown steps even if test fails
            const teardownSteps = test.teardown || [];
            try {
                await this.executeSteps(teardownSteps, variables, sql);
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

        // Run all tests twice - once with prepare: false and once with prepare: true
        for (const prepareMode of [true, false]) {
            console.log(`\n=============================`);
            console.log(`Running tests with prepare: ${prepareMode}`);
            console.log(`=============================\n`);
            this.sql = postgres({
                prepare: prepareMode,
                host: 'localhost',
                port: port,
                user: 'admin',
                password: 'quest',
                database: 'qdb',
                // Force UTC timezone
                timezone: 'UTC',
                // Configure custom type parsers
                types: {
                    // Custom parsers for integers to ensure they're returned as numbers
                    int2: {
                        from: [21],
                        parse: value => parseInt(value, 10)
                    },
                    int4: {
                        from: [23],
                        parse: value => parseInt(value, 10)
                    },
                    int8: {
                        from: [20],
                        parse: value => parseInt(value, 10)
                    },
                    // Custom parser for timestamps to match expected microsecond precision
                    timestamp: {
                        from: [1114],
                        parse: value => {
                            // Split on the decimal point if it exists
                            const [dateTimePart, fractionPart = '000000'] = value.split('.');
                            // Pad or truncate to exactly 6 digits
                            const normalizedFraction = (fractionPart + '000000').slice(0, 6);
                            // Replace space with T and add normalized fraction and Z
                            return `${dateTimePart.replace(' ', 'T')}.${normalizedFraction}Z`;
                        }
                    }
                },
                // Error handling
                onnotice: message => console.log(`NOTICE: ${message}`)
            });

            for (const test of tests) {
                const exclusions = test.exclude || [];
                if (exclusions.includes('nodejs-postgres')) {
                    console.log(`Skipping test '${test.name}' because it is excluded for nodejs-postgres.`);
                    continue;
                }

                const iterations = test.iterations || 50;
                for (let i = 0; i < iterations; i++) {
                    console.log(`Running test '${test.name}' iteration ${i + 1} with prepare: ${prepareMode}...`);
                    await this.runTest(test, globalVariables, this.sql);
                }
            }

            // Clean up connections when done
            await this.sql.end();
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