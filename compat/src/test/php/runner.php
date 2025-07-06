<?php
/**
 * QuestDB Test Runner
 * PHP Port of the Python test runner using PDO
 */

// the 'Large Result Set' test eats memory like there is no tomorrow
ini_set('memory_limit', '512M');

require 'vendor/autoload.php';

use Symfony\Component\Yaml\Yaml;

class TestRunner {
    private $connection;
    private $variables;

    public function loadYaml(string $filePath): array {
        return Yaml::parseFile($filePath);
    }

    private function substituteVariables(string $text = null, array $variables): ?string {
        if ($text === null) {
            return null;
        }
        return preg_replace_callback('/\${(\w+)}/', function($matches) use ($variables) {
            return $variables[$matches[1]] ?? $matches[0];
        }, $text);
    }

    private function adjustPlaceholderSyntax(string $query): string {
        // Replace $[n] with ?
        return preg_replace('/\$\[\d+\]/', '?', $query);
    }

    private function executeQuery($stmt, array $parameters = []) {
        if (!empty($parameters)) {
            $stmt->execute($parameters);
        } else {
            $stmt->execute();
        }

        try {
            if ($stmt->columnCount() > 0) {
                $result = $stmt->fetchAll(PDO::FETCH_NUM);
                // Normalize data formats to match Python version
                foreach ($result as &$row) {
                    foreach ($row as &$value) {
                        // Handle timestamps
                        if (preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/', $value)) {
                            // Ensure 6 decimal places for microseconds
                            if (strpos($value, '.') === false) {
                                $value .= '.000000';
                            } else {
                                // Pad existing microseconds to 6 digits
                                $parts = explode('.', $value);
                                $parts[1] = str_pad(rtrim($parts[1], '0'), 6, '0');
                                $value = implode('.', $parts);
                            }
                            // Convert space to T and add Z
                            $value = str_replace(' ', 'T', $value) . 'Z';
                        }
                        // Handle dates
                        elseif (preg_match('/^\d{4}-\d{2}-\d{2}$/', $value)) {
                            $value .= 'T00:00:00.000000Z';
                        }
                        // Handle numeric strings that should be numbers
                        elseif (is_string($value) && preg_match('/^-?\d+\.?\d*$/', $value)) {
                            if (strpos($value, '.') !== false) {
                                $value = (float)$value;
                            } else {
                                $value = (int)$value;
                            }
                        }
                        // Handle booleans (PDO returns them as strings)
                        elseif ($value === '0' || $value === '1') {
                            $value = (bool)$value;
                        }
                    }
                }
                return $result;
            } else {
                $rowCount = $stmt->rowCount();
                return $rowCount === 0 ? null : [[$rowCount]];
            }
        } catch (PDOException $e) {
            return $stmt->errorInfo()[2];
        }
    }

    private function resolveParameters(array $typedParameters, array $variables): array {
        $resolvedParameters = [];
        foreach ($typedParameters as $typedParam) {
            $type = strtolower($typedParam['type']);
            $value = $typedParam['value'];

            if (is_string($value)) {
                $resolvedStrValue = $this->substituteVariables($value, $variables);
                $this->convertAndAppendParameters($resolvedStrValue, $type, $resolvedParameters);
            } else {
                $this->convertAndAppendParameters($value, $type, $resolvedParameters);
            }
        }
        return $resolvedParameters;
    }

    private function convertAndAppendParameters($value, string $type, array &$resolvedParameters): void {
        switch ($type) {
            case 'int4':
            case 'int8':
                $resolvedParameters[] = (int)$value;
                break;
            case 'float4':
            case 'float8':
                $resolvedParameters[] = (float)$value;
                break;
            case 'boolean':
                $value = strtolower(trim($value));
                if ($value === 'true') {
                    $resolvedParameters[] = true;
                } elseif ($value === 'false') {
                    $resolvedParameters[] = false;
                } else {
                    throw new InvalidArgumentException("Invalid boolean value: {$value}");
                }
                break;
            case 'varchar':
            case 'char':
                $resolvedParameters[] = (string)$value;
                break;
            case 'timestamp':
            case 'date':
                $resolvedParameters[] = $value;
                break;
            default:
                $resolvedParameters[] = $value;
        }
    }

    private function assertResult(array $expect, $actual): void {
        if (isset($expect['result'])) {
            $expectedResult = $expect['result'];
            if (is_array($expectedResult)) {
                if (is_string($actual)) {
                    throw new RuntimeException("Expected result " . json_encode($expectedResult) . ", got status '{$actual}'");
                }
                if ($actual != $expectedResult) {
                    throw new RuntimeException("Expected result " . json_encode($expectedResult) . ", got " . json_encode($actual));
                }
            } else {
                if (strval($actual) !== strval($expectedResult)) {
                    throw new RuntimeException("Expected result '{$expectedResult}', got '{$actual}'");
                }
            }
        } elseif (isset($expect['result_contains'])) {
            if (is_string($actual)) {
                throw new RuntimeException("Expected result containing " . json_encode($expect['result_contains']) . ", got status '{$actual}'");
            }
            foreach ($expect['result_contains'] as $expectedRow) {
                $found = false;
                foreach ($actual as $actualRow) {
                    if ($actualRow == $expectedRow) {
                        $found = true;
                        break;
                    }
                }
                if (!$found) {
                    throw new RuntimeException("Expected row " . json_encode($expectedRow) . " not found in actual results.");
                }
            }
        }
    }

    private function executeLoop(array $loopDef, array $variables, PDO $connection): void {
        $loopVarName = $loopDef['as'];
        $loopVariables = $variables;

        if (isset($loopDef['over'])) {
            $iterable = $loopDef['over'];
        } elseif (isset($loopDef['range'])) {
            $start = $loopDef['range']['start'];
            $end = $loopDef['range']['end'];
            $iterable = range($start, $end);
        } else {
            throw new InvalidArgumentException("Loop must have 'over' or 'range' defined.");
        }

        foreach ($iterable as $item) {
            $loopVariables[$loopVarName] = $item;
            $this->executeSteps($loopDef['steps'], $loopVariables, $connection);
        }
    }

    private function executeStep(array $step, array $variables, PDO $connection): void {
        $queryTemplate = $step['query'] ?? null;
        $parameters = $step['parameters'] ?? [];
        $expect = $step['expect'] ?? [];

        $queryWithVars = $this->substituteVariables($queryTemplate, $variables);
        $query = $this->adjustPlaceholderSyntax($queryWithVars);

        $resolvedParameters = $this->resolveParameters($parameters, $variables);
        $stmt = $connection->prepare($query);
        $result = $this->executeQuery($stmt, $resolvedParameters);

        if (!empty($expect)) {
            $this->assertResult($expect, $result);
        }
    }

    private function executeSteps(array $steps, array $variables, PDO $connection): void {
        foreach ($steps as $step) {
            if (isset($step['loop'])) {
                $this->executeLoop($step['loop'], $variables, $connection);
            } else {
                $this->executeStep($step, $variables, $connection);
            }
        }
    }

    public function runTest(array $test, array $globalVariables, PDO $connection): void {
        $variables = array_merge($globalVariables, $test['variables'] ?? []);
        $testFailed = false;

        try {
            // Prepare phase
            $prepareSteps = $test['prepare'] ?? [];
            $this->executeSteps($prepareSteps, $variables, $connection);

            // Test steps
            $testSteps = $test['steps'] ?? [];
            $this->executeSteps($testSteps, $variables, $connection);

            echo "Test '{$test['name']}' passed.\n";
        } catch (Exception $e) {
            echo "Test '{$test['name']}' failed: {$e->getMessage()}\n";
            $testFailed = true;
        } finally {
            // Teardown phase
            $teardownSteps = $test['teardown'] ?? [];
            try {
                $this->executeSteps($teardownSteps, $variables, $connection);
            } catch (Exception $e) {
                echo "Teardown for test '{$test['name']}' failed: {$e->getMessage()}\n";
            }
        }

        if ($testFailed) {
            exit(1);
        }
    }

    public function main(string $yamlFile): void {
        $data = $this->loadYaml($yamlFile);
        $globalVariables = $data['variables'] ?? [];
        $tests = $data['tests'] ?? [];

        $port = getenv('PGPORT') ?: 8812;

        foreach ($tests as $test) {
            $iterations = $test['iterations'] ?? 50;
            $exclusions = $test['exclude'] ?? [];
            if (in_array('php', $exclusions)) {
                echo "Skipping test '{$test['name']}' due to exclusion for PHP.\n";
                continue;
            }

            for ($i = 0; $i < $iterations; $i++) {
                echo "Running test '{$test['name']}' iteration " . ($i + 1) . "...\n";

                $connection = new PDO(
                    "pgsql:host=localhost;port={$port};dbname=qdb",
                    'admin',
                    'quest',
                    [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
                );

                $this->runTest($test, $globalVariables, $connection);
                $connection = null;
            }
        }
    }
}

// Command line execution
if (php_sapi_name() === 'cli') {
    if ($argc !== 2) {
        echo "Usage: php runner.php <test_file.yaml>\n";
        exit(1);
    }

    $yamlFile = $argv[1];
    $runner = new TestRunner();
    $runner->main($yamlFile);
}