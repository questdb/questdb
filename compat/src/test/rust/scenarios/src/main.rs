use chrono::NaiveDateTime;
use regex::Regex;
use serde::Deserialize;
use serde_yaml::Value;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::process;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, NoTls};

#[derive(Debug, Deserialize)]
struct TestFile {
    variables: Option<HashMap<String, String>>,
    tests: Vec<TestCase>,
}

#[derive(Debug, Deserialize)]
struct TestCase {
    name: String,
    #[allow(dead_code)]
    description: Option<String>,
    variables: Option<HashMap<String, String>>,
    prepare: Option<Vec<Step>>,
    steps: Vec<Step>,
    teardown: Option<Vec<Step>>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Step {
    ActionStep(ActionStep),
    LoopEnvelope(LoopEnvelope),
}

#[derive(Debug, Deserialize)]
struct LoopEnvelope {
    #[serde(rename = "loop")]
    loop_: Loop,
}

#[derive(Debug, Deserialize)]
struct Loop {
    range: Option<Range>,
    over: Option<Vec<Value>>,
    #[serde(rename = "as")]
    as_name: String,
    steps: Vec<ActionStep>,
}

#[derive(Debug, Deserialize)]
struct ActionStep {
    action: String,
    query: Option<String>,
    parameters: Option<Vec<TypedParameter>>,
    expect: Option<Expect>,
}

#[derive(Debug, Deserialize)]
struct TypedParameter {
    value: Value,
    #[serde(rename = "type")]
    type_: String,
}

#[derive(Debug, Deserialize)]
struct Range {
    start: i64,
    end: i64,
}

#[derive(Debug, Deserialize)]
struct Expect {
    result: Option<Vec<Vec<Value>>>,
    result_contains: Option<Vec<Vec<Value>>>,
    error: Option<String>,
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: runner <test_file.yaml>");
        process::exit(1);
    }
    let yaml_file = &args[1];

    let yaml_content = fs::read_to_string(yaml_file).expect("Failed to read YAML file");
    let test_file: TestFile =
        serde_yaml::from_str(&yaml_content).expect("Failed to parse YAML file");

    // Database connection parameters
    let (client, connection) = tokio_postgres::connect(
        "host=localhost port=8812 user=admin password=quest dbname=qdb",
        NoTls,
    )
    .await
    .expect("Failed to connect to database");

    // Spawn the connection in the background
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Run the tests
    let mut all_tests_passed = true;
    for test in test_file.tests {
        let test_passed = run_test(&client, &test_file.variables, &test).await;
        if !test_passed {
            all_tests_passed = false;
        }
    }

    if !all_tests_passed {
        process::exit(1);
    }
}

async fn run_test(
    client: &Client,
    global_variables: &Option<HashMap<String, String>>,
    test: &TestCase,
) -> bool {
    // Merge global and local variables
    let mut variables = HashMap::new();
    if let Some(globals) = global_variables {
        variables.extend(globals.clone());
    }
    if let Some(locals) = &test.variables {
        variables.extend(locals.clone());
    }

    let mut test_failed = false;

    // Prepare phase
    if let Some(prepare_steps) = &test.prepare {
        if let Err(e) = execute_steps(client, prepare_steps, &mut variables).await {
            eprintln!("Test '{}' failed during prepare phase: {}", test.name, e);
            test_failed = true;
        }
    }

    // Test steps
    if !test_failed {
        if let Err(e) = execute_steps(client, &test.steps, &mut variables).await {
            eprintln!("Test '{}' failed: {}", test.name, e);
            test_failed = true;
        } else {
            println!("Test '{}' passed.", test.name);
        }
    }

    // Teardown phase
    if let Some(teardown_steps) = &test.teardown {
        if let Err(e) = execute_steps(client, teardown_steps, &mut variables).await {
            eprintln!("Teardown for test '{}' failed: {}", test.name, e);
            // Decide if teardown failure should affect test result
        }
    }

    !test_failed
}

async fn execute_steps(
    client: &Client,
    steps: &Vec<Step>,
    variables: &mut HashMap<String, String>,
) -> Result<(), Box<dyn std::error::Error>> {
    for step in steps {
        match step {
            Step::ActionStep(action_step) => {
                execute_step(client, action_step, variables).await?;
            }
            Step::LoopEnvelope(loop_envelope) => {
                execute_loop(client, &loop_envelope.loop_, variables).await?;
            }
        }
    }
    Ok(())
}

async fn execute_loop(
    client: &Client,
    loop_def: &Loop,
    variables: &mut HashMap<String, String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let loop_var_name = &loop_def.as_name;
    let original_value = variables.get(loop_var_name).cloned();

    let iterable = if let Some(over_values) = &loop_def.over {
        over_values.clone()
    } else if let Some(range) = &loop_def.range {
        (range.start..=range.end).map(Value::from).collect()
    } else {
        return Err("Loop must have 'over' or 'range' defined.".into());
    };

    for item in iterable {
        variables.insert(loop_var_name.clone(), value_to_string(&item, variables)?);

        for action_step in &loop_def.steps {
            execute_step(client, action_step, variables).await?;
        }
    }

    // Restore the original value of the loop variable, if any
    if let Some(value) = original_value {
        variables.insert(loop_var_name.clone(), value);
    } else {
        variables.remove(loop_var_name);
    }

    Ok(())
}

async fn execute_step(
    client: &Client,
    action_step: &ActionStep,
    variables: &mut HashMap<String, String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let action = action_step.action.to_lowercase();
    if action != "execute" && action != "query" {
        return Err(format!("Unsupported action: {}", action).into());
    }

    // Substitute variables in query
    let query_template = action_step.query.as_ref().ok_or("Missing query")?;
    let query_with_vars = substitute_variables(query_template, variables)?;

    // Replace parameter placeholders in query
    let query = replace_param_placeholders(&query_with_vars);

    // Extract and substitute parameters
    let params: Vec<Box<dyn ToSql + Sync>> = if let Some(params) = &action_step.parameters {
        extract_parameters(params, variables)?
    } else {
        Vec::new()
    };

    // Prepare the statement
    let stmt = client.prepare(&query).await?;

    // Create a vector of references to the parameters
    let params_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        params.iter().map(|p| p.as_ref()).collect();

    // Execute query
    if action == "query" {
        let result = client.query(&stmt, &params_refs).await;

        // Handle errors
        match result {
            Ok(rows) => {
                if let Some(expectation) = &action_step.expect {
                    assert_result(expectation, &rows)?;
                }
            }
            Err(e) => {
                if let Some(expectation) = &action_step.expect {
                    if let Some(expected_error) = &expectation.error {
                        let error_message = e.to_string();
                        if !error_message.contains(expected_error) {
                            return Err(format!(
                                "Expected error '{}', but got '{}'",
                                expected_error, error_message
                            )
                                .into());
                        }
                    } else {
                        return Err(e.into());
                    }
                } else {
                    return Err(e.into());
                }
            }
        }
    } else {
        let result = client.execute(&stmt, &params_refs).await;
        match result {
            Ok(rows_affected) => {
                if let Some(expectation) = &action_step.expect {
                    if let Some(expected_result) = &expectation.result {
                        if expected_result.len() != 1 || expected_result[0].len() != 1 {
                            return Err("Expected result must be a single value".into());
                        }
                        let expected_value = &expected_result[0][0];
                        if expected_value != &Value::Number(rows_affected.into()) {
                            return Err(format!(
                                "Expected result {:?}, got {:?}",
                                expected_value, rows_affected
                            )
                            .into());
                        }

                    }
                }
            }
            Err(e) => {
                if let Some(expectation) = &action_step.expect {
                    if let Some(expected_error) = &expectation.error {
                        let error_message = e.to_string();
                        if !error_message.contains(expected_error) {
                            return Err(format!(
                                "Expected error '{}', but got '{}'",
                                expected_error, error_message
                            )
                            .into());
                        }
                    } else {
                        return Err(e.into());
                    }
                } else {
                    return Err(e.into());
                }
            }
        }

    }

    Ok(())
}

fn substitute_variables(
    text: &str,
    variables: &HashMap<String, String>,
) -> Result<String, Box<dyn std::error::Error>> {
    let re = Regex::new(r"\$\{([^}]+)\}")?;
    let result = re.replace_all(text, |caps: &regex::Captures| {
        variables
            .get(&caps[1])
            .cloned()
            .unwrap_or_else(|| format!("${{{}}}", &caps[1]))
    });
    Ok(result.into_owned())
}

fn replace_param_placeholders(query: &str) -> String {
    // Replace $[n] with $n for tokio-postgres
    let re = Regex::new(r"\$\[(\d+)\]").unwrap();
    re.replace_all(query, |caps: &regex::Captures| format!("${}", &caps[1]))
        .to_string()
}

fn extract_parameters(
    parameters: &Vec<TypedParameter>,
    variables: &HashMap<String, String>,
) -> Result<Vec<Box<dyn ToSql + Sync>>, Box<dyn std::error::Error>> {
    let mut params: Vec<Box<dyn ToSql + Sync>> = Vec::new();

    for param in parameters {
        let param_value: Box<dyn ToSql + Sync> = match param.value {
            Value::Number(_) => match param.type_.as_str() {
                "int4" => {
                    if let Some(i) = param.value.as_i64() {
                        Box::new(i as i32)
                    } else {
                        return Err("Unsupported parameter type".into());
                    }
                }
                "int8" => {
                    if let Some(i) = param.value.as_i64() {
                        Box::new(i)
                    } else {
                        return Err("Unsupported parameter type".into());
                    }
                }
                "varchar" => {
                    if let Some(i) = param.value.as_i64() {
                        Box::new(i.to_string().clone())
                    } else {
                        return Err("Unsupported parameter type".into());
                    }
                }
                "timestamp" => {
                    if let Some(i) = param.value.as_str() {
                        // 2023-10-01T10:00:00.000000Z
                        let timestamp =
                            NaiveDateTime::parse_from_str(i, "%Y-%m-%dT%H:%M:%S%.fZ").unwrap();
                        Box::new(timestamp)
                    } else {
                        return Err("Unsupported parameter type".into());
                    }
                }
                _ => {
                    return Err("Unsupported parameter type".into());
                }
            },
            Value::String(..) => {
                let substituted =
                    substitute_variables(param.value.as_str().unwrap_or(""), variables)?;
                match param.type_.to_lowercase().as_str() {
                    "int4" => {
                        if let Ok(i) = substituted.parse::<i32>() {
                            Box::new(i)
                        } else {
                            return Err("Unsupported parameter type".into());
                        }
                    }
                    "timestamp" => {
                        let timestamp = NaiveDateTime::parse_from_str(
                            substituted.as_str(),
                            "%Y-%m-%dT%H:%M:%S%.fZ",
                        )
                        .unwrap();
                        Box::new(timestamp)
                    }
                    "float8" => {
                        if let Ok(i) = substituted.parse::<f64>() {
                            Box::new(i)
                        } else {
                            return Err("Unsupported parameter type".into());
                        }
                    }
                    "varchar" => Box::new(substituted),
                    _ => {
                        return Err("Unsupported parameter type".into());
                    }
                }
            }
            _ => {
                return Err("Unsupported parameter type".into());
            }
        };
        params.push(param_value);
    }
    Ok(params)
}

fn assert_result(
    expect: &Expect,
    actual: &[tokio_postgres::Row],
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(expected_result) = &expect.result {
        // Convert actual rows to Vec<Vec<Value>>
        let actual_converted = actual
            .iter()
            .map(|row| {
                (0..row.len())
                    .map(|i| get_value_as_yaml(row, i))
                    .collect::<Vec<Value>>()
            })
            .collect::<Vec<Vec<Value>>>();

        if actual_converted != *expected_result {
            return Err(format!(
                "Expected result {:?}, got {:?}",
                expected_result, actual_converted
            )
            .into());
        }
    } else if let Some(expected_contains) = &expect.result_contains {
        let actual_converted = actual
            .iter()
            .map(|row| {
                (0..row.len())
                    .map(|i| get_value_as_yaml(row, i))
                    .collect::<Vec<Value>>()
            })
            .collect::<Vec<Vec<Value>>>();

        for expected_row in expected_contains {
            if !actual_converted.contains(expected_row) {
                return Err(format!(
                    "Expected row {:?} not found in actual results.",
                    expected_row
                )
                .into());
            }
        }
    }

    Ok(())
}

fn get_value_as_yaml(row: &tokio_postgres::Row, idx: usize) -> Value {
    let column_type = row.columns()[idx].type_();

    match *column_type {
        tokio_postgres::types::Type::INT2 => {
            let val: i16 = row.get(idx);
            Value::Number(val.into())
        }

        tokio_postgres::types::Type::INT4 => {
            let val: i32 = row.get(idx);
            Value::Number(val.into())
        }

        tokio_postgres::types::Type::INT8 => {
            let val: i64 = row.get(idx);
            Value::Number(val.into())
        }

        tokio_postgres::types::Type::FLOAT4 | tokio_postgres::types::Type::FLOAT8 => {
            let val: f64 = row.get(idx);
            Value::Number(serde_yaml::Number::from(val))
        }
        tokio_postgres::types::Type::BOOL => {
            let val: bool = row.get(idx);
            Value::Bool(val)
        }
        tokio_postgres::types::Type::TIMESTAMP => {
            let val: NaiveDateTime = row.get(idx);
            // format as: "2024-10-01T10:00:00.000000Z"
            Value::String(val.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
        }
        tokio_postgres::types::Type::TEXT | tokio_postgres::types::Type::VARCHAR => {
            let val: String = row.get(idx);
            Value::String(val)
        }
        // tokio_postgres::types::Type::DATE => {
        //     let val: NaiveDate = row.get(idx);
        //     Value::String(val.to_string())
        // }
        _ => {
            // For other types, get as string
            let val: String = row.get(idx);
            Value::String(val)
        }
    }
}

fn value_to_string(
    value: &Value,
    variables: &HashMap<String, String>,
) -> Result<String, Box<dyn std::error::Error>> {
    match value {
        Value::String(s) => {
            let substituted = substitute_variables(s, variables)?;
            Ok(substituted)
        }
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        _ => Err("Unsupported loop variable type".into()),
    }
}
