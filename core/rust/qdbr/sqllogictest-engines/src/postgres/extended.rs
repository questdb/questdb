use std::fmt::Write;
use std::process::Command;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use futures::{pin_mut, StreamExt};
use pg_interval::Interval;
use postgres_types::{ToSql, Type};
use rust_decimal::Decimal;
use sqllogictest::{DBOutput, DefaultColumnType};

use super::{Extended, Postgres, Result};

macro_rules! array_process {
    ($row:ident, $row_vec:ident, $idx:ident, $t:ty) => {
        let value: Option<Vec<Option<$t>>> = $row.get($idx);
        match value {
            Some(value) => {
                let mut output = String::new();
                write!(output, "{{").unwrap();
                for (i, v) in value.iter().enumerate() {
                    match v {
                        Some(v) => {
                            write!(output, "{}", v).unwrap();
                        }
                        None => {
                            write!(output, "NULL").unwrap();
                        }
                    }
                    if i < value.len() - 1 {
                        write!(output, ",").unwrap();
                    }
                }
                write!(output, "}}").unwrap();
                $row_vec.push(output);
            }
            None => {
                $row_vec.push("NULL".to_string());
            }
        }
    };
    ($row:ident, $row_vec:ident, $idx:ident, $t:ty, $convert:ident) => {
        let value: Option<Vec<Option<$t>>> = $row.get($idx);
        match value {
            Some(value) => {
                let mut output = String::new();
                write!(output, "{{").unwrap();
                for (i, v) in value.iter().enumerate() {
                    match v {
                        Some(v) => {
                            write!(output, "{}", $convert(v)).unwrap();
                        }
                        None => {
                            write!(output, "NULL").unwrap();
                        }
                    }
                    if i < value.len() - 1 {
                        write!(output, ",").unwrap();
                    }
                }
                write!(output, "}}").unwrap();
                $row_vec.push(output);
            }
            None => {
                $row_vec.push("NULL".to_string());
            }
        }
    };
    ($self:ident, $row:ident, $row_vec:ident, $idx:ident, $t:ty, $ty_name:expr) => {
        let value: Option<Vec<Option<$t>>> = $row.get($idx);
        match value {
            Some(value) => {
                let mut output = String::new();
                write!(output, "{{").unwrap();
                for (i, v) in value.iter().enumerate() {
                    match v {
                        Some(v) => {
                            let sql = format!("select ($1::{})::varchar", stringify!($ty_name));
                            let tmp_rows = $self.client.query(&sql, &[&v]).await.unwrap();
                            let value: &str = tmp_rows.get(0).unwrap().get(0);
                            assert!(value.len() > 0);
                            write!(output, "{}", value).unwrap();
                        }
                        None => {
                            write!(output, "NULL").unwrap();
                        }
                    }
                    if i < value.len() - 1 {
                        write!(output, ",").unwrap();
                    }
                }
                write!(output, "}}").unwrap();
                $row_vec.push(output);
            }
            None => {
                $row_vec.push("NULL".to_string());
            }
        }
    };
}

macro_rules! single_process {
    ($row:ident, $row_vec:ident, $idx:ident, $t:ty) => {
        let value: Option<$t> = $row.get($idx);
        match value {
            Some(value) => {
                $row_vec.push(value.to_string());
            }
            None => {
                $row_vec.push("NULL".to_string());
            }
        }
    };
    ($row:ident, $row_vec:ident, $idx:ident, $t:ty, $convert:ident) => {
        let value: Option<$t> = $row.get($idx);
        match value {
            Some(value) => {
                $row_vec.push($convert(&value).to_string());
            }
            None => {
                $row_vec.push("NULL".to_string());
            }
        }
    };
    ($self:ident, $row:ident, $row_vec:ident, $idx:ident, $t:ty, $ty_name:expr) => {
        let value: Option<$t> = $row.get($idx);
        match value {
            Some(value) => {
                let sql = format!("select ($1::{})::varchar", stringify!($ty_name));
                let tmp_rows = $self.client.query(&sql, &[&value]).await.unwrap();
                let value: &str = tmp_rows.get(0).unwrap().get(0);
                assert!(value.len() > 0);
                $row_vec.push(value.to_string());
            }
            None => {
                $row_vec.push("NULL".to_string());
            }
        }
    };
}

fn bool_to_str(value: &bool) -> &'static str {
    if *value {
        "true"
    } else {
        "false"
    }
}

fn varchar_to_str(value: &str) -> String {
    if value.is_empty() {
        "(empty)".to_string()
    } else {
        value.to_string()
    }
}

fn float4_to_str(value: &f32) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if *value == f32::INFINITY {
        "Infinity".to_string()
    } else if *value == f32::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        format_c_exponent(format!("{:e}", value))
    }
}

fn format_c_exponent(formatted: String) -> String {
    if let Some(exponent_index) = formatted.find('e') {
        let (mantissa, exponent) = formatted.split_at(exponent_index);
        if exponent.contains('-') {
            format!("{}{}", mantissa, exponent)
        } else {
            format!("{}e+{}", mantissa, &exponent[1..])
        }
    } else {
        formatted
    }
}

fn float8_to_str(value: &f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if *value == f64::INFINITY {
        "Infinity".to_string()
    } else if *value == f64::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        format_c_exponent(format!("{:e}", value))
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for Postgres<Extended> {
    type Error = tokio_postgres::error::Error;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>> {
        let mut output = vec![];

        let stmt = self.client.prepare(sql).await?;
        let rows = self
            .client
            .query_raw(&stmt, std::iter::empty::<&(dyn ToSql + Sync)>())
            .await?;

        pin_mut!(rows);

        while let Some(row) = rows.next().await {
            let row = row?;
            let mut row_vec = vec![];

            for (idx, column) in row.columns().iter().enumerate() {
                match column.type_().clone() {
                    Type::INT2 => {
                        single_process!(row, row_vec, idx, i16);
                    }
                    Type::INT4 => {
                        single_process!(row, row_vec, idx, i32);
                    }
                    Type::INT8 => {
                        single_process!(row, row_vec, idx, i64);
                    }
                    Type::NUMERIC => {
                        single_process!(row, row_vec, idx, Decimal);
                    }
                    Type::DATE => {
                        single_process!(row, row_vec, idx, NaiveDate);
                    }
                    Type::TIME => {
                        single_process!(row, row_vec, idx, NaiveTime);
                    }
                    Type::TIMESTAMP => {
                        single_process!(row, row_vec, idx, NaiveDateTime);
                    }
                    Type::BOOL => {
                        single_process!(row, row_vec, idx, bool, bool_to_str);
                    }
                    Type::INT2_ARRAY => {
                        array_process!(row, row_vec, idx, i16);
                    }
                    Type::INT4_ARRAY => {
                        array_process!(row, row_vec, idx, i32);
                    }
                    Type::INT8_ARRAY => {
                        array_process!(row, row_vec, idx, i64);
                    }
                    Type::BOOL_ARRAY => {
                        array_process!(row, row_vec, idx, bool, bool_to_str);
                    }
                    Type::FLOAT4_ARRAY => {
                        array_process!(row, row_vec, idx, f32, float4_to_str);
                    }
                    Type::FLOAT8_ARRAY => {
                        array_process!(row, row_vec, idx, f64, float8_to_str);
                    }
                    Type::NUMERIC_ARRAY => {
                        array_process!(row, row_vec, idx, Decimal);
                    }
                    Type::DATE_ARRAY => {
                        array_process!(row, row_vec, idx, NaiveDate);
                    }
                    Type::TIME_ARRAY => {
                        array_process!(row, row_vec, idx, NaiveTime);
                    }
                    Type::TIMESTAMP_ARRAY => {
                        array_process!(row, row_vec, idx, NaiveDateTime);
                    }
                    Type::VARCHAR_ARRAY | Type::TEXT_ARRAY => {
                        array_process!(row, row_vec, idx, String, varchar_to_str);
                    }
                    Type::VARCHAR | Type::TEXT => {
                        single_process!(row, row_vec, idx, String, varchar_to_str);
                    }
                    Type::FLOAT4 => {
                        single_process!(row, row_vec, idx, f32, float4_to_str);
                    }
                    Type::FLOAT8 => {
                        single_process!(row, row_vec, idx, f64, float8_to_str);
                    }
                    Type::INTERVAL => {
                        single_process!(self, row, row_vec, idx, Interval, INTERVAL);
                    }
                    Type::TIMESTAMPTZ => {
                        single_process!(
                            self,
                            row,
                            row_vec,
                            idx,
                            DateTime<chrono::Utc>,
                            TIMESTAMPTZ
                        );
                    }
                    Type::INTERVAL_ARRAY => {
                        array_process!(self, row, row_vec, idx, Interval, INTERVAL);
                    }
                    Type::TIMESTAMPTZ_ARRAY => {
                        array_process!(self, row, row_vec, idx, DateTime<chrono::Utc>, TIMESTAMPTZ);
                    }
                    _ => {
                        todo!("Don't support {} type now.", column.type_().name())
                    }
                }
            }
            output.push(row_vec);
        }

        if output.is_empty() {
            match rows.rows_affected() {
                Some(rows) => Ok(DBOutput::StatementComplete(rows)),
                None => Ok(DBOutput::Rows {
                    types: vec![DefaultColumnType::Any; stmt.columns().len()],
                    rows: vec![],
                }),
            }
        } else {
            Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Any; output[0].len()],
                rows: output,
            })
        }
    }

    fn engine_name(&self) -> &str {
        "postgres-extended"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await
    }

    async fn run_command(command: Command) -> std::io::Result<std::process::Output> {
        tokio::process::Command::from(command).output().await
    }
}
