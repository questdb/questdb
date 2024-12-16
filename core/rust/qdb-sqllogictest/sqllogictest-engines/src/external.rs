use std::io;
use std::marker::PhantomData;
use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Buf, BytesMut};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sqllogictest::{AsyncDB, DBOutput, DateFormat, DefaultColumnType};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio_util::codec::{Decoder, FramedRead};

/// Communicates with a subprocess via its stdin/stdout.
///
/// # Protocol
///
/// Sends JSON stream:
/// ```json
/// {"sql":"SELECT 1,2"}
/// ```
///
/// Receives JSON stream:
///
/// If the query succeeds:
/// ```json
/// {"result":[["1","2"]]}
/// ```
///
/// If the query fails:
/// ```json
/// {"err":"..."}
/// ```
pub struct ExternalDriver {
    child: Child,
    stdin: ChildStdin,
    stdout: FramedRead<ChildStdout, JsonDecoder<Output>>,
}

#[derive(Serialize)]
struct Input {
    sql: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Output {
    Success { result: Vec<Vec<String>> },
    Failed { err: String },
}

#[derive(Debug, Error)]
pub enum ExternalDriverError {
    #[error("ser/de failed")]
    Json(#[from] serde_json::Error),
    #[error("io failed")]
    Io(#[from] io::Error),
    #[error("sql failed {0}")]
    Sql(String),
}

type Result<T> = std::result::Result<T, ExternalDriverError>;

impl ExternalDriver {
    /// Spawn and pipe into the subprocess with the given `cmd`.
    pub async fn connect(mut cmd: Command) -> Result<Self> {
        let cmd = cmd.stdin(Stdio::piped()).stdout(Stdio::piped());

        let mut child = cmd.spawn()?;

        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let stdout = FramedRead::new(stdout, JsonDecoder::default());

        Ok(Self {
            child,
            stdin,
            stdout,
        })
    }
}

impl Drop for ExternalDriver {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

#[async_trait]
impl AsyncDB for ExternalDriver {
    type Error = ExternalDriverError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str, _: DateFormat) -> Result<DBOutput<Self::ColumnType>> {
        let input = Input {
            sql: sql.to_string(),
        };
        let input = serde_json::to_string(&input)?;
        self.stdin.write_all(input.as_bytes()).await?;
        let output = match self.stdout.next().await {
            Some(Ok(output)) => output,
            Some(Err(e)) => return Err(e),
            None => return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into()),
        };
        match output {
            Output::Success { result } => Ok(DBOutput::Rows {
                types: vec![], /* FIXME: Fix it after https://github.com/risinglightdb/sqllogictest-rs/issues/36 is resolved. */
                rows: result,
            }),
            Output::Failed { err } => Err(ExternalDriverError::Sql(err)),
        }
    }

    fn engine_name(&self) -> &str {
        "external"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await
    }

    async fn run_command(command: std::process::Command) -> std::io::Result<std::process::Output> {
        Command::from(command).output().await
    }
}

struct JsonDecoder<T>(PhantomData<T>);

impl<T> Default for JsonDecoder<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T> Decoder for JsonDecoder<T>
where
    T: for<'de> serde::de::Deserialize<'de>,
{
    type Item = T;
    type Error = ExternalDriverError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        let mut inner = serde_json::Deserializer::from_slice(src.as_ref()).into_iter::<T>();
        match inner.next() {
            None => Ok(None),
            Some(Err(e)) if e.is_eof() => Ok(None),
            Some(Err(e)) => Err(e.into()),
            Some(Ok(v)) => {
                let len = inner.byte_offset();
                src.advance(len);
                Ok(Some(v))
            }
        }
    }
}
