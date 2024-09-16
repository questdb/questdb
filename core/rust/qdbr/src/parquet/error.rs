/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/
use std::backtrace::{Backtrace, BacktraceStatus};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// Cause of a parquet error.
#[derive(Debug, Clone)]
pub enum ParquetErrorCause {
    Parquet2(parquet2::error::Error),
    QdbMetadata(Arc<serde_json::Error>),
    Layout,
    Unsupported,
    Invalid,
    Utf8Decode(std::str::Utf8Error),
    Utf16Decode(std::char::DecodeUtf16Error),
    Io(Arc<std::io::Error>),
}

impl ParquetErrorCause {
    pub fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ParquetErrorCause::Parquet2(err) => Some(err),
            ParquetErrorCause::QdbMetadata(err) => Some(err.as_ref()),
            ParquetErrorCause::Utf8Decode(err) => Some(err),
            ParquetErrorCause::Utf16Decode(err) => Some(err),
            ParquetErrorCause::Io(err) => Some(err.as_ref()),
            _ => None,
        }
    }

    #[track_caller]
    pub fn into_err(self) -> ParquetError {
        ParquetError::new(self)
    }
}

/// An error reading or writing parquet.
#[derive(Debug, Clone)]
pub struct ParquetError {
    /// What caused the error.
    cause: ParquetErrorCause,

    /// Stack of additional contextual information,
    /// printed in reverse order.
    context: Vec<String>,

    backtrace: Arc<Backtrace>,
}

impl ParquetError {
    #[track_caller]
    pub fn new(cause: ParquetErrorCause) -> Self {
        Self {
            cause,
            context: Vec::new(),
            backtrace: Backtrace::capture().into(),
        }
    }

    #[track_caller]
    pub fn with_descr(cause: ParquetErrorCause, descr: impl Into<String>) -> Self {
        Self {
            cause,
            context: vec![descr.into()],
            backtrace: Backtrace::capture().into(),
        }
    }
}

impl Display for ParquetError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // Print the context first in reverse order.
        let source = self.cause.source();
        let last_index = self.context.len() - 1;
        for (index, context) in self.context.iter().rev().enumerate() {
            if index == last_index {
                write!(f, "{}", context)?;
            } else {
                write!(f, "{}: ", context)?;
            }
        }
        if let Some(source) = source {
            if self.context.is_empty() {
                write!(f, "{}", source)?;
            } else {
                write!(f, ": {}", source)?;
            }
        }

        if let BacktraceStatus::Captured = &self.backtrace.status() {
            write!(f, "\n{:?}", self.backtrace)?;
        }
        Ok(())
    }
}

impl std::error::Error for ParquetError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause.source()
    }
}

impl From<parquet2::error::Error> for ParquetError {
    fn from(source: parquet2::error::Error) -> Self {
        Self::new(ParquetErrorCause::Parquet2(source))
    }
}

impl From<std::io::Error> for ParquetError {
    fn from(e: std::io::Error) -> Self {
        Self::new(ParquetErrorCause::Io(Arc::new(e)))
    }
}

pub type ParquetResult<T> = Result<T, ParquetError>;

pub trait ParquetErrorExt<T> {
    fn context(self, context: &str) -> Self;
    fn with_context<F>(self, context: F) -> Self
    where
        F: FnOnce(&mut ParquetError) -> String;
}

impl<T> ParquetErrorExt<T> for ParquetResult<T> {
    fn context(self, context: &str) -> Self {
        match self {
            Ok(val) => Ok(val),
            Err(mut err) => {
                err.context.push(context.to_string());
                Err(err)
            }
        }
    }

    fn with_context<F>(self, context: F) -> Self
    where
        F: FnOnce(&mut ParquetError) -> String,
    {
        match self {
            Ok(val) => Ok(val),
            Err(mut err) => {
                let context = context(&mut err);
                err.context.push(context);
                Err(err)
            }
        }
    }
}

macro_rules! fmt_err {
    ($cause: ident, $($arg:tt)*) => {
        ParquetError::with_descr(
            crate::parquet::error::ParquetErrorCause::$cause,
            format!($($arg)*))
    };
}

pub(crate) use fmt_err;
