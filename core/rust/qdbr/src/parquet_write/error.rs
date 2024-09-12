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
use snafu::prelude::*;
use snafu::ResultExt;
use std::char::DecodeUtf16Error;
use std::sync::Arc;

#[derive(Debug, Snafu, Clone)]
#[snafu(visibility(pub(crate)))]
pub enum ParquetWriteError {
    #[snafu(display("{source}"))]
    Parquet2Error { source: parquet2::error::Error },

    #[snafu(display("{msg}"))]
    LayoutError { msg: String },

    #[snafu(display("{msg}"))]
    Unsupported { msg: String },

    #[snafu(display("invalid questdb column type: {column_type}"))]
    InvalidQuestDBColumnType { column_type: i32 },

    #[snafu(display("error decoding utf-16 code unit: {source}"))]
    Utf16DecodeError { source: DecodeUtf16Error },

    #[snafu(display("{source}"))]
    IoError {
        #[snafu(source(from(std::io::Error, Arc::new)))]
        source: Arc<std::io::Error>,
    },

    #[snafu(display("{context}: {source}"))]
    ContextualizedError {
        #[snafu(source(from(ParquetWriteError, Box::new)))]
        source: Box<ParquetWriteError>,
        context: String,
    },
}

impl From<parquet2::error::Error> for ParquetWriteError {
    fn from(source: parquet2::error::Error) -> Self {
        Self::Parquet2Error { source }
    }
}

impl From<std::io::Error> for ParquetWriteError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError { source: e.into() }
    }
}

pub type ParquetWriteResult<T> = Result<T, ParquetWriteError>;

pub trait ParquetWriteErrorExt<T> {
    fn context(self, context: impl Into<String>) -> ParquetWriteResult<T>;
    fn with_context<F>(self, f: F) -> ParquetWriteResult<T>
    where
        F: FnOnce(&mut ParquetWriteError) -> String;
}

impl<T> ParquetWriteErrorExt<T> for ParquetWriteResult<T> {
    #[track_caller]
    fn context(self, context: impl Into<String>) -> ParquetWriteResult<T> {
        ResultExt::context::<_, _>(self, ContextualizedSnafu { context: context.into() })
    }

    #[track_caller]
    fn with_context<F>(self, f: F) -> ParquetWriteResult<T>
    where
        F: FnOnce(&mut ParquetWriteError) -> String,
    {
        ResultExt::with_context(self, |e| ContextualizedSnafu { context: f(e) })
    }
}

macro_rules! fmt_write_layout_err {
    ($($arg:tt)*) => {
        ParquetWriteError::LayoutError { msg: format!($($arg)*) }
    }
}

pub(crate) use fmt_write_layout_err;

macro_rules! fmt_write_unsupported_err {
    ($($arg:tt)*) => {
        ParquetWriteError::Unsupported { msg: format!($($arg)*) }
    }
}

pub(crate) use fmt_write_unsupported_err;
