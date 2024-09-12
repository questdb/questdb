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

#[derive(Debug, Snafu, Clone)]
#[snafu(visibility(pub(crate)))]
pub enum ParquetReadError {
    #[snafu(display("{source}"))]
    Parquet2Error { source: parquet2::error::Error },

    #[snafu(display("{msg}"))]
    LayoutError { msg: String },

    #[snafu(display("{msg}"))]
    Unsupported { msg: String },

    #[snafu(display("{source}"))]
    Utf8Error { source: std::str::Utf8Error },

    #[snafu(display("{context}: {source}"))]
    ContextualizedError {
        #[snafu(source(from(ParquetReadError, Box::new)))]
        source: Box<ParquetReadError>,
        context: String,
    },
}

impl From<parquet2::error::Error> for ParquetReadError {
    fn from(source: parquet2::error::Error) -> Self {
        Self::Parquet2Error { source }
    }
}

/// An error reading Parquet data.
pub type ParquetReadResult<T> = Result<T, ParquetReadError>;

pub trait ParquetReadErrorExt<T> {
    #[allow(dead_code)]
    fn context(self, context: impl Into<String>) -> ParquetReadResult<T>;

    fn with_context<F>(self, f: F) -> ParquetReadResult<T>
    where
        F: FnOnce(&mut ParquetReadError) -> String;
}

impl<T> ParquetReadErrorExt<T> for ParquetReadResult<T> {
    #[allow(dead_code)]
    #[track_caller]
    fn context(self, context: impl Into<String>) -> ParquetReadResult<T> {
        ResultExt::context::<_, _>(self, ContextualizedSnafu { context: context.into() })
    }

    #[track_caller]
    fn with_context<F>(self, f: F) -> ParquetReadResult<T>
    where
        F: FnOnce(&mut ParquetReadError) -> String,
    {
        ResultExt::with_context(self, |e| ContextualizedSnafu { context: f(e) })
    }
}

macro_rules! fmt_read_layout_err {
    ($($arg:tt)*) => {
        ParquetReadError::LayoutError { msg: format!($($arg)*) }
    }
}

pub(crate) use fmt_read_layout_err;

macro_rules! fmt_read_unsupported_err {
    ($($arg:tt)*) => {
        ParquetReadError::Unsupported { msg: format!($($arg)*) }
    }
}

pub(crate) use fmt_read_unsupported_err;
