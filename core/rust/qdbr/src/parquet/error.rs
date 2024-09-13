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
use std::sync::Arc;

#[derive(Debug, Snafu, Clone)]
// Don't enable #[snafu(visibility(pub(crate)))]
// Instead, implement `.XXX_context` and `.with_XXX_context` methods when needed.
pub enum ParquetError {
    #[snafu(display("{source}"))]
    Parquet2 { source: parquet2::error::Error },

    #[snafu(display("{msg}"))]
    Layout { msg: String },

    #[snafu(display("{msg}"))]
    Unsupported { msg: String },

    #[snafu(display("{msg}"))]
    Invalid { msg: String },

    #[snafu(display("{source}"))]
    Utf8Decode { source: std::str::Utf8Error },

    #[snafu(display("{source}"))]
    Utf16Decode { source: std::char::DecodeUtf16Error },

    #[snafu(display("{source}"))]
    Io {
        #[snafu(source(from(std::io::Error, Arc::new)))]
        source: Arc<std::io::Error>,
    },

    #[snafu(display("{context}: {source}"))]
    Contextualized {
        #[snafu(source(from(ParquetError, Box::new)))]
        source: Box<ParquetError>,
        context: String,
    },
}

impl From<parquet2::error::Error> for ParquetError {
    fn from(source: parquet2::error::Error) -> Self {
        Self::Parquet2 { source }
    }
}

impl From<std::io::Error> for ParquetError {
    fn from(e: std::io::Error) -> Self {
        Self::Io { source: e.into() }
    }
}

/// An error reading Parquet data.
pub type ParquetResult<T> = Result<T, ParquetError>;

pub trait ParquetErrorExt<T> {
    fn context(self, context: impl Into<String>) -> ParquetResult<T>;
    fn with_context<F>(self, f: F) -> ParquetResult<T>
    where
        F: FnOnce(&mut ParquetError) -> String;
}

impl<T> ParquetErrorExt<T> for ParquetResult<T> {
    #[track_caller]
    fn context(self, context: impl Into<String>) -> ParquetResult<T> {
        ResultExt::context::<_, _>(self, ContextualizedSnafu { context: context.into() })
    }

    #[track_caller]
    fn with_context<F>(self, f: F) -> ParquetResult<T>
    where
        F: FnOnce(&mut ParquetError) -> String,
    {
        ResultExt::with_context(self, |e| ContextualizedSnafu { context: f(e) })
    }
}

macro_rules! fmt_layout_err {
    ($($arg:tt)*) => {
        ParquetError::Layout { msg: format!($($arg)*) }
    }
}

pub(crate) use fmt_layout_err;

macro_rules! fmt_unsupported_err {
    ($($arg:tt)*) => {
        ParquetError::Unsupported { msg: format!($($arg)*) }
    }
}

pub(crate) use fmt_unsupported_err;

macro_rules! fmt_invalid_err {
    ($($arg:tt)*) => {
        ParquetError::Invalid { msg: format!($($arg)*) }
    }
}

pub(crate) use fmt_invalid_err;
