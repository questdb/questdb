# Changelog

## [v0.17.2](https://github.com/jorgecarleitao/parquet2/tree/v0.17.2) (2023-04-13)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.17.1...v0.17.2)

**Fixed bugs:**

- Fixed aggregation of max statistics for primitive fields [\#217](https://github.com/jorgecarleitao/parquet2/pull/217) ([tjwilson90](https://github.com/tjwilson90))
- Fix reimport problem and clippy. [\#216](https://github.com/jorgecarleitao/parquet2/pull/216) ([RinChanNOWWW](https://github.com/RinChanNOWWW))
- Bump minimum async-stream version to 0.3.3 [\#212](https://github.com/jorgecarleitao/parquet2/pull/212) ([garrisonhess](https://github.com/garrisonhess))
- Fixed error in rle decoding [\#207](https://github.com/jorgecarleitao/parquet2/pull/207) ([ritchie46](https://github.com/ritchie46))

**Enhancements:**

- Update dependencies + a little cleanup [\#211](https://github.com/jorgecarleitao/parquet2/pull/211) ([aldanor](https://github.com/aldanor))
- Make some struct de/serializable. [\#209](https://github.com/jorgecarleitao/parquet2/pull/209) ([RinChanNOWWW](https://github.com/RinChanNOWWW))
- Removed unnecessary flushes while writing [\#206](https://github.com/jorgecarleitao/parquet2/pull/206) ([cyr](https://github.com/cyr))
- Enbaled setting `selected_rows` in the runtime. [\#205](https://github.com/jorgecarleitao/parquet2/pull/205) ([RinChanNOWWW](https://github.com/RinChanNOWWW))

## [v0.17.1](https://github.com/jorgecarleitao/parquet2/tree/v0.17.1) (2022-12-12)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.17.0...v0.17.1)

**Fixed bugs:**

- Fixed error in rle decoding [\#207](https://github.com/jorgecarleitao/parquet2/pull/207) ([ritchie46](https://github.com/ritchie46))

## [v0.17.0](https://github.com/jorgecarleitao/parquet2/tree/v0.17.0) (2022-11-30)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.16.2...v0.17.0)

**Breaking changes:**

- Improved hybrid rle decoding performance ~-40% [\#203](https://github.com/jorgecarleitao/parquet2/pull/203) ([ritchie46](https://github.com/ritchie46))
- Improved API to read column chunks [\#195](https://github.com/jorgecarleitao/parquet2/pull/195) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Removed `EncodedPage`  [\#191](https://github.com/jorgecarleitao/parquet2/pull/191) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Added `serde` support for `RowGroupMetaData`. [\#202](https://github.com/jorgecarleitao/parquet2/pull/202) ([youngsofun](https://github.com/youngsofun))

**Fixed bugs:**

- Removed un-necessary conversion [\#197](https://github.com/jorgecarleitao/parquet2/pull/197) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Avoid OOM on page streams [\#194](https://github.com/jorgecarleitao/parquet2/pull/194) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error requiring stats [\#193](https://github.com/jorgecarleitao/parquet2/pull/193) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- elide bound check in RLE decoder [\#201](https://github.com/jorgecarleitao/parquet2/pull/201) ([ritchie46](https://github.com/ritchie46))
- Replaced panics by errors on invalid pages [\#188](https://github.com/jorgecarleitao/parquet2/pull/188) ([evanrichter](https://github.com/evanrichter))

## [v0.16.2](https://github.com/jorgecarleitao/parquet2/tree/v0.16.2) (2022-08-19)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.16.1...v0.16.2)

**Fixed bugs:**

- Fixed edge cases in writing delta encoder [\#189](https://github.com/jorgecarleitao/parquet2/pull/189) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Documentation updates:**

- Improved doc generation to include features [\#190](https://github.com/jorgecarleitao/parquet2/pull/190) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.16.1](https://github.com/jorgecarleitao/parquet2/tree/v0.16.1) (2022-08-17)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.16.0...v0.16.1)

**Fixed bugs:**

- Fixed error in `FilteredHybridBitmapIter`'s trait bounds [\#187](https://github.com/jorgecarleitao/parquet2/pull/187) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.16.0](https://github.com/jorgecarleitao/parquet2/tree/v0.16.0) (2022-08-17)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.15.1...v0.16.0)

**Breaking changes:**

- Improved `Error` [\#181](https://github.com/jorgecarleitao/parquet2/pull/181) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made decoding fallible [\#178](https://github.com/jorgecarleitao/parquet2/pull/178) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved bitpacking [\#176](https://github.com/jorgecarleitao/parquet2/pull/176) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Added DELTA\_BYTE\_ARRAY encoder [\#183](https://github.com/jorgecarleitao/parquet2/pull/183) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- FixedLenByteArray max\_precision integer overflow [\#184](https://github.com/jorgecarleitao/parquet2/pull/184) ([evanrichter](https://github.com/evanrichter))

**Documentation updates:**

- enable `doc_cfg` feature [\#186](https://github.com/jorgecarleitao/parquet2/pull/186) ([ritchie46](https://github.com/ritchie46))
- Improved decoding documentation [\#180](https://github.com/jorgecarleitao/parquet2/pull/180) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.15.1](https://github.com/jorgecarleitao/parquet2/tree/v0.15.1) (2022-08-14)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.15.0...v0.15.1)

**Fixed bugs:**

- Fixed error in encoding large bitpacked deltas [\#179](https://github.com/jorgecarleitao/parquet2/pull/179) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.15.1](https://github.com/jorgecarleitao/parquet2/tree/v0.15.1) (2022-08-14)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.15.0...v0.15.1)

**Fixed bugs:**

- Fixed error in encoding large bitpacked deltas [\#179](https://github.com/jorgecarleitao/parquet2/pull/179) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.15.0](https://github.com/jorgecarleitao/parquet2/tree/v0.15.0) (2022-08-10)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.14.2...v0.15.0)

**Breaking changes:**

- Add `max_size` to `get_page_stream` and `get_page_iterator`  [\#173](https://github.com/jorgecarleitao/parquet2/issues/173)
- Optional `async` [\#174](https://github.com/jorgecarleitao/parquet2/pull/174) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Privatized `CompressionLevel` [\#170](https://github.com/jorgecarleitao/parquet2/pull/170) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Delay deserialization of dictionary pages [\#160](https://github.com/jorgecarleitao/parquet2/pull/160) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Added feature flag to use zlib-ng backend for gzip [\#165](https://github.com/jorgecarleitao/parquet2/pull/165) ([ritchie46](https://github.com/ritchie46))

**Fixed bugs:**

- Fixed OOM on malicious/malformed thrift [\#172](https://github.com/jorgecarleitao/parquet2/pull/172) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Made `compute_page_row_intervals` public [\#171](https://github.com/jorgecarleitao/parquet2/pull/171) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified interal code [\#168](https://github.com/jorgecarleitao/parquet2/pull/168) ([jorgecarleitao](https://github.com/jorgecarleitao))
- cargo fmt [\#166](https://github.com/jorgecarleitao/parquet2/pull/166) ([ritchie46](https://github.com/ritchie46))

**Testing updates:**

- Improved coverage report [\#175](https://github.com/jorgecarleitao/parquet2/pull/175) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.14.2](https://github.com/jorgecarleitao/parquet2/tree/v0.14.2) (2022-07-26)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.14.1...v0.14.2)

**Fixed bugs:**

- Fixed FileStreamer's end method to flush Parquet magic [\#163](https://github.com/jorgecarleitao/parquet2/pull/163) ([v0y4g3r](https://github.com/v0y4g3r))
- Fix compilation of parquet-tools [\#161](https://github.com/jorgecarleitao/parquet2/pull/161) ([jhorstmann](https://github.com/jhorstmann))

**Enhancements:**

- Added `Compressor::into_inner` [\#158](https://github.com/jorgecarleitao/parquet2/pull/158) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.14.1](https://github.com/jorgecarleitao/parquet2/tree/v0.14.1) (2022-07-02)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.14.0...v0.14.1)

**New features:**

- Added support for legacy lz4 decompression [\#151](https://github.com/jorgecarleitao/parquet2/pull/151) ([dantengsky](https://github.com/dantengsky))

**Enhancements:**

- Improved performance of reading [\#157](https://github.com/jorgecarleitao/parquet2/pull/157) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.14.0](https://github.com/jorgecarleitao/parquet2/tree/v0.14.0) (2022-06-27)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.13.2...v0.14.0)

**Breaking changes:**

- `split_buffer` should return `Result` [\#156](https://github.com/jorgecarleitao/parquet2/issues/156)

**Fixed bugs:**

- Removed panics on read [\#150](https://github.com/jorgecarleitao/parquet2/pull/150) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Reduced reallocations [\#153](https://github.com/jorgecarleitao/parquet2/pull/153) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Removed `AsyncSeek` requirement from page stream [\#149](https://github.com/jorgecarleitao/parquet2/pull/149) ([medwards](https://github.com/medwards))

## [v0.13.2](https://github.com/jorgecarleitao/parquet2/tree/v0.13.2) (2022-06-10)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.13.1...v0.13.2)

**Fixed bugs:**

- Fixed missing re-export of `FileMetaData` to allow using side-car API [\#148](https://github.com/jorgecarleitao/parquet2/pull/148) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.13.1](https://github.com/jorgecarleitao/parquet2/tree/v0.13.1) (2022-06-10)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.13.0...v0.13.1)

**New features:**

- Added support to write sidecar [\#147](https://github.com/jorgecarleitao/parquet2/pull/147) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.13.0](https://github.com/jorgecarleitao/parquet2/tree/v0.13.0) (2022-05-31)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.12.1...v0.13.0)

**Breaking changes:**

- Removed unused cargo feature [\#145](https://github.com/jorgecarleitao/parquet2/pull/145) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fix potential misuse of FileWriter API's \(sync + async\) [\#138](https://github.com/jorgecarleitao/parquet2/pull/138) ([TurnOfACard](https://github.com/TurnOfACard))

**New features:**

- Added new\_with\_page\_meta to PageReader [\#136](https://github.com/jorgecarleitao/parquet2/pull/136) ([ygf11](https://github.com/ygf11))
- Added compression options/levels for GZIP and BROTLI codecs. [\#132](https://github.com/jorgecarleitao/parquet2/pull/132) ([TurnOfACard](https://github.com/TurnOfACard))

**Fixed bugs:**

- Async FileStreamer does not write statistics [\#139](https://github.com/jorgecarleitao/parquet2/issues/139)
- Fixed error in compressing lz4raw with large offsets [\#140](https://github.com/jorgecarleitao/parquet2/pull/140) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Improved read of metadata [\#143](https://github.com/jorgecarleitao/parquet2/pull/143) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified async metadata read [\#137](https://github.com/jorgecarleitao/parquet2/pull/137) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Testing updates:**

- Lifted duplicated code to a function [\#141](https://github.com/jorgecarleitao/parquet2/pull/141) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved Integration test documentation and expanded tests [\#133](https://github.com/jorgecarleitao/parquet2/pull/133) ([TurnOfACard](https://github.com/TurnOfACard))

## [v0.12.1](https://github.com/jorgecarleitao/parquet2/tree/v0.12.1) (2022-05-15)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.12.0...v0.12.1)

**Enhancements:**

- Pass only necessary data when create PageReader [\#135](https://github.com/jorgecarleitao/parquet2/issues/135)

## [v0.12.0](https://github.com/jorgecarleitao/parquet2/tree/v0.12.0) (2022-04-22)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.11.0...v0.12.0)

**Breaking changes:**

- Add `CompressionOptions`, which allows for zstd compression levels. [\#128](https://github.com/jorgecarleitao/parquet2/pull/128) ([TurnOfACard](https://github.com/TurnOfACard))

**Enhancements:**

- Improved performance of RLE decoding \(-18%\) [\#130](https://github.com/jorgecarleitao/parquet2/pull/130) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved perf of bitpacking decoding \(3.5x\) [\#129](https://github.com/jorgecarleitao/parquet2/pull/129) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.11.0](https://github.com/jorgecarleitao/parquet2/tree/v0.11.0) (2022-04-15)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.10.3...v0.11.0)

**Breaking changes:**

- Renamed `ParquetError` to `Error` [\#109](https://github.com/jorgecarleitao/parquet2/issues/109)
- Made `.end` not consume the parquet `FileWriter` [\#127](https://github.com/jorgecarleitao/parquet2/pull/127) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Removed `compression` from `WriteOptions` [\#125](https://github.com/jorgecarleitao/parquet2/pull/125) ([kornholi](https://github.com/kornholi))
- Simplified API and converted some panics on read to errors [\#112](https://github.com/jorgecarleitao/parquet2/pull/112) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved typing to reduce clones and use of unwraps [\#106](https://github.com/jorgecarleitao/parquet2/pull/106) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified `PageIterator` [\#103](https://github.com/jorgecarleitao/parquet2/pull/103) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Added support for page-level filter pushdown \(indexes\) [\#102](https://github.com/jorgecarleitao/parquet2/issues/102)
- Added support for bloom filters [\#98](https://github.com/jorgecarleitao/parquet2/issues/98)
- Added optional support for LZ4 via LZ4-flex crate \(thus enabling wasm\) [\#124](https://github.com/jorgecarleitao/parquet2/pull/124) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for page-level filter pushdown \(column and offset indexes\) [\#107](https://github.com/jorgecarleitao/parquet2/pull/107) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to read column and page indexes [\#100](https://github.com/jorgecarleitao/parquet2/pull/100) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Fixed minimum version for LZ4 [\#122](https://github.com/jorgecarleitao/parquet2/pull/122) ([kornholi](https://github.com/kornholi))
- Fixed Lz4Raw compression error \(if input is tiny\)  [\#118](https://github.com/jorgecarleitao/parquet2/pull/118) ([dantengsky](https://github.com/dantengsky))
- Fixed LZ4 [\#95](https://github.com/jorgecarleitao/parquet2/pull/95) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Made offsets be always written [\#123](https://github.com/jorgecarleitao/parquet2/pull/123) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added specialized deserialization of one-level filtered pages [\#120](https://github.com/jorgecarleitao/parquet2/pull/120) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to read and use bloom filters [\#99](https://github.com/jorgecarleitao/parquet2/pull/99) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `ordinal` and `total_compressed_size` to column meta [\#96](https://github.com/jorgecarleitao/parquet2/pull/96) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added non-consuming function to get values of delta-decoder [\#94](https://github.com/jorgecarleitao/parquet2/pull/94) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Disabled bitpacking default-features and upgraded to edition 2021 [\#93](https://github.com/jorgecarleitao/parquet2/pull/93) ([light4](https://github.com/light4))

**Documentation updates:**

- Fix deployment of guide [\#115](https://github.com/jorgecarleitao/parquet2/pull/115) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Testing updates:**

- Added tests for reducing statistics [\#116](https://github.com/jorgecarleitao/parquet2/pull/116) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified tests [\#104](https://github.com/jorgecarleitao/parquet2/pull/104) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.10.3](https://github.com/jorgecarleitao/parquet2/tree/v0.10.3) (2022-03-03)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.10.2...v0.10.3)

**Fixed bugs:**

- write ColumnMetaData instead of ColumnChunk after pages. [\#90](https://github.com/jorgecarleitao/parquet2/pull/90) ([youngsofun](https://github.com/youngsofun))

## [v0.10.2](https://github.com/jorgecarleitao/parquet2/tree/v0.10.2) (2022-02-14)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.10.1...v0.10.2)

**Fixed bugs:**

- Raise error when writing a page that is too large [\#84](https://github.com/jorgecarleitao/parquet2/pull/84) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Testing updates:**

- fix fmt and typo [\#83](https://github.com/jorgecarleitao/parquet2/pull/83) ([youngsofun](https://github.com/youngsofun))

## [v0.10.1](https://github.com/jorgecarleitao/parquet2/tree/v0.10.1) (2022-02-12)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.10.0...v0.10.1)

**Enhancements:**

- Update zstd dependency [\#82](https://github.com/jorgecarleitao/parquet2/pull/82) ([jhorstmann](https://github.com/jhorstmann))
- Added file offset [\#81](https://github.com/jorgecarleitao/parquet2/pull/81) ([barrotsteindev](https://github.com/barrotsteindev))

## [v0.10.0](https://github.com/jorgecarleitao/parquet2/tree/v0.10.0) (2022-02-02)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.9.2...v0.10.0)

**Breaking changes:**

- Simplified API to write files [\#78](https://github.com/jorgecarleitao/parquet2/pull/78) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Fixed panic in reading empty values in hybrid-RLE [\#80](https://github.com/jorgecarleitao/parquet2/pull/80) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.9.2](https://github.com/jorgecarleitao/parquet2/tree/v0.9.2) (2022-01-25)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.9.0...v0.9.2)

**Fixed bugs:**

- Fixed panic in reading empty values in hybrid-RLE [\#80](https://github.com/jorgecarleitao/parquet2/pull/80) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.9.0](https://github.com/jorgecarleitao/parquet2/tree/v0.9.0) (2022-01-11)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.8.0...v0.9.0)

**Breaking changes:**

- Changed stream of groups to stream of futures of groups [\#71](https://github.com/jorgecarleitao/parquet2/pull/71) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- bitpacking: use stack-allocated temporary buffer [\#76](https://github.com/jorgecarleitao/parquet2/pull/76) ([danburkert](https://github.com/danburkert))
- Added constructor to `RowGroupMetaData` and `ColumnChunkMetaData` [\#74](https://github.com/jorgecarleitao/parquet2/pull/74) ([yjshen](https://github.com/yjshen))
- Improved performance of reading multiple pages [\#73](https://github.com/jorgecarleitao/parquet2/pull/73) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Fixed error in declaring size of compressed dict page. [\#72](https://github.com/jorgecarleitao/parquet2/pull/72) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.8.1](https://github.com/jorgecarleitao/parquet2/tree/v0.8.1) (2021-12-09)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.8.0...v0.8.1)

**Fixed bugs:**

- Fixed error in declaring size of compressed dict page. [\#72](https://github.com/jorgecarleitao/parquet2/pull/72) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.8.0](https://github.com/jorgecarleitao/parquet2/tree/v0.8.0) (2021-11-24)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.7.0...v0.8.0)

**Breaking changes:**

- Improved error message when a feature is not active [\#69](https://github.com/jorgecarleitao/parquet2/pull/69) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Fixed error in finishing iterator. [\#68](https://github.com/jorgecarleitao/parquet2/pull/68) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.7.0](https://github.com/jorgecarleitao/parquet2/tree/v0.7.0) (2021-11-13)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.6.0...v0.7.0)

**Breaking changes:**

- Use `i64`s for delta-bitpacked's interface [\#67](https://github.com/jorgecarleitao/parquet2/pull/67) ([kornholi](https://github.com/kornholi))

**New features:**

- Added basic support to read nested types [\#64](https://github.com/jorgecarleitao/parquet2/pull/64) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Fix off-by-one error in delta-bitpacked decoder [\#66](https://github.com/jorgecarleitao/parquet2/pull/66) ([kornholi](https://github.com/kornholi))

## [v0.6.0](https://github.com/jorgecarleitao/parquet2/tree/v0.6.0) (2021-10-18)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.5.0...v0.6.0)

**Breaking changes:**

- Improved performance of codec initialization [\#63](https://github.com/jorgecarleitao/parquet2/pull/63) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made `PageFilter` `Send` [\#62](https://github.com/jorgecarleitao/parquet2/pull/62) ([dantengsky](https://github.com/dantengsky))
- Alowed reusing compression buffer [\#60](https://github.com/jorgecarleitao/parquet2/pull/60) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Fixed delta-bitpacked mini-block decoding [\#56](https://github.com/jorgecarleitao/parquet2/pull/56) ([kornholi](https://github.com/kornholi))
- Add descriptor to `FixedLenStatistics` [\#54](https://github.com/jorgecarleitao/parquet2/pull/54) ([potter420](https://github.com/potter420))
- Fixed error in reading zero-width bit from hybrid RLE. [\#53](https://github.com/jorgecarleitao/parquet2/pull/53) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Added writing reduced statistics for `FixedLenByteArray` [\#55](https://github.com/jorgecarleitao/parquet2/pull/55) ([potter420](https://github.com/potter420))

## [v0.5.2](https://github.com/jorgecarleitao/parquet2/tree/v0.5.2) (2021-10-06)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.5.1...v0.5.2)

**Fixed bugs:**

- Fixed delta-bitpacked mini-block decoding [\#56](https://github.com/jorgecarleitao/parquet2/pull/56) ([kornholi](https://github.com/kornholi))
- Add descriptor to `FixedLenStatistics` [\#54](https://github.com/jorgecarleitao/parquet2/pull/54) ([potter420](https://github.com/potter420))

**Enhancements:**

- Added writing reduced statistics for `FixedLenByteArray` [\#55](https://github.com/jorgecarleitao/parquet2/pull/55) ([potter420](https://github.com/potter420))

## [v0.5.1](https://github.com/jorgecarleitao/parquet2/tree/v0.5.1) (2021-09-29)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.5.0...v0.5.1)

**Fixed bugs:**

- Fixed error in reading zero-width bit from hybrid RLE. [\#53](https://github.com/jorgecarleitao/parquet2/pull/53) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.5.0](https://github.com/jorgecarleitao/parquet2/tree/v0.5.0) (2021-09-18)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.4.0...v0.5.0)

**Breaking changes:**

- Renamed `Compression::Zsld` to `Compression::Zstd` \(typo\) [\#48](https://github.com/jorgecarleitao/parquet2/pull/48) ([vincev](https://github.com/vincev))

**Enhancements:**

- Add `null_count` method to trait `Statistics` [\#49](https://github.com/jorgecarleitao/parquet2/pull/49) ([yjshen](https://github.com/yjshen))

## [v0.4.0](https://github.com/jorgecarleitao/parquet2/tree/v0.4.0) (2021-08-28)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.3.0...v0.4.0)

**Breaking changes:**

- Make `write_*` return the number of written bytes. [\#45](https://github.com/jorgecarleitao/parquet2/issues/45)
- move `HybridRleDecoder` from `read::levels` to `encoding::hybrid_rle` [\#41](https://github.com/jorgecarleitao/parquet2/issues/41)
- Simplified split of page buffer [\#37](https://github.com/jorgecarleitao/parquet2/pull/37) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified API to get page iterator [\#36](https://github.com/jorgecarleitao/parquet2/pull/36) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Added support to write to async writers. [\#35](https://github.com/jorgecarleitao/parquet2/pull/35) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Fixed edge case of a small bitpacked. [\#43](https://github.com/jorgecarleitao/parquet2/pull/43) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in decoding RLE-hybrid. [\#40](https://github.com/jorgecarleitao/parquet2/pull/40) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Removed requirement of "Seek" on write. [\#44](https://github.com/jorgecarleitao/parquet2/pull/44) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Documentation updates:**

- Added guide to read [\#38](https://github.com/jorgecarleitao/parquet2/pull/38) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Testing updates:**

- Made tests deserializer use the correct decoder. [\#46](https://github.com/jorgecarleitao/parquet2/pull/46) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.3.0](https://github.com/jorgecarleitao/parquet2/tree/v0.3.0) (2021-08-09)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.2.0...v0.3.0)

**Breaking changes:**

- Added option to apply filter pushdown to data pages. [\#34](https://github.com/jorgecarleitao/parquet2/pull/34) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Add support to read async [\#33](https://github.com/jorgecarleitao/parquet2/pull/33) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Add support for async read [\#32](https://github.com/jorgecarleitao/parquet2/issues/32)
- Added option to apply filter pushdown to data pages. [\#34](https://github.com/jorgecarleitao/parquet2/pull/34) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.2.0](https://github.com/jorgecarleitao/parquet2/tree/v0.2.0) (2021-08-03)

[Full Changelog](https://github.com/jorgecarleitao/parquet2/compare/v0.1.0...v0.2.0)

**Enhancements:**

- Add support to write dictionary-encoded pages [\#29](https://github.com/jorgecarleitao/parquet2/issues/29)
- Upgrade zstd to ^0.9 [\#31](https://github.com/jorgecarleitao/parquet2/pull/31) ([Dandandan](https://github.com/Dandandan))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
