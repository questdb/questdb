use parquet2::compression::Compression;
use parquet2::metadata::Descriptor;
use parquet2::page::ParquetPageHeader;
use parquet2::read::{SlicePageReader, SlicedPage};
use parquet2::schema::types::{PhysicalType, PrimitiveType};
use parquet_format_safe::thrift::protocol::TCompactOutputProtocol;
use parquet_format_safe::{
    DataPageHeader as FmtDataHeader, DictionaryPageHeader as FmtDictHeader,
    Encoding as FmtEncoding, PageType as FmtPageType,
};

fn descriptor() -> Descriptor {
    Descriptor {
        primitive_type: PrimitiveType::from_physical("c".to_string(), PhysicalType::Int32),
        max_def_level: 0,
        max_rep_level: 0,
    }
}

fn write_dict_header(buf: &mut Vec<u8>, payload_len: i32) {
    let header = ParquetPageHeader {
        type_: FmtPageType::DICTIONARY_PAGE,
        uncompressed_page_size: payload_len,
        compressed_page_size: payload_len,
        crc: None,
        data_page_header: None,
        index_page_header: None,
        dictionary_page_header: Some(FmtDictHeader {
            num_values: 1,
            encoding: FmtEncoding::PLAIN,
            is_sorted: None,
        }),
        data_page_header_v2: None,
    };
    let mut prot = TCompactOutputProtocol::new(&mut *buf);
    header.write_to_out_protocol(&mut prot).unwrap();
}

fn write_data_page_v1_header(buf: &mut Vec<u8>, payload_len: i32, num_values: i32) {
    let header = ParquetPageHeader {
        type_: FmtPageType::DATA_PAGE,
        uncompressed_page_size: payload_len,
        compressed_page_size: payload_len,
        crc: None,
        data_page_header: Some(FmtDataHeader {
            num_values,
            encoding: FmtEncoding::PLAIN,
            definition_level_encoding: FmtEncoding::RLE,
            repetition_level_encoding: FmtEncoding::RLE,
            statistics: None,
        }),
        index_page_header: None,
        dictionary_page_header: None,
        data_page_header_v2: None,
    };
    let mut prot = TCompactOutputProtocol::new(&mut *buf);
    header.write_to_out_protocol(&mut prot).unwrap();
}

#[test]
fn rejects_second_dictionary_page_in_chunk() {
    let payload: [u8; 4] = [0, 0, 0, 0];
    let mut bytes: Vec<u8> = Vec::new();
    write_dict_header(&mut bytes, payload.len() as i32);
    bytes.extend_from_slice(&payload);
    write_dict_header(&mut bytes, payload.len() as i32);
    bytes.extend_from_slice(&payload);

    let mut reader =
        SlicePageReader::for_test(&bytes, descriptor(), Compression::Uncompressed, 1024 * 1024);

    let first = reader
        .next()
        .expect("first page present")
        .expect("first page ok");
    assert!(
        matches!(first, SlicedPage::Dict(_)),
        "expected first page to be a dict"
    );

    let err = reader
        .next()
        .expect("second page present")
        .expect_err("second dict page must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("at most one dictionary page"),
        "unexpected error: {msg}"
    );
}

#[test]
fn rejects_dictionary_page_after_data_page() {
    // Parquet spec: a dictionary page must be the first page of a column chunk if present.
    // Construct a stream with a non-empty data page followed by a dict page; the reader must
    // reject it on the second `next()`.
    let payload: [u8; 4] = [0, 0, 0, 0];
    let mut bytes: Vec<u8> = Vec::new();
    write_data_page_v1_header(&mut bytes, payload.len() as i32, 1);
    bytes.extend_from_slice(&payload);
    write_dict_header(&mut bytes, payload.len() as i32);
    bytes.extend_from_slice(&payload);

    let mut reader =
        SlicePageReader::for_test(&bytes, descriptor(), Compression::Uncompressed, 1024 * 1024);

    let first = reader
        .next()
        .expect("first page present")
        .expect("first page ok");
    assert!(
        matches!(first, SlicedPage::Data(_)),
        "expected first page to be a data page"
    );

    let err = reader
        .next()
        .expect("second page present")
        .expect_err("dict page after data must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("at most one dictionary page"),
        "unexpected error: {msg}"
    );
}

#[test]
fn rejects_dictionary_page_after_empty_data_page() {
    // An empty data page (num_values == 0) is legal in the Parquet format. The ordering check
    // must still trip on the trailing dict page even though no rows were observed.
    let payload: [u8; 4] = [0, 0, 0, 0];
    let mut bytes: Vec<u8> = Vec::new();
    write_data_page_v1_header(&mut bytes, payload.len() as i32, 0);
    bytes.extend_from_slice(&payload);
    write_dict_header(&mut bytes, payload.len() as i32);
    bytes.extend_from_slice(&payload);

    let mut reader =
        SlicePageReader::for_test(&bytes, descriptor(), Compression::Uncompressed, 1024 * 1024);

    let first = reader
        .next()
        .expect("first page present")
        .expect("first page ok");
    assert!(
        matches!(first, SlicedPage::Data(_)),
        "expected first page to be a data page"
    );

    let err = reader
        .next()
        .expect("second page present")
        .expect_err("dict page after empty data page must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("at most one dictionary page"),
        "unexpected error: {msg}"
    );
}

#[test]
fn accepts_dict_followed_by_data_pages() {
    // Positive case: a single dict page followed by data pages must be accepted. Guards against
    // a future regression that would set `seen_dict_page` outside the dict branch.
    let payload: [u8; 4] = [0, 0, 0, 0];
    let mut bytes: Vec<u8> = Vec::new();
    write_dict_header(&mut bytes, payload.len() as i32);
    bytes.extend_from_slice(&payload);
    write_data_page_v1_header(&mut bytes, payload.len() as i32, 1);
    bytes.extend_from_slice(&payload);
    write_data_page_v1_header(&mut bytes, payload.len() as i32, 2);
    bytes.extend_from_slice(&payload);

    let mut reader =
        SlicePageReader::for_test(&bytes, descriptor(), Compression::Uncompressed, 1024 * 1024);

    let first = reader
        .next()
        .expect("dict page present")
        .expect("dict page ok");
    assert!(matches!(first, SlicedPage::Dict(_)));

    let second = reader
        .next()
        .expect("first data page present")
        .expect("first data page ok");
    assert!(matches!(second, SlicedPage::Data(_)));

    let third = reader
        .next()
        .expect("second data page present")
        .expect("second data page ok");
    assert!(matches!(third, SlicedPage::Data(_)));
}
