use parquet2::compression::Compression;
use parquet2::metadata::Descriptor;
use parquet2::page::ParquetPageHeader;
use parquet2::read::{SlicePageReader, SlicedPage};
use parquet2::schema::types::{PhysicalType, PrimitiveType};
use parquet_format_safe::thrift::protocol::TCompactOutputProtocol;
use parquet_format_safe::{
    DictionaryPageHeader as FmtDictHeader, Encoding as FmtEncoding, PageType as FmtPageType,
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
