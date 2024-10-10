use parquet2::{deserialize::BinaryPageState, error::Result, page::DataPage};

use super::dictionary::BinaryPageDict;
use super::utils::deserialize_optional;

pub fn page_to_vec(page: &DataPage, dict: Option<&BinaryPageDict>) -> Result<Vec<Option<Vec<u8>>>> {
    assert_eq!(page.descriptor.max_rep_level, 0);

    let state = BinaryPageState::try_new(page, dict)?;

    match state {
        BinaryPageState::Optional(validity, values) => {
            deserialize_optional(validity, values.map(|x| x.map(|x| x.to_vec())))
        }
        BinaryPageState::Required(values) => values
            .map(|x| x.map(|x| x.to_vec()))
            .map(Some)
            .map(|x| x.transpose())
            .collect(),
        BinaryPageState::RequiredDictionary(dict) => dict
            .indexes
            .map(|x| x.and_then(|x| dict.dict.value(x as usize).map(|x| x.to_vec()).map(Some)))
            .collect(),
        BinaryPageState::OptionalDictionary(validity, dict) => {
            let values = dict
                .indexes
                .map(|x| x.and_then(|x| dict.dict.value(x as usize).map(|x| x.to_vec())));
            deserialize_optional(validity, values)
        }
    }
}
