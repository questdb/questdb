use parquet2::deserialize::BooleanPageState;
use parquet2::encoding::hybrid_rle::BitmapIter;
use parquet2::error::Result;
use parquet2::page::DataPage;

use crate::read::utils::deserialize_optional;

pub fn page_to_vec(page: &DataPage) -> Result<Vec<Option<bool>>> {
    assert_eq!(page.descriptor.max_rep_level, 0);
    let state = BooleanPageState::try_new(page)?;

    match state {
        BooleanPageState::Optional(validity, mut values) => {
            deserialize_optional(validity, values.by_ref().map(Ok))
        }
        BooleanPageState::Required(bitmap, length) => Ok(BitmapIter::new(bitmap, 0, length)
            .into_iter()
            .map(Some)
            .collect()),
    }
}
