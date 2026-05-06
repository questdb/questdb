use serde::{Deserialize, Serialize};
use std::fmt;

pub trait IdNumber: Sized + Copy {
    type Number: Copy
        + num_traits::PrimInt
        + num_traits::ToBytes
        + num_traits::WrappingAdd
        + bytemuck::Pod;

    fn next(&self) -> Self;

    fn wrapping_next(&self) -> Self;

    fn add(&self, n: Self::Number) -> Self;

    fn wrapping_add(&self, n: Self::Number) -> Self;

    fn value(&self) -> Self::Number;
}

#[macro_export]
macro_rules! impl_strong_id_type {
    ($name:ident, $int_type:ty) => {
        impl_strong_id_type!($name, $int_type, "");
    };
    ($name:ident, $int_type:ty, $display_prefix:tt) => {
        #[derive(Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
        #[repr(transparent)]
        pub struct $name($int_type);

        impl $name {
            pub const fn new(value: $int_type) -> Self {
                Self(value)
            }

            pub const fn zero() -> Self {
                Self(0)
            }
        }

        impl IdNumber for $name {
            type Number = $int_type;

            fn next(&self) -> Self {
                self.add(1)
            }

            fn wrapping_next(&self) -> Self {
                self.wrapping_add(1)
            }

            fn add(&self, n: $int_type) -> Self {
                Self(self.value() + n)
            }

            fn wrapping_add(&self, n: $int_type) -> Self {
                Self(self.value().wrapping_add(n))
            }

            fn value(&self) -> $int_type {
                self.0
            }
        }

        impl From<$int_type> for $name {
            fn from(value: $int_type) -> Self {
                Self(value)
            }
        }

        impl From<$name> for $int_type {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let val = self.0;
                write!(f, concat!($display_prefix, "{}"), val)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let val = self.0;
                write!(f, concat!($display_prefix, "{}"), val)
            }
        }
    };
}

// A unique identifier for a transaction as present in the `_txnlog` file.
// TxnIDs start at 0 (for table creation) and are 1+ for actual follow-up txns.
impl_strong_id_type!(TxnId, u64);

// Id of a WAL directory.
// Wal IDs start at 1!
impl_strong_id_type!(WalId, u32);

// Id of a segment within a WAL directory.
// Segment IDs start at 0!
impl_strong_id_type!(SegmentId, i32);

// Id of a transaction within a segment (not to be confused with TxnId).
// Segment txn IDs start at 0!
impl_strong_id_type!(SegmentTxnId, u32);
