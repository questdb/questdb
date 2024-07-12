use std::fmt::Debug;

/// This trait represents an Sqllogictest column type.
/// An Sqllogictest column is represented with a single character.
/// The type has to be serializable to a character.
pub trait ColumnType: Debug + PartialEq + Eq + Clone + Send + Sync {
    fn from_char(value: char) -> Option<Self>;
    fn to_char(&self) -> char;
}

/// The default Sqllogictest type.
/// The valid types are:
/// - 'T' - text, varchar results
/// - 'I' - integers
/// - 'R' - floating point numbers
/// Any other types are represented with `?`([`DefaultColumnType::Any`]).
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum DefaultColumnType {
    Text,
    Integer,
    FloatingPoint,
    Any,
}

impl ColumnType for DefaultColumnType {
    fn from_char(value: char) -> Option<Self> {
        match value {
            'T' => Some(Self::Text),
            'I' => Some(Self::Integer),
            'R' => Some(Self::FloatingPoint),
            _ => Some(Self::Any),
        }
    }

    fn to_char(&self) -> char {
        match self {
            Self::Text => 'T',
            Self::Integer => 'I',
            Self::FloatingPoint => 'R',
            Self::Any => '?',
        }
    }
}
