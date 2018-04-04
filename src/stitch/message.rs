//! Stitch API Request Messages

use std::vec::Vec;

use serde::ser::Serialize;

/// Trait that defines fields needed to send a record to stitch.
///
/// # Example
///
/// ```ignore
/// # use
/// struct User {
///     id: u64,
/// }
///
/// impl Message for User {
///     fn get_table_name(&self) -> String {
///         String::from("users")
///     }
///
///     fn get_keys(&self) -> Vec<String> {
///         vec![String::from("id")]
///     }
/// }
/// ```
pub trait Message {
    fn get_table_name(&self) -> String;

    fn get_keys(&self) -> Vec<String>;
}

/// Struct that is Serializable and Sendable to stitch.
/// Ultimately, this type is converted to a `RawUpsertRequest`
/// before sending to stitch, this is done in order to make use
/// of `serde` automatic derive `Serialize`.
#[derive(Debug)]
pub struct UpsertRequest<T>
where
    T: Message + Serialize,
{
    client_id: u32,
    sequence: u64,
    data: T,
}

impl<T> UpsertRequest<T>
where
    T: Message + Serialize,
{
    /// Creates new `UpsertRequest`.
    ///
    /// * `sequence` - Unique id. This field is used by stitch in order
    /// to know how to order messages correctly. This should be a monotonically
    /// increasing number, current time in milliseconds is often used.
    pub fn new(client_id: u32, sequence: u64, data: T) -> Self {
        UpsertRequest {
            client_id,
            sequence,
            data,
        }
    }

    /// All `UpsertRequest`s are of stitch action `upsert`.
    fn get_action(&self) -> &'static str {
        "upsert"
    }

    /// Returns the table name from the underlying `Message` type.
    fn get_table_name(&self) -> String {
        self.data.get_table_name()
    }

    /// Returns the keys from the underlying `Message` type.
    fn get_keys(&self) -> Vec<String> {
        self.data.get_keys()
    }
}

/// Serializable representation of `UpsertRecord`.
#[derive(Debug, Serialize)]
pub struct RawUpsertRequest<T>
where
    T: Message + Serialize,
{
    client_id: u32,
    sequence: u64,
    table_name: String,
    action: &'static str,
    key_names: Vec<String>,
    data: T,
}

/// Convert `UpsertRequest` into `RawUpsertRequest`.
impl<T> From<UpsertRequest<T>> for RawUpsertRequest<T>
where
    T: Message + Serialize,
{
    fn from(rec: UpsertRequest<T>) -> Self {
        RawUpsertRequest {
            client_id: rec.client_id,
            sequence: rec.sequence,
            table_name: rec.get_table_name(),
            action: rec.get_action(),
            key_names: rec.get_keys(),
            data: rec.data,
        }
    }
}
