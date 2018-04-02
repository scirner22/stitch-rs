use std::vec::Vec;

use serde::ser::Serialize;

pub trait Message {
    fn get_table_name(&self) -> String;

    fn get_keys(&self) -> Vec<String>;
}

#[derive(Debug)]
pub struct UpsertRequest<T>
    where T: Message + Serialize
{
    client_id: u32,
    sequence: u64,
    data: T,
}

impl<T> UpsertRequest<T>
    where T: Message + Serialize
{
    pub fn new(client_id: u32, sequence: u64, data: T) -> Self {
        UpsertRequest { client_id, sequence, data }
    }
}

impl<T> UpsertRequest<T>
    where T: Message + Serialize
{
    fn get_action(&self) -> String {
        String::from("upsert")
    }

    fn get_table_name(&self) -> String {
        self.data.get_table_name()
    }

    fn get_keys(&self) -> Vec<String> {
        self.data.get_keys()
    }
}


#[derive(Debug, Serialize)]
pub struct RawUpsertRequest<T>
    where T: Message + Serialize
{
    client_id: u32,
    sequence: u64,
    table_name: String,
    action: String,
    key_names: Vec<String>,
    data: T,
}

impl<T> From<UpsertRequest<T>> for RawUpsertRequest<T>
    where T: Message + Serialize
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
