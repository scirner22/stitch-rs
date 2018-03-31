use std::vec::Vec;

pub mod error;

pub trait Message {
    fn get_table_name(&self) -> String;

    fn get_keys(&self) -> Vec<String>;
}

#[derive(Debug)]
pub struct UpsertRequest<T: Message> {
    client_id: u32,
    sequence: u64,
    data: T,
}

impl<T: Message> UpsertRequest<T> {
    pub fn new(data: T) -> Self {
        UpsertRequest {
            client_id: 1,
            sequence: 1,
            data,
        }
    }
}

impl<T: Message> UpsertRequest<T> {
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
struct RawUpsertRequest<T: Message> {
    client_id: u32,
    sequence: u64,
    table_name: String,
    action: String,
    key_names: Vec<String>,
    data: T,
}

impl<T: Message> From<UpsertRequest<T>> for RawUpsertRequest<T> {
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
