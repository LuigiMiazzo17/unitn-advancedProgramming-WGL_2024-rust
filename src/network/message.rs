use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum Message {
    ServerTypeRequest,
    ServerTypeResponse(String),
}
