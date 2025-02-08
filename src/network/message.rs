use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum Message {
    ServerTypeRequest,
    ServerTypeResponse(ServerTypeMessage),
}

#[derive(Serialize, Deserialize)]
pub enum ServerTypeMessage {
    Communication,
    Content,
}
