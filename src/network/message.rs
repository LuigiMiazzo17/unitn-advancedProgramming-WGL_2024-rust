use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    Request(RequestMessage),
    Response(ResponseMessage),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RequestMessage {
    ServerType,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ResponseMessage {
    ServerType(ServerTypeMessage),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ServerTypeMessage {
    Communication,
    Content,
}
