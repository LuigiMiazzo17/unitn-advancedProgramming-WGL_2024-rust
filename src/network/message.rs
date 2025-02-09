use serde::{Deserialize, Serialize};

use wg_2024::network::NodeId;

#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    Request(Request),
    Response(Response),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    ServerType,
    GetChats,
    SendMessage(u64, String),
    CreateChat(String),
    DeleteChat(u64),
    GetMessages(u64),
    ListPublicFiles,
    GetPublicFile(String),
    WritePublicFile(String, String),
    ListPrivateFiles,
    GetPrivateFile(String),
    WritePrivateFile(String, String),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    ServerType(ServerType),
    Chats(Vec<ChatResponse>),
    NewChat(ChatResponse),
    Messages(Vec<ChatMessage>),
    Files(Vec<String>),
    File(String),
    NoSuchFile,
    NotImplemented,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ServerType {
    Communication,
    Content,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ChatResponse {
    pub id: u64,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ChatMessage {
    pub author: NodeId,
    pub message: String,
    pub timestamp: String,
}
