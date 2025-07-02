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
    ChatRequest(ChatRequest),
    ContentRequest(ContentRequest),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    ServerType(ServerType),
    ChatResponse(ChatResponse),
    ContentResponse(ContentResponse),
    NotImplemented,
    Success,
    Error(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ServerType {
    Communication,
    Content,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ChatResponseMessage {
    pub id: u64,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ChatMessage {
    pub author: NodeId,
    pub message: String,
    pub timestamp: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateChatRequest {
    pub name: String,
    pub public: bool,
    pub password: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ChatRequest {
    Join(u64, Option<String>), // chat id, optional password
    Leave(u64),                // chat id
    SendMessage(u64, String),  // chat id, message
    Create(CreateChatRequest), // chat details
    Delete(u64),               // chat id
    GetChats,                  // list all chats
    GetMessages(u64),          // chat id
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ChatResponse {
    Chats(Vec<ChatResponseMessage>), // list of chats
    Messages(Vec<ChatMessage>),      // messages in a chat
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ContentRequest {
    ListPublicFiles,
    GetPublicFile(String),
    WritePublicFile(String, String),
    ListPrivateFiles,
    GetPrivateFile(String),
    WritePrivateFile(String, String),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ContentResponse {
    Files(Vec<String>), // list of files
    File(String),       // file content
}
