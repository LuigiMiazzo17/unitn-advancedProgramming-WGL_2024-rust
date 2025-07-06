use std::fmt::Display;

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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Response {
    ServerType(ServerType),
    ChatResponse(ChatResponse),
    ContentResponse(ContentResponse),
    NotImplemented,
    Success,
    Error(String),
}

impl Display for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Response::ServerType(server_type) => write!(f, "Server type is {}", server_type),
            Response::ChatResponse(chat_response) => write!(f, "{}", chat_response),
            Response::ContentResponse(content_response) => {
                write!(f, "{}", content_response)
            }
            Response::NotImplemented => write!(f, "Not implemented"),
            Response::Success => write!(f, "Request was successful"),
            Response::Error(err) => write!(f, "Error: {}", err),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ServerType {
    Communication,
    Content,
}

impl Display for ServerType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerType::Communication => write!(f, "Communication"),
            ServerType::Content => write!(f, "Content"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChatResponseMessage {
    pub id: u64,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChatMessage {
    pub author: NodeId,
    pub message: String,
    pub timestamp: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateChatRequest {
    pub name: String,
    pub public: bool,
    pub password: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ChatRequest {
    Join(u64, Option<String>), // chat id, optional password
    Leave(u64),                // chat id
    SendMessage(u64, String),  // chat id, message
    Create(CreateChatRequest), // chat details
    Delete(u64),               // chat id
    GetChats,                  // list all chats
    GetMessages(u64),          // chat id
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ChatResponse {
    Chats(Vec<ChatResponseMessage>), // list of chats
    Messages(Vec<ChatMessage>),      // messages in a chat
}

impl Display for ChatResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChatResponse::Chats(chats) => {
                if chats.is_empty() {
                    write!(f, "No available chats.")
                } else {
                    let mut s = "Available chats: ".to_string();
                    for chat in chats {
                        let chat_info = format!("{} (ID: {})", chat.name, chat.id);
                        s.push_str(&chat_info);
                        s.push_str(", ");
                    }
                    write!(f, "{}", s)
                }
            }
            ChatResponse::Messages(messages) => {
                if messages.is_empty() {
                    write!(f, "No messages in this chat.")
                } else {
                    let mut s = String::new();
                    for message in messages {
                        s.push_str(&format!(
                            "[{}] {}: {}\n",
                            message.timestamp, message.author, message.message
                        ));
                    }
                    write!(f, "Messages:\n{}", s)
                }
            }
        }
    }
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ContentResponse {
    Files(Vec<String>), // list of files
    File(String),       // file content
}

impl Display for ContentResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContentResponse::Files(files) => {
                if files.is_empty() {
                    write!(f, "No files available.")
                } else {
                    let file_list = files.join(", ");
                    write!(f, "Available files: {}", file_list)
                }
            }
            ContentResponse::File(content) => write!(f, "File content: {}", content),
        }
    }
}
