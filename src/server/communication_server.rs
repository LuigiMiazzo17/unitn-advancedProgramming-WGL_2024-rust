use chrono::Utc;
use log::warn;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use wg_2024::network::NodeId;

use crate::network::message::{ChatMessage, ChatResponse, Message, Request, Response, ServerType};
use crate::network::{NodeTrait, SimControllerMessage};

pub struct CommunicationServer {
    chats: HashMap<u64, Chat>,
    base_path: PathBuf,
}

#[derive(Serialize, Deserialize)]
struct Chat {
    name: String,
    messages: Vec<ChatMessage>,
}

impl NodeTrait for CommunicationServer {
    fn handle_message(&mut self, peer_id: NodeId, message: Message) -> Option<Response> {
        match message {
            Message::Request(request) => match request {
                Request::ServerType => Some(self.handle_server_type_request()),
                Request::GetChats => Some(self.get_chats()),
                Request::SendMessage(chat_id, message) => {
                    self.add_message_to_chat(chat_id, peer_id, message);
                    None
                }
                Request::CreateChat(chat_name) => Some(self.create_chat(chat_name)),
                Request::DeleteChat(chat_id) => {
                    self.delete_chat(chat_id);
                    None
                }
                Request::GetMessages(chat_id) => Some(self.get_chat_messages(chat_id)),
                _ => Some(Response::NotImplemented),
            },
            Message::Response(_) => {
                warn!("Received response message from peer");
                None
            }
        }
    }

    fn stop(&mut self) {
        for (chat_id, chat) in &self.chats {
            let path = self.base_path.join(chat_id.to_string());
            let chat = serde_json::to_string(chat).unwrap();
            fs::write(path, chat).unwrap();
        }
    }

    fn get_node_type(&self) -> wg_2024::packet::NodeType {
        wg_2024::packet::NodeType::Server
    }

    fn get_node_type_str(&self) -> &str {
        "CommunicationServer"
    }

    fn handle_control_message(
        &mut self,
        message: SimControllerMessage,
        send_message_to_peer: &mut dyn FnMut(NodeId, Option<u64>, Message),
    ) {
        match message {
            SimControllerMessage::SendMessageToPeer(peer_id, message) => {
                send_message_to_peer(peer_id, None, message);
            }
        }
    }
}

impl CommunicationServer {
    pub fn new(node_id: NodeId, base_path: String) -> Self {
        let path = PathBuf::new()
            .join(base_path)
            .join(node_id.to_string())
            .join("communication");
        let mut chats = HashMap::new();

        if !path.exists() {
            fs::create_dir_all(&path).unwrap();
        }

        for entry in fs::read_dir(&path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let chat_id = path.file_name().unwrap().to_str().unwrap().parse().unwrap();
            let chat = fs::read_to_string(&path).unwrap();
            let chat: Chat = serde_json::from_str(&chat).expect("Failed to parse chat");
            chats.insert(chat_id, chat);
        }

        CommunicationServer {
            chats,
            base_path: path,
        }
    }

    fn handle_server_type_request(&self) -> Response {
        Response::ServerType(ServerType::Communication)
    }

    fn get_chats(&self) -> Response {
        let chats = self
            .chats
            .iter()
            .map(|(id, chat)| ChatResponse {
                id: *id,
                name: chat.name.clone(),
            })
            .collect();
        Response::Chats(chats)
    }

    fn add_message_to_chat(&mut self, chat_id: u64, author: NodeId, message: String) {
        let chat = self.chats.get_mut(&chat_id).unwrap();
        chat.messages.push(ChatMessage {
            author,
            message,
            timestamp: Utc::now().to_rfc3339(),
        });
    }

    fn create_chat(&mut self, name: String) -> Response {
        let id = rand::random();

        self.chats.insert(
            id,
            Chat {
                name: name.clone(),
                messages: Default::default(),
            },
        );

        Response::NewChat(ChatResponse { id, name })
    }

    fn delete_chat(&mut self, chat_id: u64) {
        self.chats.remove(&chat_id);
        if let Err(e) = fs::remove_file(self.base_path.join(chat_id.to_string())) {
            println!("Failed to delete chat: {}", e);
        }
    }

    fn get_chat_messages(&self, chat_id: u64) -> Response {
        let chat = self.chats.get(&chat_id).unwrap();
        let messages = chat
            .messages
            .iter()
            .map(|message| ChatMessage {
                author: message.author,
                message: message.message.clone(),
                timestamp: message.timestamp.clone(),
            })
            .collect();
        Response::Messages(messages)
    }
}
