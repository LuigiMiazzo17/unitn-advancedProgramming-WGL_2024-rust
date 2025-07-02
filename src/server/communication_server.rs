use chrono::Utc;
use log::{error, info, trace, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use wg_2024::network::NodeId;

use crate::network::message::{
    ChatMessage, ChatRequest, ChatResponse, ChatResponseMessage, CreateChatRequest, Message,
    Request, Response, ServerType,
};
use crate::network::{NodeTrait, SimControllerMessage};

pub struct CommunicationServer {
    chats: HashMap<u64, Chat>,
    base_path: PathBuf,
}

#[derive(Serialize, Deserialize)]
struct Chat {
    name: String,
    public: bool,
    password: Option<String>,
    registered_peers: Vec<NodeId>,
    messages: Vec<ChatMessage>,
}

impl NodeTrait for CommunicationServer {
    fn handle_message(&mut self, peer_id: NodeId, message: Message) -> Option<Response> {
        match message {
            Message::Request(request) => match request {
                Request::ServerType => Some(self.handle_server_type_request()),
                Request::ChatRequest(chat_request) => {
                    info!("Received ChatRequest from peer {}", peer_id);
                    Some(match chat_request {
                        ChatRequest::Create(create_chat_request) => {
                            self.create_chat(peer_id, create_chat_request)
                        }
                        ChatRequest::Join(chat_id, password) => {
                            self.join_chat(peer_id, chat_id, password)
                        }
                        ChatRequest::Leave(chat_id) => self.leave_chat(peer_id, chat_id),
                        ChatRequest::Delete(chat_id) => self.delete_chat(chat_id),
                        ChatRequest::SendMessage(chat_id, message) => {
                            self.add_message_to_chat(chat_id, peer_id, message)
                        }
                        ChatRequest::GetChats => self.get_chats(),
                        ChatRequest::GetMessages(chat_id) => self.get_chat_messages(chat_id),
                    })
                }
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
    ) -> Option<(NodeId, Option<u64>, Message)> {
        match message {
            SimControllerMessage::SendMessageToPeer(peer_id, message) => {
                Some((peer_id, None, message))
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
            .filter(|(_, chat)| chat.public)
            .map(|(id, chat)| ChatResponseMessage {
                id: *id,
                name: chat.name.clone(),
            })
            .collect();
        Response::ChatResponse(ChatResponse::Chats(chats))
    }

    fn add_message_to_chat(&mut self, chat_id: u64, author: NodeId, message: String) -> Response {
        trace!("Adding message to chat {} by peer {}", chat_id, author);
        let chat = self.chats.get_mut(&chat_id);
        match chat {
            Some(chat) => {
                if !chat.registered_peers.contains(&author) {
                    warn!("Peer {} is not registered in chat {}", author, chat_id);
                    Response::ChatResponse(ChatResponse::Error("Not in chat".to_string()))
                } else {
                    trace!("Message added to chat {} by peer {}", chat_id, author);
                    chat.messages.push(ChatMessage {
                        author,
                        message,
                        timestamp: Utc::now().to_rfc3339(),
                    });
                    Response::Success
                }
            }
            None => {
                warn!("Chat {} does not exist", chat_id);
                Response::ChatResponse(ChatResponse::Error("Chat does not exist".to_string()))
            }
        }
    }

    fn create_chat(&mut self, peer_id: NodeId, chat_request: CreateChatRequest) -> Response {
        let id = rand::random();

        self.chats.insert(id, {
            Chat {
                name: chat_request.name,
                public: chat_request.public,
                password: chat_request.password,
                registered_peers: vec![peer_id],
                messages: Vec::new(),
            }
        });

        Response::Success
    }

    fn delete_chat(&mut self, chat_id: u64) -> Response {
        if self.chats.remove(&chat_id).is_none() {
            warn!("Chat {} does not exist", chat_id);
            return Response::ChatResponse(ChatResponse::Error("Chat does not exist".to_string()));
        }
        if let Err(e) = fs::remove_file(self.base_path.join(chat_id.to_string())) {
            error!("Failed to delete chat file {}: {}", chat_id, e);
            Response::ChatResponse(ChatResponse::Error("Failed to delete chat".to_string()))
        } else {
            info!("Chat {} deleted successfully", chat_id);
            Response::Success
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
        Response::ChatResponse(ChatResponse::Messages(messages))
    }

    fn join_chat(&mut self, peer_id: NodeId, chat_id: u64, password: Option<String>) -> Response {
        info!("Peer {} requested to join chat {}", peer_id, chat_id);
        if let Some(chat) = self.chats.get_mut(&chat_id) {
            if chat.password.as_ref() == password.as_ref() {
                if !chat.registered_peers.contains(&peer_id) {
                    chat.registered_peers.push(peer_id);
                    trace!("Peer {} joined chat {}", peer_id, chat_id);
                    Response::Success
                } else {
                    warn!("Peer {} is already in chat {}", peer_id, chat_id);
                    Response::ChatResponse(ChatResponse::Error("Already in chat".to_string()))
                }
            } else {
                warn!(
                    "Peer {} failed to join chat {}: wrong password",
                    peer_id, chat_id
                );
                Response::ChatResponse(ChatResponse::Error("Wrong password".to_string()))
            }
        } else {
            warn!("Chat {} does not exist", chat_id);
            Response::ChatResponse(ChatResponse::Error("Chat does not exist".to_string()))
        }
    }

    fn leave_chat(&mut self, peer_id: NodeId, chat_id: u64) -> Response {
        info!("Peer {} requested to leave chat {}", peer_id, chat_id);
        if let Some(chat) = self.chats.get_mut(&chat_id) {
            if let Some(pos) = chat.registered_peers.iter().position(|&id| id == peer_id) {
                chat.registered_peers.remove(pos);
                trace!("Peer {} left chat {}", peer_id, chat_id);
                Response::Success
            } else {
                warn!("Peer {} is not in chat {}", peer_id, chat_id);
                Response::ChatResponse(ChatResponse::Error("Not in chat".to_string()))
            }
        } else {
            warn!("Chat {} does not exist", chat_id);
            Response::ChatResponse(ChatResponse::Error("Chat does not exist".to_string()))
        }
    }
}
