use chrono::{DateTime, Utc};
use std::collections::HashMap;

use wg_2024::network::NodeId;

use crate::network::message::{
    ChatMessage as ChatMessageResponse, ChatResponse, Message, Request, Response, ServerType,
};
use crate::network::NodeTrait;

pub struct CommunicationServer {
    chats: HashMap<u64, Chat>,
}

struct Chat {
    name: String,
    messages: Vec<ChatMessage>,
}

struct ChatMessage {
    author: NodeId,
    message: String,
    timestamp: DateTime<Utc>,
}

impl NodeTrait for CommunicationServer {
    fn handle_message(&mut self, peer_id: NodeId, message: Message) -> Option<Message> {
        match message {
            Message::Request(request) => match request {
                Request::ServerType => Some(Message::Response(self.handle_server_type_request())),
                Request::GetChats => Some(Message::Response(self.get_chats())),
                Request::SendMessage(chat_id, message) => {
                    self.add_message_to_chat(chat_id, peer_id, message);
                    None
                }
                Request::CreateChat(chat_name) => {
                    Some(Message::Response(self.create_chat(chat_name)))
                }
                Request::DeleteChat(chat_id) => {
                    self.delete_chat(chat_id);
                    None
                }
                Request::GetMessages(chat_id) => {
                    Some(Message::Response(self.get_chat_messages(chat_id)))
                }
                _ => Some(Message::Response(Response::NotImplemented)),
            },
            Message::Response(response) => {
                // TODO: This is useless
                println!("Received response: {:?}", response);
                None
            }
        }
    }
}

impl CommunicationServer {
    pub fn new() -> Self {
        CommunicationServer {
            chats: Default::default(),
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
            timestamp: Utc::now(),
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
    }

    fn get_chat_messages(&self, chat_id: u64) -> Response {
        let chat = self.chats.get(&chat_id).unwrap();
        let messages = chat
            .messages
            .iter()
            .map(|message| ChatMessageResponse {
                author: message.author,
                message: message.message.clone(),
                timestamp: message.timestamp.to_rfc3339(),
            })
            .collect();
        Response::Messages(messages)
    }
}
