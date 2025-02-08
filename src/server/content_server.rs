use wg_2024::network::NodeId;

use crate::network::message::{Message, ServerTypeMessage};
use crate::network::NodeTrait;
use crate::server::ServerTrait;

pub struct ContentServer {}

impl ServerTrait for ContentServer {
    fn new() -> Self {
        ContentServer {}
    }
}

impl NodeTrait for ContentServer {
    fn handle_message(&self, peer_id: NodeId, message: Message) -> Option<Message> {
        match message {
            Message::ServerTypeRequest => Some(self.handle_server_type_request()),
            _ => {
                // Unsupported message type
                todo!()
            }
        }
    }
}

impl ContentServer {
    pub fn handle_server_type_request(&self) -> Message {
        Message::ServerTypeResponse(ServerTypeMessage::Content)
    }
}
