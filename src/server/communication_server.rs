use wg_2024::network::NodeId;

use crate::network::message::{Message, ServerTypeMessage};
use crate::network::NodeTrait;
use crate::server::ServerTrait;

pub struct CommunicationServer {}

impl ServerTrait for CommunicationServer {
    fn new() -> Self {
        CommunicationServer {}
    }
}

impl NodeTrait for CommunicationServer {
    fn handle_message(&self, peer_id: NodeId, message: Message) -> Option<Message> {
        match message {
            Message::ServerTypeRequest => Some(self.handle_server_type_request()),
            _ => {
                todo!()
            }
        }
    }
}

impl CommunicationServer {
    pub fn handle_server_type_request(&self) -> Message {
        Message::ServerTypeResponse(ServerTypeMessage::Communication)
    }
}
