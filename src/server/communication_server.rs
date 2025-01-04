use wg_2024::network::NodeId;

use crate::network::message::Message;
use crate::network::NodeTrait;
use crate::server::ServerTrait;

pub struct CommunicationServer {}

impl ServerTrait for CommunicationServer {
    fn new() -> Self {
        CommunicationServer {}
    }
}

impl NodeTrait for CommunicationServer {
    fn handle_message(&self, message: Message) {
        match message {
            Message::ServerTypeRequest => {
                let response = self.handle_server_type_request();
                self.send_message(0, response);
            }
            _ => {
                // Unsupported message type
                todo!()
            }
        }
    }

    fn send_message(&self, recipient: NodeId, message: Message) {
        // Send the message
    }
}

impl CommunicationServer {
    pub fn handle_server_type_request(&self) -> Message {
        Message::ServerTypeResponse("Communication".to_string())
    }
}
