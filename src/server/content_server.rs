use wg_2024::network::NodeId;

use crate::network::message::Message;
use crate::network::NodeTrait;
use crate::server::ServerTrait;

pub struct ContentServer {}

impl ServerTrait for ContentServer {
    fn new() -> Self {
        ContentServer {}
    }
}

impl NodeTrait for ContentServer {
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

impl ContentServer {
    pub fn handle_server_type_request(&self) -> Message {
        Message::ServerTypeResponse("Content".to_string())
    }
}
