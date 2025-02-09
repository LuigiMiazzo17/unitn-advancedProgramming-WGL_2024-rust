use wg_2024::network::NodeId;

use crate::network::message::{Message, RequestMessage, ResponseMessage, ServerTypeMessage};
use crate::network::NodeTrait;
use crate::server::ServerTrait;

#[derive(Debug)]
pub struct ContentServer {}

impl ServerTrait for ContentServer {
    fn new() -> Self {
        ContentServer {}
    }
}

impl NodeTrait for ContentServer {
    fn handle_message(&self, peer_id: NodeId, message: Message) -> Option<Message> {
        match message {
            Message::Request(request) => match request {
                RequestMessage::ServerType => {
                    Some(Message::Response(self.handle_server_type_request()))
                }
            },
            Message::Response(response) => {
                // TODO: This is useless
                println!("Received response: {:?}", response);
                None
            }
        }
    }
}

impl ContentServer {
    pub fn handle_server_type_request(&self) -> ResponseMessage {
        ResponseMessage::ServerType(ServerTypeMessage::Content)
    }
}
