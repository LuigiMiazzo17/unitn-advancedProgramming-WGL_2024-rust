use wg_2024::network::NodeId;

use crate::network::message::{Message, RequestMessage, ResponseMessage, ServerTypeMessage};
use crate::network::NodeTrait;
use crate::server::ServerTrait;

#[derive(Debug)]
pub struct CommunicationServer {}

impl ServerTrait for CommunicationServer {
    fn new() -> Self {
        CommunicationServer {}
    }
}

impl NodeTrait for CommunicationServer {
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

impl CommunicationServer {
    pub fn handle_server_type_request(&self) -> ResponseMessage {
        ResponseMessage::ServerType(ServerTypeMessage::Communication)
    }
}
