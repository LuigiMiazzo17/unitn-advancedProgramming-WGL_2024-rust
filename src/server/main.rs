use wg_2024::network::NodeId;

use crate::network::message::Message;
use crate::network::NodeTrait;
pub use crate::server::communication_server::CommunicationServer;
pub use crate::server::content_server::ContentServer;

pub trait ServerTrait {
    fn new() -> Self;
}

#[derive(Debug)]
pub enum ServerType {
    Communication(CommunicationServer),
    Content(ContentServer),
}

pub struct Server {
    server_type: ServerType,
}

impl Server {
    pub fn handle_message(&self, peer_id: NodeId, message: Message) -> Option<Message> {
        match &self.server_type {
            ServerType::Communication(server) => server.handle_message(peer_id, message),
            ServerType::Content(server) => server.handle_message(peer_id, message),
        }
    }

    pub fn new(server_type: ServerType) -> Self {
        Server { server_type }
    }
}
