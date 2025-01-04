use crate::network::message::Message;
use crate::network::NodeTrait;
use crate::server::communication_server::CommunicationServer;
use crate::server::content_server::ContentServer;

pub trait ServerTrait {
    fn new() -> Self;
}

enum ServerType {
    Communication(CommunicationServer),
    Content(ContentServer),
}

pub struct Server {
    server_type: ServerType,
}

impl Server {
    pub fn handle_message(&self, message: Message) {
        match &self.server_type {
            ServerType::Communication(server) => server.handle_message(message),
            ServerType::Content(server) => server.handle_message(message),
        }
    }
}
