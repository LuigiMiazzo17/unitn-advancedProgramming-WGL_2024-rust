use crossbeam::channel::Sender;
use wg_2024::network::NodeId;
use wg_2024::packet::NodeType;

use log::{debug, error, warn};

use crate::network::message::{Message, Response};
use crate::network::{ClientControlMessage, NodeTrait, ResponseFromNetwork, SimControllerMessage};

pub struct WebBrowser {
    node_id: NodeId,
    client_controller_send: Sender<ClientControlMessage>,
}

impl NodeTrait for WebBrowser {
    fn handle_message(&mut self, peer_id: NodeId, message: Message) -> Option<Response> {
        match message {
            Message::Response(response) => {
                debug!("Server sent response message to peer {peer_id}: {response:?}");
                if let Err(e) = self.client_controller_send.send(
                    ClientControlMessage::ReturnResponseFromNetwork(ResponseFromNetwork {
                        peer_id: self.node_id,
                        server_id: peer_id,
                        message: response,
                    }),
                ) {
                    error!("Failed to send response from server to client controller: {e}");
                }
            }
            Message::Request(_) => {
                warn!("Server received request message from peer {peer_id}");
            }
        }
        None
    }

    fn stop(&mut self) {}

    fn get_node_type(&self) -> NodeType {
        wg_2024::packet::NodeType::Client
    }

    fn get_node_type_str(&self) -> &str {
        "WebBrowser"
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

impl WebBrowser {
    pub fn new(node_id: NodeId, client_controller_send: Sender<ClientControlMessage>) -> Self {
        WebBrowser {
            node_id,
            client_controller_send,
        }
    }
}
