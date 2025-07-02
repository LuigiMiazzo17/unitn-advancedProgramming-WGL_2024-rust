use wg_2024::network::NodeId;
use wg_2024::packet::NodeType;

use crate::network::message::{Message, Response};
use crate::network::{NodeTrait, SimControllerMessage};

pub struct WebBrowser {}

impl NodeTrait for WebBrowser {
    fn handle_message(&mut self, _: NodeId, message: Message) -> Option<Response> {
        match message {
            Message::Response(response) => Some(response),
            Message::Request(request) => {
                // TODO: This is useless
                println!("Received request: {request:?}");
                None
            }
        }
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
