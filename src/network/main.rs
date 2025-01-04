use crossbeam::channel::{select_biased, Receiver, Sender};
use std::collections::HashMap;

use wg_2024::network::NodeId;
use wg_2024::packet::Packet;

use crate::network::message::Message;
use crate::server::Server;

pub trait NodeTrait {
    fn handle_message(&self, message: Message);
    fn send_message(&self, recipient: NodeId, message: Message);
}

pub enum NodeType {
    Server(Server),
    // Client(Client),
}

pub struct Node {
    id: NodeId,
    node_type: NodeType,
    packet_recv: Receiver<Packet>,
    packet_send: HashMap<NodeId, Sender<Packet>>,
    network_topology: HashMap<NodeId, Vec<NodeId>>,
}

impl Node {
    pub fn new(id: NodeId, node_type: NodeType, packet_recv: Receiver<Packet>) -> Self {
        Node {
            id,
            node_type,
            packet_recv,
            packet_send: Default::default(),
            network_topology: Default::default(),
        }
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn node_type(&self) -> &NodeType {
        &self.node_type
    }

    pub fn run(&mut self) {
        loop {
            select_biased! {
                recv(self.packet_recv) -> packet => {
                    if let Ok(packet) = packet {
                        self.handle_packet(packet);
                    }
                    else {
                        break;
                    }
                }
            }
        }
    }

    pub fn handle_packet(&self, packet: Packet) {}

    pub fn handle_message(&self, message: Message) {
        match &self.node_type {
            NodeType::Server(server) => server.handle_message(message),
        }
    }
}
