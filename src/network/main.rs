use crossbeam::channel::{select_biased, Receiver, Sender};
use std::collections::HashMap;

use wg_2024::network::NodeId;
use wg_2024::packet::{Fragment, Packet, PacketType};

use crate::network::message::Message;
use crate::server::Server;

pub trait NodeTrait {
    fn handle_message(&self, peer_id: NodeId, message: Message) -> Option<Message>;
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
    fragment_store: HashMap<(NodeId, u64), Vec<u8>>,
}

impl Node {
    pub fn new(id: NodeId, node_type: NodeType, packet_recv: Receiver<Packet>) -> Self {
        Node {
            id,
            node_type,
            packet_recv,
            packet_send: Default::default(),
            network_topology: Default::default(),
            fragment_store: Default::default(),
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

    pub fn handle_packet(&mut self, packet: Packet) {
        match packet.pack_type {
            PacketType::MsgFragment(frgmt) => {
                self.handle_fragment(packet.routing_header.hops[0], packet.session_id, frgmt)
            }
            _ => todo!(),
        }
    }

    fn handle_fragment(&mut self, peer_id: NodeId, session_id: u64, msg_fragment: Fragment) {
        // TODO: Implement go back N or selective repeat

        let fragment = self
            .fragment_store
            .entry((peer_id, session_id))
            .or_default();

        fragment.extend_from_slice(&msg_fragment.data);

        if msg_fragment.fragment_index == msg_fragment.total_n_fragments - 1 {
            // TODO : Handle error here
            let message: Message = bincode::deserialize(
                &self
                    .fragment_store
                    .remove(&(peer_id, session_id))
                    .expect("Fragment not found"),
            )
            .expect("Failed to deserialize message");

            self.handle_message(peer_id, message);
        }
    }

    pub fn handle_message(&self, peer_id: NodeId, message: Message) {
        self.send_message(
            peer_id,
            match &self.node_type {
                NodeType::Server(server) => server.handle_message(peer_id, message),
            },
        )
    }

    fn send_message(&self, peer_id: NodeId, message: Option<Message>) {
        if let Some(message) = message {
            let data = bincode::serialize(&message).expect("Failed to serialize message");

            let fragment_size = wg_2024::packet::FRAGMENT_DSIZE;

            for (i, chunk) in data
                .chunks_exact(fragment_size)
                .map(|chunk| chunk.try_into().expect("Unable to convert chunk"))
                .enumerate()
            {
                let fragment = Fragment {
                    fragment_index: i as u64,
                    total_n_fragments: (data.len() as f64 / fragment_size as f64).ceil() as u64,
                    data: chunk,
                    length: chunk.len() as u8,
                };

                let packet = Packet::new_fragment(
                    wg_2024::network::SourceRoutingHeader {
                        hops: vec![self.id, peer_id], // TODO: Implement routing
                        hop_index: 1,
                    },
                    i as u64,
                    fragment,
                );

                self.packet_send
                    .get(&peer_id)
                    .expect("Peer not found")
                    .send(packet)
                    .expect("Failed to send packet");
            }
        }
    }
}
