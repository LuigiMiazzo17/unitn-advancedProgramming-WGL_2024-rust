use crossbeam::channel::{select_biased, unbounded, Receiver, Sender};
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use wg_2024::network::NodeId;
use wg_2024::packet::{
    Ack, FloodRequest, FloodResponse, Fragment, NodeType as FloodNodeType, Packet, PacketType,
};

use crate::network::message::Message;
use crate::network::message_constructor::MessageConstructor;
use crate::network::network_discovery_protocol::parse_network_from_flood_responses;
use crate::network::packet_sender::PacketSender;
use crate::server::{CommunicationServer, ContentServer};

const MAX_WAIT_FLOOD_RESPONSE: Duration = Duration::from_millis(50);

pub trait NodeTrait {
    fn handle_message(&mut self, peer_id: NodeId, message: Message) -> Option<Message>;
    fn stop(&mut self);
    fn get_node_type(&self) -> FloodNodeType;
}

#[derive(Debug)]
pub enum NodeCommand {
    Quit,
    AddNeighboor((NodeId, Sender<Packet>)),
    RemoveNeighboor(NodeId),
    SendMessage((NodeId, Message)),
}

pub struct Node {
    id: NodeId,
    node_type: Box<dyn NodeTrait>,
    packet_recv: Receiver<Packet>,
    command_recv: Receiver<NodeCommand>,
    neighbors: Arc<Mutex<HashMap<NodeId, Sender<Packet>>>>,
    network_topology: Arc<Mutex<HashMap<NodeId, Vec<NodeId>>>>,
    fragment_constructor_store: HashMap<(NodeId, u64), MessageConstructor>,
    packet_send: Option<Sender<(NodeId, u64, PacketType)>>,
    acked_packets_send: Option<Sender<(NodeId, u64, u64)>>,
    network_discovery_last_id: u64,
    network_discovery_ongoing: Arc<AtomicBool>,
    network_discovery_responses: Vec<FloodResponse>,
    network_discovery_start_time: Instant,
}

impl Node {
    fn new(
        id: NodeId,
        node_type: Box<dyn NodeTrait>,
        command_recv: Receiver<NodeCommand>,
        packet_recv: Receiver<Packet>,
    ) -> Self {
        Node {
            id,
            node_type,
            packet_recv,
            command_recv,
            neighbors: Default::default(),
            network_topology: Default::default(),
            fragment_constructor_store: Default::default(),
            packet_send: None,
            acked_packets_send: None,
            network_discovery_ongoing: Arc::new(AtomicBool::new(false)),
            network_discovery_start_time: Instant::now(),
            network_discovery_responses: Default::default(),
            network_discovery_last_id: 0,
        }
    }

    pub fn new_content_server(
        id: NodeId,
        command_recv: Receiver<NodeCommand>,
        packet_recv: Receiver<Packet>,
        base_path: String,
    ) -> Self {
        Self::new(
            id,
            Box::new(ContentServer::new(id, base_path)),
            command_recv,
            packet_recv,
        )
    }

    pub fn new_communication_server(
        id: NodeId,
        command_recv: Receiver<NodeCommand>,
        packet_recv: Receiver<Packet>,
        base_path: String,
    ) -> Self {
        Self::new(
            id,
            Box::new(CommunicationServer::new(id, base_path)),
            command_recv,
            packet_recv,
        )
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn run(&mut self) {
        // initialize packet sender
        let (fragment_send, fragment_recv) = unbounded();
        self.packet_send = Some(fragment_send);
        let (acked_fragments_send, acked_fragments_recv) = unbounded();
        self.acked_packets_send = Some(acked_fragments_send);

        // spawn packet sender thread
        let send_queue_t = thread::Builder::new()
            .name(format!("node-packet-sender-{}", self.id))
            .spawn({
                let node_id = self.id;
                let network_discovery_ongoing = self.network_discovery_ongoing.clone();
                let network_topology = self.network_topology.clone();
                let neighbors = self.neighbors.clone();
                move || {
                    let mut fragment_sender = PacketSender::new(
                        node_id,
                        fragment_recv,
                        acked_fragments_recv,
                        network_discovery_ongoing,
                        network_topology,
                        neighbors,
                    );
                    fragment_sender.run();
                }
            })
            .expect("Failed to spawn node fragment sender thread");

        // Trigger Network Discovery to initialize the network topology
        self.trigger_network_discovery();

        loop {
            let mut timeout = Duration::MAX;

            // Check if we should stop network discovery, or we shoud wait for more responses, by
            // setting the timeout to the remaining time to wait for responses
            if self
                .network_discovery_ongoing
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                if self.network_discovery_start_time.elapsed() > MAX_WAIT_FLOOD_RESPONSE {
                    // Network discovery has finished, update network topology
                    self.network_discovery_ongoing
                        .store(false, std::sync::atomic::Ordering::Relaxed);

                    *self
                        .network_topology
                        .lock()
                        .expect("Failed to lock network topology") =
                        parse_network_from_flood_responses(&self.network_discovery_responses);

                    self.network_discovery_responses.clear();
                } else {
                    // Wait for more responses
                    timeout = MAX_WAIT_FLOOD_RESPONSE
                        .checked_sub(self.network_discovery_start_time.elapsed())
                        .unwrap_or(Duration::from_secs(0));
                }
            }

            select_biased! {
                recv(self.command_recv) -> command => {
                    match command {
                        Ok(message) => match message {
                            NodeCommand::Quit => {
                                self.node_type.stop();
                                break;
                    },
                            NodeCommand::AddNeighboor((neighboor_id, sender)) => {
                                self.neighbors.lock().expect("Failed to lock neighboors map").insert(neighboor_id, sender);
                                self.trigger_network_discovery();
                            },
                            NodeCommand::RemoveNeighboor(neighboor_id) => {
                                self.neighbors.lock().expect("Failed to lock packet send").remove(&neighboor_id);
                                self.trigger_network_discovery();
                            },
                            NodeCommand::SendMessage((node_id, message)) => {
                                self.send_data_message(node_id, message);
                            },
                        }
                        Err(_) => break,
                    }
                }
                recv(self.packet_recv) -> packet => {
                    if let Ok(packet) = packet {
                        self.handle_packet(packet);
                    }
                    else {
                        break;
                    }
                }
                default(timeout) => {}
            }
        }

        send_queue_t
            .join()
            .expect("Failed to join fragment sender thread");
    }

    fn handle_packet(&mut self, packet: Packet) {
        match packet.pack_type {
            PacketType::MsgFragment(frgmt) => {
                self.handle_fragment(packet.routing_header.hops[0], packet.session_id, frgmt)
            }
            PacketType::Ack(ack) => {
                self.acked_packets_send
                    .as_ref()
                    .unwrap()
                    .send((
                        packet.routing_header.hops[0],
                        packet.session_id,
                        ack.fragment_index,
                    ))
                    .expect("Failed to send acked fragment to fragment sender");
            }
            PacketType::FloodRequest(flood_request) => {
                self.handle_flood_request(flood_request, packet.session_id);
            }
            PacketType::FloodResponse(flood_responses) => {
                if flood_responses.flood_id == self.network_discovery_last_id {
                    self.network_discovery_responses.push(flood_responses);
                }
            }
            _ => todo!(),
        }
    }

    fn handle_flood_request(&self, flood_request: FloodRequest, session_id: u64) {
        // Handle Flood Request
        let mut path_trace = flood_request.path_trace;

        path_trace.push((self.id, self.node_type.get_node_type()));

        let flood_response = FloodResponse {
            flood_id: flood_request.flood_id,
            path_trace: path_trace.clone(),
        };

        self.packet_send
            .as_ref()
            .unwrap()
            .send((
                path_trace.last().unwrap().0,
                session_id,
                PacketType::FloodResponse(flood_response),
            ))
            .expect("Failed to send flood request");
    }

    fn trigger_network_discovery(&mut self) {
        self.network_discovery_responses.clear();
        self.network_discovery_last_id = rand::random();

        // Trigger Flood Request
        let flood_request = FloodRequest::initialize(
            self.network_discovery_last_id,
            self.id,
            self.node_type.get_node_type(),
        );

        self.packet_send
            .as_ref()
            .unwrap()
            .send((
                self.id,
                rand::random(),
                PacketType::FloodRequest(flood_request),
            ))
            .expect("Failed to send flood request");

        self.network_discovery_ongoing
            .store(true, std::sync::atomic::Ordering::Relaxed);

        self.network_discovery_start_time = Instant::now();
    }

    fn handle_fragment(&mut self, peer_id: NodeId, session_id: u64, msg_fragment: Fragment) {
        // Handle recived fragment
        let fragment_index = msg_fragment.fragment_index;

        let constructor = self
            .fragment_constructor_store
            .entry((peer_id, session_id))
            .or_insert(MessageConstructor::new(msg_fragment.total_n_fragments));

        // Add fragment to constructor
        if let Ok(optional_buffer) = constructor.add_packet(msg_fragment) {
            // Return Ack for received fragment
            self.packet_send
                .as_ref()
                .unwrap()
                .send((peer_id, session_id, PacketType::Ack(Ack { fragment_index })))
                .expect("Failed to send ack");

            // If message is complete, handle it
            if let Some(buffer) = optional_buffer {
                self.fragment_constructor_store
                    .remove(&(peer_id, session_id));
                self.handle_data_message(
                    peer_id,
                    bincode::deserialize(&buffer).expect("Failed to deserialize message"),
                );
            }
        } else {
            // I'd like to handle this error by returning a Nack, but the current implementation
            // of the protocol doesn't allow for it (no Nack packet type defined for this job).
            self.fragment_constructor_store
                .remove(&(peer_id, session_id));
        }
    }

    fn handle_data_message(&mut self, peer_id: NodeId, message: Message) {
        // Handle High-level message
        if let Some(response) = self.node_type.handle_message(peer_id, message) {
            self.send_data_message(peer_id, response);
        }
    }

    fn send_data_message(&mut self, peer_id: NodeId, message: Message) {
        let data = bincode::serialize(&message).expect("Failed to serialize message");

        let fragment_size = wg_2024::packet::FRAGMENT_DSIZE;
        let session_id: u64 = rand::random();

        for (i, chunk) in data.chunks(fragment_size).enumerate() {
            let mut buff = [0u8; wg_2024::packet::FRAGMENT_DSIZE];
            buff[..chunk.len()].copy_from_slice(chunk);
            let fragment = Fragment {
                fragment_index: i as u64,
                total_n_fragments: (data.len() as f64 / fragment_size as f64).ceil() as u64,
                data: buff,
                length: chunk.len() as u8,
            };

            self.packet_send
                .as_ref()
                .unwrap()
                .send((peer_id, session_id, PacketType::MsgFragment(fragment)))
                .expect("Failed to send fragment");
        }
    }
}
