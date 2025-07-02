use crossbeam::channel::{select_biased, unbounded, Receiver, Sender};
use log::{debug, error, trace};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use wg_2024::network::NodeId;
use wg_2024::packet::{
    Ack, FloodRequest, FloodResponse, Fragment, NackType, NodeType, Packet, PacketType,
};

use crate::network::message::{Message, Response};
use crate::network::message_constructor::MessageConstructor;
use crate::network::network_discovery_protocol::NetDiscovery;
use crate::network::packet_sender::{
    PacketSender, PacketSenderControlMessage, PacketSenderDutyMessage,
};
use crate::server::{CommunicationServer, ContentServer};

const MAX_WAIT_FLOOD_RESPONSE: Duration = Duration::from_millis(50);

pub trait NodeTrait {
    fn handle_message(&mut self, peer_id: NodeId, message: Message) -> Option<Response>;
    fn stop(&mut self);
    fn get_node_type(&self) -> NodeType;
    fn get_node_type_str(&self) -> &str;
    fn handle_control_message(
        &mut self,
        message: SimControllerMessage,
    ) -> Option<(NodeId, Option<u64>, Message)>;
}

pub enum SimControllerMessage {
    SendMessageToPeer(NodeId, Message),
}

pub enum NodeCommand {
    Quit,
    AddNeighbour((NodeId, Sender<Packet>)),
    RemoveNeighbour(NodeId),
    SendMessage(SimControllerMessage),
}

pub struct Node {
    id: NodeId,
    inner_node: Box<dyn NodeTrait>,
    packet_recv: Receiver<Packet>,
    command_recv: Receiver<NodeCommand>,
    neighbors: Arc<Mutex<HashMap<NodeId, Sender<Packet>>>>,
    network_topology: Arc<Mutex<HashMap<NodeId, Vec<NodeId>>>>,
    fragment_constructors: HashMap<(NodeId, u64), MessageConstructor>,
    packet_send: Option<Sender<PacketSenderDutyMessage>>,
    net_d: NetDiscovery,
    log_target: String,
}

impl Node {
    fn new(
        id: NodeId,
        inner_node: Box<dyn NodeTrait>,
        command_recv: Receiver<NodeCommand>,
        packet_recv: Receiver<Packet>,
    ) -> Self {
        let log_target = format!("{}-{}", inner_node.get_node_type_str(), id);
        Node {
            id,
            inner_node,
            packet_recv,
            command_recv,
            neighbors: Default::default(),
            network_topology: Default::default(),
            fragment_constructors: Default::default(),
            packet_send: None,
            net_d: NetDiscovery::new(),
            log_target,
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
        trace!(target: &self.log_target, "Starting node");
        // initialize packet sender
        let (fragment_send, fragment_recv) = unbounded();
        let (packet_sender_send, packet_sender_recv) = unbounded();
        self.packet_send = Some(fragment_send);

        // spawn packet sender thread
        let send_queue_t = match thread::Builder::new()
            .name(format!("node-packet-sender-{}", self.id))
            .spawn({
                let node_id = self.id;
                let network_discovery_ongoing = self.net_d.get_ongoing_ref();
                let network_topology = self.network_topology.clone();
                let neighbors = self.neighbors.clone();
                let log_target = format!("{}-packet-sender", &self.log_target);
                move || {
                    let mut fragment_sender = PacketSender::new(
                        node_id,
                        fragment_recv,
                        packet_sender_send,
                        network_discovery_ongoing,
                        network_topology,
                        neighbors,
                        log_target,
                    );
                    fragment_sender.run();
                }
            }) {
            Ok(t) => t,
            Err(e) => {
                error!(target: &self.log_target, "Failed to spawn packet sender thread: {}", e);
                return;
            }
        };

        // Trigger Network Discovery to initialize the network topology
        self.trigger_network_discovery();

        loop {
            if send_queue_t.is_finished() {
                error!(target: &self.log_target, "Packet sender thread has finished, while node is still running");
                break;
            }

            let mut timeout = Duration::MAX;

            // Check if we should stop network discovery, or we shoud wait for more responses, by
            // setting the timeout to the remaining time to wait for responses
            if self.net_d.get_ongoing() {
                if self.net_d.get_elapsed() > MAX_WAIT_FLOOD_RESPONSE {
                    // Network discovery has finished, update network topology
                    self.net_d.set_ongoing(false);

                    *self
                        .network_topology
                        .lock()
                        .expect("Failed to lock network topology") = self.net_d.parse_network();
                } else {
                    // Wait for more responses
                    timeout = MAX_WAIT_FLOOD_RESPONSE
                        .checked_sub(self.net_d.get_elapsed())
                        .unwrap_or(Duration::from_secs(0));
                }
            }

            select_biased! {
                recv(self.command_recv) -> command => {
                    match command {
                        Ok(message) => match message {
                            NodeCommand::Quit => {
                                self.inner_node.stop();
                                break;
                            },
                            NodeCommand::AddNeighbour((neighbour_id, sender)) => {
                                self.neighbors.lock().expect("Failed to lock neighbours map").insert(neighbour_id, sender);
                                self.trigger_network_discovery();
                            },
                            NodeCommand::RemoveNeighbour(neighbour_id) => {
                                self.neighbors.lock().expect("Failed to lock packet send").remove(&neighbour_id);
                                self.trigger_network_discovery();
                            },
                            NodeCommand::SendMessage(message) => {
                                if let Some((peer_id, session_id, message)) = self.inner_node.handle_control_message(message) {
                                    self.send_data_message(peer_id, session_id, message);
                                }
                            },
                        }
                        Err(_) => break,
                    }
                }
                recv(packet_sender_recv) -> packet_sender_message => {
                    match packet_sender_message {
                        Ok(message) => match message {
                            PacketSenderControlMessage::TriggerNetworkDiscovery => {
                                self.trigger_network_discovery();
                            }
                        }
                        Err(_) => {
                            error!(target: &self.log_target, "Packet sender channel closed, stopping node");
                            break;
                        }
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

        self.packet_send
            .as_ref()
            .unwrap()
            .send(PacketSenderDutyMessage::Quit)
            .unwrap();
        self.packet_send = None;

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
                self.packet_send
                    .as_ref()
                    .unwrap()
                    .send(PacketSenderDutyMessage::AckedFragment((
                        packet.routing_header.hops[0],
                        packet.session_id,
                        ack.fragment_index,
                    )))
                    .expect("Failed to send acked fragment to fragment sender");
            }
            PacketType::FloodRequest(flood_request) => {
                self.handle_flood_request(flood_request, packet.session_id);
            }
            PacketType::FloodResponse(flood_responses) => {
                if flood_responses.flood_id == self.net_d.get_id() {
                    self.net_d.add_response(flood_responses);
                }
            }
            PacketType::Nack(nack) => match nack.nack_type {
                NackType::UnexpectedRecipient(_)
                | NackType::DestinationIsDrone
                | NackType::ErrorInRouting(_) => {
                    self.trigger_network_discovery();
                }
                _ => {}
            },
        }
    }

    fn handle_flood_request(&self, flood_request: FloodRequest, session_id: u64) {
        // Handle Flood Request
        let mut path_trace = flood_request.path_trace;

        path_trace.push((self.id, self.inner_node.get_node_type()));

        let flood_response = FloodResponse {
            flood_id: flood_request.flood_id,
            path_trace: path_trace.clone(),
        };

        self.packet_send
            .as_ref()
            .unwrap()
            .send(PacketSenderDutyMessage::Packet((
                path_trace.last().unwrap().0,
                session_id,
                PacketType::FloodResponse(flood_response),
            )))
            .expect("Failed to send flood request");
    }

    fn trigger_network_discovery(&mut self) {
        debug!(target: &self.log_target, "Triggering network discovery");
        self.net_d.init();

        // Trigger Flood Request
        let flood_request = FloodRequest::initialize(
            self.net_d.get_id(),
            self.id,
            self.inner_node.get_node_type(),
        );

        self.packet_send
            .as_ref()
            .unwrap()
            .send(PacketSenderDutyMessage::Packet((
                self.id,
                rand::random(),
                PacketType::FloodRequest(flood_request),
            )))
            .expect("Failed to send flood request");
    }

    fn handle_fragment(&mut self, peer_id: NodeId, session_id: u64, msg_fragment: Fragment) {
        trace!(target: &self.log_target, "Received fragment with index {}", msg_fragment.fragment_index);
        // Handle recived fragment
        let fragment_index = msg_fragment.fragment_index;

        let constructor = self
            .fragment_constructors
            .entry((peer_id, session_id))
            .or_insert(MessageConstructor::new(
                format!("{}-msg-constructor", self.log_target,),
                msg_fragment.total_n_fragments,
            ));

        // Add fragment to constructor
        if let Ok(optional_buffer) = constructor.add_packet(msg_fragment) {
            trace!(target: &self.log_target, "Fragment added to constructor");
            // Return Ack for received fragment
            if self
                .packet_send
                .as_ref()
                .unwrap()
                .send(PacketSenderDutyMessage::Packet((
                    peer_id,
                    session_id,
                    PacketType::Ack(Ack { fragment_index }),
                )))
                .is_err()
            {
                error!(target: &self.log_target, "Failed to send ack for fragment");
            }

            // If message is complete, handle it
            if let Some(buffer) = optional_buffer {
                debug!(target: &self.log_target, "Message with session_id '{}' complete", session_id);
                self.fragment_constructors.remove(&(peer_id, session_id));
                self.handle_data_message(
                    peer_id,
                    session_id,
                    bincode::deserialize(&buffer).expect("Failed to deserialize message"),
                );
            }
        } else {
            // I'd like to handle this error by returning a Nack, but the current implementation
            // of the protocol doesn't allow for it (no Nack packet type defined for this job).
            error!(target: &self.log_target, "Failed to add fragment to constructor, dropping message");
            self.fragment_constructors.remove(&(peer_id, session_id));
        }
    }

    fn handle_data_message(&mut self, peer_id: NodeId, session_id: u64, message: Message) {
        debug!(target: &self.log_target, "Handling message with session_id '{}'", session_id);
        // Handle High-level message
        if let Some(response) = self.inner_node.handle_message(peer_id, message) {
            debug!(target: &self.log_target, "Message generated response, forwarding message with session_id '{}'", session_id);
            // if a request triggers a response, we will sand it back, keeping the session id
            self.send_data_message(peer_id, Some(session_id), Message::Response(response));
        }
    }

    pub fn send_data_message(
        &mut self,
        peer_id: NodeId,
        session_id: Option<u64>,
        message: Message,
    ) {
        let session_id = session_id.unwrap_or(rand::random());
        trace!(target: &self.log_target, "Sending message to peer '{}' with session id '{}'", peer_id, session_id);

        let data = match bincode::serialize(&message) {
            Ok(data) => data,
            Err(e) => {
                error!(target: &self.log_target, "Failed to serialize message: {}", e);
                return;
            }
        };

        for (i, chunk) in data.chunks(wg_2024::packet::FRAGMENT_DSIZE).enumerate() {
            let mut buff = [0u8; wg_2024::packet::FRAGMENT_DSIZE];
            buff[..chunk.len()].copy_from_slice(chunk);
            let fragment = Fragment {
                fragment_index: i as u64,
                total_n_fragments: (data.len() as f64 / wg_2024::packet::FRAGMENT_DSIZE as f64)
                    .ceil() as u64,
                data: buff,
                length: chunk.len() as u8,
            };

            if self
                .packet_send
                .as_ref()
                .unwrap()
                .send(PacketSenderDutyMessage::Packet((
                    peer_id,
                    session_id,
                    PacketType::MsgFragment(fragment),
                )))
                .is_err()
            {
                error!(target: &self.log_target, "Failed to send fragment");
                return;
            }
        }
    }
}
