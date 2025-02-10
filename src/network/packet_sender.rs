use crossbeam::channel::{select_biased, Receiver, Sender};
use log::{debug, error, info, trace, warn};
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use wg_2024::network::{NodeId, SourceRoutingHeader};
use wg_2024::packet::{FloodRequest, FloodResponse, Packet, PacketType};

const PACKET_RESEND_BACK_OFF_TIME: Duration = Duration::from_millis(150);
const PACKET_RESEND_MAX_RETRIES: u32 = 5;
const PACKET_RECV_TIMEOUT: Duration = Duration::from_millis(25);

pub struct PacketSender {
    node_id: NodeId,
    master_recv: Receiver<PacketSenderMessage>,
    packet_send_queue: Vec<PacketQueueItem>,
    ackable_packet_send_queue: Vec<AckablePacketQueueItem>,
    flood_packets_queue: Vec<PacketQueueItem>,
    neighbors: Arc<Mutex<HashMap<NodeId, Sender<Packet>>>>,
    network_discovery_ongoing: Arc<AtomicBool>,
    network_topology: Arc<Mutex<HashMap<NodeId, Vec<NodeId>>>>,
    log_target: String,
}

pub enum PacketSenderMessage {
    Packet((NodeId, u64, PacketType)), // (peer_id, session_id, fragment)
    AckedFragment((NodeId, u64, u64)), // (peer_id, session_id, fragment_index)
    Quit,
}

#[derive(Clone)]
struct PacketQueueItem {
    peer_id: NodeId,
    session_id: u64,
    packet_type: PacketType,
}

struct AckablePacketQueueItem {
    common: PacketQueueItem,
    last_send: Instant,
    retries: u32,
}

impl PacketSender {
    pub fn new(
        node_id: NodeId,
        packet_recv: Receiver<PacketSenderMessage>,
        network_discovery_ongoing: Arc<AtomicBool>,
        network_topology: Arc<Mutex<HashMap<NodeId, Vec<NodeId>>>>,
        neighbors: Arc<Mutex<HashMap<NodeId, Sender<Packet>>>>,
        log_target: String,
    ) -> Self {
        trace!(target: &log_target, "Creating new PacketSender");
        PacketSender {
            node_id,
            master_recv: packet_recv,
            packet_send_queue: Default::default(),
            ackable_packet_send_queue: Default::default(),
            flood_packets_queue: Default::default(),
            neighbors,
            network_discovery_ongoing,
            network_topology,
            log_target,
        }
    }

    pub fn run(&mut self) {
        trace!(target: &self.log_target, "Starting PacketSender");
        loop {
            // select the timeout based on the packet queues
            let mut timeout = Duration::from_secs(0);

            // we don't check the flood packets because they are always consumed
            if self.packet_send_queue.is_empty() && self.ackable_packet_send_queue.is_empty() {
                // here, nothing to do, wait indefinitely for a packet
                timeout = Duration::MAX;
            }
            if self.packet_send_queue.is_empty() {
                // only ackable packets to send, try to get new packets but break after timeout
                timeout = PACKET_RECV_TIMEOUT;
            }

            select_biased! {
                recv(self.master_recv) -> message => {
                    if let Ok(message) = message {
                        match message {
                            PacketSenderMessage::Packet((peer_id, session_id, packet_type)) => {
                                debug!(target: &self.log_target, "Received new packet {:?}", packet_type);
                                self.handle_new_packet(peer_id, session_id, packet_type)
                            },
                            PacketSenderMessage::AckedFragment((peer_id, session_id, fragment_index)) => {
                                trace!(target: &self.log_target, "Received AckedFragment message with fragment index '{}' on session '{}'", fragment_index, session_id);
                                self.retain_acked_fragments(peer_id, session_id, fragment_index)
                            },
                            PacketSenderMessage::Quit => {
                                info!(target: &self.log_target, "Received Quit message, stopping PacketSender");
                                break
                        }

                        }
                    }
                    else {
                        break;
                    }
                }
                default(timeout) => {}
            };

            self.process_queue();
        }
    }

    fn retain_acked_fragments(&mut self, peer_id: NodeId, session_id: u64, fragment_index: u64) {
        self.ackable_packet_send_queue.retain(|queue_item| {
            !(queue_item.common.peer_id == peer_id
                && queue_item.common.session_id == session_id
                && match &queue_item.common.packet_type {
                    PacketType::MsgFragment(f) => f.fragment_index == fragment_index,
                    _ => {
                        error!(target: &self.log_target, "Received AckedFragment message for non-fragment packet");
                        unreachable!()
                    },
                })
        });
    }

    fn handle_new_packet(&mut self, peer_id: NodeId, session_id: u64, packet_type: PacketType) {
        trace!(target: &self.log_target, "Handling new packet {:?}", packet_type);
        // sort possible new packet, if is a fragment, we want to wait for acknolegment
        match packet_type {
            PacketType::MsgFragment(_) => {
                self.ackable_packet_send_queue.push(AckablePacketQueueItem {
                    common: PacketQueueItem {
                        peer_id,
                        session_id,
                        packet_type,
                    },
                    last_send: Instant::now() - PACKET_RESEND_BACK_OFF_TIME,
                    retries: 0,
                });
            }
            PacketType::FloodRequest(_) | PacketType::FloodResponse(_) => {
                self.flood_packets_queue.push(PacketQueueItem {
                    peer_id,
                    session_id,
                    packet_type,
                })
            }
            _ => {
                self.packet_send_queue.push(PacketQueueItem {
                    peer_id,
                    session_id,
                    packet_type,
                });
            }
        };
    }

    fn process_queue(&mut self) {
        trace!(target: &self.log_target, "Processing packet queues, retaining acked packets");
        // drop packets that are not acked after MAX_RETRIES
        self.ackable_packet_send_queue
            .retain(|queue_item| {
                if queue_item.retries < PACKET_RESEND_MAX_RETRIES {
                    true
                } else {
                    warn!(target: &self.log_target, "Packet not acked after '{}' retries, dropping", queue_item.retries);
                    false
                }
            });

        // if network discovery is ongoing, we don't send any packets
        if !self
            .network_discovery_ongoing
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            // consume all non-acked packets, by routing them
            debug!(target: &self.log_target, "Network discovery is not ongoing, sending standard packets");
            trace!(target: &self.log_target, "Sending standard packets");
            let packet_send_queue = std::mem::take(&mut self.packet_send_queue);
            for packet in packet_send_queue.into_iter() {
                self.route_packet(packet);
            }

            // try to send all ackable packets, but only if they are ready
            trace!(target: &self.log_target, "Sending ackable packets");
            let mut ackable_packet_send_queue = std::mem::take(&mut self.ackable_packet_send_queue);
            for packet in ackable_packet_send_queue.iter_mut() {
                if packet.last_send.elapsed() >= PACKET_RESEND_BACK_OFF_TIME {
                    self.route_packet(packet.common.clone());
                    packet.last_send = Instant::now();
                    packet.retries += 1;
                }
            }
            self.ackable_packet_send_queue = ackable_packet_send_queue;
        }

        // consume all flood packets, even if network discovery is ongoing
        trace!(target: &self.log_target, "Sending flood packets");
        let flood_packets_queue = std::mem::take(&mut self.flood_packets_queue);
        for packet in flood_packets_queue.into_iter() {
            match packet.packet_type {
                PacketType::FloodRequest(flood_request) => {
                    self.flood_network_with_request(flood_request, packet.session_id);
                }
                PacketType::FloodResponse(flood_response) => {
                    self.response_to_flood_request(flood_response, packet.session_id);
                }
                _ => {
                    error!(target: &self.log_target, "Flood packet queue contains non-flood packets");
                    unreachable!()
                }
            }
        }
    }

    fn flood_network_with_request(&self, flood_request: FloodRequest, session_id: u64) {
        debug!(target: &self.log_target, "Flood request received, flooding network");
        let packet_send = match self.neighbors.lock() {
            Ok(neighbors) => neighbors,
            Err(e) => {
                error!(target: &self.log_target, "Failed to lock neighbors mutex: {}", e);
                return;
            }
        };

        let packet = Packet {
            routing_header: SourceRoutingHeader {
                hops: vec![],
                hop_index: 0,
            },
            session_id,
            pack_type: PacketType::FloodRequest(flood_request),
        };

        for (k, sender) in packet_send.iter() {
            trace!(target: &self.log_target, "Sending flood request to neighbour '{}", k);
            sender.send(packet.clone()).expect("Failed to send packet");
        }
    }

    fn response_to_flood_request(&self, flood_response: FloodResponse, session_id: u64) {
        trace!(target: &self.log_target, "Responding to flood request");

        let packet_send = match self.neighbors.lock() {
            Ok(neighbors) => neighbors,
            Err(e) => {
                error!(target: &self.log_target, "Failed to lock neighbors mutex: {}", e);
                return;
            }
        };

        let path = flood_response
            .path_trace
            .iter()
            .rev()
            .cloned()
            .map(|(node_id, _)| node_id)
            .collect();

        debug!(target: &self.log_target, "Returning flood response with id '{}' with path '{:?}'", flood_response.flood_id, path);

        let packet = Packet {
            routing_header: SourceRoutingHeader {
                hops: path,
                hop_index: 1,
            },
            session_id,
            pack_type: PacketType::FloodResponse(flood_response),
        };

        let sender = match packet_send.get(&packet.routing_header.hops[1]) {
            Some(sender) => sender,
            None => {
                warn!(target: &self.log_target, "Failed to get sender for flood response");
                return;
            }
        };

        if sender.send(packet).is_err() {
            error!(target: &self.log_target, "Failed to send packet");
        }
    }

    fn route_packet(&self, packet: PacketQueueItem) {
        trace!(target: &self.log_target, "Routing packet {:?}", packet.packet_type);
        let packet_send = match self.neighbors.lock() {
            Ok(neighbors) => neighbors,
            Err(e) => {
                error!(target: &self.log_target, "Failed to lock neighbors mutex: {}", e);
                return;
            }
        };

        let path = match self.get_route_to_peer(packet.peer_id) {
            Some(path) => path,
            None => {
                warn!(target: &self.log_target, "Failed to get route to peer '{}'", packet.peer_id);
                // TODO: Consider triggering a network discovery
                return;
            }
        };

        let packet = Packet {
            routing_header: SourceRoutingHeader {
                hops: path,
                hop_index: 1,
            },
            session_id: packet.session_id,
            pack_type: packet.packet_type,
        };

        let sender = match packet_send.get(&packet.routing_header.hops[1]) {
            Some(sender) => sender,
            None => {
                error!(target: &self.log_target, "Failed to get sender for packet, destination was '{}' but path was '{:?}'", packet.routing_header.hops[1], packet.routing_header.hops);
                return;
            }
        };
        if sender.send(packet).is_err() {
            error!(target: &self.log_target, "Failed to send packet");
        }
    }

    fn get_route_to_peer(&self, peer_id: NodeId) -> Option<Vec<NodeId>> {
        trace!(target: &self.log_target, "Getting route to peer '{}'", peer_id);
        let net_topology = match self.network_topology.lock() {
            Ok(topology) => topology,
            Err(e) => {
                error!(target: &self.log_target, "Failed to lock network topology mutex: {}", e);
                return None;
            }
        };

        let mut visited = vec![self.node_id];
        let mut queue = vec![vec![self.node_id]];

        while !queue.is_empty() {
            let path = queue.remove(0);
            let last_node = *path.last().unwrap();

            if last_node == peer_id {
                trace!(target: &self.log_target, "Found route to peer '{}': {:?}", peer_id, path);
                return Some(path);
            }

            if let Some(neighbours) = net_topology.get(&last_node) {
                for neighbour in neighbours.iter() {
                    if !visited.contains(neighbour) {
                        let mut new_path = path.clone();
                        new_path.push(*neighbour);
                        visited.push(*neighbour);
                        queue.push(new_path);
                    }
                }
            }
        }

        warn!(target: &self.log_target, "Failed to find route to peer '{}'", peer_id);
        None
    }
}
