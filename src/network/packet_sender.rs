use crossbeam::channel::{Receiver, RecvTimeoutError, Sender};
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use wg_2024::network::{NodeId, SourceRoutingHeader};
use wg_2024::packet::{Packet, PacketType};

const PACKET_RESEND_BACK_OFF_TIME: Duration = Duration::from_millis(150);
const PACKET_RESEND_MAX_RETRIES: u32 = 5;
const PACKET_RECV_TIMEOUT: Duration = Duration::from_millis(25);

pub struct PacketSender {
    node_id: NodeId,
    packet_recv: Receiver<(NodeId, u64, PacketType)>, // (peer_id, session_id, fragment)
    packet_send_queue: Vec<PacketQueueItem>,
    ackable_packet_send_queue: Vec<AckablePacketQueueItem>,
    flood_packets_queue: Vec<PacketQueueItem>,
    neighbors: Arc<Mutex<HashMap<NodeId, Sender<Packet>>>>,
    acked_fragments_recv: Receiver<(NodeId, u64, u64)>, // (peer_id, session_id, fragment_index)
    network_discovery_ongoing: Arc<AtomicBool>,
    network_topology: Arc<Mutex<HashMap<NodeId, Vec<NodeId>>>>,
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
        packet_recv: Receiver<(NodeId, u64, PacketType)>,
        acked_fragments_recv: Receiver<(NodeId, u64, u64)>,
        network_discovery_ongoing: Arc<AtomicBool>,
        network_topology: Arc<Mutex<HashMap<NodeId, Vec<NodeId>>>>,
        neighbors: Arc<Mutex<HashMap<NodeId, Sender<Packet>>>>,
    ) -> Self {
        PacketSender {
            node_id,
            packet_recv,
            packet_send_queue: Default::default(),
            ackable_packet_send_queue: Default::default(),
            flood_packets_queue: Default::default(),
            neighbors,
            acked_fragments_recv,
            network_discovery_ongoing,
            network_topology,
        }
    }

    pub fn run(&mut self) {
        loop {
            // if queue is empty, we can just wait till new packet comes in
            let optional_new_packet = if self.packet_send_queue.is_empty() {
                if let Ok((peer_id, session_id, packet_type)) = self.packet_recv.recv() {
                    Some((peer_id, session_id, packet_type))
                } else {
                    // if we can't receive packet, we break the loop
                    break;
                }
            } else {
                match self.packet_recv.recv_timeout(PACKET_RECV_TIMEOUT) {
                    Ok((peer_id, session_id, packet_type)) => {
                        Some((peer_id, session_id, packet_type))
                    }
                    Err(err) => match err {
                        RecvTimeoutError::Disconnected => {
                            // if we can't receive packet, we break the loop
                            break;
                        }
                        RecvTimeoutError::Timeout => None,
                    },
                }
            };

            // sort possible new packet, if is a fragment, we want to wait for acknolegment
            if let Some((peer_id, session_id, packet_type)) = optional_new_packet {
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
            };

            self.process_packets();
        }
    }

    fn process_packets(&mut self) {
        // retain acked fragments
        // !(peer_id == peer_id and sess_id == sess_id and f_index == f_index)
        while let Ok((peer_id, session_id, fragment_index)) = self.acked_fragments_recv.try_recv() {
            self.ackable_packet_send_queue.retain(|queue_item| {
                !(queue_item.common.peer_id == peer_id
                    && queue_item.common.session_id == session_id
                    && match &queue_item.common.packet_type {
                        PacketType::MsgFragment(f) => f.fragment_index == fragment_index,
                        _ => unreachable!(),
                    })
            });
        }

        // drop packets that are not acked after MAX_RETRIES
        self.ackable_packet_send_queue
            .retain(|queue_item| queue_item.retries < PACKET_RESEND_MAX_RETRIES);

        // if network discovery is ongoing, we don't send any packets
        if !self
            .network_discovery_ongoing
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            // consume all non-acked packets, by routing them
            let packet_send_queue = std::mem::take(&mut self.packet_send_queue);
            for packet in packet_send_queue.into_iter() {
                self.route_packet(packet);
            }

            // try to send all ackable packets, but only if they are ready
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
        let flood_packets_queue = std::mem::take(&mut self.flood_packets_queue);
        for packet in flood_packets_queue.into_iter() {
            match packet.packet_type {
                PacketType::FloodRequest(flood_request) => {
                    self.flood_network_with_request(flood_request, packet.session_id);
                }
                PacketType::FloodResponse(flood_response) => {
                    self.response_to_flood_request(flood_response, packet.session_id);
                }
                _ => unreachable!(),
            }
        }
    }

    fn flood_network_with_request(
        &self,
        flood_request: wg_2024::packet::FloodRequest,
        session_id: u64,
    ) {
        let packet_send = self.neighbors.lock().unwrap();
        let packet = Packet {
            routing_header: SourceRoutingHeader {
                hops: vec![],
                hop_index: 0,
            },
            session_id,
            pack_type: PacketType::FloodRequest(flood_request),
        };
        for sender in packet_send.values() {
            sender.send(packet.clone()).expect("Failed to send packet");
        }
    }

    fn response_to_flood_request(
        &self,
        flood_response: wg_2024::packet::FloodResponse,
        session_id: u64,
    ) {
        let packet_send = self.neighbors.lock().unwrap();
        let path = flood_response
            .path_trace
            .iter()
            .rev()
            .cloned()
            .map(|(node_id, _)| node_id)
            .collect();

        let packet = Packet {
            routing_header: SourceRoutingHeader {
                hops: path,
                hop_index: 1,
            },
            session_id,
            pack_type: PacketType::FloodResponse(flood_response),
        };
        let sender = packet_send
            .get(&packet.routing_header.hops[1])
            .expect("No sender found");
        sender.send(packet).expect("Failed to send packet");
    }

    fn route_packet(&self, packet: PacketQueueItem) {
        let packet_send = self.neighbors.lock().unwrap();
        let path = self
            .get_route_to_peer(packet.peer_id)
            .expect("No route found");
        let packet = Packet {
            routing_header: SourceRoutingHeader {
                hops: path,
                hop_index: 1,
            },
            session_id: packet.session_id,
            pack_type: packet.packet_type,
        };
        let sender = packet_send
            .get(&packet.routing_header.hops[1])
            .expect("No sender found");
        sender.send(packet).expect("Failed to send packet");
    }

    fn get_route_to_peer(&self, peer_id: NodeId) -> Option<Vec<NodeId>> {
        let net_topology = self.network_topology.lock().unwrap();

        let mut visited = vec![self.node_id];
        let mut queue = vec![vec![self.node_id]];

        while !queue.is_empty() {
            let path = queue.remove(0);
            let last_node = *path.last().unwrap();

            if last_node == peer_id {
                return Some(path);
            }

            if let Some(neighboors) = net_topology.get(&last_node) {
                for neighboor in neighboors.iter() {
                    if !visited.contains(neighboor) {
                        let mut new_path = path.clone();
                        new_path.push(*neighboor);
                        visited.push(*neighboor);
                        queue.push(new_path);
                    }
                }
            }
        }

        None
    }
}
