use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use wg_2024::network::NodeId;
use wg_2024::packet::FloodResponse;

pub struct NetDiscovery {
    last_id: u64,
    ongoing: Arc<AtomicBool>,
    responses: Vec<FloodResponse>,
    start_time: Instant,
}

impl NetDiscovery {
    pub fn new() -> Self {
        NetDiscovery {
            last_id: 0,
            ongoing: Arc::new(AtomicBool::new(false)),
            responses: Default::default(),
            start_time: Instant::now(),
        }
    }

    pub fn parse_network(&mut self) -> HashMap<NodeId, Vec<NodeId>> {
        fn insert_hop(
            network_config: &mut HashMap<NodeId, Vec<NodeId>>,
            node: NodeId,
            hop: NodeId,
        ) {
            if let Some(hops) = network_config.get_mut(&node) {
                if !hops.contains(&hop) {
                    hops.push(hop);
                }
            } else {
                network_config.insert(node, vec![hop]);
            }
        }

        let mut received_network_config = HashMap::new();

        for flood_response in &self.responses {
            for (i, (hop, _)) in flood_response.path_trace.clone().into_iter().enumerate() {
                if i != flood_response.path_trace.len() - 1 {
                    if let Some(next_hop) = flood_response.path_trace.get(i + 1) {
                        insert_hop(&mut received_network_config, hop, next_hop.0);
                    }
                }

                if i != 0 {
                    if let Some(prev_hop) = flood_response.path_trace.get(i - 1) {
                        insert_hop(&mut received_network_config, hop, prev_hop.0);
                    }
                }
            }
        }

        self.responses.clear();

        received_network_config
    }

    pub fn add_response(&mut self, response: FloodResponse) {
        self.responses.push(response);
    }

    pub fn get_id(&self) -> u64 {
        self.last_id
    }

    pub fn get_ongoing_ref(&self) -> Arc<AtomicBool> {
        self.ongoing.clone()
    }

    pub fn get_ongoing(&self) -> bool {
        self.ongoing.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn set_ongoing(&self, value: bool) {
        self.ongoing
            .store(value, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn get_elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    pub fn init(&mut self) {
        self.last_id = rand::random();
        self.set_ongoing(true);
        self.start_time = Instant::now();
        self.responses.clear();
    }
}
