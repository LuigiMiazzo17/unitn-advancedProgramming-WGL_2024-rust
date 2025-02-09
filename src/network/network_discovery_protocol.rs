use std::collections::HashMap;

use wg_2024::network::NodeId;
use wg_2024::packet::FloodResponse;

pub fn parse_network_from_flood_responses(
    flood_responses: &Vec<FloodResponse>,
) -> HashMap<NodeId, Vec<NodeId>> {
    fn insert_hop(network_config: &mut HashMap<NodeId, Vec<NodeId>>, node: NodeId, hop: NodeId) {
        if let Some(hops) = network_config.get_mut(&node) {
            if !hops.contains(&hop) {
                hops.push(hop);
            }
        } else {
            network_config.insert(node, vec![hop]);
        }
    }

    let mut received_network_config = HashMap::new();

    for flood_response in flood_responses {
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

    received_network_config
}
