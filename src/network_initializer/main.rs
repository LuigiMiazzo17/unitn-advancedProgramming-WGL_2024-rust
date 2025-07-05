use crate::network_initializer::config::Config;
use crate::simulation_controller::SimulationController;
use crate::utils::ClientType;
use crate::utils::ServerType;
use log::debug;
use std::collections::HashSet;
use std::fs;
use wg_2024::network::NodeId;

pub fn parse_config(file: &str) -> anyhow::Result<Config> {
    let file_str = fs::read_to_string(file)?;
    let conf = toml::from_str(&file_str)?;
    debug!("Loaded config: {:?}", conf);
    Ok(conf)
}

fn safe_edge_add(hs: &mut HashSet<(NodeId, NodeId)>, edge: (NodeId, NodeId)) {
    if !hs.contains(&edge) || !hs.contains(&(edge.1, edge.0)) {
        hs.insert(edge);
    } else {
        debug!("Edge {:?} already exists, skipping.", edge);
    }
}

#[allow(clippy::type_complexity)]
pub fn spawn_network(config: Config) -> anyhow::Result<SimulationController> {
    let mut sim_cnt = SimulationController::new();
    let mut edges = HashSet::new();

    for drone_cfg in config.drone.into_iter() {
        sim_cnt.add_drone(Some(drone_cfg.id), None, Some(drone_cfg.pdr))?;
        for connected_id in drone_cfg.connected_node_ids.into_iter() {
            safe_edge_add(&mut edges, (drone_cfg.id, connected_id));
        }
    }

    for server_cfg in config.server.into_iter() {
        let server_type = match server_cfg.server_type.as_str() {
            "Content" => ServerType::Content,
            "Communication" => ServerType::Communication,
            _ => panic!("Unknown server type: {}", server_cfg.server_type),
        };

        sim_cnt.add_server(Some(server_cfg.id), server_type)?;

        for connected_id in server_cfg.connected_drone_ids.into_iter() {
            safe_edge_add(&mut edges, (server_cfg.id, connected_id));
        }
    }

    for client_cfg in config.client.into_iter() {
        let client_type = match client_cfg.client_type.as_str() {
            "ChatClient" => ClientType::Chat,
            "WebBrowser" => ClientType::Web,
            _ => panic!("Unknown client type: {}", client_cfg.client_type),
        };

        sim_cnt.add_client(Some(client_cfg.id), client_type)?;

        for connected_id in client_cfg.connected_drone_ids.into_iter() {
            safe_edge_add(&mut edges, (client_cfg.id, connected_id));
        }
    }

    for edge in edges.into_iter() {
        if let Err(e) = sim_cnt.add_edge(edge.0, edge.1) {
            debug!("Failed to add edge {:?}: {}", edge, e);
            return Err(anyhow::anyhow!("Failed to add edge {:?}: {}", edge, e));
        }
    }

    Ok(sim_cnt)
}
