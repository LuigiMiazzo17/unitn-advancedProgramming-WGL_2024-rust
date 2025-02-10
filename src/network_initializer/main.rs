use crossbeam::channel::{unbounded, Receiver, Sender};
use log::debug;
use std::collections::HashMap;
use std::fs;
use std::thread;

use crate::network::Node;
use crate::network::NodeCommand;
use crate::network_initializer::config::Config;

use wg_2024_rust::drone::RustDrone;

use wg_2024::controller::{DroneCommand, DroneEvent};
use wg_2024::drone::Drone;
use wg_2024::network::NodeId;

pub fn parse_config(file: &str) -> anyhow::Result<Config> {
    let file_str = fs::read_to_string(file)?;
    let conf = toml::from_str(&file_str)?;
    debug!("Loaded config: {:?}", conf);
    Ok(conf)
}

#[allow(clippy::type_complexity)]
pub fn spawn_network(
    config: Config,
) -> anyhow::Result<(
    HashMap<NodeId, Sender<DroneCommand>>,
    HashMap<NodeId, Sender<NodeCommand>>,
    Receiver<DroneEvent>,
    Vec<thread::JoinHandle<()>>,
    Vec<thread::JoinHandle<()>>,
)> {
    let mut controller_drones = HashMap::new();
    let (node_event_send, node_event_recv) = unbounded();

    let mut controller_server = HashMap::new();

    let mut packet_channels = HashMap::new();
    for drone in config.drone.iter() {
        packet_channels.insert(drone.id, unbounded());
    }
    for client in config.client.iter() {
        packet_channels.insert(client.id, unbounded());
    }
    for server in config.server.iter() {
        packet_channels.insert(server.id, unbounded());
    }

    let mut d_handles = Vec::new();
    for drone in config.drone.into_iter() {
        // controller
        let (controller_drone_send, controller_drone_recv) = unbounded();
        controller_drones.insert(drone.id, controller_drone_send);
        let node_event_send = node_event_send.clone();
        // packet
        let packet_recv = packet_channels[&drone.id].1.clone();
        let packet_send = drone
            .connected_node_ids
            .into_iter()
            .map(|id| (id, packet_channels[&id].0.clone()))
            .collect();

        d_handles.push(
            thread::Builder::new()
                .name(format!("drone{}", drone.id))
                .spawn(move || {
                    let mut drone = RustDrone::new(
                        drone.id,
                        node_event_send,
                        controller_drone_recv,
                        packet_recv,
                        packet_send,
                        drone.pdr,
                    );

                    drone.run();
                })?,
        );
    }

    let mut s_handles = Vec::new();
    for server in config.server.into_iter() {
        // controller
        let (controller_server_send, controller_server_recv) = unbounded();
        // packet
        let packet_recv = packet_channels[&server.id].1.clone();

        s_handles.push(
            thread::Builder::new()
                .name(format!("server{}", server.id))
                .spawn(move || {
                    let mut server = if server.server_type == "Content" {
                        Node::new_content_server(
                            server.id,
                            controller_server_recv,
                            packet_recv,
                            server.base_path,
                        )
                    } else if server.server_type == "Communication" {
                        Node::new_communication_server(
                            server.id,
                            controller_server_recv,
                            packet_recv,
                            server.base_path,
                        )
                    } else {
                        panic!("Unknown server type: {}", server.server_type);
                    };

                    server.run();
                })?,
        );

        for connected_id in server.connected_drone_ids.iter() {
            controller_server_send
                .send(NodeCommand::AddNeighbour((
                    *connected_id,
                    packet_channels[connected_id].0.clone(),
                )))
                .unwrap();
        }

        controller_server.insert(server.id, controller_server_send);
    }

    Ok((
        controller_drones,
        controller_server,
        node_event_recv,
        d_handles,
        s_handles,
    ))
}
